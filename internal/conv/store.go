package conv

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

// DefaultKeyPrefix starts every Redis key of conv.
const DefaultKeyPrefix = "conv:"

var (
	//go:embed lua/prelude.lua
	luaPrelude string
	//go:embed lua/message.lua
	luaMessage string
	//go:embed lua/deletion.lua
	luaDeletion string
	//go:embed lua/removal.lua
	luaRemoval string
	//go:embed lua/sweep.lua
	luaSweep string
)

// Outcomes of an input event, as Result.Outcome.
const (
	OutcomeStored             = "stored"
	OutcomeNotAdmitted        = "not_admitted"
	OutcomeNoOpenConversation = "no_open_conversation"
	OutcomeDuplicate          = "duplicate"
	// OutcomeEdited: an edit replaced the text and language tags of a
	// stored message.
	OutcomeEdited = "edited"
	// OutcomeEditDropped: an edit of a message that is not stored, or that
	// a pending deletion or removal covers.
	OutcomeEditDropped = "edit_dropped"
	// OutcomeRemoved: a control event removed stored messages.
	OutcomeRemoved = "removed"
	// OutcomePending: a deletion of a message that is not stored was
	// remembered.
	OutcomePending = "pending"
)

// PartitionedMessage is a message event and the partition of the events
// topic it came from.
type PartitionedMessage struct {
	Partition int32
	Message   event.Message
}

// Result is what one script call did.
type Result struct {
	// Outcome is one of the Outcome constants, or empty for a sweep.
	Outcome string
	// Admitted, Refused and Ended count admission changes.
	Admitted, Refused, Ended int
	// Opened and Reopened count new sessions. Reopened is part of Opened.
	Opened, Reopened int
	// Closed counts closed sessions by reason.
	Closed map[event.CloseReason]int
	// Answered is true when a company message answered the customer, with
	// ResponseTime the real response time.
	Answered     bool
	ResponseTime time.Duration
	// Events counts the conversation events appended to outboxes.
	Events int
	// Outboxes are the partitions whose outbox got new entries.
	Outboxes []int32
	// TextRemoved is true when a message was counted but its text was not
	// stored because of a deletion or removal that came first.
	TextRemoved bool
	// Due is the number of deadlines a sweep found. A sweep that found its
	// limit should run again.
	Due, PurgeDue int
	// Purged counts messages whose text a sweep removed after the close.
	Purged int
}

// Store applies events to the conversation state in Redis. It is safe for
// concurrent use.
type Store struct {
	rdb   redis.UniversalClient
	rules Rules
	base  []any

	message, deletion, removal, sweep *redis.Script
	prefix                            string
}

// NewStore validates the rules and prepares the scripts. prefix starts every
// key, DefaultKeyPrefix when empty.
func NewStore(rdb redis.UniversalClient, rules Rules, prefix string) (*Store, error) {
	if rdb == nil {
		return nil, errors.New("new store: redis client is nil")
	}
	if err := rules.Validate(); err != nil {
		return nil, fmt.Errorf("new store: %w", err)
	}
	if prefix == "" {
		prefix = DefaultKeyPrefix
	}
	return &Store{
		rdb:      rdb,
		rules:    rules,
		base:     rules.args(prefix),
		message:  redis.NewScript(luaPrelude + luaMessage),
		deletion: redis.NewScript(luaPrelude + luaDeletion),
		removal:  redis.NewScript(luaPrelude + luaRemoval),
		sweep:    redis.NewScript(luaPrelude + luaSweep),
		prefix:   prefix,
	}, nil
}

// Rules returns the rules of the store.
func (s *Store) Rules() Rules { return s.rules }

func (s *Store) key(parts ...string) string {
	return s.prefix + strings.Join(parts, "")
}

type scriptCall struct {
	script *redis.Script
	args   []any
}

func (s *Store) call(script *redis.Script, args ...any) scriptCall {
	all := make([]any, 0, len(s.base)+len(args))
	all = append(all, s.base...)
	all = append(all, args...)
	return scriptCall{script: script, args: all}
}

// run sends the calls in one pipeline, in order. Each call is atomic on its
// own. When Redis does not know a script (after a restart of Redis), the
// scripts are loaded and the whole pipeline runs again. That is safe because
// every call that already ran finds its event ID marker.
func (s *Store) run(ctx context.Context, calls []scriptCall) ([]Result, error) {
	if len(calls) == 0 {
		return nil, nil
	}
	for attempt := 0; ; attempt++ {
		pipe := s.rdb.Pipeline()
		cmds := make([]*redis.Cmd, len(calls))
		for i, c := range calls {
			cmds[i] = c.script.EvalSha(ctx, pipe, nil, c.args...)
		}
		_, err := pipe.Exec(ctx)
		if err != nil && attempt == 0 && redis.HasErrorPrefix(err, "NOSCRIPT") {
			if lerr := s.load(ctx); lerr != nil {
				return nil, lerr
			}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("redis scripts: %w", err)
		}
		out := make([]Result, len(cmds))
		for i, cmd := range cmds {
			r, err := parseResult(cmd)
			if err != nil {
				return nil, err
			}
			out[i] = r
		}
		return out, nil
	}
}

// load puts every script into the Redis script cache.
func (s *Store) load(ctx context.Context) error {
	for _, sc := range []*redis.Script{s.message, s.deletion, s.removal, s.sweep} {
		if err := sc.Load(ctx, s.rdb).Err(); err != nil {
			return fmt.Errorf("load redis script: %w", err)
		}
	}
	return nil
}

// ApplyMessages applies message events in the given order.
func (s *Store) ApplyMessages(ctx context.Context, msgs []PartitionedMessage) ([]Result, error) {
	calls := make([]scriptCall, len(msgs))
	for i, pm := range msgs {
		m := pm.Message
		calls[i] = s.call(s.message,
			m.EventID,
			strconv.FormatInt(int64(pm.Partition), 10),
			strconv.FormatInt(m.SourceTime.UnixMicro(), 10),
			m.MessageID,
			m.CompanyID,
			m.CustomerID,
			string(m.Direction),
			m.Text,
			m.Lang,
			string(m.Channel),
			string(event.ConversationKey(m.CompanyID, m.CustomerID)),
			strings.Join(m.Langs, ","),
			edited(m.Edited),
		)
	}
	return s.run(ctx, calls)
}

// ApplyControls applies deletions and author removals in the given order.
func (s *Store) ApplyControls(ctx context.Context, controls []event.Control) ([]Result, error) {
	calls := make([]scriptCall, 0, len(controls))
	for _, c := range controls {
		switch v := c.(type) {
		case event.Deletion:
			calls = append(calls, s.call(s.deletion,
				v.EventID, v.MessageID, strconv.FormatInt(v.SourceTime.UnixMicro(), 10)))
		case event.AuthorRemoval:
			calls = append(calls, s.call(s.removal,
				v.EventID, v.AuthorID, strconv.FormatInt(v.SourceTime.UnixMicro(), 10)))
		default:
			return nil, fmt.Errorf("apply controls: unsupported type %T", c)
		}
	}
	return s.run(ctx, calls)
}

// Sweep closes up to limit conversations of a partition whose deadline is at
// or before the watermark of the partition, and removes the text of up to
// limit conversations that closed TextKeptAfterClose before it.
func (s *Store) Sweep(ctx context.Context, partition int32, limit int) (Result, error) {
	res, err := s.run(ctx, []scriptCall{s.call(s.sweep, strconv.FormatInt(int64(partition), 10), limit)})
	if err != nil {
		return Result{}, err
	}
	return res[0], nil
}

// Watermarks returns the watermark of every partition that has one.
func (s *Store) Watermarks(ctx context.Context) (map[int32]time.Time, error) {
	vals, err := s.rdb.HGetAll(ctx, s.key("wm")).Result()
	if err != nil {
		return nil, fmt.Errorf("read watermarks: %w", err)
	}
	out := make(map[int32]time.Time, len(vals))
	for k, v := range vals {
		p, err := strconv.ParseInt(k, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("read watermarks: partition %q: %w", k, err)
		}
		us, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("read watermarks: partition %d: %w", p, err)
		}
		out[int32(p)] = time.UnixMicro(us).UTC()
	}
	return out, nil
}

// OutboxEntry is one entry of an outbox. Err is set when the entry cannot be
// turned into a valid conversation event.
type OutboxEntry struct {
	ID    string
	Event event.Conversation
	Err   error
}

// Outbox returns up to n entries from the head of the outbox of a partition,
// oldest first.
func (s *Store) Outbox(ctx context.Context, partition int32, n int64) ([]OutboxEntry, error) {
	msgs, err := s.rdb.XRangeN(ctx, s.outboxKey(partition), "-", "+", n).Result()
	if err != nil {
		return nil, fmt.Errorf("read outbox %d: %w", partition, err)
	}
	out := make([]OutboxEntry, len(msgs))
	for i, m := range msgs {
		ev, err := entryEvent(m.Values)
		out[i] = OutboxEntry{ID: m.ID, Event: ev, Err: err}
	}
	return out, nil
}

// RemoveFromOutbox deletes entries by ID after Kafka confirmed them.
func (s *Store) RemoveFromOutbox(ctx context.Context, partition int32, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	if err := s.rdb.XDel(ctx, s.outboxKey(partition), ids...).Err(); err != nil {
		return fmt.Errorf("remove from outbox %d: %w", partition, err)
	}
	return nil
}

func (s *Store) outboxKey(partition int32) string {
	return s.key("ob:", strconv.FormatInt(int64(partition), 10))
}

// Stats are counts of shared state, read from Redis.
type Stats struct {
	// AdmittedCompanies is the number of companies whose last customer
	// message is not more than AdmissionIdle before the newest watermark.
	AdmittedCompanies int64
	// OpenConversations is the number of open sessions: one deadline entry
	// each.
	OpenConversations int64
	// OutboxPending is the number of conversation events not yet confirmed
	// by Kafka.
	OutboxPending int64
}

// Stats reads the counts over partitions 0 to partitions-1.
func (s *Store) Stats(ctx context.Context, partitions int32) (Stats, error) {
	wms, err := s.Watermarks(ctx)
	if err != nil {
		return Stats{}, err
	}
	var newest time.Time
	for _, wm := range wms {
		if wm.After(newest) {
			newest = wm
		}
	}
	pipe := s.rdb.Pipeline()
	var admitted *redis.IntCmd
	if !newest.IsZero() {
		from := newest.Add(-s.rules.AdmissionIdle).UnixMicro()
		admitted = pipe.ZCount(ctx, s.key("adm"), strconv.FormatInt(from, 10), "+inf")
	}
	open := make([]*redis.IntCmd, partitions)
	pending := make([]*redis.IntCmd, partitions)
	for p := range partitions {
		ps := strconv.FormatInt(int64(p), 10)
		open[p] = pipe.ZCard(ctx, s.key("dl:", ps))
		pending[p] = pipe.XLen(ctx, s.key("ob:", ps))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return Stats{}, fmt.Errorf("read stats: %w", err)
	}
	var st Stats
	if admitted != nil {
		st.AdmittedCompanies = admitted.Val()
	}
	for p := range partitions {
		st.OpenConversations += open[p].Val()
		st.OutboxPending += pending[p].Val()
	}
	return st, nil
}

// parseResult reads the flat key and value list that lua/prelude.lua
// result() returns.
func parseResult(cmd *redis.Cmd) (Result, error) {
	vals, err := cmd.Slice()
	if err != nil {
		return Result{}, fmt.Errorf("redis script: %w", err)
	}
	if len(vals)%2 != 0 {
		return Result{}, fmt.Errorf("redis script: odd result length %d", len(vals))
	}
	var r Result
	for i := 0; i < len(vals); i += 2 {
		k, _ := vals[i].(string)
		v, _ := vals[i+1].(string)
		n, _ := strconv.ParseInt(v, 10, 64)
		switch {
		case k == "result":
			r.Outcome = v
		case k == "admitted":
			r.Admitted = int(n)
		case k == "admission_refused":
			r.Refused = int(n)
		case k == "admission_ended":
			r.Ended = int(n)
		case k == "opened":
			r.Opened = int(n)
		case k == "reopened":
			r.Reopened = int(n)
		case k == "response_us":
			r.Answered = true
			r.ResponseTime = time.Duration(n) * time.Microsecond
		case k == "text_removed":
			r.TextRemoved = true
		case k == "due":
			r.Due = int(n)
		case k == "purge_due":
			r.PurgeDue = int(n)
		case k == "purged":
			r.Purged = int(n)
		case k == "ob":
			for part := range strings.SplitSeq(v, ",") {
				p, err := strconv.ParseInt(part, 10, 32)
				if err != nil {
					return Result{}, fmt.Errorf("redis script: outbox partition %q: %w", part, err)
				}
				r.Outboxes = append(r.Outboxes, int32(p))
			}
		case strings.HasPrefix(k, "closed_"):
			if r.Closed == nil {
				r.Closed = map[event.CloseReason]int{}
			}
			r.Closed[event.CloseReason(strings.TrimPrefix(k, "closed_"))] += int(n)
		case strings.HasPrefix(k, "ev_"):
			r.Events += int(n)
		}
	}
	return r, nil
}

// entryEvent builds a conversation event from the flat fields that
// lua/prelude.lua emit() writes, and validates it.
func entryEvent(values map[string]any) (event.Conversation, error) {
	str := func(k string) string {
		v, _ := values[k].(string)
		return v
	}
	var errs []error
	num := func(k string) int64 {
		v := str(k)
		if v == "" {
			return 0
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			errs = append(errs, fmt.Errorf("field %s: %w", k, err))
		}
		return n
	}
	micros := func(k string) time.Time {
		n := num(k)
		if n <= 0 {
			return time.Time{}
		}
		return time.UnixMicro(n).UTC()
	}
	c := event.Conversation{
		Type:             event.ConversationType(str("ty")),
		Channel:          event.Channel(str("ch")),
		CompanyID:        str("co"),
		CustomerID:       str("cu"),
		Session:          uint64(num("s")),
		Seq:              uint64(num("q")),
		SourceTime:       micros("t"),
		OpenedAt:         micros("o"),
		Lang:             str("l"),
		CustomerMessages: uint64(num("nc")),
		CompanyMessages:  uint64(num("nk")),
		MessageID:        str("m"),
		Direction:        event.Direction(str("d")),
		ResponseTime:     time.Duration(num("r")) * time.Microsecond,
		CloseReason:      event.CloseReason(str("cr")),
	}
	if err := errors.Join(errs...); err != nil {
		return c, fmt.Errorf("outbox entry: %w", err)
	}
	if err := c.Validate(); err != nil {
		return c, fmt.Errorf("outbox entry: %w", err)
	}
	return c, nil
}

// edited is the script argument for Message.Edited.
func edited(b bool) string {
	if b {
		return "1"
	}
	return "0"
}
