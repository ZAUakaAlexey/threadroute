package conv

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

// This file is the read-only text access of decision 0011, option 2A: the
// gateway reads the text of one conversation from Redis through this
// package, so the key layout stays inside conv.

// StoredMessage is one message of a conversation as conv holds it in Redis.
// It is built from the text hash of the message (lua/message.lua: fields k,
// d, t, x and the edit marker e), so reading one message needs no other key.
type StoredMessage struct {
	// ID is the message ID, the same value as event.Message.MessageID and
	// the MessageID of the conversation events.
	ID string
	// Direction says whether the customer or the company wrote it.
	Direction event.Direction
	// SourceTime is the witnessed time of the message (source time,
	// decision 0007), with microsecond precision.
	SourceTime time.Time
	// Text is the post text as it is stored now: the text of the last
	// edit, if there was one. It can be empty, for a post without text.
	Text string
	// Edited reports that an edit replaced the text after the message was
	// stored (decision 0006). The edit path of lua/message.lua sets the
	// field e of the text hash. A message edited before conv wrote that
	// field reports false.
	Edited bool
}

// TextReader reads message text from the Redis state of conv. It never
// writes. It is safe for concurrent use.
//
// What a caller may rely on:
//
//   - Only stored messages are returned. A message whose text was removed
//     (a deletion by its author, the removal of its author, the purge after
//     the close, or the limit of the newest messages per conversation) is
//     absent. There is no placeholder for it. A caller learns about a
//     deletion or an author removal from the TypeMessageRemoved
//     conversation event, never from a missing entry.
//   - A message can disappear between two calls, and also between the
//     reading of the list and the reading of one message inside one call.
//     Such a message is left out; it is not an error.
//   - Messages are ordered by source time, oldest first, with the message
//     ID as the tie breaker. A late message can sort before messages that
//     an earlier call returned (decision 0007), so a caller replaces its
//     list with the new one and never only appends.
//   - At most the newest Rules.MessagesKept messages of the conversation
//     exist (decision 0007, 50 by default). While the text of a closed
//     session is still kept and a new session opened, the list holds
//     messages of both sessions.
//   - The text is a copy at the moment of reading. The caller must not
//     keep it after it has used it (decision 0011, point 2): no cache, no
//     log, no metric.
//   - The text of a conversation stays until the conversation closes plus
//     Rules.TextKeptAfterClose (10 minutes by default). A conversation that
//     conv does not know, or whose text is gone, gives an empty list and no
//     error.
//   - Cost: Messages makes two round trips to Redis, one ZRANGE and then
//     one pipeline with one HMGET per listed message, so at most
//     1 + Rules.MessagesKept commands. Message makes one HMGET. The reader
//     uses read commands only, so it never changes a key or its lifetime.
type TextReader struct {
	rdb    redis.UniversalClient
	prefix string
}

// NewTextReader returns a reader for the keys that start with prefix,
// DefaultKeyPrefix when empty. It must be the prefix of the conv Store that
// writes the state.
func NewTextReader(rdb redis.UniversalClient, prefix string) (*TextReader, error) {
	if rdb == nil {
		return nil, errors.New("new text reader: redis client is nil")
	}
	if prefix == "" {
		prefix = DefaultKeyPrefix
	}
	return &TextReader{rdb: rdb, prefix: prefix}, nil
}

// Messages returns the stored messages of the conversation of a company and
// a customer, oldest first. See TextReader for what the result promises.
func (r *TextReader) Messages(ctx context.Context, companyID, customerID string) ([]StoredMessage, error) {
	key := string(event.ConversationKey(companyID, customerID))
	ids, err := r.rdb.ZRange(ctx, r.prefix+"m:"+key, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("read messages: %w", err)
	}
	if len(ids) == 0 {
		return []StoredMessage{}, nil
	}
	pipe := r.rdb.Pipeline()
	cmds := make([]*redis.SliceCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HMGet(ctx, r.textKey(id), textFields...)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("read messages: %w", err)
	}
	out := make([]StoredMessage, 0, len(ids))
	for i, cmd := range cmds {
		msg, ok, err := parseStored(ids[i], key, cmd.Val())
		if err != nil {
			return nil, fmt.Errorf("read messages: %w", err)
		}
		if ok {
			out = append(out, msg)
		}
	}
	// The set is ordered by source time and then by ID already. Sorting by
	// the stored time keeps the promise also if the two ever differ.
	slices.SortStableFunc(out, func(a, b StoredMessage) int {
		if c := a.SourceTime.Compare(b.SourceTime); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})
	return out, nil
}

// Message returns one stored message of the conversation of a company and a
// customer. ok is false when the message is not stored, or when it belongs
// to another conversation: the reader checks the conversation key that conv
// stores with the text, so a caller cannot read text of a conversation it
// did not name. The gateway calls it when a "message added" event arrives
// for a conversation a browser watches.
func (r *TextReader) Message(ctx context.Context, companyID, customerID, messageID string) (msg StoredMessage, ok bool, err error) {
	key := string(event.ConversationKey(companyID, customerID))
	vals, err := r.rdb.HMGet(ctx, r.textKey(messageID), textFields...).Result()
	if err != nil {
		return StoredMessage{}, false, fmt.Errorf("read message: %w", err)
	}
	msg, ok, err = parseStored(messageID, key, vals)
	if err != nil {
		return StoredMessage{}, false, fmt.Errorf("read message: %w", err)
	}
	return msg, ok, nil
}

func (r *TextReader) textKey(messageID string) string {
	return r.prefix + "x:" + messageID
}

// textFields are the fields of a text hash that the reader needs, in the
// order parseStored expects: conversation key, direction, source time, text
// and the edit marker.
var textFields = []string{"k", "d", "t", "x", "e"}

// parseStored builds a message from the HMGET values of textFields. ok is
// false when the hash is gone or belongs to another conversation than key.
// The error names fields only, never the text.
func parseStored(id, key string, vals []any) (StoredMessage, bool, error) {
	if len(vals) != len(textFields) {
		return StoredMessage{}, false, fmt.Errorf("message %s: %d fields, want %d", id, len(vals), len(textFields))
	}
	str := func(i int) (string, bool) {
		s, ok := vals[i].(string)
		return s, ok
	}
	k, ok := str(0)
	if !ok || k != key {
		return StoredMessage{}, false, nil
	}
	dir, _ := str(1)
	ts, _ := str(2)
	us, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return StoredMessage{}, false, fmt.Errorf("message %s: field t: %w", id, err)
	}
	text, _ := str(3)
	edited, _ := str(4)
	return StoredMessage{
		ID:         id,
		Direction:  event.Direction(dir),
		SourceTime: time.UnixMicro(us).UTC(),
		Text:       text,
		Edited:     edited == "1",
	}, true, nil
}
