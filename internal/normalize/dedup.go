package normalize

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// DedupKeyPrefix starts the name of every deduplication key in Redis. The
// rest of the name is the event ID.
const DedupKeyPrefix = "threadroute:normalize:dedup:"

// Position is the place of a record in the raw topic.
type Position struct {
	Partition int32
	Offset    int64
}

// String returns the position as "<partition>:<offset>", the value stored in
// a deduplication key.
func (p Position) String() string {
	return strconv.FormatInt(int64(p.Partition), 10) + ":" + strconv.FormatInt(p.Offset, 10)
}

// Verdict is the answer of the deduplication check for one event.
type Verdict int

// Verdicts (decision 0003).
const (
	// VerdictFirst means the event ID was not seen yet. The key is now set
	// to this position. Process the record.
	VerdictFirst Verdict = iota + 1
	// VerdictSamePosition means the key holds this same position: the same
	// record is processed again after a failure (an aborted transaction or
	// a crash). Process the record.
	VerdictSamePosition
	// VerdictDuplicate means the key holds another position: the event was
	// already seen in another raw record. Skip the record.
	VerdictDuplicate
)

// Process reports whether a record with this verdict must be processed.
func (v Verdict) Process() bool {
	return v == VerdictFirst || v == VerdictSamePosition
}

// DedupItem is one event to check.
type DedupItem struct {
	EventID  string
	Position Position
}

// Deduper is the first deduplication layer of decision 0003: one Redis key
// per event ID, set only if it does not exist, with a limited lifetime.
//
// The value of a key is the raw position of the record that set it. Redis is
// not part of the Kafka transaction, so after an aborted transaction the
// keys of its records are already set. When the same records are read
// again, their keys hold their own positions, and the records are processed
// again instead of being dropped as duplicates of themselves. Only a key
// that holds another position marks a real duplicate.
//
// A key is never refreshed: its lifetime counts from the first time the
// event was seen.
type Deduper struct {
	client   redis.Cmdable
	lifetime time.Duration
}

// NewDeduper returns a Deduper over client. lifetime is the key lifetime,
// usually defaults.DedupKeyLifetime. It must be at least one millisecond.
func NewDeduper(client redis.Cmdable, lifetime time.Duration) (*Deduper, error) {
	if client == nil {
		return nil, errors.New("new deduper: redis client is nil")
	}
	if lifetime < time.Millisecond {
		return nil, fmt.Errorf("new deduper: key lifetime %s is less than 1ms", lifetime)
	}
	return &Deduper{client: client, lifetime: lifetime}, nil
}

// DedupKey returns the Redis key for an event ID.
func DedupKey(eventID string) string {
	return DedupKeyPrefix + eventID
}

// Check returns one verdict per item, in the same order, in one round trip
// to Redis.
//
// Every item is one "SET key position NX GET PX lifetime" command, and all
// commands of a call go in one pipeline. With NX and GET together the
// command sets the key only if it does not exist and returns the old value,
// so one command gives the whole answer. This needs Redis 7.0 or newer.
// Redis runs the commands of a pipeline in order, so when two items of one
// call have the same event ID, the second one sees the first one's position.
//
// Any Redis error fails the whole call, and no verdict is returned. The
// caller must then stop without committing: the check is never skipped
// (decision 0003). Some keys of the call may already be set then. That is
// safe, because a retry of the same records finds their own positions.
func (d *Deduper) Check(ctx context.Context, items []DedupItem) ([]Verdict, error) {
	if len(items) == 0 {
		return nil, nil
	}
	pipe := d.client.Pipeline()
	cmds := make([]*redis.StatusCmd, len(items))
	for i, it := range items {
		cmds[i] = pipe.SetArgs(ctx, DedupKey(it.EventID), it.Position.String(), redis.SetArgs{
			Mode: "NX",
			TTL:  d.lifetime,
			Get:  true,
		})
	}
	// Exec returns the first command error, and redis.Nil counts as one.
	// redis.Nil is the normal answer for a new key, so each command is
	// checked on its own below.
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("dedup check of %d events: %w", len(items), err)
	}
	verdicts := make([]Verdict, len(items))
	for i, cmd := range cmds {
		old, err := cmd.Result()
		switch {
		case errors.Is(err, redis.Nil):
			verdicts[i] = VerdictFirst
		case err != nil:
			return nil, fmt.Errorf("dedup check of %d events: %w", len(items), err)
		case old == items[i].Position.String():
			verdicts[i] = VerdictSamePosition
		default:
			verdicts[i] = VerdictDuplicate
		}
	}
	return verdicts, nil
}
