package bus

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/source"
)

// cursorPartition is the only partition of TopicCursor.
const cursorPartition int32 = 0

// Window sizes and fetch limits of ReadCursor.
const (
	// cursorWindowFirst is the number of offsets in the first window, the
	// newest part of the topic. In normal use the newest record is a valid
	// cursor for the only source, so this one window is enough.
	cursorWindowFirst int64 = 512
	// cursorWindowMax caps the window size. Each step back doubles the
	// window up to this size, so a key that was written long ago is found
	// in a few steps, and one window still fits in memory.
	cursorWindowMax int64 = 64 << 10
	// cursorFetchBytes limits one fetch response. The client fetches ahead
	// of what it has read, so this also bounds how much it reads past the
	// end of a window.
	cursorFetchBytes int32 = 256 << 10
)

// ReadCursor returns the cursor of the newest valid record for sourceName in
// TopicCursor, as the topic is at the time of the call.
//
// It returns false when the topic is empty or has no valid record for the
// source. A tombstone (a record with a null value) means an operator reset
// the position on purpose: if it is newer than every valid record for the
// source, ReadCursor returns false (start from live) and logs that at info
// level. A non-empty value that cannot be decoded is logged at warn level
// and skipped. Records for other sources are ignored. A missing topic is an
// error, because EnsureTopics must run first and starting from live by
// mistake would skip events.
//
// The topic gets one record per delivered batch, so it can hold millions of
// records before compaction removes the old ones. ReadCursor therefore
// reads it backwards, from the end, in windows of offsets:
//
//  1. The first window is the last cursorWindowFirst offsets before the end
//     offset, [end-n, end), but never below the log start offset.
//  2. The window is read from its first offset to its last, and the
//     records for the source are kept in order.
//  3. The kept records are checked from the newest to the oldest. A
//     tombstone decides "not found". A valid value decides "found". A
//     malformed value is logged and skipped.
//  4. If nothing decided, the next window is the one just before it, twice
//     as large (up to cursorWindowMax). When the log start offset is
//     reached without a decision, the result is "not found".
//
// So the cost depends on how far back the newest record for the source is,
// not on the size of the topic.
//
// It builds two short-lived clients, one to list the offsets and one to
// read. opts are applied to both, after the built-in options.
func ReadCursor(
	ctx context.Context,
	brokers []string,
	sourceName string,
	logger *slog.Logger,
	opts ...kgo.Opt,
) (source.Cursor, bool, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	start, end, err := listCursorOffsets(ctx, brokers, opts)
	if err != nil {
		return source.Cursor{}, false, fmt.Errorf("read cursor: %w", err)
	}
	if end <= start {
		return source.Cursor{}, false, nil
	}

	window := cursorWindowFirst
	hi := end
	lo := max(start, hi-window)
	all := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			TopicCursor: {cursorPartition: kgo.NewOffset().At(lo)},
		}),
		kgo.FetchMaxBytes(cursorFetchBytes),
		kgo.FetchMaxPartitionBytes(cursorFetchBytes),
	}
	all = append(all, opts...)
	client, err := kgo.NewClient(all...)
	if err != nil {
		return source.Cursor{}, false, fmt.Errorf("read cursor: %w", err)
	}
	defer client.Close()

	key := CursorKey(sourceName)
	for {
		recs, err := readCursorWindow(ctx, client, key, lo, hi)
		if err != nil {
			return source.Cursor{}, false, fmt.Errorf("read cursor: offsets %d to %d: %w", lo, hi, err)
		}
		if c, found, decided := newestCursor(ctx, logger, sourceName, recs); decided {
			return c, found, nil
		}
		if lo <= start {
			return source.Cursor{}, false, nil
		}
		hi = lo
		window = min(window*2, cursorWindowMax)
		lo = max(start, hi-window)
		// The partition is being consumed (the window before returned
		// at least one record), so SetOffsets moves it and drops any
		// data the client fetched ahead.
		client.SetOffsets(map[string]map[int32]kgo.EpochOffset{
			TopicCursor: {cursorPartition: {Epoch: -1, Offset: lo}},
		})
	}
}

// listCursorOffsets returns the log start offset and the end offset (high
// watermark) of the TopicCursor partition, using a separate client, because
// the reading client needs the first offset before it is built.
func listCursorOffsets(ctx context.Context, brokers []string, opts []kgo.Opt) (start, end int64, err error) {
	client, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)...)
	if err != nil {
		return 0, 0, err
	}
	defer client.Close()
	return cursorOffsets(ctx, kadm.NewClient(client))
}

// readCursorWindow reads the records in [lo, hi) and returns the ones with
// the given key, in offset order.
//
// The window is complete when the client returns a record at hi-1 or later.
// Such a record always exists: hi-1 is the last offset before the end on the
// first window, and on later windows the end offset minus one is above hi.
// Compaction never removes the newest record of a partition, and this topic
// has no transaction markers. Offsets inside the window can be missing after
// compaction, so the check is "at least hi-1", not "equal to hi-1".
func readCursorWindow(ctx context.Context, client *kgo.Client, key []byte, lo, hi int64) ([]*kgo.Record, error) {
	var out []*kgo.Record
	for {
		fetches := client.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			var joined []error
			for _, fe := range errs {
				joined = append(joined, fmt.Errorf("fetch %s/%d: %w", fe.Topic, fe.Partition, fe.Err))
			}
			return nil, errors.Join(joined...)
		}
		complete := false
		fetches.EachRecord(func(r *kgo.Record) {
			if r.Offset >= hi-1 {
				complete = true
			}
			if r.Offset < lo || r.Offset >= hi || !bytes.Equal(r.Key, key) {
				return
			}
			out = append(out, r)
		})
		if complete {
			return out, nil
		}
	}
}

// newestCursor checks the records of one window from the newest to the
// oldest. decided is false when the window has no tombstone and no valid
// value for the key, and an older window must be read.
func newestCursor(ctx context.Context, logger *slog.Logger, sourceName string, recs []*kgo.Record) (c source.Cursor, found, decided bool) {
	for _, r := range slices.Backward(recs) {
		if r.Value == nil {
			logger.InfoContext(ctx, "cursor was reset by a tombstone, starting from live",
				"topic", TopicCursor, "offset", r.Offset, "source", sourceName)
			return source.Cursor{}, false, true
		}
		rec, err := UnmarshalCursor(r.Value)
		if err != nil {
			logger.WarnContext(ctx, "skipping malformed cursor record",
				"topic", TopicCursor, "offset", r.Offset, "source", sourceName, "error", err)
			continue
		}
		return source.Cursor{Host: rec.Host, Witnessed: rec.Witnessed, Seq: rec.Seq}, true, true
	}
	return source.Cursor{}, false, false
}

// cursorOffsets returns the log start offset and the end offset (high
// watermark) of the TopicCursor partition.
func cursorOffsets(ctx context.Context, adm *kadm.Client) (start, end int64, err error) {
	starts, err := adm.ListStartOffsets(ctx, TopicCursor)
	if err != nil {
		return 0, 0, fmt.Errorf("list start offsets: %w", err)
	}
	ends, err := adm.ListEndOffsets(ctx, TopicCursor)
	if err != nil {
		return 0, 0, fmt.Errorf("list end offsets: %w", err)
	}
	s, err := listedOffset(starts)
	if err != nil {
		return 0, 0, fmt.Errorf("list start offsets: %w", err)
	}
	e, err := listedOffset(ends)
	if err != nil {
		return 0, 0, fmt.Errorf("list end offsets: %w", err)
	}
	return s, e, nil
}

func listedOffset(offsets kadm.ListedOffsets) (int64, error) {
	if err := offsets.Error(); err != nil {
		return 0, fmt.Errorf("topic %s: %w", TopicCursor, err)
	}
	o, ok := offsets.Lookup(TopicCursor, cursorPartition)
	if !ok {
		return 0, fmt.Errorf("topic %s partition %d not listed", TopicCursor, cursorPartition)
	}
	return o.Offset, nil
}
