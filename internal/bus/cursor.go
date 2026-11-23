package bus

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/source"
)

// cursorPartition is the only partition of TopicCursor.
const cursorPartition int32 = 0

// ReadCursor reads TopicCursor from its start to its end at the time of the
// call and returns the cursor of the newest valid record for sourceName.
//
// It returns false when the topic is empty or has no valid record for the
// source. A tombstone (a record with a null value) means an operator reset
// the position on purpose: if it is newer than every valid record for the
// source, ReadCursor returns false (start from live) and logs that at info
// level. A non-empty value that cannot be decoded is logged at warn level
// and skipped. A missing topic is an error, because EnsureTopics must run
// first and starting from live by mistake would skip events.
//
// It builds its own short-lived client. opts are applied after the built-in
// options.
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
	all := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			TopicCursor: {cursorPartition: kgo.NewOffset().AtStart()},
		}),
	}
	all = append(all, opts...)
	client, err := kgo.NewClient(all...)
	if err != nil {
		return source.Cursor{}, false, fmt.Errorf("read cursor: %w", err)
	}
	defer client.Close()

	start, end, err := cursorOffsets(ctx, kadm.NewClient(client))
	if err != nil {
		return source.Cursor{}, false, fmt.Errorf("read cursor: %w", err)
	}
	if end <= start {
		return source.Cursor{}, false, nil
	}

	key := CursorKey(sourceName)
	var (
		last          CursorRecord
		found         bool
		resetAtOffset int64 = -1 // offset of a tombstone newer than last
	)
	// The last offset below end always exists: compaction never removes
	// the newest record, and this topic has no transaction markers.
	for {
		fetches := client.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return source.Cursor{}, false, fmt.Errorf("read cursor: %w", err)
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			var joined []error
			for _, fe := range errs {
				joined = append(joined, fmt.Errorf("fetch %s/%d: %w", fe.Topic, fe.Partition, fe.Err))
			}
			return source.Cursor{}, false, fmt.Errorf("read cursor: %w", errors.Join(joined...))
		}

		reachedEnd := false
		fetches.EachRecord(func(r *kgo.Record) {
			if reachedEnd || r.Offset >= end {
				reachedEnd = true
				return
			}
			if r.Offset == end-1 {
				reachedEnd = true
			}
			if !bytes.Equal(r.Key, key) {
				return
			}
			if r.Value == nil {
				last, found, resetAtOffset = CursorRecord{}, false, r.Offset
				return
			}
			rec, err := UnmarshalCursor(r.Value)
			if err != nil {
				logger.WarnContext(ctx, "skipping malformed cursor record",
					"topic", TopicCursor, "offset", r.Offset, "source", sourceName, "error", err)
				return
			}
			last, found, resetAtOffset = rec, true, -1
		})
		if reachedEnd {
			break
		}
	}

	if resetAtOffset >= 0 {
		logger.InfoContext(ctx, "cursor was reset by a tombstone, starting from live",
			"topic", TopicCursor, "offset", resetAtOffset, "source", sourceName)
	}
	if !found {
		return source.Cursor{}, false, nil
	}
	return source.Cursor{Host: last.Host, Witnessed: last.Witnessed, Seq: last.Seq}, true, nil
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
