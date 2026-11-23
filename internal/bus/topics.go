// Package bus holds the names and settings that every service must agree on
// to use Kafka as the event log (decisions 0001, 0003, 0006 and 0007):
// topic names, partition counts, topic configs, record headers and the
// cursor record.
//
// It also has the Kafka client code built on these names: topic setup
// (EnsureTopics), the raw writer (RawWriter) and the cursor reader
// (ReadCursor).
package bus

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

// Topic names.
const (
	// TopicRaw holds Bluesky source events as JSON, keyed by author DID.
	TopicRaw = "raw.bluesky"
	// TopicEvents holds channel-agnostic message events from normalize,
	// keyed by conversation key (decision 0006).
	TopicEvents = "events"
	// TopicControl holds deletions and author removals from normalize,
	// keyed by author ID (decision 0006).
	TopicControl = "events.control"
	// TopicConversations holds conversation events from conv, keyed by
	// conversation key (decision 0007).
	TopicConversations = "conversations"
	// TopicCursor holds the resume position of the source reader, keyed by
	// source name. It is compacted.
	TopicCursor = "ingest.cursor"
)

// Partition counts. A count can be raised later but not lowered, and raising
// it changes which partition a key maps to.
const (
	PartitionsRaw           int32 = 6
	PartitionsEvents        int32 = 16
	PartitionsControl       int32 = 6
	PartitionsConversations int32 = 16
	PartitionsCursor        int32 = 1
)

// ReplicationFactor is used for every topic while Kafka runs as one node.
const ReplicationFactor int16 = 1

// Defaults and fixed values for TopicRaw.
const (
	// DefaultRawRetention is how long TopicRaw keeps records.
	DefaultRawRetention = 24 * time.Hour
	// DefaultRawMaxBytes is the size limit of the whole TopicRaw topic:
	// 8 GB (decimal). It is split evenly between the partitions.
	DefaultRawMaxBytes int64 = 8_000_000_000
	// RawSegmentInterval is segment.ms for TopicRaw. Kafka deletes whole
	// segments, so a record can live up to retention plus this interval.
	RawSegmentInterval = time.Hour
	// RawSegmentBytesMax is the largest segment.bytes for TopicRaw. With
	// the default limits each partition gets segments of this size.
	RawSegmentBytesMax int64 = 64 << 20 // 64 MiB
	// RawSegmentBytesMin is the smallest segment.bytes for TopicRaw. It is
	// believed to be the lowest value Kafka 4.x accepts (KIP-1030). This is
	// not checked by the tests, because the fake cluster accepts any value.
	RawSegmentBytesMin int64 = 1 << 20 // 1 MiB
	// RawMinMaxBytes is the smallest valid RawLimits.MaxBytes: every
	// partition needs room for one open segment and at least one closed
	// segment of the minimum size.
	RawMinMaxBytes = 2 * RawSegmentBytesMin * int64(PartitionsRaw)
)

// Kafka topic config keys used here.
const (
	ConfigCleanupPolicy  = "cleanup.policy"
	ConfigRetentionMs    = "retention.ms"
	ConfigRetentionBytes = "retention.bytes"
	ConfigSegmentMs      = "segment.ms"
	ConfigSegmentBytes   = "segment.bytes"
	// ConfigMessageTimestampType set to TimestampLogAppendTime makes the
	// broker stamp each record with the time it was appended, so time
	// retention counts from the write to the topic, not from the event time.
	ConfigMessageTimestampType = "message.timestamp.type"
	// Compaction settings, used by TopicCursor.
	ConfigMinCleanableDirtyRatio = "min.cleanable.dirty.ratio"
	ConfigMinCompactionLagMs     = "min.compaction.lag.ms"
	ConfigMaxCompactionLagMs     = "max.compaction.lag.ms"
	ConfigDeleteRetentionMs      = "delete.retention.ms"

	CleanupDelete  = "delete"
	CleanupCompact = "compact"

	TimestampLogAppendTime = "LogAppendTime"
)

// TopicSpec is everything needed to create a topic. The field types match
// the arguments of kadm.Client.CreateTopic, so the values can be passed
// directly.
type TopicSpec struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]*string
}

// RawLimits are the size and time limits of TopicRaw.
type RawLimits struct {
	// Retention is retention.ms. It must be at least RawSegmentInterval.
	Retention time.Duration
	// MaxBytes is the size limit of the whole topic, over all partitions.
	MaxBytes int64
}

// DefaultRawLimits returns the limits from decision 0001.
func DefaultRawLimits() RawLimits {
	return RawLimits{Retention: DefaultRawRetention, MaxBytes: DefaultRawMaxBytes}
}

// Validate reports every invalid field.
func (l RawLimits) Validate() error {
	var errs []error
	if l.Retention < RawSegmentInterval {
		errs = append(errs, fmt.Errorf("retention %s is shorter than the segment interval %s", l.Retention, RawSegmentInterval))
	}
	if l.Retention%time.Millisecond != 0 {
		errs = append(errs, fmt.Errorf("retention %s is not a whole number of milliseconds", l.Retention))
	}
	if l.MaxBytes < RawMinMaxBytes {
		errs = append(errs, fmt.Errorf("max bytes %d is less than %d: each of the %d partitions needs room for two segments of at least %d bytes", l.MaxBytes, RawMinMaxBytes, PartitionsRaw, RawSegmentBytesMin))
	}
	return errors.Join(errs...)
}

// partitionShare is the total limit divided by the partition count, rounded
// down.
func (l RawLimits) partitionShare() int64 {
	return l.MaxBytes / int64(PartitionsRaw)
}

// SegmentBytes returns segment.bytes for TopicRaw: a quarter of the
// per-partition share, kept between RawSegmentBytesMin and
// RawSegmentBytesMax. The result is only meaningful for valid limits.
//
// A quarter keeps at least three closed segments under retention.bytes, so
// size-based deletion removes a small part of the partition at a time.
func (l RawLimits) SegmentBytes() int64 {
	return min(max(l.partitionShare()/4, RawSegmentBytesMin), RawSegmentBytesMax)
}

// PerPartitionBytes returns retention.bytes for TopicRaw: the per-partition
// share minus one segment.
//
// Kafka only deletes closed segments, so a partition can hold up to
// retention.bytes plus one open segment. Leaving room for that segment keeps
// partitions x (retention.bytes + segment.bytes) within the total limit.
// The result is only meaningful for valid limits.
func (l RawLimits) PerPartitionBytes() int64 {
	return l.partitionShare() - l.SegmentBytes()
}

// RawTopicSpec returns the spec of TopicRaw for the given limits.
func RawTopicSpec(l RawLimits) (TopicSpec, error) {
	if err := l.Validate(); err != nil {
		return TopicSpec{}, fmt.Errorf("topic %s: %w", TopicRaw, err)
	}
	return TopicSpec{
		Name:              TopicRaw,
		Partitions:        PartitionsRaw,
		ReplicationFactor: ReplicationFactor,
		Configs: map[string]*string{
			ConfigCleanupPolicy:        ptr(CleanupDelete),
			ConfigRetentionMs:          ptr(strconv.FormatInt(l.Retention.Milliseconds(), 10)),
			ConfigRetentionBytes:       ptr(strconv.FormatInt(l.PerPartitionBytes(), 10)),
			ConfigSegmentMs:            ptr(strconv.FormatInt(RawSegmentInterval.Milliseconds(), 10)),
			ConfigSegmentBytes:         ptr(strconv.FormatInt(l.SegmentBytes(), 10)),
			ConfigMessageTimestampType: ptr(TimestampLogAppendTime),
		},
	}, nil
}

// Retention of the stage 2 topics (decisions 0004 and 0006). Size-based
// retention is not decided for them, so retention.bytes is not set and the
// broker default applies.
const (
	// EventsRetention is retention.ms of TopicEvents (decision 0006).
	EventsRetention = 6 * time.Hour
	// ControlRetention is retention.ms of TopicControl (decision 0004).
	ControlRetention = 6 * time.Hour
	// ConversationsRetention is retention.ms of TopicConversations
	// (decision 0004).
	ConversationsRetention = 24 * time.Hour
	// Stage2SegmentInterval is segment.ms of the three topics above.
	// Retention only deletes closed segments, and with the broker default
	// a segment closes after 7 days or 1 GiB, so without this setting a
	// small topic would keep records far longer than its retention. With 1
	// hour a record lives up to retention plus 1 hour, which matches
	// decision 0004: a deleted post leaves TopicEvents "within about 7
	// hours".
	Stage2SegmentInterval = time.Hour
)

// EventsTopicSpec returns the spec of TopicEvents: delete cleanup,
// EventsRetention, Stage2SegmentInterval, and LogAppendTime timestamps.
func EventsTopicSpec() TopicSpec {
	return timeRetainedSpec(TopicEvents, PartitionsEvents, EventsRetention)
}

// ControlTopicSpec returns the spec of TopicControl: delete cleanup,
// ControlRetention, Stage2SegmentInterval, and LogAppendTime timestamps.
func ControlTopicSpec() TopicSpec {
	return timeRetainedSpec(TopicControl, PartitionsControl, ControlRetention)
}

// ConversationsTopicSpec returns the spec of TopicConversations: delete
// cleanup, ConversationsRetention, Stage2SegmentInterval, and LogAppendTime
// timestamps.
func ConversationsTopicSpec() TopicSpec {
	return timeRetainedSpec(TopicConversations, PartitionsConversations, ConversationsRetention)
}

// timeRetainedSpec returns a delete-cleanup spec with time retention only.
//
// message.timestamp.type is LogAppendTime for the same reason as on
// TopicRaw: retention counts from the write to the topic, not from the
// source time. Otherwise a catch-up or a replay of old events would be
// deleted at once.
func timeRetainedSpec(name string, partitions int32, retention time.Duration) TopicSpec {
	return TopicSpec{
		Name:              name,
		Partitions:        partitions,
		ReplicationFactor: ReplicationFactor,
		Configs: map[string]*string{
			ConfigCleanupPolicy:        ptr(CleanupDelete),
			ConfigRetentionMs:          ptr(msString(retention)),
			ConfigSegmentMs:            ptr(msString(Stage2SegmentInterval)),
			ConfigMessageTimestampType: ptr(TimestampLogAppendTime),
		},
	}
}

// Compaction settings of TopicCursor.
//
// Ingest writes one cursor record after every delivered batch, about 20
// records per second or 1.6 million per day, and only the newest record
// per key matters. Kafka never compacts the open (active) segment, and with
// the broker defaults a segment closes only after 7 days or 1 GiB. So with
// compaction alone nothing is cleaned for days. These settings close
// segments often and let the cleaner run soon after, so the topic stays at
// a few MiB.
const (
	// cursorSegmentInterval is segment.ms: the open segment closes after 10
	// minutes, so at most about 10 minutes of records wait in it, out of
	// the cleaner's reach. Kafka accepts 1 ms or more.
	cursorSegmentInterval = 10 * time.Minute
	// cursorSegmentBytes is segment.bytes: the open segment also closes at
	// 1 MiB, the smallest value Kafka 4.x accepts. At the current write
	// rate this is reached every few minutes.
	cursorSegmentBytes int64 = 1 << 20
	// cursorMinDirtyRatio is min.cleanable.dirty.ratio: the cleaner works
	// on the topic as soon as 1% of it is not yet cleaned. Here almost the
	// whole topic is old copies of one key, so cleaning is cheap. Kafka
	// accepts 0 to 1.
	cursorMinDirtyRatio = "0.01"
	// cursorMinCompactionLag is min.compaction.lag.ms: 0, a record can be
	// removed by compaction at once. No reader needs old cursor values.
	// Kafka accepts 0 or more.
	cursorMinCompactionLag = time.Duration(0)
	// cursorMaxCompactionLag is max.compaction.lag.ms: a record must be
	// compacted within 15 minutes even if the dirty ratio is not reached.
	// Kafka accepts 1 ms or more, and it must not be lower than
	// min.compaction.lag.ms.
	cursorMaxCompactionLag = 15 * time.Minute
	// cursorDeleteRetention is delete.retention.ms: a tombstone (an
	// operator reset) is kept for 1 hour after compaction, then removed. A
	// reader that starts later finds no record for the key, which gives
	// the same result as the tombstone: start from live. Kafka accepts 0
	// or more.
	cursorDeleteRetention = time.Hour
)

// CursorTopicSpec returns the spec of TopicCursor, a compacted topic with one
// partition. See the compaction settings above for the reason of each
// config.
func CursorTopicSpec() TopicSpec {
	return TopicSpec{
		Name:              TopicCursor,
		Partitions:        PartitionsCursor,
		ReplicationFactor: ReplicationFactor,
		Configs: map[string]*string{
			ConfigCleanupPolicy:          ptr(CleanupCompact),
			ConfigSegmentMs:              ptr(msString(cursorSegmentInterval)),
			ConfigSegmentBytes:           ptr(strconv.FormatInt(cursorSegmentBytes, 10)),
			ConfigMinCleanableDirtyRatio: ptr(cursorMinDirtyRatio),
			ConfigMinCompactionLagMs:     ptr(msString(cursorMinCompactionLag)),
			ConfigMaxCompactionLagMs:     ptr(msString(cursorMaxCompactionLag)),
			ConfigDeleteRetentionMs:      ptr(msString(cursorDeleteRetention)),
		},
	}
}

func msString(d time.Duration) string { return strconv.FormatInt(d.Milliseconds(), 10) }

func ptr(s string) *string { return &s }
