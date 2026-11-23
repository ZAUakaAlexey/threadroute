// Package bus holds the names and settings that every service must agree on
// to use Kafka as the event log (decisions 0001 and 0003): topic names,
// partition counts, topic configs, record headers and the cursor record.
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
	// TopicEvents holds channel-agnostic events from normalize, keyed by
	// conversation key.
	TopicEvents = "events"
	// TopicCursor holds the resume position of the source reader, keyed by
	// source name. It is compacted.
	TopicCursor = "ingest.cursor"
)

// Partition counts. A count can be raised later but not lowered, and raising
// it changes which partition a key maps to.
const (
	PartitionsRaw    int32 = 6
	PartitionsEvents int32 = 16
	PartitionsCursor int32 = 1
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

// EventsTopicSpec returns the spec of TopicEvents.
//
// Decision 0001 leaves its limits to a later decision, so only the cleanup
// policy is set and retention comes from the broker defaults.
func EventsTopicSpec() TopicSpec {
	return TopicSpec{
		Name:              TopicEvents,
		Partitions:        PartitionsEvents,
		ReplicationFactor: ReplicationFactor,
		Configs: map[string]*string{
			ConfigCleanupPolicy: ptr(CleanupDelete),
		},
	}
}

// CursorTopicSpec returns the spec of TopicCursor, a compacted topic with one
// partition.
func CursorTopicSpec() TopicSpec {
	return TopicSpec{
		Name:              TopicCursor,
		Partitions:        PartitionsCursor,
		ReplicationFactor: ReplicationFactor,
		Configs: map[string]*string{
			ConfigCleanupPolicy: ptr(CleanupCompact),
		},
	}
}

func ptr(s string) *string { return &s }
