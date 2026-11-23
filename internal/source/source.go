// Package source defines the contract between a channel connector and the
// ingest service (decisions 0001, 0002 and 0003).
//
// A connector reads one source, for example the Bluesky Jetstream, and hands
// ready-to-produce events to ingest in batches. Ingest does not understand the
// events. It only needs the fields in RawEvent to build one Kafka record.
// Nothing outside the connector imports the source SDK.
package source

import (
	"context"
	"time"
)

// Kind is the kind of a source event. The values match the Jetstream wire
// format.
type Kind string

// Kinds the connector subscribes to (decision 0003).
const (
	KindCommit  Kind = "commit"
	KindAccount Kind = "account"
	KindSync    Kind = "sync"
)

// RawEvent is one source event, ready to be written as one Kafka record.
type RawEvent struct {
	// Key is the record key: the author ID (a DID for Bluesky).
	Key string
	// ID is the content-based event ID (see CommitEventID, AccountEventID
	// and SyncEventID). It is the same on every source host, except for
	// the rare sync event without rev and time (see SyncEventID).
	ID string
	// Kind is the event kind.
	Kind Kind
	// Witnessed is when the serving source host first saw the event, with
	// microsecond precision. It is approximate between hosts.
	Witnessed time.Time
	// Payload is the event serialised as JSON, exactly as the SDK decoded it.
	Payload []byte
}

// Batch is a group of events delivered together, plus the position to resume
// from once every event in the batch is stored.
type Batch struct {
	Events []RawEvent
	Cursor Cursor
}

// Cursor is a resume position in the source.
//
// Resume works by Witnessed, which is valid on every host. Seq is only valid
// on Host, the host that issued it, and is kept for diagnostics.
type Cursor struct {
	// Host is the source host that delivered the batch. It is informational
	// and may be empty: the SDK does not report which host served a batch
	// after a failover.
	Host string
	// Witnessed is the highest witnessed time in the batch, with
	// microsecond precision. When the source gives no witnessed time (the
	// SDK reports 0), the connector leaves the zero time.Time here, not
	// time.UnixMicro(0).
	Witnessed time.Time
	// Seq is the sequence number of the event with the highest witnessed
	// time in the batch, valid on Host only. It is 0 when Witnessed is
	// unset.
	Seq uint64
}

// IsZero reports whether the cursor is unset. A cursor is unset when it has
// no witnessed time, because the witnessed time is the only field a resume
// uses. An unset cursor means "start from live".
func (c Cursor) IsZero() bool {
	return c.Witnessed.IsZero()
}

// Sink receives batches from a connector, in source order.
//
// Rules for a Sink:
//   - It may block, for example while the Kafka producer buffer is full. The
//     connector must then stop reading from the source, so the pressure
//     reaches the source instead of a queue that grows in memory.
//   - After the call returns, the sink owns the batch, its events and their
//     payload bytes. The connector must not reuse or change them, because a
//     producer can keep the bytes until delivery.
//   - A returned error is fatal: the connector stops and Run returns that
//     error, wrapped.
type Sink func(ctx context.Context, b Batch) error

// Connector streams events from one source.
type Connector interface {
	// Run delivers batches to sink until ctx is done or a fatal error
	// happens.
	//
	// from is the position to start at. An unset cursor (see Cursor.IsZero)
	// means "start from live".
	//
	// Delivery is at-least-once. A resume is inclusive, and a host change
	// rewinds by a few seconds, so an event can be delivered again. Consumers
	// drop repeats by RawEvent.ID.
	//
	// Run reconnects on its own after a network or host failure. It does not
	// return for those.
	//
	// Run returns nil when ctx is cancelled. It returns an error only for a
	// fatal failure, including any error from sink.
	Run(ctx context.Context, from Cursor, sink Sink) error
}
