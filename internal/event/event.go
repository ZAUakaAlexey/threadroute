// Package event holds the channel-agnostic events that flow after normalize
// (decisions 0006 and 0007), and the pure functions every service needs to
// read and write them.
//
//   - Message is a message event in the events topic. normalize writes it and
//     conv reads it.
//   - Deletion and AuthorRemoval are control events in the events.control
//     topic. normalize writes them and conv reads them.
//   - Conversation is a conversation event in the conversations topic. conv
//     writes it and the router and the gateway read it.
//   - Routing is a routing event in the routing topic. The router writes
//     it, and the simulator and the gateway read it (decisions 0009 and
//     0010).
//
// # Wire format
//
// Every event is one JSON object. Times are unix microseconds in fields that
// end in "_us", the unit the source uses, so they round-trip exactly. Readers
// ignore unknown fields, so a newer writer can add fields. Encoding and
// decoding both validate, and a validation error names every missing or
// invalid field. Validation errors never contain post text, because logs
// never contain post text (decision 0004).
//
// # Event ID
//
// The event ID of a message or control event is the content-based ID of the
// source event it was made from (decision 0003). One source event gives at
// most one normalised event, so the ID stays unique. Services after
// normalize must be idempotent on this ID, because some duplicates can pass
// the deduplication in normalize (decision 0003).
package event

import (
	"errors"
	"fmt"
	"time"
)

// Channel names the source a message came from. It is part of every event so
// a reader can tell channels apart. Other channels add their own values.
type Channel string

// ChannelBluesky is the channel of the Bluesky Jetstream source.
const ChannelBluesky Channel = "bluesky"

// ErrUnknownKind is returned, wrapped, when a control event has an unknown
// kind, or a conversation or routing event has an unknown type. A newer
// writer may add kinds, so a reader can check for it with errors.Is, skip
// the record and count it.
var ErrUnknownKind = errors.New("unknown event kind")

// micros returns t as unix microseconds, or 0 for the zero time.
func micros(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMicro()
}

// fromMicros is the reverse of micros. A value of 0 or less gives the zero
// time, which validation then reports as not set.
func fromMicros(us int64) time.Time {
	if us <= 0 {
		return time.Time{}
	}
	return time.UnixMicro(us).UTC()
}

// fieldErrors collects one error per invalid field. The field names are the
// JSON names, so an error points at the wire field.
type fieldErrors []error

func (e *fieldErrors) missing(field string) {
	*e = append(*e, fmt.Errorf("%s is not set", field))
}

func (e *fieldErrors) invalid(field, format string, args ...any) {
	*e = append(*e, fmt.Errorf("%s: "+format, append([]any{field}, args...)...))
}

// requireString reports field as missing when v is empty.
func (e *fieldErrors) requireString(field, v string) {
	if v == "" {
		e.missing(field)
	}
}

// requireTime reports field as missing when t is unset or not after the
// unix epoch.
func (e *fieldErrors) requireTime(field string, t time.Time) {
	if t.IsZero() || t.UnixMicro() <= 0 {
		e.missing(field)
	}
}

func (e fieldErrors) join() error {
	return errors.Join(e...)
}
