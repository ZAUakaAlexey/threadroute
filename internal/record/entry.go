// Package record copies the raw topic into private, compressed files for load
// tests and reads them back (decisions 0001 and 0004).
//
// A recording is a directory of segments. Each segment is a zstd-compressed
// file with one JSON line per Kafka record (NDJSON). A segment is written
// under a temporary name and renamed when it is complete, so a reader only
// sees complete segments.
//
// The files hold real post text and author IDs. They are created with owner
// only permissions, must never live inside a git working tree, and are
// deleted after the retention age (14 days by default).
package record

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"
)

// ErrInvalidEntry is returned by Writer.Append for an entry that cannot be
// stored as one JSON line. The error never contains the key or the value.
var ErrInvalidEntry = errors.New("invalid entry")

// Entry is one Kafka record of the raw topic as stored in a segment.
// A replay rebuilds the record key, the headers, the value and the relative
// timing from it.
//
// An entry has two times. Timestamp is the record timestamp, which on the
// raw topic is the broker append time: when ingest wrote the record, not
// when the event happened. Witnessed is the event time from the source.
// A replay paces records by Witnessed when it is set, and by Timestamp
// otherwise.
type Entry struct {
	// Partition and Offset are the position of the record in the raw topic.
	Partition int32
	Offset    int64
	// Timestamp is the record timestamp (broker append time). It is stored
	// with microsecond precision.
	Timestamp time.Time
	// Witnessed is the event time from the bus.HeaderEventWitnessed header,
	// with microsecond precision. The zero time means the header was absent
	// or invalid.
	Witnessed time.Time
	// Key is the record key, the author DID. It must be valid UTF-8.
	Key string
	// EventID and EventKind are the values of the bus.HeaderEventID and
	// bus.HeaderEventKind headers. An empty string means the header was not
	// set.
	EventID   string
	EventKind string
	// Value is the record value. It must be valid JSON.
	Value json.RawMessage
}

// entryJSON is the line format of an entry. The field names are part of the
// file format that the replay tool reads, so they must not change.
type entryJSON struct {
	Partition   int32           `json:"partition"`
	Offset      int64           `json:"offset"`
	TimestampUS int64           `json:"ts_us"`
	WitnessedUS int64           `json:"witnessed_us,omitempty"`
	Key         string          `json:"key,omitempty"`
	EventID     string          `json:"event_id,omitempty"`
	EventKind   string          `json:"event_kind,omitempty"`
	Value       json.RawMessage `json:"value"`
}

// validate checks that the entry can be written. Messages never include the
// key or the value, because they hold user data.
func (e Entry) validate() error {
	var errs []error
	if !json.Valid(e.Value) {
		errs = append(errs, errors.New("value is not valid JSON"))
	}
	if !e.Witnessed.IsZero() && e.Witnessed.UnixMicro() <= 0 {
		errs = append(errs, errors.New("witnessed time is not after 1970"))
	}
	if !utf8.ValidString(e.Key) {
		errs = append(errs, errors.New("key is not valid UTF-8"))
	}
	if !utf8.ValidString(e.EventID) {
		errs = append(errs, errors.New("event ID is not valid UTF-8"))
	}
	if !utf8.ValidString(e.EventKind) {
		errs = append(errs, errors.New("event kind is not valid UTF-8"))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("%w: partition %d offset %d: %w", ErrInvalidEntry, e.Partition, e.Offset, err)
	}
	return nil
}

// marshalLine encodes the entry as one JSON line ending in a newline.
// encoding/json compacts a RawMessage, so a value with newlines still takes
// one line. HTML escaping is off, so characters such as &, <, > and U+2028
// in post text are stored as they are and the value keeps its bytes apart
// from whitespace.
func (e Entry) marshalLine() ([]byte, error) {
	if err := e.validate(); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.Grow(len(e.Value) + len(e.Key) + len(e.EventID) + 128)
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	err := enc.Encode(entryJSON{
		Partition:   e.Partition,
		Offset:      e.Offset,
		TimestampUS: e.Timestamp.UnixMicro(),
		WitnessedUS: witnessedUS(e.Witnessed),
		Key:         e.Key,
		EventID:     e.EventID,
		EventKind:   e.EventKind,
		Value:       e.Value,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: partition %d offset %d: encode", ErrInvalidEntry, e.Partition, e.Offset)
	}
	// Encode ends the line with a newline.
	return buf.Bytes(), nil
}

// unmarshalLine decodes one line. The error does not include the line.
func unmarshalLine(line []byte) (Entry, error) {
	var j entryJSON
	if err := json.Unmarshal(line, &j); err != nil {
		var syn *json.SyntaxError
		if errors.As(err, &syn) {
			return Entry{}, fmt.Errorf("not valid JSON at byte %d", syn.Offset)
		}
		var typ *json.UnmarshalTypeError
		if errors.As(err, &typ) {
			return Entry{}, fmt.Errorf("field %q has the wrong type", typ.Field)
		}
		return Entry{}, errors.New("cannot decode entry")
	}
	if len(j.Value) == 0 {
		return Entry{}, errors.New("value is missing")
	}
	if j.WitnessedUS < 0 {
		return Entry{}, errors.New("witnessed_us is negative")
	}
	var witnessed time.Time
	if j.WitnessedUS > 0 {
		witnessed = time.UnixMicro(j.WitnessedUS).UTC()
	}
	return Entry{
		Partition: j.Partition,
		Offset:    j.Offset,
		Timestamp: time.UnixMicro(j.TimestampUS).UTC(),
		Witnessed: witnessed,
		Key:       j.Key,
		EventID:   j.EventID,
		EventKind: j.EventKind,
		Value:     j.Value,
	}, nil
}

// witnessedUS returns the witnessed time in unix microseconds, or 0 for the
// zero time, so the field is left out.
func witnessedUS(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMicro()
}

// parseWitnessed parses a bus.HeaderEventWitnessed value: unix microseconds
// as a positive decimal integer. It returns false for any other value.
func parseWitnessed(v []byte) (time.Time, bool) {
	us, err := strconv.ParseInt(string(v), 10, 64)
	if err != nil || us <= 0 {
		return time.Time{}, false
	}
	return time.UnixMicro(us).UTC(), true
}
