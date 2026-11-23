package bus

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Record header names on TopicRaw.
const (
	// HeaderEventID carries the content-based event ID (decision 0003).
	HeaderEventID = "event-id"
	// HeaderEventKind carries the source event kind, for example "commit".
	HeaderEventKind = "event-kind"
)

// SourceBluesky is the source name of the Bluesky reader. It is the record
// key of its cursor in TopicCursor.
const SourceBluesky = "bluesky"

// CursorKey returns the TopicCursor record key for a source name.
func CursorKey(source string) []byte {
	return []byte(source)
}

// CursorRecord is the value of a TopicCursor record: the resume position of
// a source reader.
type CursorRecord struct {
	// Host is the source host that delivered the last stored batch. It is
	// informational and may be empty, because the SDK does not report which
	// host served a batch after a failover.
	Host string
	// Witnessed is the witnessed time of the last stored batch, with
	// microsecond precision. A resume uses this field.
	Witnessed time.Time
	// Seq is the sequence number of the last stored batch, valid on Host
	// only.
	Seq uint64
	// WrittenAt is when the record was written.
	WrittenAt time.Time
}

// cursorJSON is the wire form of CursorRecord. The witnessed time is stored
// as unix microseconds, the unit the source uses, so it round-trips exactly.
type cursorJSON struct {
	Host        string    `json:"host"`
	WitnessedUS int64     `json:"witnessed_us"`
	Seq         uint64    `json:"seq"`
	WrittenAt   time.Time `json:"written_at"`
}

// MarshalCursor encodes a cursor record as JSON.
func MarshalCursor(r CursorRecord) ([]byte, error) {
	if err := r.validate(); err != nil {
		return nil, fmt.Errorf("marshal cursor: %w", err)
	}
	b, err := json.Marshal(cursorJSON{
		Host:        r.Host,
		WitnessedUS: r.Witnessed.UnixMicro(),
		Seq:         r.Seq,
		WrittenAt:   r.WrittenAt.UTC(),
	})
	if err != nil {
		return nil, fmt.Errorf("marshal cursor: %w", err)
	}
	return b, nil
}

// UnmarshalCursor decodes a cursor record. Unknown fields are ignored, so a
// newer writer can add fields. An empty value (a compaction tombstone) is an
// error.
func UnmarshalCursor(b []byte) (CursorRecord, error) {
	var j cursorJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return CursorRecord{}, fmt.Errorf("unmarshal cursor: %w", err)
	}
	r := CursorRecord{
		Host:      j.Host,
		Seq:       j.Seq,
		WrittenAt: j.WrittenAt,
	}
	if j.WitnessedUS > 0 {
		r.Witnessed = time.UnixMicro(j.WitnessedUS).UTC()
	}
	if err := r.validate(); err != nil {
		return CursorRecord{}, fmt.Errorf("unmarshal cursor: %w", err)
	}
	return r, nil
}

func (r CursorRecord) validate() error {
	var errs []error
	if r.Witnessed.IsZero() || r.Witnessed.UnixMicro() <= 0 {
		errs = append(errs, errors.New("witnessed time is not set"))
	}
	if r.WrittenAt.IsZero() {
		errs = append(errs, errors.New("written time is not set"))
	}
	return errors.Join(errs...)
}
