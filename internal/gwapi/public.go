package gwapi

import (
	"encoding/json"
	"fmt"
)

// PublicSnapshot is the JSON answer of PublicSnapshotPath (decision 0013,
// point 1). The gateway rebuilds it every PublicSnapshotEvery, and the web
// server may cache it for that long.
//
// It holds totals only: no conversation key, no company or customer ID or
// pseudonym, no operator ID, no message ID and no text (decisions 0004 and
// 0013). The only strings are languages. A test checks the whole field
// set against an allow-list, so a field that could carry a key cannot be
// added without changing that test.
type PublicSnapshot struct {
	// Version is ProtocolVersion.
	Version int `json:"version"`
	// GeneratedAtUS is the machine time at which the gateway built it.
	GeneratedAtUS int64 `json:"generated_at_us"`
	// Stale reports that the gateway's data is stale (see Status).
	Stale bool `json:"stale"`
	// SpeedFactor is the speed factor of the router; durations are model
	// time.
	SpeedFactor float64 `json:"speed_factor"`
	// Totals are the same numbers as in the supervisor view.
	Totals Totals `json:"totals"`
	// Queues are the queues by language, sorted by language. Never null.
	Queues []PublicQueue `json:"queues"`
}

// PublicQueue is the queue of one language on the public page. It has a
// duration instead of a time, because the snapshot is cached.
type PublicQueue struct {
	Lang    string `json:"lang"`
	Waiting uint64 `json:"waiting"`
	Offered uint64 `json:"offered"`
	// OldestWaitUS is how long the conversation that has waited longest
	// has waited, in model time, when the snapshot was built. 0 when none
	// waits.
	OldestWaitUS int64 `json:"oldest_wait_us"`
	// MissedAssignment is as in QueueRow.
	MissedAssignment uint64 `json:"missed_assignment"`
}

// Validate reports every missing or invalid field.
func (p PublicSnapshot) Validate() error {
	var errs fieldErrors
	if p.Version != ProtocolVersion {
		errs.invalid("version", "is %d, want %d", p.Version, ProtocolVersion)
	}
	if p.GeneratedAtUS <= 0 {
		errs.missing("generated_at_us")
	}
	if !(p.SpeedFactor > 0) {
		errs.invalid("speed_factor", "is not positive")
	}
	errs.within("totals", p.Totals.validate)
	if p.Queues == nil {
		errs.missing("queues")
	}
	for i, q := range p.Queues {
		field := fmt.Sprintf("queues[%d]", i)
		checkLang(&errs, field+".lang", q.Lang)
		if i > 0 && q.Lang <= p.Queues[i-1].Lang {
			errs.invalid(field+".lang", "is not sorted after the one before")
		}
		if q.OldestWaitUS < 0 || q.MissedAssignment > q.Waiting+q.Offered {
			errs.invalid(field, "oldest_wait_us is negative or missed_assignment is more than waiting plus offered")
		}
	}
	return errs.join()
}

// EncodePublic validates and encodes a public snapshot.
func EncodePublic(p PublicSnapshot) ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("encode public snapshot: %w", err)
	}
	b, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("encode public snapshot: %w", err)
	}
	return b, nil
}

// DecodePublic decodes and validates a public snapshot.
func DecodePublic(b []byte) (PublicSnapshot, error) {
	var p PublicSnapshot
	if err := json.Unmarshal(b, &p); err != nil {
		return PublicSnapshot{}, fmt.Errorf("decode public snapshot: %w", err)
	}
	if err := p.Validate(); err != nil {
		return PublicSnapshot{}, fmt.Errorf("decode public snapshot: %w", err)
	}
	return p, nil
}
