package bluesky

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bluesky-social/jetstream"

	"github.com/ZAUakaAlexey/threadroute/internal/source"
)

// Reasons for skipping an event. They are the values of the "reason" label
// of skipped_total.
const (
	skipUnsubscribedKind = "unsubscribed_kind"
	skipMissingDID       = "missing_did"
	skipMissingPayload   = "missing_payload"
	skipEncodeFailed     = "encode_failed"
)

// skipReasons lists every skip reason, so the metric starts at zero for each.
var skipReasons = []string{skipUnsubscribedKind, skipMissingDID, skipMissingPayload, skipEncodeFailed}

// subscribedKinds lists the kinds the connector subscribes to (decision 0003).
var subscribedKinds = []source.Kind{source.KindCommit, source.KindAccount, source.KindSync}

// mapResult is the outcome of mapping one SDK batch.
type mapResult struct {
	// batch holds the delivered events and the batch cursor.
	batch source.Batch
	// kinds counts delivered events by kind.
	kinds map[source.Kind]int
	// skipped counts skipped events by reason.
	skipped map[string]int
	// zeroWitnessed counts delivered events with no witnessed time.
	zeroWitnessed int
	// payloadBytes is the total payload size of the delivered events.
	payloadBytes int
}

// mapBatch turns one SDK batch into a source.Batch. It is a pure function: it
// does not change the SDK events, and every byte slice and string in the
// result is owned by the result, so the sink may keep them.
//
// The batch cursor is the highest witnessed time among the delivered events
// and the seq of that event. An event without a witnessed time never lowers
// it. If no delivered event has a witnessed time, the cursor stays unset, as
// source.Cursor documents.
func mapBatch(events []jetstream.Event) mapResult {
	res := mapResult{
		kinds:   make(map[source.Kind]int, len(subscribedKinds)),
		skipped: make(map[string]int),
	}
	if len(events) > 0 {
		res.batch.Events = make([]source.RawEvent, 0, len(events))
	}
	for i := range events {
		ev := &events[i]
		raw, reason := mapEvent(ev)
		if reason != "" {
			res.skipped[reason]++
			continue
		}
		res.batch.Events = append(res.batch.Events, raw)
		res.kinds[raw.Kind]++
		res.payloadBytes += len(raw.Payload)
		if raw.Witnessed.IsZero() {
			res.zeroWitnessed++
			continue
		}
		if raw.Witnessed.After(res.batch.Cursor.Witnessed) {
			res.batch.Cursor = source.Cursor{Witnessed: raw.Witnessed, Seq: ev.Seq}
		}
	}
	return res
}

// mapEvent maps one SDK event. It returns a skip reason when the event must
// not be delivered.
func mapEvent(ev *jetstream.Event) (source.RawEvent, string) {
	if !isSubscribed(ev.Kind) {
		return source.RawEvent{}, skipUnsubscribedKind
	}
	if ev.DID == "" {
		return source.RawEvent{}, skipMissingDID
	}
	id, ok := eventID(ev)
	if !ok {
		return source.RawEvent{}, skipMissingPayload
	}
	payload, err := encodePayload(ev)
	if err != nil {
		return source.RawEvent{}, skipEncodeFailed
	}
	return source.RawEvent{
		// Clone the DID: on some SDK paths strings point into a shared
		// buffer, and the sink keeps the key.
		Key:       strings.Clone(ev.DID),
		ID:        id,
		Kind:      source.Kind(ev.Kind),
		Witnessed: witnessedTime(ev.WitnessedAtUS),
		Payload:   payload,
	}, ""
}

func isSubscribed(k jetstream.Kind) bool {
	for _, s := range subscribedKinds {
		if string(s) == string(k) {
			return true
		}
	}
	return false
}

// eventID builds the content-based event ID. It returns false when the
// payload for the event kind is missing.
func eventID(ev *jetstream.Event) (string, bool) {
	switch ev.Kind {
	case jetstream.KindCommit:
		if ev.Commit == nil {
			return "", false
		}
		c := ev.Commit
		return source.CommitEventID(ev.DID, c.Collection, c.Rkey, c.Rev, string(c.Operation)), true
	case jetstream.KindAccount:
		if ev.Account == nil {
			return "", false
		}
		a := ev.Account
		return source.AccountEventID(ev.DID, a.Active, a.Status, a.Time), true
	case jetstream.KindSync:
		if ev.Sync == nil {
			return "", false
		}
		return source.SyncEventID(ev.DID, ev.Sync.Rev, ev.Sync.Time, witnessedTime(ev.WitnessedAtUS)), true
	default:
		return "", false
	}
}

// witnessedTime converts the SDK witnessed time. The SDK reports 0 when the
// source gave no witnessed time; that maps to the zero time.Time, not to
// time.UnixMicro(0).
func witnessedTime(us int64) time.Time {
	if us <= 0 {
		return time.Time{}
	}
	return time.UnixMicro(us).UTC()
}

// encodePayload serialises the event as JSON without the binary CBOR copy of
// the record (decision 0001). It works on shallow copies, so the SDK event
// is not changed. The output is a new buffer owned by the caller.
func encodePayload(ev *jetstream.Event) ([]byte, error) {
	out := *ev
	if ev.Commit != nil {
		commit := *ev.Commit
		commit.RecordCBOR = nil
		out.Commit = &commit
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	// Keep "<", ">" and "&" as they are, so the payload matches the source
	// text and is not larger than needed.
	enc.SetEscapeHTML(false)
	if err := enc.Encode(&out); err != nil {
		return nil, fmt.Errorf("encode event: %w", err)
	}
	// Encode adds a newline at the end.
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}
