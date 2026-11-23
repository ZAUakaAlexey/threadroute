package bluesky

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream"

	"github.com/ZAUakaAlexey/threadroute/internal/source"
)

// Synthetic test data. None of it comes from the live network.
const (
	testDID        = "did:plc:testalice0000000000000000"
	testDID2       = "did:plc:testbob00000000000000000"
	testCollection = "app.bsky.feed.post"
	testRkey       = "aaa111"
	testRev        = "3testrev00001"
	testText       = "synthetic reply one"
)

// testBase is a fixed witnessed time for tests.
var testBase = time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)

func witnessedUS(offset time.Duration) int64 {
	return testBase.Add(offset).UnixMicro()
}

func commitEvent(op jetstream.Operation, seq uint64, offset time.Duration) jetstream.Event {
	c := &jetstream.Commit{
		Operation:  op,
		Collection: testCollection,
		Rkey:       testRkey,
		Rev:        testRev,
	}
	if op != jetstream.OpDelete {
		c.CID = "bafytestcid0000000000000000000000000000000000000000000"
		c.Record = map[string]any{
			"$type":     testCollection,
			"text":      testText,
			"createdAt": "2026-01-02T03:04:05.000Z",
		}
		c.RecordCBOR = []byte{0xa3, 0x01, 0x02, 0x03}
	}
	return jetstream.Event{
		DID:           testDID,
		Seq:           seq,
		TimeUS:        witnessedUS(offset),
		WitnessedAtUS: witnessedUS(offset),
		Kind:          jetstream.KindCommit,
		Commit:        c,
	}
}

func accountEvent(active bool, seq uint64, offset time.Duration) jetstream.Event {
	a := &jetstream.Account{DID: testDID2, Active: active, Seq: 777, Time: "2026-01-02T03:04:05.000Z"}
	if !active {
		a.Status = "deleted"
	}
	return jetstream.Event{
		DID:           testDID2,
		Seq:           seq,
		TimeUS:        witnessedUS(offset),
		WitnessedAtUS: witnessedUS(offset),
		Kind:          jetstream.KindAccount,
		Account:       a,
	}
}

func syncEvent(seq uint64, offset time.Duration) jetstream.Event {
	return jetstream.Event{
		DID:           testDID,
		Seq:           seq,
		TimeUS:        witnessedUS(offset),
		WitnessedAtUS: witnessedUS(offset),
		Kind:          jetstream.KindSync,
		Sync:          &jetstream.Sync{DID: testDID, Rev: testRev, Seq: 888, Time: "2026-01-02T03:04:05.000Z"},
	}
}

func TestMapBatchEvents(t *testing.T) {
	noWitness := commitEvent(jetstream.OpCreate, 9, 0)
	noWitness.WitnessedAtUS = 0

	missingDID := commitEvent(jetstream.OpCreate, 10, 0)
	missingDID.DID = ""

	identity := jetstream.Event{
		DID: testDID, Seq: 11, WitnessedAtUS: witnessedUS(0), Kind: jetstream.KindIdentity,
		Identity: &jetstream.Identity{DID: testDID, Handle: "alice.test", Seq: 1},
	}

	syncNoRevNoTime := syncEvent(13, 0)
	syncNoRevNoTime.Sync.Rev = ""
	syncNoRevNoTime.Sync.Time = ""

	nilCommit := commitEvent(jetstream.OpCreate, 12, 0)
	nilCommit.Commit = nil

	tests := []struct {
		name       string
		ev         jetstream.Event
		wantSkip   string
		wantKey    string
		wantID     string
		wantKind   source.Kind
		wantWit    time.Time
		wantRecord bool
		wantField  string // top-level payload field that must be present
	}{
		{
			name: "commit create", ev: commitEvent(jetstream.OpCreate, 1, 0),
			wantKey: testDID, wantKind: source.KindCommit, wantWit: testBase, wantRecord: true, wantField: "commit",
			wantID: "c:did:plc:testalice0000000000000000:app.bsky.feed.post:aaa111:3testrev00001:create",
		},
		{
			name: "commit update", ev: commitEvent(jetstream.OpUpdate, 2, time.Second),
			wantKey: testDID, wantKind: source.KindCommit, wantWit: testBase.Add(time.Second), wantRecord: true, wantField: "commit",
			wantID: "c:did:plc:testalice0000000000000000:app.bsky.feed.post:aaa111:3testrev00001:update",
		},
		{
			name: "commit delete", ev: commitEvent(jetstream.OpDelete, 3, 0),
			wantKey: testDID, wantKind: source.KindCommit, wantWit: testBase, wantField: "commit",
			wantID: "c:did:plc:testalice0000000000000000:app.bsky.feed.post:aaa111:3testrev00001:delete",
		},
		{
			name: "account active", ev: accountEvent(true, 4, 0),
			wantKey: testDID2, wantKind: source.KindAccount, wantWit: testBase, wantField: "account",
			wantID: "a:did:plc:testbob00000000000000000:1::2026-01-02T03%3A04%3A05.000Z",
		},
		{
			name: "account not active", ev: accountEvent(false, 5, 0),
			wantKey: testDID2, wantKind: source.KindAccount, wantWit: testBase, wantField: "account",
			wantID: "a:did:plc:testbob00000000000000000:0:deleted:2026-01-02T03%3A04%3A05.000Z",
		},
		{
			name: "sync", ev: syncEvent(6, 0),
			wantKey: testDID, wantKind: source.KindSync, wantWit: testBase, wantField: "sync",
			wantID: "s:did:plc:testalice0000000000000000:3testrev00001:2026-01-02T03%3A04%3A05.000Z",
		},
		{
			name: "sync without rev and time falls back to witnessed time", ev: syncNoRevNoTime,
			wantKey: testDID, wantKind: source.KindSync, wantWit: testBase, wantField: "sync",
			wantID: "s:did:plc:testalice0000000000000000:w1767323045123456",
		},
		{
			name: "zero witnessed time is delivered with zero time", ev: noWitness,
			wantKey: testDID, wantKind: source.KindCommit, wantWit: time.Time{}, wantRecord: true, wantField: "commit",
			wantID: "c:did:plc:testalice0000000000000000:app.bsky.feed.post:aaa111:3testrev00001:create",
		},
		{name: "missing DID", ev: missingDID, wantSkip: skipMissingDID},
		{name: "unsubscribed kind", ev: identity, wantSkip: skipUnsubscribedKind},
		{name: "missing payload", ev: nilCommit, wantSkip: skipMissingPayload},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := mapBatch([]jetstream.Event{tt.ev})
			if tt.wantSkip != "" {
				if len(res.batch.Events) != 0 {
					t.Fatalf("got %d events, want 0", len(res.batch.Events))
				}
				if res.skipped[tt.wantSkip] != 1 {
					t.Fatalf("skipped = %v, want one %q", res.skipped, tt.wantSkip)
				}
				return
			}
			if len(res.batch.Events) != 1 {
				t.Fatalf("got %d events, want 1 (skipped %v)", len(res.batch.Events), res.skipped)
			}
			got := res.batch.Events[0]
			if got.Key != tt.wantKey {
				t.Errorf("Key = %q, want %q", got.Key, tt.wantKey)
			}
			if got.ID != tt.wantID {
				t.Errorf("ID = %q, want %q", got.ID, tt.wantID)
			}
			if got.Kind != tt.wantKind {
				t.Errorf("Kind = %q, want %q", got.Kind, tt.wantKind)
			}
			if !got.Witnessed.Equal(tt.wantWit) || got.Witnessed.IsZero() != tt.wantWit.IsZero() {
				t.Errorf("Witnessed = %v, want %v", got.Witnessed, tt.wantWit)
			}
			if res.kinds[tt.wantKind] != 1 {
				t.Errorf("kinds = %v, want one %q", res.kinds, tt.wantKind)
			}
			if res.payloadBytes != len(got.Payload) {
				t.Errorf("payloadBytes = %d, want %d", res.payloadBytes, len(got.Payload))
			}
			checkPayload(t, got.Payload, tt.wantField, tt.wantRecord)
		})
	}
}

// checkPayload checks that the payload is valid JSON, has no CBOR copy of
// the record, and still has the decoded record when one is expected.
func checkPayload(t *testing.T, payload []byte, field string, wantRecord bool) {
	t.Helper()
	var top map[string]json.RawMessage
	if err := json.Unmarshal(payload, &top); err != nil {
		t.Fatalf("payload is not valid JSON: %v", err)
	}
	for _, k := range []string{"did", "kind", "cursor", "time_us", field} {
		if _, ok := top[k]; !ok {
			t.Errorf("payload has no %q field", k)
		}
	}
	if bytes.Contains(payload, []byte("record_cbor")) {
		t.Errorf("payload still has record_cbor")
	}
	if field != "commit" {
		return
	}
	var commit map[string]json.RawMessage
	if err := json.Unmarshal(top["commit"], &commit); err != nil {
		t.Fatalf("commit is not a JSON object: %v", err)
	}
	_, hasRecord := commit["record"]
	if hasRecord != wantRecord {
		t.Errorf("payload has record = %v, want %v", hasRecord, wantRecord)
	}
	if wantRecord {
		var rec map[string]any
		if err := json.Unmarshal(commit["record"], &rec); err != nil {
			t.Fatalf("record is not a JSON object: %v", err)
		}
		if rec["text"] != testText {
			t.Errorf("record text = %v, want %q", rec["text"], testText)
		}
	}
}

// TestAccountIDIgnoresSeqAndHost checks that the same account event seen on
// two hosts, with a different host seq, account seq and witnessed time, gets
// the same ID.
func TestAccountIDIgnoresSeqAndHost(t *testing.T) {
	east := accountEvent(false, 1001, 0)
	west := accountEvent(false, 77, 250*time.Millisecond)
	west.Account.Seq = east.Account.Seq - 504800

	res := mapBatch([]jetstream.Event{east, west})
	if len(res.batch.Events) != 2 {
		t.Fatalf("got %d events, want 2", len(res.batch.Events))
	}
	if a, b := res.batch.Events[0].ID, res.batch.Events[1].ID; a != b {
		t.Fatalf("account IDs differ between hosts: %q != %q", a, b)
	}
}

func TestMapBatchDoesNotChangeOrShareSDKMemory(t *testing.T) {
	events := []jetstream.Event{commitEvent(jetstream.OpCreate, 1, 0)}
	cborBefore := bytes.Clone(events[0].Commit.RecordCBOR)

	res := mapBatch(events)
	if len(res.batch.Events) != 1 {
		t.Fatalf("got %d events, want 1", len(res.batch.Events))
	}

	// The SDK event itself must not be changed by the mapping.
	if !bytes.Equal(events[0].Commit.RecordCBOR, cborBefore) {
		t.Fatalf("mapping changed the SDK RecordCBOR")
	}

	payload := res.batch.Events[0].Payload
	saved := bytes.Clone(payload)
	key := res.batch.Events[0].Key

	// Change the source event after the batch was built, as the SDK may do
	// with its buffers once the iteration moves on.
	events[0].DID = "did:plc:testchanged000000000000000"
	events[0].Commit.Record["text"] = "synthetic changed text"
	events[0].Commit.RecordCBOR[0] = 0xff
	events[0].Commit.Rkey = "zzz999"
	events[0] = jetstream.Event{}

	if !bytes.Equal(payload, saved) {
		t.Fatalf("payload changed after the source event was modified")
	}
	if key != testDID {
		t.Fatalf("key changed to %q", key)
	}
}

func TestMapBatchCursor(t *testing.T) {
	zero := func(ev jetstream.Event) jetstream.Event {
		ev.WitnessedAtUS = 0
		return ev
	}
	tests := []struct {
		name     string
		events   []jetstream.Event
		wantWit  time.Time
		wantSeq  uint64
		wantZero int
	}{
		{
			name:    "empty batch has unset cursor",
			events:  nil,
			wantWit: time.Time{},
		},
		{
			name: "highest witnessed time and its seq",
			events: []jetstream.Event{
				commitEvent(jetstream.OpCreate, 10, 1*time.Second),
				commitEvent(jetstream.OpCreate, 11, 3*time.Second),
				commitEvent(jetstream.OpCreate, 12, 2*time.Second),
			},
			wantWit: testBase.Add(3 * time.Second), wantSeq: 11,
		},
		{
			name: "zero witnessed time after the highest does not lower it",
			events: []jetstream.Event{
				commitEvent(jetstream.OpCreate, 10, 1*time.Second),
				commitEvent(jetstream.OpCreate, 11, 2*time.Second),
				zero(commitEvent(jetstream.OpCreate, 12, 0)),
			},
			wantWit: testBase.Add(2 * time.Second), wantSeq: 11, wantZero: 1,
		},
		{
			name: "zero witnessed time first does not count",
			events: []jetstream.Event{
				zero(accountEvent(true, 9, 0)),
				syncEvent(10, time.Second),
			},
			wantWit: testBase.Add(time.Second), wantSeq: 10, wantZero: 1,
		},
		{
			name: "all zero witnessed times leave the cursor unset",
			events: []jetstream.Event{
				zero(commitEvent(jetstream.OpCreate, 10, 0)),
				zero(syncEvent(11, 0)),
			},
			wantWit: time.Time{}, wantSeq: 0, wantZero: 2,
		},
		{
			name: "skipped events do not move the cursor",
			events: []jetstream.Event{
				commitEvent(jetstream.OpCreate, 10, 1*time.Second),
				{DID: testDID, Seq: 99, WitnessedAtUS: witnessedUS(time.Hour), Kind: jetstream.KindIdentity, Identity: &jetstream.Identity{DID: testDID}},
			},
			wantWit: testBase.Add(time.Second), wantSeq: 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := mapBatch(tt.events)
			c := res.batch.Cursor
			if !c.Witnessed.Equal(tt.wantWit) || c.IsZero() != tt.wantWit.IsZero() {
				t.Errorf("cursor witnessed = %v, want %v", c.Witnessed, tt.wantWit)
			}
			if c.Seq != tt.wantSeq {
				t.Errorf("cursor seq = %d, want %d", c.Seq, tt.wantSeq)
			}
			if c.Host != "" {
				t.Errorf("cursor host = %q, want empty", c.Host)
			}
			if res.zeroWitnessed != tt.wantZero {
				t.Errorf("zeroWitnessed = %d, want %d", res.zeroWitnessed, tt.wantZero)
			}
		})
	}
}
