package normalize

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
)

// Synthetic, clearly fake test data (decision 0004).
const (
	testAlice = "did:plc:testalice0000000000000000"
	testBob   = "did:plc:testbob000000000000000000"
	testCarol = "did:plc:testcarol0000000000000000"
	testText  = "synthetic reply one"
	testRev   = "3testrev00001"
)

// testWitnessed is a fixed witnessed time with microsecond precision.
var testWitnessed = time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)

func postURI(did, rkey string) string {
	return "at://" + did + "/app.bsky.feed.post/" + rkey
}

// The values below have the shape of the raw topic: the JSON of a Jetstream
// SDK event without the CBOR copy (decision 0001). They are built by hand
// with the field names of the SDK, because nothing outside the connector
// imports the SDK (decision 0002).

// replyRecord returns a post record. Empty root and parent give a post that
// is not a reply.
func replyRecord(rootURI, parentURI string, langs ...string) map[string]any {
	rec := map[string]any{
		"$type":     "app.bsky.feed.post",
		"text":      testText,
		"createdAt": "2026-01-02T03:04:05.000Z",
	}
	if len(langs) > 0 {
		rec["langs"] = langs
	}
	if rootURI != "" || parentURI != "" {
		rec["reply"] = map[string]any{
			"root":   map[string]any{"uri": rootURI, "cid": "bafytestcidroot"},
			"parent": map[string]any{"uri": parentURI, "cid": "bafytestcidparent"},
		}
	}
	return rec
}

func eventJSON(t testing.TB, ev map[string]any) []byte {
	t.Helper()
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal test event: %v", err)
	}
	return b
}

func baseEvent(did, kind string) map[string]any {
	us := testWitnessed.UnixMicro()
	return map[string]any{
		"did":             did,
		"cursor":          12345,
		"time_us":         us,
		"witnessed_at_us": us,
		"kind":            kind,
	}
}

func commitValue(t testing.TB, did, op, collection, rkey string, record map[string]any) []byte {
	t.Helper()
	ev := baseEvent(did, "commit")
	c := map[string]any{"operation": op, "collection": collection, "rkey": rkey, "rev": testRev}
	if record != nil {
		c["cid"] = "bafytestcid0000"
		c["record"] = record
	}
	ev["commit"] = c
	return eventJSON(t, ev)
}

func accountValue(t testing.TB, did string, active bool, status string) []byte {
	t.Helper()
	ev := baseEvent(did, "account")
	a := map[string]any{"did": did, "active": active, "seq": 777, "time": "2026-01-02T03:04:05.000Z"}
	if status != "" {
		a["status"] = status
	}
	ev["account"] = a
	return eventJSON(t, ev)
}

func syncValue(t testing.TB, did string) []byte {
	t.Helper()
	ev := baseEvent(did, "sync")
	ev["sync"] = map[string]any{"did": did, "rev": testRev, "seq": 888, "time": "2026-01-02T03:04:05.000Z"}
	return eventJSON(t, ev)
}

func commitID(did, op, rkey string) string {
	return "c:" + did + ":app.bsky.feed.post:" + rkey + ":" + testRev + ":" + op
}

// rawRecord builds a record as ingest writes it to the raw topic. An empty
// eventID leaves the header out. A zero witnessed time leaves the witnessed
// header out.
func rawRecord(key string, value []byte, eventID, kind string, witnessed time.Time) *kgo.Record {
	r := &kgo.Record{Topic: bus.TopicRaw, Key: []byte(key), Value: value}
	if eventID != "" {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: bus.HeaderEventID, Value: []byte(eventID)})
	}
	r.Headers = append(r.Headers, kgo.RecordHeader{Key: bus.HeaderEventKind, Value: []byte(kind)})
	if !witnessed.IsZero() {
		r.Headers = append(r.Headers, kgo.RecordHeader{
			Key: bus.HeaderEventWitnessed, Value: []byte(strconv.FormatInt(witnessed.UnixMicro(), 10)),
		})
	}
	return r
}

func testContext(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}
