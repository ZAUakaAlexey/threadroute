package normalize

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

func TestMapMessages(t *testing.T) {
	rootURI := postURI(testAlice, "root000")
	bobReplyURI := postURI(testBob, "bbb222")

	tests := []struct {
		name  string
		did   string
		op    string
		rkey  string
		post  map[string]any
		kind  Kind
		want  event.Message
		check func(t *testing.T, r Result)
	}{
		{
			name: "reply by another person",
			did:  testBob, op: "create", rkey: "aaa111",
			post: replyRecord(rootURI, rootURI, "en-US", "de"),
			kind: KindCustomerMessage,
			want: event.Message{
				CompanyID: testAlice, CustomerID: testBob, Direction: event.FromCustomer,
				ParentID: rootURI, Langs: []string{"en-US", "de"}, Lang: "en",
			},
		},
		{
			name: "reply by a third person to a customer",
			did:  testCarol, op: "create", rkey: "aaa111",
			post: replyRecord(rootURI, bobReplyURI),
			kind: KindCustomerMessage,
			want: event.Message{
				CompanyID: testAlice, CustomerID: testCarol, Direction: event.FromCustomer,
				ParentID: bobReplyURI, Langs: []string{}, Lang: "und",
			},
		},
		{
			name: "reply by the thread author to another person",
			did:  testAlice, op: "create", rkey: "aaa111",
			post: replyRecord(rootURI, bobReplyURI, "pt_BR"),
			kind: KindCompanyMessage,
			want: event.Message{
				CompanyID: testAlice, CustomerID: testBob, Direction: event.FromCompany,
				ParentID: bobReplyURI, Langs: []string{"pt_BR"}, Lang: "pt",
			},
		},
		{
			name: "edited reply is classified like a new one and marked as an edit",
			did:  testBob, op: "update", rkey: "aaa111",
			post: replyRecord(rootURI, rootURI, "en"),
			kind: KindCustomerMessage,
			want: event.Message{
				CompanyID: testAlice, CustomerID: testBob, Direction: event.FromCustomer,
				ParentID: rootURI, Langs: []string{"en"}, Lang: "en", Edited: true,
			},
		},
		{
			name: "edited company reply is marked as an edit",
			did:  testAlice, op: "update", rkey: "aaa111",
			post: replyRecord(rootURI, bobReplyURI, "en"),
			kind: KindCompanyMessage,
			want: event.Message{
				CompanyID: testAlice, CustomerID: testBob, Direction: event.FromCompany,
				ParentID: bobReplyURI, Langs: []string{"en"}, Lang: "en", Edited: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := commitID(tt.did, tt.op, tt.rkey)
			rec := rawRecord(tt.did, commitValue(t, tt.did, tt.op, "app.bsky.feed.post", tt.rkey, tt.post), id, "commit", testWitnessed)
			r := Map(rec)
			if r.Kind != tt.kind {
				t.Fatalf("Kind = %q (drop reason %q), want %q", r.Kind, r.DropReason, tt.kind)
			}
			if r.Topic != bus.TopicEvents {
				t.Errorf("Topic = %q, want %q", r.Topic, bus.TopicEvents)
			}
			if want := string(event.ConversationKey(tt.want.CompanyID, tt.want.CustomerID)); string(r.Key) != want {
				t.Errorf("Key = %q, want %q", r.Key, want)
			}
			if r.EventID != id || !r.SourceTime.Equal(testWitnessed) {
				t.Errorf("EventID, SourceTime = %q, %s", r.EventID, r.SourceTime)
			}
			got, err := event.UnmarshalMessage(r.Value)
			if err != nil {
				t.Fatalf("UnmarshalMessage() error = %v", err)
			}
			want := tt.want
			want.EventID = id
			want.Channel = event.ChannelBluesky
			want.SourceTime = testWitnessed
			want.MessageID = postURI(tt.did, tt.rkey)
			want.Text = testText
			if !reflect.DeepEqual(got, want) {
				t.Errorf("message =\n%+v\nwant\n%+v", got, want)
			}
			out := r.OutputRecord()
			if out == nil || out.Topic != bus.TopicEvents || len(out.Headers) != 1 ||
				out.Headers[0].Key != bus.HeaderEventID || string(out.Headers[0].Value) != id {
				t.Errorf("OutputRecord() = %+v", out)
			}
		})
	}
}

func TestMapControl(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value func(t *testing.T) []byte
		id    string
		kind  Kind
		want  event.Control
	}{
		{
			name: "deletion",
			key:  testBob,
			value: func(t *testing.T) []byte {
				return commitValue(t, testBob, "delete", "app.bsky.feed.post", "aaa111", nil)
			},
			id:   commitID(testBob, "delete", "aaa111"),
			kind: KindDeletion,
			want: event.Deletion{
				EventID: commitID(testBob, "delete", "aaa111"), Channel: event.ChannelBluesky, SourceTime: testWitnessed,
				MessageID: postURI(testBob, "aaa111"), AuthorID: testBob,
			},
		},
		{
			name:  "account not active",
			key:   testBob,
			value: func(t *testing.T) []byte { return accountValue(t, testBob, false, "deleted") },
			id:    "a:" + testBob + ":0:deleted:2026-01-02T03%3A04%3A05.000Z",
			kind:  KindAuthorRemoval,
			want: event.AuthorRemoval{
				EventID: "a:" + testBob + ":0:deleted:2026-01-02T03%3A04%3A05.000Z", Channel: event.ChannelBluesky,
				SourceTime: testWitnessed, AuthorID: testBob, Reason: event.RemovalInactive, Status: "deleted",
			},
		},
		{
			name:  "account not active without status",
			key:   testBob,
			value: func(t *testing.T) []byte { return accountValue(t, testBob, false, "") },
			id:    "a:" + testBob + ":0::t",
			kind:  KindAuthorRemoval,
			want: event.AuthorRemoval{
				EventID: "a:" + testBob + ":0::t", Channel: event.ChannelBluesky,
				SourceTime: testWitnessed, AuthorID: testBob, Reason: event.RemovalInactive,
			},
		},
		{
			name:  "sync",
			key:   testAlice,
			value: func(t *testing.T) []byte { return syncValue(t, testAlice) },
			id:    "s:" + testAlice + ":" + testRev + ":t",
			kind:  KindAuthorRemoval,
			want: event.AuthorRemoval{
				EventID: "s:" + testAlice + ":" + testRev + ":t", Channel: event.ChannelBluesky,
				SourceTime: testWitnessed, AuthorID: testAlice, Reason: event.RemovalSync,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := Map(rawRecord(tt.key, tt.value(t), tt.id, "x", testWitnessed))
			if r.Kind != tt.kind {
				t.Fatalf("Kind = %q (drop reason %q), want %q", r.Kind, r.DropReason, tt.kind)
			}
			if r.Topic != bus.TopicControl {
				t.Errorf("Topic = %q, want %q", r.Topic, bus.TopicControl)
			}
			if string(r.Key) != tt.want.Author() {
				t.Errorf("Key = %q, want %q", r.Key, tt.want.Author())
			}
			got, err := event.UnmarshalControl(r.Value)
			if err != nil {
				t.Fatalf("UnmarshalControl() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("control =\n%+v\nwant\n%+v", got, tt.want)
			}
		})
	}
}

func TestMapDrops(t *testing.T) {
	rootURI := postURI(testAlice, "root000")
	bobID := commitID(testBob, "create", "aaa111")
	reply := func(t *testing.T, post map[string]any) []byte {
		return commitValue(t, testBob, "create", "app.bsky.feed.post", "aaa111", post)
	}
	withReply := func(reply map[string]any) map[string]any {
		p := replyRecord("", "")
		p["reply"] = reply
		return p
	}

	tests := []struct {
		name   string
		value  func(t *testing.T) []byte
		id     string
		noTime bool
		reason string
	}{
		{name: "post without a reply", value: func(t *testing.T) []byte { return reply(t, replyRecord("", "")) }, reason: DropNotReply},
		{name: "thread author replies to own post", value: func(t *testing.T) []byte {
			return commitValue(t, testAlice, "create", "app.bsky.feed.post", "aaa111", replyRecord(rootURI, postURI(testAlice, "ccc333")))
		}, reason: DropSelfReply},
		{name: "account active", value: func(t *testing.T) []byte { return accountValue(t, testBob, true, "") }, reason: DropAccountActive},
		{name: "commit for another collection", value: func(t *testing.T) []byte {
			return commitValue(t, testBob, "create", "app.bsky.feed.like", "aaa111", map[string]any{"subject": map[string]any{"uri": rootURI}})
		}, reason: DropOtherCollection},
		{name: "deletion in another collection", value: func(t *testing.T) []byte {
			return commitValue(t, testBob, "delete", "app.bsky.feed.like", "aaa111", nil)
		}, reason: DropOtherCollection},
		{name: "identity event", value: func(t *testing.T) []byte {
			ev := baseEvent(testBob, "identity")
			ev["identity"] = map[string]any{"did": testBob, "seq": 1, "time": "t"}
			return eventJSON(t, ev)
		}, reason: DropUnknownKind},
		{name: "malformed json", value: func(*testing.T) []byte { return []byte(`{"did":"did:plc:testbob`) }, reason: DropDecodeFailed},
		{name: "not an object", value: func(*testing.T) []byte { return []byte(`"synthetic"`) }, reason: DropDecodeFailed},
		{name: "empty value", value: func(*testing.T) []byte { return nil }, reason: DropDecodeFailed},
		{name: "no did", value: func(t *testing.T) []byte {
			return eventJSON(t, map[string]any{"kind": "sync", "sync": map[string]any{}})
		}, reason: DropDecodeFailed},
		{name: "no kind", value: func(t *testing.T) []byte { return eventJSON(t, map[string]any{"did": testBob}) }, reason: DropDecodeFailed},
		{name: "commit without commit object", value: func(t *testing.T) []byte { return eventJSON(t, baseEvent(testBob, "commit")) }, reason: DropDecodeFailed},
		{name: "account without account object", value: func(t *testing.T) []byte { return eventJSON(t, baseEvent(testBob, "account")) }, reason: DropDecodeFailed},
		{name: "sync without sync object", value: func(t *testing.T) []byte { return eventJSON(t, baseEvent(testBob, "sync")) }, reason: DropDecodeFailed},
		{name: "create without record", value: func(t *testing.T) []byte { return reply(t, nil) }, reason: DropDecodeFailed},
		{name: "unknown operation", value: func(t *testing.T) []byte {
			return commitValue(t, testBob, "resync", "app.bsky.feed.post", "aaa111", replyRecord(rootURI, rootURI))
		}, reason: DropDecodeFailed},
		{name: "langs of the wrong type", value: func(t *testing.T) []byte {
			p := replyRecord(rootURI, rootURI)
			p["langs"] = "en"
			return reply(t, p)
		}, reason: DropDecodeFailed},
		{name: "reply without root", value: func(t *testing.T) []byte {
			return reply(t, withReply(map[string]any{"parent": map[string]any{"uri": rootURI}}))
		}, reason: DropBadReplyRef},
		{name: "reply without parent", value: func(t *testing.T) []byte {
			return reply(t, withReply(map[string]any{"root": map[string]any{"uri": rootURI}}))
		}, reason: DropBadReplyRef},
		{name: "reply with empty root uri", value: func(t *testing.T) []byte { return reply(t, replyRecord("", rootURI)) }, reason: DropBadReplyRef},
		{name: "root uri not at scheme", value: func(t *testing.T) []byte {
			return reply(t, replyRecord("https://example.test/post/1", rootURI))
		}, reason: DropBadReplyRef},
		{name: "root uri with a handle", value: func(t *testing.T) []byte {
			return reply(t, replyRecord("at://alice.example.test/app.bsky.feed.post/root000", rootURI))
		}, reason: DropBadReplyRef},
		{name: "root uri without record key", value: func(t *testing.T) []byte {
			return reply(t, replyRecord("at://"+testAlice+"/app.bsky.feed.post", rootURI))
		}, reason: DropBadReplyRef},
		{name: "parent uri with extra part", value: func(t *testing.T) []byte {
			return reply(t, replyRecord(rootURI, rootURI+"/extra"))
		}, reason: DropBadReplyRef},
		{name: "parent uri with empty did", value: func(t *testing.T) []byte {
			return reply(t, replyRecord(rootURI, "at://did:/app.bsky.feed.post/root000"))
		}, reason: DropBadReplyRef},
		{name: "parent is the post itself", value: func(t *testing.T) []byte {
			return reply(t, replyRecord(rootURI, postURI(testBob, "aaa111")))
		}, reason: DropInvalidEvent},
		{name: "missing event id header", id: "-", value: func(t *testing.T) []byte { return reply(t, replyRecord(rootURI, rootURI)) }, reason: DropMissingEventID},
		{name: "no witnessed time anywhere", noTime: true, value: func(t *testing.T) []byte {
			ev := baseEvent(testBob, "sync")
			delete(ev, "witnessed_at_us")
			delete(ev, "time_us")
			ev["sync"] = map[string]any{"did": testBob}
			return eventJSON(t, ev)
		}, reason: DropMissingTime},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := bobID
			switch tt.id {
			case "-":
				id = ""
			case "":
			default:
				id = tt.id
			}
			witnessed := testWitnessed
			if tt.noTime {
				witnessed = time.Time{}
			}
			r := Map(rawRecord(testBob, tt.value(t), id, "x", witnessed))
			if !r.Dropped() || r.DropReason != tt.reason {
				t.Fatalf("Map() = kind %q, reason %q, want drop %q", r.Kind, r.DropReason, tt.reason)
			}
			if r.EventID != id {
				t.Errorf("EventID = %q, want %q", r.EventID, id)
			}
			if r.Topic != "" || r.Key != nil || r.Value != nil || r.OutputRecord() != nil {
				t.Errorf("a drop has output: %+v", r)
			}
			if !strings.Contains(strings.Join(dropReasons, ","), tt.reason) {
				t.Errorf("reason %q is not in dropReasons", tt.reason)
			}
		})
	}
}

// TestMapSourceTime checks each step of the order: the witnessed header,
// then witnessed_at_us, then time_us, then a drop.
func TestMapSourceTime(t *testing.T) {
	headerTime := testWitnessed.Add(3 * time.Second)
	witnessedTime := testWitnessed
	displayTime := testWitnessed.Add(-7 * time.Second)

	// syncWith returns a sync event with the given payload times. A zero
	// time leaves the field out.
	syncWith := func(t *testing.T, witnessed, display time.Time) []byte {
		ev := baseEvent(testAlice, "sync")
		delete(ev, "witnessed_at_us")
		delete(ev, "time_us")
		if !witnessed.IsZero() {
			ev["witnessed_at_us"] = witnessed.UnixMicro()
		}
		if !display.IsZero() {
			ev["time_us"] = display.UnixMicro()
		}
		ev["sync"] = map[string]any{"did": testAlice, "rev": testRev}
		return eventJSON(t, ev)
	}
	tests := []struct {
		name        string
		header      string // "" no header, "h" headerTime, other: raw value
		witnessed   time.Time
		display     time.Time
		want        time.Time
		wantDisplay bool
	}{
		{name: "header first", header: "h", witnessed: witnessedTime, display: displayTime, want: headerTime},
		{name: "header alone", header: "h", want: headerTime},
		{name: "witnessed_at_us when no header", witnessed: witnessedTime, display: displayTime, want: witnessedTime},
		{name: "witnessed_at_us when header is not a number", header: "soon", witnessed: witnessedTime, display: displayTime, want: witnessedTime},
		{name: "witnessed_at_us when header is zero", header: "0", witnessed: witnessedTime, display: displayTime, want: witnessedTime},
		{name: "time_us when no witnessed time", display: displayTime, want: displayTime, wantDisplay: true},
		{name: "time_us when header is invalid", header: "-5", display: displayTime, want: displayTime, wantDisplay: true},
		{name: "drop when none", header: "", want: time.Time{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := syncWith(t, tt.witnessed, tt.display)
			rec := rawRecord(testAlice, value, "s:id", "sync", time.Time{})
			switch tt.header {
			case "":
			case "h":
				rec = rawRecord(testAlice, value, "s:id", "sync", headerTime)
			default:
				rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: bus.HeaderEventWitnessed, Value: []byte(tt.header)})
			}
			r := Map(rec)
			if tt.want.IsZero() {
				if !r.Dropped() || r.DropReason != DropMissingTime || r.DisplayTime {
					t.Fatalf("Map() = %+v, want drop %q", r, DropMissingTime)
				}
				return
			}
			if r.Dropped() {
				t.Fatalf("dropped: %s", r.DropReason)
			}
			if !r.SourceTime.Equal(tt.want) || r.DisplayTime != tt.wantDisplay {
				t.Errorf("SourceTime, DisplayTime = %s, %v, want %s, %v", r.SourceTime, r.DisplayTime, tt.want, tt.wantDisplay)
			}
			c, err := event.UnmarshalControl(r.Value)
			if err != nil {
				t.Fatal(err)
			}
			if got := c.(event.AuthorRemoval).SourceTime; !got.Equal(tt.want) {
				t.Errorf("payload source time = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestMapDoesNotChangeRecord(t *testing.T) {
	value := commitValue(t, testBob, "create", "app.bsky.feed.post", "aaa111", replyRecord(postURI(testAlice, "r"), postURI(testAlice, "r")))
	rec := rawRecord(testBob, value, commitID(testBob, "create", "aaa111"), "commit", testWitnessed)
	before := string(rec.Value)
	headers := len(rec.Headers)
	_ = Map(rec)
	if string(rec.Value) != before || len(rec.Headers) != headers {
		t.Error("Map changed the record")
	}
}

func TestURIAuthor(t *testing.T) {
	tests := []struct {
		uri  string
		want string
		ok   bool
	}{
		{uri: postURI(testAlice, "aaa111"), want: testAlice, ok: true},
		{uri: "at://did:web:example.test/app.bsky.feed.post/aaa111", want: "did:web:example.test", ok: true},
		{uri: "", ok: false},
		{uri: "at://", ok: false},
		{uri: "at://" + testAlice, ok: false},
		{uri: "at://" + testAlice + "/app.bsky.feed.post/", ok: false},
		{uri: "at:///app.bsky.feed.post/aaa111", ok: false},
		{uri: "AT://" + testAlice + "/app.bsky.feed.post/aaa111", ok: false},
	}
	for _, tt := range tests {
		got, ok := uriAuthor(tt.uri)
		if got != tt.want || ok != tt.ok {
			t.Errorf("uriAuthor(%q) = %q, %v, want %q, %v", tt.uri, got, ok, tt.want, tt.ok)
		}
	}
}

// BenchmarkMap measures the pure mapping of a customer reply, the most
// expensive common case.
func BenchmarkMap(b *testing.B) {
	root := postURI(testAlice, "root000")
	value := commitValue(b, testBob, "create", "app.bsky.feed.post", "aaa111", replyRecord(root, root, "en"))
	rec := rawRecord(testBob, value, commitID(testBob, "create", "aaa111"), "commit", testWitnessed)
	b.ReportAllocs()
	b.SetBytes(int64(len(value)))
	start := time.Now()
	for b.Loop() {
		if r := Map(rec); r.Kind != KindCustomerMessage {
			b.Fatalf("Kind = %q", r.Kind)
		}
	}
	b.ReportMetric(float64(b.N)/time.Since(start).Seconds(), "records/s")
}
