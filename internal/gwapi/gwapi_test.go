package gwapi

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// decodeFixture returns the server message of a fixture.
func decodeFixture[T ServerMessage](t *testing.T, name string, a Audience) T {
	t.Helper()
	m, err := DecodeServer(readFixture(t, name), a)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	v, ok := m.(T)
	if !ok {
		t.Fatalf("%s: got %T", name, m)
	}
	return v
}

var (
	demoOperator   = Audience{Level: LevelDemo, View: ViewOperator}
	fullOperator   = Audience{Level: LevelFull, View: ViewOperator}
	demoSupervisor = Audience{Level: LevelDemo, View: ViewSupervisor}
	fullSupervisor = Audience{Level: LevelFull, View: ViewSupervisor}
)

func TestServerValidate(t *testing.T) {
	opSnap := func() State { return decodeFixture[State](t, "server_state_operator_snapshot_demo.json", demoOperator) }
	supSnap := func() State {
		return decodeFixture[State](t, "server_state_supervisor_snapshot_full.json", fullSupervisor)
	}
	supUpd := func() State {
		return decodeFixture[State](t, "server_state_supervisor_update_demo.json", demoSupervisor)
	}
	msgsFull := func() Messages { return decodeFixture[Messages](t, "server_messages_snapshot_full.json", fullOperator) }
	welcome := func() Welcome { return decodeFixture[Welcome](t, "server_welcome_operator_demo.json", demoOperator) }

	tests := []struct {
		name   string
		msg    func() ServerMessage
		a      Audience
		fields []string
	}{
		{name: "welcome for another level", a: fullOperator, msg: func() ServerMessage { return welcome() }, fields: []string{"level"}},
		{name: "welcome with a simulated operator", a: demoOperator, msg: func() ServerMessage {
			w := welcome()
			w.OperatorID = "sim-op-0001"
			return w
		}, fields: []string{"operator_id"}},
		{name: "welcome without targets", a: demoOperator, msg: func() ServerMessage {
			w := welcome()
			w.AssignmentTargetUS, w.Version, w.Limits.MaxWatched = 0, 2, 0
			return w
		}, fields: []string{"assignment_target_us", "version", "limits"}},
		{name: "supervisor welcome with operator", a: demoSupervisor, msg: func() ServerMessage {
			w := welcome()
			w.View = ViewSupervisor
			return w
		}, fields: []string{"operator_id and resume_token"}},
		{name: "stale without reason", a: demoOperator, msg: func() ServerMessage { return Status{Stale: true} }, fields: []string{"reason"}},
		{name: "fresh with reason", a: demoOperator, msg: func() ServerMessage { return Status{Reason: StaleLagging} }, fields: []string{"reason"}},
		{name: "first state is no snapshot", a: demoOperator, msg: func() ServerMessage {
			s := opSnap()
			s.Snapshot = false
			return s
		}, fields: []string{"snapshot"}},
		{name: "operator snapshot without me", a: demoOperator, msg: func() ServerMessage {
			s := opSnap()
			s.Me = nil
			return s
		}, fields: []string{"me"}},
		{name: "supervisor collections for an operator", a: demoOperator, msg: func() ServerMessage {
			s := opSnap()
			s.Totals = &Totals{}
			return s
		}, fields: []string{"totals"}},
		{name: "operator collections for a supervisor", a: fullSupervisor, msg: func() ServerMessage {
			s := supSnap()
			s.MyConversations = &Changes[OperatorConversation]{}
			return s
		}, fields: []string{"my_conversations"}},
		{name: "remove in a snapshot", a: fullSupervisor, msg: func() ServerMessage {
			s := supSnap()
			s.Teams.Remove = []string{"x"}
			return s
		}, fields: []string{"teams.remove"}},
		{name: "ID set and removed", a: demoSupervisor, msg: func() ServerMessage {
			s := supUpd()
			s.Conversations.Remove = append(s.Conversations.Remove, s.Conversations.Set[0].ID)
			return s
		}, fields: []string{"conversations.remove"}},
		{name: "full IDs at the demo level", a: demoSupervisor, msg: func() ServerMessage {
			s := supSnap()
			s.Seq = 2
			return s
		}, fields: []string{"conversations.set[0].id", "company", "customer", "teams.set[0].id", "operators.set[0].team"}},
		{name: "key that does not match", a: fullSupervisor, msg: func() ServerMessage {
			s := supSnap()
			s.Conversations.Set[0].Customer = "did:plc:testdave000000000000000"
			return s
		}, fields: []string{"does not match"}},
		{name: "held row without operator", a: fullSupervisor, msg: func() ServerMessage {
			s := supSnap()
			s.Conversations.Set[0].OperatorID = ""
			return s
		}, fields: []string{"operator_id"}},
		{name: "operator kind does not match", a: fullSupervisor, msg: func() ServerMessage {
			s := supSnap()
			s.Operators.Set[0].Kind = KindHuman
			return s
		}, fields: []string{"kind"}},
		{name: "offer without deadline", a: demoOperator, msg: func() ServerMessage {
			s := opSnap()
			s.MyConversations.Set[1].AcceptByUS = 0
			return s
		}, fields: []string{"accept_by_us"}},
		{name: "queue without oldest time", a: fullSupervisor, msg: func() ServerMessage {
			s := supSnap()
			s.Queues.Set[0].OldestQueuedAtUS = 0
			return s
		}, fields: []string{"oldest_queued_at_us"}},
		{name: "text at the demo level", a: demoOperator, msg: func() ServerMessage {
			m := msgsFull()
			m.ConversationID = "ps-k2m3n4p5q6r7"
			for i := range m.Messages {
				m.Messages[i].ID = "ps-msgaaaa2222" + string(rune('2'+i))
			}
			return m
		}, fields: []string{"text"}},
		{name: "length does not match", a: fullOperator, msg: func() ServerMessage {
			m := msgsFull()
			m.Messages[0].Length++
			return m
		}, fields: []string{"length"}},
		{name: "snapshot out of order", a: fullOperator, msg: func() ServerMessage {
			m := msgsFull()
			m.Messages[0], m.Messages[1] = m.Messages[1], m.Messages[0]
			return m
		}, fields: []string{"ordered"}},
		{name: "null messages", a: fullOperator, msg: func() ServerMessage {
			return Messages{ConversationID: "a|b", Snapshot: true}
		}, fields: []string{"messages"}},
		{name: "removed without reason", a: fullOperator, msg: func() ServerMessage {
			return MessageRemoved{ConversationID: "a|b", MessageID: "m1"}
		}, fields: []string{"reason"}},
		{name: "rejected without code", a: demoOperator, msg: func() ServerMessage {
			return Result{RequestID: "r1", Outcome: OutcomeRejected}
		}, fields: []string{"code"}},
		{name: "done with code", a: demoOperator, msg: func() ServerMessage {
			return Result{RequestID: "r1", Outcome: OutcomeDone, Code: CodeInternal}
		}, fields: []string{"code and message"}},
		{name: "unknown code", a: demoOperator, msg: func() ServerMessage {
			return Result{RequestID: "r1", Outcome: OutcomeRejected, Code: "nope"}
		}, fields: []string{"code"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := tt.msg()
			err := m.Validate(tt.a)
			if err == nil {
				t.Fatal("Validate() error = nil, want an error")
			}
			for _, f := range tt.fields {
				if !strings.Contains(err.Error(), f) {
					t.Errorf("error %q does not name %s", err, f)
				}
			}
			// Errors never carry post text (decision 0004).
			if strings.Contains(err.Error(), "synthetic") {
				t.Errorf("error %q contains post text", err)
			}
			if _, err := EncodeServer(m, tt.a); err == nil {
				t.Error("EncodeServer() error = nil, want an error")
			}
		})
	}
}

// TestPublicSnapshotFields fails when a field is added to the public
// snapshot, or to a type inside it, without adding it to this allow-list.
// Before a field is added here, check that it can never hold a
// conversation key, a company or customer ID or pseudonym, an operator ID,
// a message ID or text (decisions 0004 and 0013).
func TestPublicSnapshotFields(t *testing.T) {
	allowed := []string{
		"generated_at_us",
		"queues",
		"queues.lang",
		"queues.missed_assignment",
		"queues.offered",
		"queues.oldest_wait_us",
		"queues.waiting",
		"speed_factor",
		"stale",
		"totals",
		"totals.companies_active",
		"totals.conversations",
		"totals.conversations.assigned",
		"totals.conversations.offered",
		"totals.conversations.open",
		"totals.conversations.resolved",
		"totals.conversations.waiting",
		"totals.humans_online",
		"totals.messages_per_second",
		"totals.missed_assignment",
		"totals.missed_first_response",
		"totals.operators_online",
		"totals.slots_capacity",
		"totals.slots_used",
		"totals.wait_p50_us",
		"totals.wait_p90_us",
		"version",
	}
	// The only strings are languages, which Validate checks.
	stringFields := []string{"queues.lang"}

	var got, gotStrings []string
	var walk func(prefix string, typ reflect.Type)
	walk = func(prefix string, typ reflect.Type) {
		for f := range typ.Fields() {
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			if name == "" || name == "-" || !f.IsExported() {
				t.Errorf("%s%s: every field needs an explicit JSON name", prefix, f.Name)
				continue
			}
			path := prefix + name
			got = append(got, path)
			ft := f.Type
			if ft.Kind() == reflect.Slice {
				ft = ft.Elem()
			}
			switch ft.Kind() {
			case reflect.Struct:
				walk(path+".", ft)
			case reflect.String:
				gotStrings = append(gotStrings, path)
			case reflect.Bool, reflect.Int, reflect.Int64, reflect.Uint64, reflect.Float64:
			default:
				// Maps (string keys), interfaces, pointers, byte slices
				// and raw JSON could carry anything.
				t.Errorf("%s: kind %s is not allowed in the public snapshot", path, ft.Kind())
			}
		}
	}
	walk("", reflect.TypeFor[PublicSnapshot]())
	slices.Sort(got)
	if !slices.Equal(got, allowed) {
		t.Errorf("public snapshot fields:\n got %v\nwant %v", got, allowed)
	}
	if !slices.Equal(gotStrings, stringFields) {
		t.Errorf("string fields %v, want only %v", gotStrings, stringFields)
	}

	// The fixture has every allowed field and nothing else.
	var raw map[string]any
	if err := json.Unmarshal(readFixture(t, "public_snapshot.json"), &raw); err != nil {
		t.Fatal(err)
	}
	var paths []string
	var collect func(prefix string, v any)
	collect = func(prefix string, v any) {
		switch x := v.(type) {
		case map[string]any:
			for k, e := range x {
				paths = append(paths, prefix+k)
				collect(prefix+k+".", e)
			}
		case []any:
			if len(x) > 0 {
				collect(prefix, x[0])
			}
		}
	}
	collect("", raw)
	slices.Sort(paths)
	if !slices.Equal(paths, allowed) {
		t.Errorf("public_snapshot.json fields:\n got %v\nwant %v", paths, allowed)
	}
}

func TestPublicSnapshotValidate(t *testing.T) {
	p, err := DecodePublic(readFixture(t, "public_snapshot.json"))
	if err != nil {
		t.Fatal(err)
	}
	p.Queues = append(p.Queues, PublicQueue{Lang: "ps-abcdefgh2345"}, PublicQueue{Lang: "en"})
	p.Version, p.SpeedFactor = 2, 0
	_, err = EncodePublic(p)
	for _, f := range []string{"version", "speed_factor", "queues[2].lang", "queues[3].lang"} {
		if err == nil || !strings.Contains(err.Error(), f) {
			t.Errorf("EncodePublic() error = %v, want it to name %s", err, f)
		}
	}
	if _, err := EncodePublic(PublicSnapshot{Version: ProtocolVersion, GeneratedAtUS: 1, SpeedFactor: 1}); err == nil {
		t.Error("EncodePublic() with null queues error = nil")
	}
}

func TestErrorCodes(t *testing.T) {
	for _, c := range opapi.Codes {
		if !FromOpapi(c).Valid() {
			t.Errorf("operator API code %q is not a gwapi code", c)
		}
	}
	seen := map[ErrorCode]bool{}
	for _, c := range Codes {
		if seen[c] {
			t.Errorf("code %q is listed twice", c)
		}
		seen[c] = true
	}
	if len(Codes) != len(opapi.Codes)+6 {
		t.Errorf("%d codes, want the %d of the operator API and 6 of the gateway", len(Codes), len(opapi.Codes))
	}
	if !CodeRateLimited.Retry() || !CodeUnavailable.Retry() || !CodeSettingsTooSoon.Retry() ||
		CodeOfferExpired.Retry() || CodeSettingsConflict.Retry() || CodeSettingsOff.Retry() {
		t.Error("Retry() is wrong")
	}
}

func TestCloseCodes(t *testing.T) {
	names := map[string]bool{}
	for _, c := range CloseCodes {
		n := c.Name()
		if n == "unknown" || names[n] {
			t.Errorf("close code %d has name %q", c, n)
		}
		names[n] = true
		if c < 1000 || (c >= 1016 && c < 4000) || c > 4999 {
			t.Errorf("close code %d is outside the ranges of RFC 6455", c)
		}
	}
	if CloseCode(4999).Name() != "unknown" {
		t.Error("an unknown code has a name")
	}
	if CloseBadMessage.Reconnect() || CloseUnsupportedVersion.Reconnect() || CloseReplaced.Reconnect() || !CloseGoingAway.Reconnect() {
		t.Error("Reconnect() is wrong")
	}
}

func TestPseudonym(t *testing.T) {
	key1, key2 := []byte("synthetic day key one"), []byte("synthetic day key two")
	id := "did:plc:testalice0000000000000000"
	p := Pseudonym(key1, id)
	if !ValidPseudonym(p) {
		t.Errorf("Pseudonym() = %q is not valid", p)
	}
	if Pseudonym(key1, id) != p {
		t.Error("Pseudonym() is not stable for the same key")
	}
	if Pseudonym(key2, id) == p || Pseudonym(key1, id+"x") == p {
		t.Error("Pseudonym() is the same for another key or ID")
	}
	if strings.Contains(p, "alice") {
		t.Errorf("Pseudonym() = %q holds a part of the ID", p)
	}
	for _, s := range []string{"", "ps-", "ps-abc", "ps-ABCDEFGH2345", "ps-abcdefgh2341", "xx-abcdefgh2345", "ps-abcdefgh23456"} {
		if ValidPseudonym(s) {
			t.Errorf("ValidPseudonym(%q) = true", s)
		}
	}
}

func TestKindOf(t *testing.T) {
	tests := map[string]Kind{
		"web-test0001": KindHuman,
		"web-":         KindSimulated,
		"sim-op-0001":  KindSimulated,
		"webx-1":       KindSimulated,
	}
	for id, want := range tests {
		if got := KindOf(id); got != want {
			t.Errorf("KindOf(%q) = %s, want %s", id, got, want)
		}
	}
	if !event.ValidRoutingID(HumanOperatorPrefix + "a") {
		t.Error("a human operator ID is not a valid routing ID")
	}
}

func TestEncodeClient(t *testing.T) {
	b, err := EncodeClient(Hello{Version: ProtocolVersion, View: ViewSupervisor})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"type":"hello","version":1,"view":"supervisor"}`; string(b) != want {
		t.Errorf("EncodeClient() = %s, want %s", b, want)
	}
	if _, err := EncodeClient(Action{RequestID: "r1"}); err == nil {
		t.Error("EncodeClient() of an invalid action error = nil")
	}
}

func TestDefaultLimits(t *testing.T) {
	w := decodeFixture[Welcome](t, "server_welcome_operator_demo.json", demoOperator)
	if w.Limits != DefaultLimits() {
		t.Errorf("fixture limits %+v, want %+v", w.Limits, DefaultLimits())
	}
	if w.TickMS != int(Tick.Milliseconds()) {
		t.Errorf("fixture tick %d, want %d", w.TickMS, Tick.Milliseconds())
	}
}

// TestAudienceFailsClosed checks that a server message is never encoded
// for an audience the package does not know, so a bug in the gateway that
// loses the level cannot send text or IDs.
func TestAudienceFailsClosed(t *testing.T) {
	for _, f := range allFixtures(t) {
		if f.Direction != "server" {
			continue
		}
		raw := f.read(t)
		for _, a := range []Audience{{}, {Level: "public", View: f.View}, {Level: f.Level, View: "admin"}} {
			if _, err := DecodeServer(raw, a); err == nil || !strings.Contains(err.Error(), "audience") {
				t.Errorf("%s for audience %+v: error = %v, want an audience error", f.File, a, err)
			}
		}
	}
	m := decodeFixture[Messages](t, "server_messages_snapshot_full.json", fullOperator)
	if err := m.Validate(Audience{Level: "", View: ViewOperator}); err == nil {
		t.Error("Messages.Validate() with text for an empty level error = nil")
	}
}
