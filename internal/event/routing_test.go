package event

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Synthetic operator and offer IDs.
const (
	testOperator  = "sim-op-0001"
	testOperator2 = "sim-op-0002"
	testOffer     = "offer-000001"
)

// testMachineTime is a fixed machine time with microseconds.
var testMachineTime = time.UnixMicro(1_767_323_100_000_001).UTC()

func validRouting(typ RoutingType) Routing {
	r := Routing{
		Type:       typ,
		CompanyID:  testCompany,
		CustomerID: testCustomer,
		Session:    1,
		Seq:        1,
		At:         testMachineTime,
		Lang:       "en",
		QueuedAt:   testMachineTime.Add(-5 * time.Second),
	}
	switch typ {
	case TypeQueued:
		r.QueuedAt = testMachineTime
		r.QueueReason = QueueNew
	case TypeOffered:
		r.Seq = 2
		r.OperatorID, r.OfferID = testOperator, testOffer
		r.AcceptBy = testMachineTime.Add(20 * time.Second)
	case TypeAccepted:
		r.Seq = 3
		r.OperatorID, r.OfferID = testOperator, testOffer
		r.Wait = 7*time.Second + 500*time.Microsecond
	case TypeOfferExpired:
		r.Seq = 3
		r.OperatorID, r.OfferID = testOperator, testOffer
		r.OfferEndReason = OfferTimeout
	case TypeReplied:
		r.Seq = 4
		r.QueuedAt = time.Time{}
		r.OperatorID, r.OfferID = testOperator, testOffer
		r.FirstReply = true
		r.ResponseTime = 45 * time.Second
	case TypeResolved:
		r.Seq = 5
		r.QueuedAt = time.Time{}
		r.OperatorID, r.OfferID = testOperator, testOffer
	case TypeSLAMissed:
		r.Seq = 3
		r.Priority = 1
		r.SLATarget = SLAAssignment
		r.Measured = 61 * time.Second
		r.Target = 60 * time.Second
	case TypeEnded:
		r.Seq = 6
		r.QueuedAt = time.Time{}
		r.EndReason = CloseInactivity
	case TypeTeamFormed, TypeTeamResized, TypeTeamReleased:
		r.CustomerID, r.Session, r.Lang, r.QueuedAt = "", 0, "", time.Time{}
		r.Members = []string{testOperator, testOperator2}
		r.ActiveLoad = 20
		if typ == TypeTeamReleased {
			r.ActiveLoad = 3
			r.ReleaseReason = TeamLowLoad
		}
	}
	return r
}

var allRoutingTypes = []RoutingType{
	TypeQueued, TypeOffered, TypeAccepted, TypeOfferExpired, TypeReplied, TypeResolved,
	TypeSLAMissed, TypeEnded, TypeTeamFormed, TypeTeamResized, TypeTeamReleased,
}

func TestRoutingRoundTrip(t *testing.T) {
	var tests []struct {
		name string
		in   Routing
	}
	add := func(name string, r Routing) {
		tests = append(tests, struct {
			name string
			in   Routing
		}{name, r})
	}
	for _, typ := range allRoutingTypes {
		add(string(typ), validRouting(typ))
	}

	acceptedAtOnce := validRouting(TypeAccepted)
	acceptedAtOnce.Wait = 0
	add("accepted at once", acceptedAtOnce)
	repliedAtOnce := validRouting(TypeReplied)
	repliedAtOnce.ResponseTime = 0
	add("first reply at once", repliedAtOnce)
	laterReply := validRouting(TypeReplied)
	laterReply.FirstReply, laterReply.ResponseTime = false, 0
	add("later reply", laterReply)
	teamQueue := validRouting(TypeQueued)
	teamQueue.TeamQueue, teamQueue.Priority, teamQueue.Session, teamQueue.Seq = true, 2, 3, 40
	teamQueue.QueueReason = QueueTeamFormed
	add("team queue", teamQueue)
	slaWithOperator := validRouting(TypeSLAMissed)
	slaWithOperator.SLATarget, slaWithOperator.QueuedAt = SLAFirstResponse, time.Time{}
	slaWithOperator.OperatorID, slaWithOperator.OfferID = testOperator, testOffer
	add("first response missed", slaWithOperator)
	endedHeld := validRouting(TypeEnded)
	endedHeld.OperatorID, endedHeld.OfferID = testOperator, testOffer
	endedHeld.EndReason = CloseCompanyRemoved
	add("ended while held", endedHeld)
	releasedEmpty := validRouting(TypeTeamReleased)
	releasedEmpty.ActiveLoad = 0
	releasedEmpty.ReleaseReason = TeamAdmissionEnded
	add("released without load", releasedEmpty)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := MarshalRouting(tt.in)
			if err != nil {
				t.Fatalf("MarshalRouting() error = %v", err)
			}
			// Routing events carry no post text and no message URI
			// (decision 0004).
			if bytes.Contains(b, []byte(testText)) || bytes.Contains(b, []byte("at://")) {
				t.Errorf("encoded event %s contains post text or a URI", b)
			}
			out, err := UnmarshalRouting(b)
			if err != nil {
				t.Fatalf("UnmarshalRouting() error = %v", err)
			}
			if !reflect.DeepEqual(out, tt.in) {
				t.Errorf("round trip:\n got %+v\nwant %+v", out, tt.in)
			}
		})
	}
}

func TestRoutingKey(t *testing.T) {
	for _, typ := range allRoutingTypes {
		r := validRouting(typ)
		want := ConversationKey(testCompany, testCustomer)
		if typ.IsTeam() {
			want = []byte(testCompany)
		}
		if !bytes.Equal(r.Key(), want) {
			t.Errorf("%s: Key() = %q, want %q", typ, r.Key(), want)
		}
	}
}

func TestMarshalRoutingFormat(t *testing.T) {
	tests := []struct {
		name string
		in   Routing
		want string
	}{
		{
			name: "offered",
			in:   validRouting(TypeOffered),
			want: `{"type":"offered","company_id":"` + testCompany + `","customer_id":"` + testCustomer + `",` +
				`"session":1,"seq":2,"at_us":1767323100000001,"lang":"en","team_queue":false,"priority":0,` +
				`"queued_at_us":1767323095000001,` +
				`"operator_id":"sim-op-0001","offer_id":"offer-000001","accept_by_us":1767323120000001}`,
		},
		{
			name: "accepted at once",
			in: func() Routing {
				r := validRouting(TypeAccepted)
				r.Wait = 0
				return r
			}(),
			want: `{"type":"accepted","company_id":"` + testCompany + `","customer_id":"` + testCustomer + `",` +
				`"session":1,"seq":3,"at_us":1767323100000001,"lang":"en","team_queue":false,"priority":0,` +
				`"queued_at_us":1767323095000001,"operator_id":"sim-op-0001","offer_id":"offer-000001","wait_us":0}`,
		},
		{
			name: "sla missed",
			in:   validRouting(TypeSLAMissed),
			want: `{"type":"sla_missed","company_id":"` + testCompany + `","customer_id":"` + testCustomer + `",` +
				`"session":1,"seq":3,"at_us":1767323100000001,"lang":"en","team_queue":false,"priority":1,` +
				`"queued_at_us":1767323095000001,` +
				`"sla_target":"assignment","measured_us":61000000,"target_us":60000000}`,
		},
		{
			name: "team formed",
			in:   validRouting(TypeTeamFormed),
			want: `{"type":"team_formed","company_id":"` + testCompany + `","seq":1,"at_us":1767323100000001,` +
				`"members":["sim-op-0001","sim-op-0002"],"active_load":20}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := MarshalRouting(tt.in)
			if err != nil {
				t.Fatalf("MarshalRouting() error = %v", err)
			}
			if string(b) != tt.want {
				t.Errorf("MarshalRouting() =\n%s\nwant\n%s", b, tt.want)
			}
		})
	}
}

func TestRoutingValidate(t *testing.T) {
	tests := []struct {
		name   string
		typ    RoutingType
		change func(r *Routing)
		fields []string
	}{
		{name: "zero value", typ: TypeQueued, change: func(r *Routing) { *r = Routing{} }, fields: []string{
			"type", "company_id", "seq", "at_us",
		}},
		{name: "conversation without key fields", typ: TypeQueued, change: func(r *Routing) {
			r.CustomerID, r.Session, r.Lang, r.QueuedAt = "", 0, "", time.Time{}
		}, fields: []string{"customer_id", "session", "lang", "queued_at_us"}},
		{name: "company is customer", typ: TypeQueued, change: func(r *Routing) { r.CustomerID = testCompany }, fields: []string{"customer_id"}},
		{name: "bad lang", typ: TypeQueued, change: func(r *Routing) { r.Lang = "EN" }, fields: []string{"lang"}},
		{name: "queued after at", typ: TypeQueued, change: func(r *Routing) { r.QueuedAt = r.At.Add(time.Second) }, fields: []string{"queued_at_us"}},
		{name: "queued without reason", typ: TypeQueued, change: func(r *Routing) { r.QueueReason = "" }, fields: []string{"queue_reason"}},
		{name: "queued with unknown reason", typ: TypeQueued, change: func(r *Routing) { r.QueueReason = "bored" }, fields: []string{"queue_reason"}},
		{name: "queued with operator", typ: TypeQueued, change: func(r *Routing) { r.OperatorID = testOperator }, fields: []string{"operator_id and offer_id"}},
		{name: "offered without offer", typ: TypeOffered, change: func(r *Routing) { r.OperatorID, r.OfferID = "", "" }, fields: []string{"operator_id", "offer_id"}},
		{name: "offered with bad IDs", typ: TypeOffered, change: func(r *Routing) { r.OperatorID, r.OfferID = "op/1", strings.Repeat("x", 65) }, fields: []string{"operator_id", "offer_id"}},
		{name: "offered without deadline", typ: TypeOffered, change: func(r *Routing) { r.AcceptBy = time.Time{} }, fields: []string{"accept_by_us"}},
		{name: "deadline before at", typ: TypeOffered, change: func(r *Routing) { r.AcceptBy = r.At.Add(-time.Second) }, fields: []string{"accept_by_us"}},
		{name: "deadline on accepted", typ: TypeAccepted, change: func(r *Routing) { r.AcceptBy = r.At }, fields: []string{"accept_by_us"}},
		{name: "negative wait", typ: TypeAccepted, change: func(r *Routing) { r.Wait = -time.Second }, fields: []string{"wait_us"}},
		{name: "wait on offered", typ: TypeOffered, change: func(r *Routing) { r.Wait = time.Second }, fields: []string{"wait_us"}},
		{name: "expired without reason", typ: TypeOfferExpired, change: func(r *Routing) { r.OfferEndReason = "" }, fields: []string{"offer_end_reason"}},
		{name: "expired with unknown reason", typ: TypeOfferExpired, change: func(r *Routing) { r.OfferEndReason = "lost" }, fields: []string{"offer_end_reason"}},
		{name: "replied without operator", typ: TypeReplied, change: func(r *Routing) { r.OperatorID = "" }, fields: []string{"operator_id"}},
		{name: "later reply with response time", typ: TypeReplied, change: func(r *Routing) { r.FirstReply = false }, fields: []string{"response_time_us"}},
		{name: "negative response time", typ: TypeReplied, change: func(r *Routing) { r.ResponseTime = -time.Second }, fields: []string{"response_time_us"}},
		{name: "reply fields on resolved", typ: TypeResolved, change: func(r *Routing) { r.FirstReply = true }, fields: []string{"first_reply"}},
		{name: "sla without target", typ: TypeSLAMissed, change: func(r *Routing) { r.SLATarget, r.Target = "", 0 }, fields: []string{"sla_target", "target_us"}},
		{name: "sla not missed", typ: TypeSLAMissed, change: func(r *Routing) { r.Measured = 59 * time.Second }, fields: []string{"measured_us"}},
		{name: "sla with unknown target", typ: TypeSLAMissed, change: func(r *Routing) { r.SLATarget = "mood" }, fields: []string{"sla_target"}},
		{name: "sla with half an offer", typ: TypeSLAMissed, change: func(r *Routing) { r.OperatorID = testOperator }, fields: []string{"offer_id"}},
		{name: "ended without reason", typ: TypeEnded, change: func(r *Routing) { r.EndReason = "" }, fields: []string{"end_reason"}},
		{name: "ended with unknown reason", typ: TypeEnded, change: func(r *Routing) { r.EndReason = "tired" }, fields: []string{"end_reason"}},
		{name: "members on conversation event", typ: TypeEnded, change: func(r *Routing) { r.Members = []string{testOperator} }, fields: []string{"members"}},
		{name: "team with conversation fields", typ: TypeTeamFormed, change: func(r *Routing) {
			r.CustomerID, r.Session, r.Lang, r.TeamQueue, r.Priority = testCustomer, 1, "en", true, 1
			r.OperatorID, r.OfferID = testOperator, testOffer
		}, fields: []string{"customer_id", "session", "lang", "team_queue", "priority", "operator_id and offer_id"}},
		{name: "team without members", typ: TypeTeamResized, change: func(r *Routing) { r.Members = nil }, fields: []string{"members"}},
		{name: "team with bad members", typ: TypeTeamResized, change: func(r *Routing) {
			r.Members = []string{testOperator, "", testOperator}
		}, fields: []string{"members", `"" is not`, "listed twice"}},
		{name: "released without reason", typ: TypeTeamReleased, change: func(r *Routing) { r.ReleaseReason = "" }, fields: []string{"release_reason"}},
		{name: "released with unknown reason", typ: TypeTeamReleased, change: func(r *Routing) { r.ReleaseReason = "bored" }, fields: []string{"release_reason"}},
		{name: "release reason on formed", typ: TypeTeamFormed, change: func(r *Routing) { r.ReleaseReason = TeamLowLoad }, fields: []string{"release_reason"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := validRouting(tt.typ)
			tt.change(&r)
			assertFieldErrors(t, r.Validate(), tt.fields...)
			_, err := MarshalRouting(r)
			assertFieldErrors(t, err, tt.fields...)
		})
	}

	t.Run("every reason is valid", func(t *testing.T) {
		for _, reason := range []QueueReason{QueueNew, QueueReopened, QueueCustomerMessage, QueueOfferExpired,
			QueueOfferRefused, QueueOperatorOffline, QueueTeamFormed, QueueTeamReleased, QueueCatchUp} {
			r := validRouting(TypeQueued)
			r.QueueReason = reason
			if err := r.Validate(); err != nil {
				t.Errorf("queue reason %q: Validate() error = %v", reason, err)
			}
		}
		for _, reason := range []OfferEndReason{OfferTimeout, OfferRefused, OfferOperatorOffline} {
			r := validRouting(TypeOfferExpired)
			r.OfferEndReason = reason
			if err := r.Validate(); err != nil {
				t.Errorf("offer end reason %q: Validate() error = %v", reason, err)
			}
		}
		for _, reason := range []CloseReason{CloseInactivity, CloseCustomerRemoved, CloseCompanyRemoved} {
			r := validRouting(TypeEnded)
			r.EndReason = reason
			if err := r.Validate(); err != nil {
				t.Errorf("end reason %q: Validate() error = %v", reason, err)
			}
		}
	})
}

func TestUnmarshalRouting(t *testing.T) {
	resolved := `{"type":"resolved","company_id":"a","customer_id":"b","session":2,"seq":9,` +
		`"at_us":1767323100000001,"lang":"und","operator_id":"op-1","offer_id":"7"`

	t.Run("unknown fields are ignored", func(t *testing.T) {
		r, err := UnmarshalRouting([]byte(resolved + `,"handled_by":"x"}`))
		if err != nil {
			t.Fatalf("UnmarshalRouting() error = %v", err)
		}
		if r.Type != TypeResolved || r.Session != 2 || r.Seq != 9 || r.OfferID != "7" {
			t.Errorf("UnmarshalRouting() = %+v", r)
		}
	})
	t.Run("unknown type wraps ErrUnknownKind", func(t *testing.T) {
		_, err := UnmarshalRouting([]byte(`{"type":"transferred"}`))
		if !errors.Is(err, ErrUnknownKind) {
			t.Errorf("UnmarshalRouting() error = %v, want ErrUnknownKind", err)
		}
	})

	bad := []struct {
		name   string
		value  string
		fields []string
	}{
		{name: "empty", value: ""},
		{name: "null", value: `null`, fields: []string{"type", "company_id", "seq", "at_us"}},
		{name: "accepted without wait", value: `{"type":"accepted","company_id":"a","customer_id":"b","session":1,"seq":3,` +
			`"at_us":10,"lang":"en","queued_at_us":5,"operator_id":"op-1","offer_id":"7"}`, fields: []string{"wait_us"}},
		{name: "first reply without response time", value: `{"type":"replied","company_id":"a","customer_id":"b","session":1,"seq":4,` +
			`"at_us":10,"lang":"en","operator_id":"op-1","offer_id":"7","first_reply":true}`, fields: []string{"response_time_us"}},
		{name: "wait on resolved", value: resolved + `,"wait_us":5}`, fields: []string{"wait_us"}},
		{name: "priority on team event", value: `{"type":"team_formed","company_id":"a","seq":1,"at_us":10,` +
			`"members":["op-1"],"active_load":20,"priority":2,"team_queue":true}`, fields: []string{"priority", "team_queue"}},
		{name: "negative seq", value: resolved + `,"seq":-1}`},
	}
	for _, tt := range bad {
		t.Run(tt.name, func(t *testing.T) {
			_, err := UnmarshalRouting([]byte(tt.value))
			assertFieldErrors(t, err, tt.fields...)
			if errors.Is(err, ErrUnknownKind) {
				t.Errorf("error %v wraps ErrUnknownKind, want a plain error", err)
			}
		})
	}
}

func TestValidRoutingID(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"sim-op-0001", true},
		{"offer_12.3", true},
		{"7", true},
		{strings.Repeat("a", MaxRoutingIDLen), true},
		{"", false},
		{strings.Repeat("a", MaxRoutingIDLen+1), false},
		{"op/1", false},
		{"op 1", false},
		{"did:plc:x", false},
		{"op%2F", false},
	}
	for _, tt := range tests {
		if got := ValidRoutingID(tt.in); got != tt.want {
			t.Errorf("ValidRoutingID(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}
