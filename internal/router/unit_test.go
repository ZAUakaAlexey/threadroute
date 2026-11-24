package router

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgdb"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Config)
		errs []string
	}{
		{name: "default", edit: func(*Config) {}},
		{name: "zero", edit: func(c *Config) { *c = Config{} }, errs: []string{
			"speed factor", "capacity 0", "accept timeout", "assignment target", "first response target",
			"team release time", "team release load", "team min size",
		}},
		{name: "thresholds equal", edit: func(c *Config) { c.TeamFormLoad = c.TeamReleaseLoad }, errs: []string{"not above"}},
		{name: "sizes swapped", edit: func(c *Config) { c.TeamMinSize, c.TeamMaxSize = 5, 4 }, errs: []string{"max size"}},
		{name: "sub-microsecond", edit: func(c *Config) { c.AcceptTimeout = 1500 * time.Nanosecond }, errs: []string{"microseconds"}},
		{name: "capacity too big", edit: func(c *Config) { c.Capacity = opapi.MaxCapacity + 1 }, errs: []string{"capacity"}},
		{name: "factor too big", edit: func(c *Config) { c.SpeedFactor = 1e6 }, errs: []string{"speed factor"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := DefaultConfig()
			tt.edit(&c)
			err := c.Validate()
			if len(tt.errs) == 0 {
				if err != nil {
					t.Fatalf("Validate() = %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want %v", tt.errs)
			}
			for _, w := range tt.errs {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not contain %q", err, w)
				}
			}
		})
	}
}

func TestDefaultConfigValues(t *testing.T) {
	c := DefaultConfig()
	want := Config{
		SpeedFactor: 1, Capacity: 4, AcceptTimeout: 20 * time.Second,
		AssignmentTarget: 60 * time.Second, FirstResponseTarget: 120 * time.Second,
		TeamFormLoad: 20, TeamReleaseLoad: 8, TeamReleaseAfter: 15 * time.Minute, TeamMinSize: 2, TeamMaxSize: 20,
	}
	if c != want {
		t.Errorf("DefaultConfig() = %+v, want the values of decisions 0009 and 0010 %+v", c, want)
	}
	s := c.Settings()
	if err := s.Validate(); err != nil || s.AcceptTimeout() != 20*time.Second || s.CapacityDefault != 4 {
		t.Errorf("Settings() = %+v, %v", s, err)
	}
}

func TestModelTime(t *testing.T) {
	tests := []struct {
		factor           float64
		model, machine   time.Duration
		machineToModel   time.Duration
		machineToModelIn time.Duration
	}{
		{factor: 1, model: 20 * time.Second, machine: 20 * time.Second, machineToModelIn: time.Second, machineToModel: time.Second},
		{factor: 50, model: 20 * time.Second, machine: 400 * time.Millisecond, machineToModelIn: 1200 * time.Millisecond, machineToModel: 60 * time.Second},
		{factor: 3, model: time.Second, machine: 333333 * time.Microsecond, machineToModelIn: -time.Second, machineToModel: 0},
		{factor: 10000, model: time.Microsecond, machine: time.Microsecond, machineToModelIn: time.Microsecond, machineToModel: 10 * time.Millisecond},
	}
	for _, tt := range tests {
		c := DefaultConfig()
		c.SpeedFactor = tt.factor
		if got := c.machine(tt.model); got != tt.machine {
			t.Errorf("factor %v: machine(%s) = %s, want %s", tt.factor, tt.model, got, tt.machine)
		}
		if got := c.model(tt.machineToModelIn); got != tt.machineToModel {
			t.Errorf("factor %v: model(%s) = %s, want %s", tt.factor, tt.machineToModelIn, got, tt.machineToModel)
		}
	}
}

func TestTeamSize(t *testing.T) {
	c := DefaultConfig()
	for _, tt := range []struct {
		load int64
		want int
	}{{0, 2}, {1, 2}, {8, 2}, {9, 3}, {20, 5}, {21, 6}, {80, 20}, {81, 20}, {1000, 20}} {
		if got := c.teamSize(tt.load); got != tt.want {
			t.Errorf("teamSize(%d) = %d, want %d", tt.load, got, tt.want)
		}
	}
}

// TestOutcome checks every action against every offer state, with the
// codes listed in opapi for each pattern.
func TestOutcome(t *testing.T) {
	const done, already = opapi.ResultDone, opapi.ResultAlreadyDone
	type want struct {
		res  opapi.Result
		code opapi.ErrorCode
	}
	states := []string{offerOffered, offerAccepted, offerExpired, offerRefused, offerEnded, offerResolved, offerAssignmentEnded}
	table := map[action][]want{
		actAccept: {{res: done}, {res: already}, {code: opapi.CodeOfferExpired}, {code: opapi.CodeOfferRefused},
			{code: opapi.CodeOfferEnded}, {res: already}, {res: already}},
		actRefuse: {{res: done}, {code: opapi.CodeOfferAccepted}, {code: opapi.CodeOfferExpired}, {res: already},
			{code: opapi.CodeOfferEnded}, {code: opapi.CodeOfferAccepted}, {code: opapi.CodeOfferAccepted}},
		actReply: {{code: opapi.CodeOfferNotAccepted}, {res: done}, {code: opapi.CodeOfferExpired},
			{code: opapi.CodeOfferRefused}, {code: opapi.CodeOfferEnded}, {code: opapi.CodeAssignmentEnded},
			{code: opapi.CodeAssignmentEnded}},
		actResolve: {{code: opapi.CodeOfferNotAccepted}, {res: done}, {code: opapi.CodeOfferExpired},
			{code: opapi.CodeOfferRefused}, {code: opapi.CodeOfferEnded}, {res: already}, {code: opapi.CodeAssignmentEnded}},
	}
	// The codes each pattern may answer, from the comments in opapi.
	allowed := map[action][]opapi.ErrorCode{
		actAccept:  {opapi.CodeOfferExpired, opapi.CodeOfferRefused, opapi.CodeOfferEnded},
		actRefuse:  {opapi.CodeOfferAccepted, opapi.CodeOfferExpired, opapi.CodeOfferEnded},
		actReply:   {opapi.CodeOfferNotAccepted, opapi.CodeOfferExpired, opapi.CodeOfferRefused, opapi.CodeOfferEnded, opapi.CodeAssignmentEnded},
		actResolve: {opapi.CodeOfferNotAccepted, opapi.CodeOfferExpired, opapi.CodeOfferRefused, opapi.CodeOfferEnded, opapi.CodeAssignmentEnded},
	}
	check := func(a action, state string, past, seen bool, w want) {
		t.Helper()
		res, err := outcome(a, state, past, seen)
		var ae *ActionError
		switch {
		case w.code != "":
			if !errors.As(err, &ae) || ae.Code != w.code || !ae.Code.Rejected() {
				t.Errorf("%s on %s (past %v): %q, %v; want code %s", a, state, past, res, err, w.code)
			}
			if ae != nil && !slices.Contains(allowed[a], ae.Code) {
				t.Errorf("%s answers %s, which opapi does not list for it", a, ae.Code)
			}
		case err != nil || res != w.res:
			t.Errorf("%s on %s (past %v): %q, %v; want %q", a, state, past, res, err, w.res)
		}
	}
	for a, wants := range table {
		for i, st := range states {
			check(a, st, false, false, wants[i])
		}
		// Past the accept timeout an open offer counts as expired, and an
		// answered one is not affected.
		check(a, offerOffered, true, false, want{code: opapi.CodeOfferExpired})
		check(a, offerAccepted, true, false, wants[1])
	}
	// A reply ID seen before is a repeat, whatever the state.
	for _, st := range states {
		check(actReply, st, false, true, want{res: already})
	}
	if _, err := outcome(actAccept, "bogus", false, false); err == nil || code(err) != "error" {
		t.Errorf("unknown state: %v", err)
	}
}

// code returns the ActionError code of err, "" for nil, or "error".
func code(err error) opapi.ErrorCode {
	if err == nil {
		return ""
	}
	if ae, ok := errors.AsType[*ActionError](err); ok {
		return ae.Code
	}
	return "error"
}

func convEv(typ event.ConversationType, session, seq uint64) event.Conversation {
	ev := event.Conversation{Type: typ, Session: session, Seq: seq, Direction: event.FromCustomer}
	if typ == event.TypeClosed {
		ev.CloseReason = event.CloseCustomerRemoved
		ev.Direction = ""
	}
	return ev
}

func TestPlanApply(t *testing.T) {
	st := func(session, seq int64, state string) *stored {
		return &stored{Session: session, Seq: seq, State: state}
	}
	company := func(ev event.Conversation) event.Conversation { ev.Direction = event.FromCompany; return ev }
	tests := []struct {
		name string
		cur  *stored
		ev   event.Conversation
		want applyPlan
	}{
		{"new conversation", nil, convEv(event.TypeOpened, 1, 1),
			applyPlan{startSession: true, queue: event.QueueNew}},
		{"new, later session", nil, convEv(event.TypeOpened, 3, 9),
			applyPlan{startSession: true, queue: event.QueueReopened}},
		{"new, first event is a close", nil, convEv(event.TypeClosed, 2, 7),
			applyPlan{startSession: true, closed: true}},
		{"repeat", st(1, 3, stateWaiting), convEv(event.TypeMessageAdded, 1, 3), applyPlan{skip: true}},
		{"older", st(2, 5, stateWaiting), convEv(event.TypeMessageAdded, 1, 4), applyPlan{skip: true}},
		{"older session, higher seq", st(2, 5, stateWaiting), convEv(event.TypeMessageAdded, 1, 9), applyPlan{skip: true}},
		{"reset after close", st(3, 12, stateClosed), convEv(event.TypeOpened, 1, 1),
			applyPlan{reset: true, startSession: true, queue: event.QueueNew}},
		{"1/1 while open is a repeat", st(3, 12, stateAssigned), convEv(event.TypeOpened, 1, 1), applyPlan{skip: true}},
		{"1/1 repeat of 1/1", st(1, 1, stateClosed), convEv(event.TypeOpened, 1, 1), applyPlan{skip: true}},
		{"1/1 message is no reset", st(3, 12, stateClosed), convEv(event.TypeMessageAdded, 1, 1), applyPlan{skip: true}},
		{"reopen", st(1, 4, stateClosed), convEv(event.TypeOpened, 2, 5),
			applyPlan{startSession: true, queue: event.QueueReopened, preferPrevious: true}},
		{"later session, close lost", st(1, 4, stateAssigned), convEv(event.TypeOpened, 2, 6),
			applyPlan{endSession: true, endReason: event.CloseInactivity, startSession: true,
				queue: event.QueueReopened, preferPrevious: true}},
		{"later session, closed", st(1, 4, stateWaiting), convEv(event.TypeClosed, 2, 6),
			applyPlan{endSession: true, endReason: event.CloseInactivity, startSession: true, closed: true}},
		{"close", st(1, 4, stateOffered), convEv(event.TypeClosed, 1, 5),
			applyPlan{endSession: true, endReason: event.CloseCustomerRemoved}},
		{"close twice", st(1, 5, stateClosed), convEv(event.TypeClosed, 1, 6), applyPlan{}},
		{"customer message after resolve", st(1, 4, stateResolved), convEv(event.TypeMessageAdded, 1, 5),
			applyPlan{queue: event.QueueCustomerMessage, preferPrevious: true}},
		{"company message after resolve", st(1, 4, stateResolved), company(convEv(event.TypeMessageAdded, 1, 5)), applyPlan{}},
		{"customer message while assigned", st(1, 4, stateAssigned), convEv(event.TypeMessageAdded, 1, 5), applyPlan{}},
		{"customer message while pending", st(1, 4, statePending), convEv(event.TypeMessageAdded, 1, 5), applyPlan{}},
		{"answered", st(1, 4, stateAssigned), company(convEv(event.TypeAnswered, 1, 5)), applyPlan{realResponse: true}},
		{"answered in a new conversation", nil, company(convEv(event.TypeAnswered, 1, 2)),
			applyPlan{startSession: true, queue: event.QueueNew, realResponse: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := planApply(tt.cur, tt.ev); got != tt.want {
				t.Errorf("planApply() = %+v\nwant          %+v", got, tt.want)
			}
		})
	}
}

func TestPickJoin(t *testing.T) {
	op := func(id string, live int32, langs ...string) opInfo { return opInfo{ID: id, Langs: langs, Live: live} }
	cands := []opInfo{
		op("a", 0, "en"), op("b", 0, "en", "ja"), op("c", 0, "en"), op("d", 0, "ja"),
		op("e", 1, "fr"), op("f", 2, "en"),
	}
	tests := []struct {
		name    string
		loads   []langLoad
		k       int
		members []opInfo
		want    []string
	}{
		{"most needed language first, one per language per round",
			[]langLoad{{"en", 9}, {"ja", 2}, {"fr", 1}}, 4, nil, []string{"a", "b", "e", "c"}},
		{"a candidate with two needed languages covers both",
			[]langLoad{{"ja", 2}, {"en", 1}}, 2, nil, []string{"b", "a"}},
		{"members already cover a language",
			[]langLoad{{"en", 4}, {"ja", 4}}, 1, []opInfo{op("m", 0, "en")}, []string{"b"}},
		{"fill up when the needs are met",
			[]langLoad{{"fr", 1}}, 3, nil, []string{"e", "a", "b"}},
		{"no candidates", []langLoad{{"en", 4}}, 2, nil, nil},
		{"k is 0", []langLoad{{"en", 40}}, 0, nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := cands
			if tt.name == "no candidates" {
				c = nil
			}
			if got := pickJoin(tt.loads, 4, tt.k, tt.members, c); !slices.Equal(got, tt.want) {
				t.Errorf("pickJoin() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPickLeave(t *testing.T) {
	op := func(id string, live int32, langs ...string) opInfo { return opInfo{ID: id, Langs: langs, Live: live} }
	members := []opInfo{op("a", 3, "en"), op("b", 0, "en"), op("c", 1, "ja"), op("d", 0, "en", "fr"), op("e", 0, "de")}
	loads := []langLoad{{"en", 5}, {"ja", 1}, {"fr", 1}}
	tests := []struct {
		k    int
		want []string
	}{
		{1, []string{"b"}},
		{2, []string{"b", "e"}},
		{3, []string{"b", "e", "a"}},
		{5, []string{"b", "e", "a", "c", "d"}},
	}
	for _, tt := range tests {
		if got := pickLeave(loads, tt.k, members); !slices.Equal(got, tt.want) {
			t.Errorf("pickLeave(k=%d) = %v, want %v", tt.k, got, tt.want)
		}
	}
}

func TestOutboxSequence(t *testing.T) {
	at := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	c := &pgdb.Conversation{CompanyID: "did:plc:testco", CustomerID: "did:plc:testcu", ConvSession: 2, Lang: "en", RouteSeq: 7}
	var ob outbox
	q := convEvent(event.TypeQueued, at)
	q.QueuedAt, q.QueueReason = at, event.QueueReopened
	if err := ob.addConv(c, q); err != nil {
		t.Fatal(err)
	}
	if err := ob.addConv(c, convEvent(event.TypeEnded, at)); err == nil {
		t.Fatal("an ended event without a reason was accepted")
	}
	co := &pgdb.Company{CompanyID: "did:plc:testco"}
	if err := ob.addTeam(co, event.Routing{Type: event.TypeTeamFormed, At: at, Members: []string{"sim-op-1"}, ActiveLoad: 20}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ob.seqs, []int64{8, 1}) || !slices.Equal(ob.families, []string{familyConversation, familyTeam}) {
		t.Errorf("outbox = %v %v", ob.seqs, ob.families)
	}
	ev, err := event.UnmarshalRouting(ob.payloads[0])
	if err != nil || ev.Session != 2 || ev.Lang != "en" || string(ob.keys[0]) != "did:plc:testco|did:plc:testcu" {
		t.Errorf("payload = %+v, %v", ev, err)
	}
}

func TestNowNeverBefore(t *testing.T) {
	clock := newFakeClock()
	s := &Service{clock: clock, cfg: DefaultConfig()}
	later := clock.Now().Add(time.Second)
	if got := s.now(nil, &later); !got.Equal(later) {
		t.Errorf("now() = %s, want %s", got, later)
	}
	clock.Advance(1500 * time.Nanosecond)
	if got := s.now(); got.Nanosecond()%1000 != 0 {
		t.Errorf("now() = %s, not whole microseconds", got)
	}
}
