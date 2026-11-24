//go:build integration

package router

import (
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// TestIntegrationRouter runs every operation on a real PostgreSQL, one
// scenario per subtest on an emptied database, and checks the invariants
// after each.
func TestIntegrationRouter(t *testing.T) {
	pool := startDB(t, 8)
	tests := []struct {
		name string
		cfg  func(*Config)
		fn   func(t *testing.T, e *env)
	}{
		{name: "open offer accept reply resolve and their repeats", fn: testHappyPath},
		{name: "unknown operator offer and bad input", fn: testNotFound},
		{name: "offer timeout then next operator with the first skipped", fn: testTimeout},
		{name: "refuse", fn: testRefuse},
		{name: "offline returns held conversations with waiting time kept", fn: testOffline},
		{name: "reopen prefers the previous operator", fn: testReopen},
		{name: "customer message after resolve", fn: testMessageAfterResolve},
		{name: "close by conv ends an offer and an assignment", fn: testClose},
		{name: "later session while the earlier is still held", fn: testLostClose},
		{name: "reset exception", fn: testReset},
		{name: "catch-up", fn: testCatchUp},
		{name: "SLA targets", fn: testSLA},
		{name: "queue order", fn: testQueueOrder},
		{name: "speed factor", cfg: func(c *Config) { c.SpeedFactor = 50 }, fn: testSpeedFactor},
		{name: "capacity cut below the load", fn: testCapacityCut},
		{name: "teams form resize release and the language rule", fn: testTeams},
		{name: "a full team queue does not hide the shared queue", cfg: func(c *Config) {
			c.Capacity, c.TeamFormLoad, c.TeamReleaseLoad, c.TeamMinSize, c.TeamMaxSize = 1, 3, 1, 1, 1
		}, fn: testFullTeamQueue},
		{name: "outbox read side", fn: testOutbox},
		{name: "counts", fn: testCounts},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			if tt.cfg != nil {
				tt.cfg(&cfg)
			}
			e := newEnv(t, pool, cfg)
			tt.fn(t, e)
			requireInvariants(t, pool)
		})
	}
}

func wantResult(t *testing.T, what string, got opapi.Result, err error, want opapi.Result) {
	t.Helper()
	if err != nil || got != want {
		t.Fatalf("%s = %q, %v; want %q", what, got, err, want)
	}
}

func wantCode(t *testing.T, what string, err error, want opapi.ErrorCode) {
	t.Helper()
	if got := code(err); got != want {
		t.Fatalf("%s error = %v (code %q), want code %q", what, err, got, want)
	}
}

func wantTypes(t *testing.T, evs []event.Routing, want string) {
	t.Helper()
	if got := types(evs); got != want {
		t.Fatalf("events = %s\nwant     %s", got, want)
	}
	for i, ev := range evs {
		if ev.Seq != uint64(i+1) {
			t.Fatalf("event %d has seq %d", i, ev.Seq)
		}
	}
}

func testHappyPath(t *testing.T, e *env) {
	ctx := t.Context()
	e.putOperator(operator(0), 0, "en")
	e.online(operator(0))
	s := newStream(company(0), customer(0), "en")
	t0 := e.clock.Now()

	ev := s.opened()
	if r := e.apply(ev, false); !r.Queued || r.Events != 1 {
		t.Fatalf("opened: %+v", r)
	}
	if r := e.apply(ev, false); !r.Skipped || r.Events != 0 {
		t.Fatalf("opened again: %+v", r)
	}
	if r := e.assign(); r.Offered != 1 {
		t.Fatalf("assign: %+v", r)
	}
	if r := e.assign(); r.Offered != 0 {
		t.Fatalf("assign again: %+v", r)
	}
	offer, op, _ := e.liveOffer(s)

	e.clock.Advance(3 * time.Second)
	res, err := e.svc.Accept(ctx, op, offer)
	wantResult(t, "accept", res, err, opapi.ResultDone)
	res, err = e.svc.Accept(ctx, op, offer)
	wantResult(t, "accept again", res, err, opapi.ResultAlreadyDone)
	_, err = e.svc.Refuse(ctx, op, offer)
	wantCode(t, "refuse after accept", err, opapi.CodeOfferAccepted)

	e.clock.Advance(5 * time.Second)
	res, err = e.svc.Reply(ctx, op, offer, "r1")
	wantResult(t, "reply", res, err, opapi.ResultDone)
	res, err = e.svc.Reply(ctx, op, offer, "r1")
	wantResult(t, "reply again", res, err, opapi.ResultAlreadyDone)
	res, err = e.svc.Reply(ctx, op, offer, "r2")
	wantResult(t, "second reply", res, err, opapi.ResultDone)

	res, err = e.svc.Resolve(ctx, op, offer)
	wantResult(t, "resolve", res, err, opapi.ResultDone)
	res, err = e.svc.Resolve(ctx, op, offer)
	wantResult(t, "resolve again", res, err, opapi.ResultAlreadyDone)
	_, err = e.svc.Reply(ctx, op, offer, "r3")
	wantCode(t, "reply after resolve", err, opapi.CodeAssignmentEnded)
	res, err = e.svc.Accept(ctx, op, offer)
	wantResult(t, "accept after resolve", res, err, opapi.ResultAlreadyDone)

	evs := e.events(s.key())
	wantTypes(t, evs, "queued(new),offered,accepted,replied,replied,resolved")
	if !evs[0].QueuedAt.Equal(t0) || evs[1].OperatorID != op || evs[1].OfferID != offer ||
		!evs[1].AcceptBy.Equal(t0.Add(20*time.Second)) {
		t.Errorf("queued/offered = %+v / %+v", evs[0], evs[1])
	}
	if evs[2].Wait != 3*time.Second {
		t.Errorf("accepted wait = %s", evs[2].Wait)
	}
	if !evs[3].FirstReply || evs[3].ResponseTime != 8*time.Second || evs[4].FirstReply {
		t.Errorf("replies = %+v / %+v", evs[3], evs[4])
	}
	if st := e.convState(s); st != stateResolved {
		t.Errorf("state = %s", st)
	}
	if o, _, _ := e.liveOffer(s); o != "" {
		t.Errorf("live offer %s after resolve", o)
	}
}

func testNotFound(t *testing.T, e *env) {
	ctx := t.Context()
	e.putOperator(operator(0), 0, "en")
	e.putOperator(operator(1), 0, "en")
	e.online(operator(0))
	s := newStream(company(0), customer(0), "en")
	e.apply(s.opened(), false)
	e.assign()
	offer, op, _ := e.liveOffer(s)

	_, err := e.svc.Accept(ctx, "sim-op-unknown", offer)
	wantCode(t, "accept by unknown operator", err, opapi.CodeOperatorNotFound)
	_, err = e.svc.Accept(ctx, op, "of-999999")
	wantCode(t, "accept unknown offer", err, opapi.CodeOfferNotFound)
	_, err = e.svc.Accept(ctx, operator(1), offer)
	wantCode(t, "accept offer of another operator", err, opapi.CodeOfferNotFound)
	_, err = e.svc.Accept(ctx, op, "bad/id")
	wantCode(t, "accept bad ID", err, opapi.CodeInvalidRequest)
	_, err = e.svc.Reply(ctx, op, offer, "")
	wantCode(t, "reply without ID", err, opapi.CodeInvalidRequest)
	_, err = e.svc.Online(ctx, "sim-op-unknown")
	wantCode(t, "online unknown", err, opapi.CodeOperatorNotFound)
	_, err = e.svc.Offline(ctx, "sim-op-unknown")
	wantCode(t, "offline unknown", err, opapi.CodeOperatorNotFound)
	_, err = e.svc.GetOperator(ctx, "sim-op-unknown")
	wantCode(t, "get unknown", err, opapi.CodeOperatorNotFound)
	_, err = e.svc.PutOperator(ctx, operator(2), opapi.OperatorRequest{})
	wantCode(t, "put without languages", err, opapi.CodeInvalidRequest)

	res, err := e.svc.Online(ctx, op)
	wantResult(t, "online again", res, err, opapi.ResultAlreadyDone)
	r, err := e.svc.PutOperator(ctx, operator(1), opapi.OperatorRequest{Languages: []string{"en"}})
	wantResult(t, "put same", r.Result, err, opapi.ResultAlreadyDone)
	r, err = e.svc.PutOperator(ctx, operator(1), opapi.OperatorRequest{Languages: []string{"und", "en"}, Capacity: 2})
	wantResult(t, "put changed", r.Result, err, opapi.ResultDone)
	want := opapi.Operator{ID: operator(1), Languages: []string{"en", "und"}, Capacity: 2, Presence: opapi.PresenceOffline}
	if !slices.Equal(r.Operator.Languages, want.Languages) || r.Operator.Capacity != 2 || r.Operator.Presence != want.Presence {
		t.Errorf("operator = %+v", r.Operator)
	}
}

func testTimeout(t *testing.T, e *env) {
	ctx := t.Context()
	a, b := operator(0), operator(1)
	for _, id := range []string{a, b} {
		e.putOperator(id, 0, "en")
		e.online(id)
	}
	s := newStream(company(0), customer(0), "en")
	t0 := e.clock.Now()
	e.apply(s.opened(), false)
	e.assign()
	offerA, op, _ := e.liveOffer(s)
	if op != a {
		t.Fatalf("first offer to %s, want %s (same load, lower ID)", op, a)
	}

	e.clock.Advance(19 * time.Second)
	if n, err := e.svc.ExpireOffers(ctx, 10); err != nil || n != 0 {
		t.Fatalf("expire before the timeout = %d, %v", n, err)
	}
	e.clock.Advance(time.Second)
	// The timeout has passed but the timer has not run: a late accept is
	// rejected and changes nothing.
	_, err := e.svc.Accept(ctx, a, offerA)
	wantCode(t, "late accept", err, opapi.CodeOfferExpired)
	if n, err := e.svc.ExpireOffers(ctx, 10); err != nil || n != 1 {
		t.Fatalf("expire = %d, %v", n, err)
	}
	if n, _ := e.svc.ExpireOffers(ctx, 10); n != 0 {
		t.Fatalf("expire again = %d", n)
	}
	_, err = e.svc.Accept(ctx, a, offerA)
	wantCode(t, "accept after expiry", err, opapi.CodeOfferExpired)
	_, err = e.svc.Refuse(ctx, a, offerA)
	wantCode(t, "refuse after expiry", err, opapi.CodeOfferExpired)

	// a has the lowest load and the lowest ID, but is skipped.
	e.assign()
	offerB, op, _ := e.liveOffer(s)
	if op != b {
		t.Fatalf("second offer to %s, want %s", op, b)
	}
	evs := e.events(s.key())
	wantTypes(t, evs, "queued(new),offered,offer_expired,queued(offer_expired),offered")
	if evs[2].OfferEndReason != event.OfferTimeout || !evs[3].QueuedAt.Equal(t0) || !evs[4].QueuedAt.Equal(t0) {
		t.Errorf("the waiting time was not kept: %+v", evs[3])
	}

	// b times out too. Every operator is skipped now, so the skips are
	// cleared and the conversation is offered again.
	e.clock.Advance(20 * time.Second)
	if n, _ := e.svc.ExpireOffers(ctx, 10); n != 1 {
		t.Fatalf("expire b = %d", n)
	}
	e.assign()
	offer3, op, _ := e.liveOffer(s)
	if op != a || offer3 == offerA || offer3 == offerB {
		t.Fatalf("third offer %s to %s, want a new offer to %s", offer3, op, a)
	}
}

func testRefuse(t *testing.T, e *env) {
	ctx := t.Context()
	a, b := operator(0), operator(1)
	for _, id := range []string{a, b} {
		e.putOperator(id, 0, "en")
		e.online(id)
	}
	s := newStream(company(0), customer(0), "en")
	e.apply(s.opened(), false)
	e.assign()
	offer, op, _ := e.liveOffer(s)
	res, err := e.svc.Refuse(ctx, op, offer)
	wantResult(t, "refuse", res, err, opapi.ResultDone)
	res, err = e.svc.Refuse(ctx, op, offer)
	wantResult(t, "refuse again", res, err, opapi.ResultAlreadyDone)
	_, err = e.svc.Accept(ctx, op, offer)
	wantCode(t, "accept after refuse", err, opapi.CodeOfferRefused)
	_, err = e.svc.Resolve(ctx, op, offer)
	wantCode(t, "resolve after refuse", err, opapi.CodeOfferRefused)
	if st := e.convState(s); st != stateWaiting {
		t.Fatalf("state = %s", st)
	}
	e.assign()
	if _, op2, _ := e.liveOffer(s); op2 != b {
		t.Fatalf("offer after refuse to %s, want %s", op2, b)
	}
	evs := e.events(s.key())
	wantTypes(t, evs, "queued(new),offered,offer_expired,queued(offer_refused),offered")
	if evs[2].OfferEndReason != event.OfferRefused {
		t.Errorf("end reason = %s", evs[2].OfferEndReason)
	}
}

func testOffline(t *testing.T, e *env) {
	ctx := t.Context()
	a, b := operator(0), operator(1)
	e.putOperator(a, 0, "en")
	e.putOperator(b, 0, "en")
	e.online(a)
	s1 := newStream(company(0), customer(1), "en")
	s2 := newStream(company(0), customer(2), "en")
	t0 := e.clock.Now()
	e.apply(s1.opened(), false)
	e.apply(s2.opened(), false)
	if r := e.assign(); r.Offered != 2 {
		t.Fatalf("assign = %+v", r)
	}
	held, _, _ := e.liveOffer(s1)
	offered, _, _ := e.liveOffer(s2)
	if _, err := e.svc.Accept(ctx, a, held); err != nil {
		t.Fatal(err)
	}

	e.clock.Advance(10 * time.Second)
	res, err := e.svc.Offline(ctx, a)
	wantResult(t, "offline", res, err, opapi.ResultDone)
	res, err = e.svc.Offline(ctx, a)
	wantResult(t, "offline again", res, err, opapi.ResultAlreadyDone)
	for _, s := range []*convStream{s1, s2} {
		if st := e.convState(s); st != stateWaiting {
			t.Fatalf("%s state = %s", s.customer, st)
		}
	}
	_, err = e.svc.Resolve(ctx, a, held)
	wantCode(t, "resolve after offline", err, opapi.CodeAssignmentEnded)
	_, err = e.svc.Accept(ctx, a, offered)
	wantCode(t, "accept after offline", err, opapi.CodeOfferEnded)
	_, err = e.svc.Reply(ctx, a, held, "r1")
	wantCode(t, "reply after offline", err, opapi.CodeAssignmentEnded)

	ev1 := e.events(s1.key())
	wantTypes(t, ev1, "queued(new),offered,accepted,queued(operator_offline)")
	ev2 := e.events(s2.key())
	wantTypes(t, ev2, "queued(new),offered,offer_expired,queued(operator_offline)")
	if !ev1[3].QueuedAt.Equal(t0) || !ev2[3].QueuedAt.Equal(t0) || ev2[2].OfferEndReason != event.OfferOperatorOffline {
		t.Errorf("waiting time not kept: %+v / %+v", ev1[3], ev2[3])
	}
	var skips int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM offer_skips`).Scan(&skips); err != nil || skips != 0 {
		t.Errorf("skips after offline = %d, %v; want 0", skips, err)
	}

	e.online(b)
	e.assign()
	o, op, _ := e.liveOffer(s1)
	if op != b {
		t.Fatalf("s1 offered to %s", op)
	}
	e.clock.Advance(2 * time.Second)
	if _, err := e.svc.Accept(ctx, b, o); err != nil {
		t.Fatal(err)
	}
	ev1 = e.events(s1.key())
	if w := ev1[len(ev1)-1].Wait; w != 12*time.Second {
		t.Errorf("wait after offline = %s, want 12s (the waiting time continues from the first queue entry)", w)
	}
}

// busy gives the operator one assigned conversation of another customer.
func (e *env) busy(op string, i int) {
	e.t.Helper()
	s := newStream(company(9), customer(900+i), "en")
	e.apply(s.opened(), false)
	e.assign()
	o, got, _ := e.liveOffer(s)
	if got != op {
		e.t.Fatalf("busy conversation offered to %s, want %s", got, op)
	}
	if _, err := e.svc.Accept(e.t.Context(), op, o); err != nil {
		e.t.Fatal(err)
	}
}

func testReopen(t *testing.T, e *env) {
	ctx := t.Context()
	a, b := operator(0), operator(1)
	e.putOperator(a, 0, "en")
	e.putOperator(b, 0, "en")
	e.online(a)
	s := newStream(company(0), customer(0), "en")
	e.apply(s.opened(), false)
	e.assign()
	o, _, _ := e.liveOffer(s)
	if _, err := e.svc.Accept(ctx, a, o); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Resolve(ctx, a, o); err != nil {
		t.Fatal(err)
	}
	e.busy(a, 1) // a has load 1 now, b has 0
	e.online(b)
	e.apply(s.closed(), false)
	r := e.apply(s.opened(), false)
	if !r.Queued {
		t.Fatalf("reopen: %+v", r)
	}
	e.assign()
	if _, op, _ := e.liveOffer(s); op != a {
		t.Fatalf("reopened conversation offered to %s, want the previous operator %s", op, a)
	}
	evs := e.events(s.key())
	wantTypes(t, evs, "queued(new),offered,accepted,resolved,ended,queued(reopened),offered")
	if evs[4].OperatorID != "" || evs[5].Session != 2 {
		t.Errorf("ended = %+v, queued = %+v", evs[4], evs[5])
	}

	// Not online: the preference gives way to the other operator.
	s2 := newStream(company(0), customer(1), "en")
	e.apply(s2.opened(), false)
	e.assign()
	o2, op2, _ := e.liveOffer(s2)
	if _, err := e.svc.Accept(ctx, op2, o2); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Resolve(ctx, op2, o2); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Offline(ctx, op2); err != nil {
		t.Fatal(err)
	}
	e.apply(s2.closed(), false)
	e.apply(s2.opened(), false)
	e.assign()
	if _, op, _ := e.liveOffer(s2); op == op2 || op == "" {
		t.Fatalf("reopen with the previous operator offline offered to %q", op)
	}
}

func testMessageAfterResolve(t *testing.T, e *env) {
	ctx := t.Context()
	a, b := operator(0), operator(1)
	e.putOperator(a, 0, "en")
	e.putOperator(b, 0, "en")
	e.online(a)
	s := newStream(company(0), customer(0), "en")
	t0 := e.clock.Now()
	e.apply(s.opened(), false)
	e.assign()
	o, _, _ := e.liveOffer(s)
	e.svc.Accept(ctx, a, o)
	e.svc.Resolve(ctx, a, o)
	e.busy(a, 1)
	e.online(b)

	e.clock.Advance(30 * time.Second)
	if r := e.apply(s.message(event.FromCompany), false); r.Queued {
		t.Fatal("a company message after resolve queued the conversation")
	}
	if r := e.apply(s.message(event.FromCustomer), false); !r.Queued {
		t.Fatalf("customer message after resolve: %+v", r)
	}
	e.assign()
	if _, op, _ := e.liveOffer(s); op != a {
		t.Fatalf("offered to %s, want the same operator %s", op, a)
	}
	evs := e.events(s.key())
	wantTypes(t, evs, "queued(new),offered,accepted,resolved,queued(customer_message_after_resolve),offered")
	if !evs[4].QueuedAt.Equal(t0.Add(30 * time.Second)) {
		t.Errorf("queued at %s, want a new waiting time", evs[4].QueuedAt)
	}
	var start time.Time
	e.pool.QueryRow(ctx, `SELECT sla_started_at FROM conversations WHERE customer_id = $1`, s.customer).Scan(&start)
	if !start.Equal(t0) {
		t.Errorf("SLA start = %s, want the first queue entry %s", start, t0)
	}
}

func testClose(t *testing.T, e *env) {
	ctx := t.Context()
	a := operator(0)
	e.putOperator(a, 0, "en")
	e.online(a)
	s1 := newStream(company(0), customer(1), "en")
	s2 := newStream(company(0), customer(2), "en")
	e.apply(s1.opened(), false)
	e.apply(s2.opened(), false)
	e.assign()
	offered, _, _ := e.liveOffer(s1)
	held, _, _ := e.liveOffer(s2)
	e.svc.Accept(ctx, a, held)

	closeEv := s1.closed()
	if r := e.apply(closeEv, false); !r.Ended {
		t.Fatalf("close: %+v", r)
	}
	if r := e.apply(closeEv, false); !r.Skipped {
		t.Fatalf("close again: %+v", r)
	}
	e.apply(s2.closed(), false)
	_, err := e.svc.Accept(ctx, a, offered)
	wantCode(t, "accept after close", err, opapi.CodeOfferEnded)
	_, err = e.svc.Resolve(ctx, a, held)
	wantCode(t, "resolve after close", err, opapi.CodeAssignmentEnded)
	_, err = e.svc.Reply(ctx, a, held, "r1")
	wantCode(t, "reply after close", err, opapi.CodeAssignmentEnded)

	ev1 := e.events(s1.key())
	wantTypes(t, ev1, "queued(new),offered,ended")
	ev2 := e.events(s2.key())
	wantTypes(t, ev2, "queued(new),offered,accepted,ended")
	if ev1[2].OfferID != offered || ev2[3].OfferID != held || ev2[3].EndReason != event.CloseInactivity {
		t.Errorf("ended = %+v / %+v", ev1[2], ev2[3])
	}
	var live int
	e.pool.QueryRow(ctx, `SELECT count(*) FROM offers WHERE state IN ('offered', 'accepted')`).Scan(&live)
	if live != 0 {
		t.Errorf("live slots after close = %d", live)
	}
}

func testLostClose(t *testing.T, e *env) {
	ctx := t.Context()
	a := operator(0)
	e.putOperator(a, 0, "en")
	e.online(a)
	s := newStream(company(0), customer(0), "en")
	e.apply(s.opened(), false)
	e.assign()
	o, _, _ := e.liveOffer(s)
	e.svc.Accept(ctx, a, o)
	s.closed() // conv closed the session, but the router never gets the event
	r := e.apply(s.opened(), false)
	if !r.Ended || !r.Queued {
		t.Fatalf("later session: %+v", r)
	}
	evs := e.events(s.key())
	wantTypes(t, evs, "queued(new),offered,accepted,ended,queued(reopened)")
	if evs[3].Session != 1 || evs[3].OfferID != o || evs[4].Session != 2 {
		t.Errorf("ended %+v queued %+v", evs[3], evs[4])
	}
	e.assign()
	if _, op, _ := e.liveOffer(s); op != a {
		t.Errorf("offered to %s, want the previous operator", op)
	}
}

func testReset(t *testing.T, e *env) {
	s := newStream(company(0), customer(0), "en")
	e.apply(s.opened(), false)
	e.apply(s.message(event.FromCustomer), false)
	e.apply(s.closed(), false)

	// conv forgot the conversation and starts again at 1/1.
	s.forget()
	r := e.apply(s.opened(), false)
	if !r.Reset || !r.Queued {
		t.Fatalf("opened 1/1 after close: %+v", r)
	}
	// While the conversation is open, 1/1 is a repeat and is skipped.
	s.forget()
	if r := e.apply(s.opened(), false); !r.Skipped || r.Reset {
		t.Fatalf("opened 1/1 while open: %+v", r)
	}
	// The routing sequence does not restart.
	wantTypes(t, e.events(s.key()), "queued(new),ended,queued(new)")
}

func testCatchUp(t *testing.T, e *env) {
	ctx := t.Context()
	e.putOperator(operator(0), 0, "en")
	e.online(operator(0))
	short := newStream(company(0), customer(1), "en")
	long := newStream(company(0), customer(2), "en")
	e.apply(short.opened(), true)
	if r := e.apply(short.closed(), true); r.Ended || r.Events != 0 {
		t.Fatalf("close during catch-up: %+v", r)
	}
	if r := e.apply(long.opened(), true); !r.Pending || r.Queued || r.Events != 0 {
		t.Fatalf("open during catch-up: %+v", r)
	}
	if r := e.assign(); r.Offered != 0 {
		t.Fatalf("assign during catch-up offered %d", r.Offered)
	}
	e.clock.Advance(time.Hour)
	t1 := e.clock.Now()
	n, err := e.svc.EndCatchUp(ctx, 1)
	if err != nil || n != 1 {
		t.Fatalf("EndCatchUp = %d, %v", n, err)
	}
	if n, _ := e.svc.EndCatchUp(ctx, 1); n != 0 {
		t.Fatalf("EndCatchUp again = %d", n)
	}
	if evs := e.events(short.key()); len(evs) != 0 {
		t.Fatalf("a conversation opened and closed in the backlog has events %s", types(evs))
	}
	evs := e.events(long.key())
	wantTypes(t, evs, "queued(catch_up)")
	if !evs[0].QueuedAt.Equal(t1) {
		t.Errorf("queued at %s, want the end of the catch-up %s", evs[0].QueuedAt, t1)
	}
	// The SLA targets start at the end of the catch-up, not at open.
	e.clock.Advance(59 * time.Second)
	if n, _ := e.svc.CheckSLA(ctx, 10); n != 0 {
		t.Errorf("SLA misses 59s after the catch-up = %d", n)
	}
	if r := e.assign(); r.Offered != 1 {
		t.Errorf("assign after catch-up = %+v", r)
	}
}

func testSLA(t *testing.T, e *env) {
	ctx := t.Context()
	a := operator(0)
	e.putOperator(a, 0, "en")
	s := newStream(company(0), customer(0), "en")
	e.apply(s.opened(), false)
	e.clock.Advance(59 * time.Second)
	if n, _ := e.svc.CheckSLA(ctx, 10); n != 0 {
		t.Fatalf("misses at 59s = %d", n)
	}
	e.clock.Advance(time.Second)
	if n, err := e.svc.CheckSLA(ctx, 10); err != nil || n != 1 {
		t.Fatalf("misses at 60s = %d, %v", n, err)
	}
	if n, _ := e.svc.CheckSLA(ctx, 10); n != 0 {
		t.Fatalf("misses again = %d", n)
	}
	e.online(a)
	e.assign()
	o, _, _ := e.liveOffer(s)
	e.svc.Accept(ctx, a, o)
	e.clock.Advance(60 * time.Second)
	if n, _ := e.svc.CheckSLA(ctx, 10); n != 1 {
		t.Fatalf("first response misses at 120s = %d", n)
	}
	if n, _ := e.svc.CheckSLA(ctx, 10); n != 0 {
		t.Fatalf("misses again = %d", n)
	}
	e.clock.Advance(time.Second)
	e.svc.Reply(ctx, a, o, "r1")

	evs := e.events(s.key())
	wantTypes(t, evs, "queued(new),sla_missed,offered,accepted,sla_missed,replied")
	m1, m2 := evs[1], evs[4]
	if m1.SLATarget != event.SLAAssignment || m1.Measured != 60*time.Second || m1.Target != 60*time.Second || m1.Priority != 1 {
		t.Errorf("assignment miss = %+v", m1)
	}
	if m2.SLATarget != event.SLAFirstResponse || m2.Measured != 120*time.Second || m2.Priority != 2 {
		t.Errorf("first response miss = %+v", m2)
	}
	if evs[5].ResponseTime != 121*time.Second || evs[5].Priority != 2 {
		t.Errorf("replied = %+v", evs[5])
	}
}

func testQueueOrder(t *testing.T, e *env) {
	ctx := t.Context()
	a := operator(0)
	e.putOperator(a, 1, "en")
	older := newStream(company(0), customer(1), "en")
	newer := newStream(company(0), customer(2), "en")
	urgent := newStream(company(0), customer(3), "en")
	nobody := newStream(company(0), customer(4), "pt")
	e.apply(nobody.opened(), false)
	e.clock.Advance(time.Second)
	e.apply(older.opened(), false)
	e.clock.Advance(time.Second)
	e.apply(newer.opened(), false)
	e.clock.Advance(time.Second)
	e.apply(urgent.opened(), false)
	if _, err := e.pool.Exec(ctx, `UPDATE conversations SET priority = 1 WHERE customer_id = $1`, urgent.customer); err != nil {
		t.Fatal(err)
	}
	e.online(a)
	var order []string
	for range 3 {
		e.assign()
		for _, s := range []*convStream{older, newer, urgent} {
			if o, _, st := e.liveOffer(s); st == offerOffered {
				order = append(order, s.customer)
				e.svc.Accept(ctx, a, o)
				e.svc.Resolve(ctx, a, o)
			}
		}
	}
	// Priority first, then waiting time. The pt conversation, which no
	// operator can serve, waits longest and blocks nobody.
	want := []string{urgent.customer, older.customer, newer.customer}
	if !slices.Equal(order, want) {
		t.Errorf("order = %v, want %v", order, want)
	}
	if st := e.convState(nobody); st != stateWaiting {
		t.Errorf("pt conversation is %s", st)
	}
}

func testSpeedFactor(t *testing.T, e *env) {
	ctx := t.Context()
	a := operator(0)
	e.putOperator(a, 0, "en")
	e.online(a)
	s := newStream(company(0), customer(0), "en")
	t0 := e.clock.Now()
	e.apply(s.opened(), false)
	e.assign()
	o, _, _ := e.liveOffer(s)
	e.clock.Advance(time.Second)
	e.svc.Accept(ctx, a, o)
	evs := e.events(s.key())
	if want := t0.Add(400 * time.Millisecond); !evs[1].AcceptBy.Equal(want) {
		t.Errorf("accept by %s, want %s (20s model time at factor 50)", evs[1].AcceptBy, want)
	}
	// The accept came after the machine deadline, so it was rejected.
	if len(evs) != 2 {
		t.Fatalf("events %s", types(evs))
	}
	e.svc.ExpireOffers(ctx, 10)
	e.assign() // the skips are cleared: a is the only operator
	o, _, _ = e.liveOffer(s)
	e.clock.Advance(200 * time.Millisecond)
	if _, err := e.svc.Accept(ctx, a, o); err != nil {
		t.Fatal(err)
	}
	evs = e.events(s.key())
	if w := evs[len(evs)-1].Wait; w != 60*time.Second {
		t.Errorf("wait = %s, want 60s model time for 1.2s machine time", w)
	}
	if got := e.svc.Settings(); got.SpeedFactor != 50 || got.AcceptTimeoutUS != 20_000_000 || got.CapacityDefault != 4 {
		t.Errorf("settings = %+v", got)
	}
}

func testCapacityCut(t *testing.T, e *env) {
	ctx := t.Context()
	a := operator(0)
	e.putOperator(a, 3, "en")
	e.online(a)
	var streams []*convStream
	for i := range 4 {
		s := newStream(company(0), customer(i), "en")
		streams = append(streams, s)
		e.apply(s.opened(), false)
	}
	if r := e.assign(); r.Offered != 3 {
		t.Fatalf("assign = %+v", r)
	}
	e.putOperator(a, 1, "en")
	resolveFirst := func() {
		for _, s := range streams {
			if o, _, st := e.liveOffer(s); st == offerOffered {
				e.svc.Accept(ctx, a, o)
				e.svc.Resolve(ctx, a, o)
				return
			}
		}
	}
	for want := range []int{0, 0, 1} {
		resolveFirst()
		r := e.assign()
		if (r.Offered == 1) != (want == 2) {
			t.Fatalf("after %d resolves offered %d", want+1, r.Offered)
		}
	}
}

// members returns the sorted members of the team of a company.
func (e *env) members(co string) []string {
	e.t.Helper()
	rows, err := e.pool.Query(e.t.Context(), `SELECT id FROM operators WHERE dedicated_company_id = $1 ORDER BY id`, co)
	if err != nil {
		e.t.Fatal(err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		e.t.Fatal(err)
	}
	return ids
}

func ops(ids ...int) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = operator(id)
	}
	return out
}

func testTeams(t *testing.T, e *env) {
	ctx := t.Context()
	// Shared pool: eight en operators, two ja, and one fr that is offline
	// when the team forms.
	for i := range 8 {
		e.putOperator(operator(i), 0, "en")
	}
	e.putOperator(operator(8), 0, "ja")
	e.putOperator(operator(9), 0, "ja")
	e.putOperator(operator(10), 0, "fr")
	for i := range 10 {
		e.online(operator(i))
	}
	co := company(1)
	var streams []*convStream
	open := func(lang string) *convStream {
		s := newStream(co, customer(len(streams)), lang)
		streams = append(streams, s)
		e.apply(s.opened(), false)
		return s
	}
	// One conversation is offered to the shared pool before the team
	// forms. Its offer runs to its end.
	early := open("en")
	e.assign()
	earlyOffer, earlyOp, _ := e.liveOffer(early)
	if earlyOp != operator(0) {
		t.Fatalf("early offer to %s", earlyOp)
	}
	for range 14 {
		open("en")
	}
	for range 3 {
		open("ja")
	}
	fr := open("fr")

	// 19 active conversations: below the threshold.
	if r, err := e.svc.CheckTeams(ctx); err != nil || r.Formed != 0 {
		t.Fatalf("check at 19: %+v, %v", r, err)
	}
	open("en")
	r, err := e.svc.CheckTeams(ctx)
	if err != nil || r.Formed != 1 || r.Moved != 18 {
		t.Fatalf("check at 20: %+v, %v", r, err)
	}
	// Size ceil(20/4) = 5. Languages by need: en needs 4, ja 1, fr 1, but
	// no online pool operator has fr. Lowest load first, so operator 0,
	// which holds the early offer, is not taken.
	if got, want := e.members(co), ops(1, 2, 3, 4, 8); !slices.Equal(got, want) {
		t.Fatalf("members = %v, want %v", got, want)
	}
	if op, _ := e.svc.GetOperator(ctx, operator(1)); op.TeamCompanyID != co {
		t.Errorf("operator 1 = %+v", op)
	}
	teamEvs := e.events(event.CompanyKey(co))
	wantTypes(t, teamEvs, "team_formed")
	if teamEvs[0].ActiveLoad != 20 || !slices.Equal(teamEvs[0].Members, ops(1, 2, 3, 4, 8)) {
		t.Errorf("team_formed = %+v", teamEvs[0])
	}
	// The language rule: no member has fr, so fr stays in the shared queue.
	wantTypes(t, e.events(fr.key()), "queued(new)")
	wantTypes(t, e.events(streams[1].key()), "queued(new),queued(team_formed)")

	// Another company's conversation goes to the shared pool only.
	other := newStream(company(2), customer(100), "en")
	e.apply(other.opened(), false)
	e.online(operator(10))
	e.assign()
	members := ops(1, 2, 3, 4, 8)
	for _, s := range streams[1:] {
		_, op, st := e.liveOffer(s)
		switch {
		case s == fr:
			if op != operator(10) {
				t.Errorf("fr offered to %q, want the shared fr operator", op)
			}
		case st != offerOffered || !slices.Contains(members, op):
			t.Errorf("%s offered to %q (%s), want a team member", s.customer, op, st)
		}
	}
	if _, op, _ := e.liveOffer(other); slices.Contains(members, op) || op == "" {
		t.Errorf("other company offered to %q", op)
	}
	res, err := e.svc.Accept(ctx, earlyOp, earlyOffer)
	wantResult(t, "accept of the offer made before the team formed", res, err, opapi.ResultDone)

	// Resize: 28 active, size 7. en needs 2 more, fr 1, ja none.
	for range 8 {
		open("en")
	}
	if r, err := e.svc.CheckTeams(ctx); err != nil || r.Resized != 1 {
		t.Fatalf("check at 28: %+v, %v", r, err)
	}
	if got, want := e.members(co), ops(1, 2, 3, 4, 6, 8, 10); !slices.Equal(got, want) {
		t.Fatalf("members after growth = %v, want %v", got, want)
	}

	// Release: close all but six offered conversations, keep one new one
	// waiting in the team queue. 7 active, below 8.
	late := open("en")
	for _, s := range streams {
		if s == late || s == early || slices.Index(streams, s) <= 5 {
			continue
		}
		e.apply(s.closed(), false)
	}
	if r, err := e.svc.CheckTeams(ctx); err != nil || r.Resized != 1 || r.Released != 0 {
		t.Fatalf("check at 7 active: %+v, %v", r, err)
	}
	if got := e.members(co); len(got) != 2 {
		t.Fatalf("members after shrink = %v, want 2", got)
	}
	e.clock.Advance(15*time.Minute - time.Second)
	if r, _ := e.svc.CheckTeams(ctx); r.Released != 0 || r.Resized != 0 {
		t.Fatalf("check after 14m59s: %+v", r)
	}
	held := map[string]string{}
	for _, s := range streams[1:6] {
		o, _, _ := e.liveOffer(s)
		held[s.customer] = o
	}
	e.clock.Advance(time.Second)
	if r, err := e.svc.CheckTeams(ctx); err != nil || r.Released != 1 || r.Moved != 1 {
		t.Fatalf("check after 15m: %+v, %v", r, err)
	}
	if got := e.members(co); len(got) != 0 {
		t.Fatalf("members after release = %v", got)
	}
	teamEvs = e.events(event.CompanyKey(co))
	wantTypes(t, teamEvs, "team_formed,team_resized,team_resized,team_released")
	if rel := teamEvs[3]; rel.ReleaseReason != event.TeamLowLoad || len(rel.Members) != 2 || rel.ActiveLoad != 7 {
		t.Errorf("team_released = %+v", rel)
	}
	lateEvs := e.events(late.key())
	wantTypes(t, lateEvs, "queued(new),queued(team_released)")
	if !lateEvs[0].TeamQueue || lateEvs[1].TeamQueue {
		t.Errorf("late conversation queues: %+v", lateEvs)
	}
	// Offers held by the members run on.
	for _, s := range streams[1:6] {
		if o, _, _ := e.liveOffer(s); o != held[s.customer] {
			t.Errorf("%s offer %q changed to %q on release", s.customer, held[s.customer], o)
		}
	}
}

func testOutbox(t *testing.T, e *env) {
	ctx := t.Context()
	for i := range 3 {
		s := newStream(company(0), customer(i), "en")
		e.apply(s.opened(), false)
		e.apply(s.closed(), false)
	}
	recs, err := e.svc.NextOutbox(ctx, 4)
	if err != nil || len(recs) != 4 {
		t.Fatalf("NextOutbox = %d, %v", len(recs), err)
	}
	for i, r := range recs {
		ev, err := event.UnmarshalRouting(r.Value)
		if err != nil || string(ev.Key()) != string(r.Key) || ev.Seq != r.Seq || ev.Type != r.Type || r.Team {
			t.Fatalf("record %d = %+v, %v", i, r, err)
		}
	}
	if recs[0].Seq != 1 || recs[1].Seq != 2 || recs[1].Type != event.TypeEnded {
		t.Errorf("records = %+v", recs)
	}
	ids := []int64{recs[0].ID, recs[1].ID, recs[2].ID}
	if n, err := e.svc.MarkSent(ctx, ids); err != nil || n != 3 {
		t.Fatalf("MarkSent = %d, %v", n, err)
	}
	if n, _ := e.svc.MarkSent(ctx, ids); n != 0 {
		t.Fatalf("MarkSent again = %d", n)
	}
	recs, _ = e.svc.NextOutbox(ctx, 10)
	if len(recs) != 3 || recs[0].ID != ids[2]+1 {
		t.Fatalf("unsent after MarkSent = %+v", recs)
	}
	e.clock.Advance(time.Minute)
	if n, err := e.svc.PruneOutbox(ctx, e.clock.Now(), 2); err != nil || n != 2 {
		t.Fatalf("PruneOutbox = %d, %v", n, err)
	}
	if n, _ := e.svc.PruneOutbox(ctx, e.clock.Now(), 10); n != 1 {
		t.Fatalf("PruneOutbox again = %d", n)
	}
	// Pruned rows are gone, so the sequence check below runs on what is
	// left: restore a clean outbox for requireInvariants.
	if _, err := e.pool.Exec(ctx, `TRUNCATE outbox; UPDATE conversations SET route_seq = 0`); err != nil {
		t.Fatal(err)
	}
}

func testCounts(t *testing.T, e *env) {
	ctx := t.Context()
	e.putOperator(operator(0), 1, "en")
	e.online(operator(0))
	for i := range 3 {
		s := newStream(company(0), customer(i), "en")
		e.apply(s.opened(), false)
	}
	e.assign()
	c, err := e.svc.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c.Conversations[stateWaiting] != 2 || c.Conversations[stateOffered] != 1 || c.LiveSlots != 1 ||
		len(c.Queues) != 1 || c.Queues[0].Waiting != 2 || c.Queues[0].Lang != "en" || c.UnsentOutbox != 4 ||
		c.OldestUnsent.IsZero() || len(c.Operators) != 1 || c.Operators[0].N != 1 {
		t.Errorf("counts = %+v", c)
	}
}

func testFullTeamQueue(t *testing.T, e *env) {
	ctx := t.Context()
	e.putOperator(operator(0), 0, "en")
	e.online(operator(0))
	for i := range 3 {
		e.apply(newStream(company(0), customer(i), "en").opened(), false)
	}
	if r, err := e.svc.CheckTeams(ctx); err != nil || r.Formed != 1 || r.Moved != 3 {
		t.Fatalf("CheckTeams = %+v, %v", r, err)
	}
	if r := e.assign(); r.Offered != 1 {
		t.Fatalf("assign to the team = %+v", r)
	}
	// The only member is full. Two older team conversations wait ahead of
	// a conversation of another company, which a free shared operator can
	// take.
	e.putOperator(operator(1), 0, "en")
	e.online(operator(1))
	e.clock.Advance(time.Second)
	other := newStream(company(1), customer(10), "en")
	e.apply(other.opened(), false)
	r, err := e.svc.Assign(ctx, 2)
	if err != nil || r.Offered != 1 || r.Scanned != 1 {
		t.Fatalf("Assign(2) = %+v, %v; want the other company's conversation offered", r, err)
	}
	if _, op, _ := e.liveOffer(other); op != operator(1) {
		t.Fatalf("other company offered to %q", op)
	}
}
