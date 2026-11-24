//go:build integration

package router

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// TestIntegrationStage4 covers the router changes of decision 0011:
// operator events, their refresh, the previous pair of a requeue and
// "message removed" events. Every subtest ends with the invariants, which
// include the operator family and the previous pair.
func TestIntegrationStage4(t *testing.T) {
	pool := startDB(t, 8)
	tests := []struct {
		name string
		cfg  func(*Config)
		fn   func(t *testing.T, e *env)
	}{
		{name: "operator events of put, online and offline", fn: testOperatorEvents},
		{name: "operator events of a team join and leave", cfg: func(c *Config) {
			c.Capacity, c.TeamFormLoad, c.TeamReleaseLoad, c.TeamMinSize, c.TeamMaxSize = 1, 3, 1, 1, 1
			// The test is about operator events, not the cap: with one
			// operator online the default share of one half gives a cap of
			// 0, and no team could form.
			c.TeamMaxShare = 1
		}, fn: testOperatorTeamEvents},
		{name: "operator refresh", fn: testOperatorRefresh},
		{name: "operator created before operator events", fn: testLegacyOperator},
		{name: "previous operator and offer on every requeue", fn: testPreviousPair},
		{name: "message removed in every conversation state", fn: testMessageRemoved},
		{name: "settings carry the SLA targets", fn: testSettingsTargets},
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

// opEvents returns the operator events of one operator in outbox order.
func (e *env) opEvents(id string) []event.Routing {
	e.t.Helper()
	evs := e.events(event.OperatorKey(id))
	for _, ev := range evs {
		if !ev.Type.IsOperator() {
			e.t.Fatalf("event %s under the key of operator %s", ev.Type, id)
		}
	}
	return evs
}

// wantOp checks one operator event.
func wantOp(t *testing.T, ev event.Routing, typ event.RoutingType, seq uint64, capacity int,
	presence event.Presence, company string, langs ...string,
) {
	t.Helper()
	if ev.Type != typ || ev.Seq != seq || ev.Capacity != capacity || ev.Presence != presence ||
		ev.DedicatedCompanyID != company || !slices.Equal(ev.Languages, langs) {
		t.Errorf("operator event = %+v\nwant %s seq %d capacity %d %s company %q languages %v",
			ev, typ, seq, capacity, presence, company, langs)
	}
}

func testOperatorEvents(t *testing.T, e *env) {
	ctx := t.Context()
	a := operator(0)
	put := func(capacity int, langs ...string) opapi.Result {
		t.Helper()
		r, err := e.svc.PutOperator(ctx, a, opapi.OperatorRequest{Languages: langs, Capacity: capacity})
		if err != nil {
			t.Fatal(err)
		}
		return r.Result
	}
	steps := []struct {
		name   string
		do     func() (opapi.Result, error)
		result opapi.Result
		// events is the number of operator events after the step.
		events int
	}{
		{"create", func() (opapi.Result, error) { return put(0, "und", "en"), nil }, opapi.ResultDone, 1},
		{"put same", func() (opapi.Result, error) { return put(0, "en", "und"), nil }, opapi.ResultAlreadyDone, 1},
		{"put same capacity given", func() (opapi.Result, error) { return put(4, "en", "und"), nil }, opapi.ResultAlreadyDone, 1},
		{"capacity", func() (opapi.Result, error) { return put(2, "en", "und"), nil }, opapi.ResultDone, 2},
		{"languages", func() (opapi.Result, error) { return put(2, "en", "ja"), nil }, opapi.ResultDone, 3},
		{"online", func() (opapi.Result, error) { return e.svc.Online(ctx, a) }, opapi.ResultDone, 4},
		{"online again", func() (opapi.Result, error) { return e.svc.Online(ctx, a) }, opapi.ResultAlreadyDone, 4},
		{"put same while online", func() (opapi.Result, error) { return put(2, "ja", "en"), nil }, opapi.ResultAlreadyDone, 4},
		{"offline", func() (opapi.Result, error) { return e.svc.Offline(ctx, a) }, opapi.ResultDone, 5},
		{"offline again", func() (opapi.Result, error) { return e.svc.Offline(ctx, a) }, opapi.ResultAlreadyDone, 5},
	}
	for _, st := range steps {
		e.clock.Advance(time.Second)
		res, err := st.do()
		wantResult(t, st.name, res, err, st.result)
		if n := len(e.opEvents(a)); n != st.events {
			t.Fatalf("%s: %d operator events, want %d", st.name, n, st.events)
		}
	}
	evs := e.opEvents(a)
	wantTypes(t, evs, "operator_updated,operator_updated,operator_updated,operator_online,operator_offline")
	off := event.PresenceOffline
	wantOp(t, evs[0], event.TypeOperatorUpdated, 1, 4, off, "", "en", "und")
	wantOp(t, evs[1], event.TypeOperatorUpdated, 2, 2, off, "", "en", "und")
	wantOp(t, evs[2], event.TypeOperatorUpdated, 3, 2, off, "", "en", "ja")
	wantOp(t, evs[3], event.TypeOperatorOnline, 4, 2, event.PresenceOnline, "", "en", "ja")
	wantOp(t, evs[4], event.TypeOperatorOffline, 5, 2, off, "", "en", "ja")
	if !evs[4].At.After(evs[3].At) {
		t.Errorf("offline at %s is not after online at %s", evs[4].At, evs[3].At)
	}

	// Unknown operators and bad input write nothing.
	_, err := e.svc.Online(ctx, "sim-op-unknown")
	wantCode(t, "online unknown", err, opapi.CodeOperatorNotFound)
	_, err = e.svc.PutOperator(ctx, operator(1), opapi.OperatorRequest{Languages: []string{"EN"}})
	wantCode(t, "put bad language", err, opapi.CodeInvalidRequest)
	var n int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE family = 'operator'`).Scan(&n); err != nil || n != 5 {
		t.Errorf("operator events in the outbox = %d, %v; want 5", n, err)
	}

	// Offline with held conversations: the operator event comes first, the
	// conversation events after it, all in one transaction.
	e.online(a)
	s := newStream(company(0), customer(0), "en")
	e.apply(s.opened(), false)
	e.assign()
	if _, op, _ := e.liveOffer(s); op != a {
		t.Fatalf("offered to %q", op)
	}
	if _, err := e.svc.Offline(ctx, a); err != nil {
		t.Fatal(err)
	}
	rows, err := e.pool.Query(ctx, `SELECT family || ' ' || event_type FROM outbox ORDER BY id DESC LIMIT 3`)
	if err != nil {
		t.Fatal(err)
	}
	newest, err := pgx.CollectRows(rows, pgx.RowTo[string])
	want := []string{"conversation queued", "conversation offer_expired", "operator operator_offline"}
	if err != nil || !slices.Equal(newest, want) {
		t.Errorf("newest outbox rows = %v, %v; want %v", newest, err, want)
	}
}

func testOperatorTeamEvents(t *testing.T, e *env) {
	ctx := t.Context()
	a := operator(0)
	e.putOperator(a, 0, "en")
	e.online(a)
	co := company(0)
	var streams []*convStream
	for i := range 3 {
		s := newStream(co, customer(i), "en")
		streams = append(streams, s)
		e.apply(s.opened(), false)
	}
	if r, err := e.svc.CheckTeams(ctx); err != nil || r.Formed != 1 {
		t.Fatalf("CheckTeams = %+v, %v", r, err)
	}
	evs := e.opEvents(a)
	wantTypes(t, evs, "operator_updated,operator_online,operator_updated")
	wantOp(t, evs[2], event.TypeOperatorUpdated, 3, 1, event.PresenceOnline, co, "en")
	// A check that changes nothing writes no operator event.
	if _, err := e.svc.CheckTeams(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(e.opEvents(a)); n != 3 {
		t.Fatalf("%d operator events after a check without a change", n)
	}

	// The operator joins a team while it holds nothing; it then leaves
	// with the release, and the snapshot shows the shared pool again.
	for _, s := range streams {
		e.apply(s.closed(), false)
	}
	if _, err := e.svc.CheckTeams(ctx); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(15 * time.Minute)
	if r, err := e.svc.CheckTeams(ctx); err != nil || r.Released != 1 {
		t.Fatalf("release = %+v, %v", r, err)
	}
	evs = e.opEvents(a)
	wantTypes(t, evs, "operator_updated,operator_online,operator_updated,operator_updated")
	wantOp(t, evs[3], event.TypeOperatorUpdated, 4, 1, event.PresenceOnline, "", "en")

	// Joins and leaves of a resize use the same code. testTeams covers
	// them through the invariant that the newest operator event equals
	// the operator row.
	teamEvs := e.events(event.CompanyKey(co))
	wantTypes(t, teamEvs, "team_formed,team_released")
}

func testOperatorRefresh(t *testing.T, e *env) {
	ctx := t.Context()
	const refresh = 6 * time.Hour
	a, b, c := operator(0), operator(1), operator(2)
	e.putOperator(a, 0, "en")
	e.putOperator(b, 0, "ja")
	e.clock.Advance(time.Hour)
	e.putOperator(c, 0, "fr")
	e.online(a)

	before := func() time.Time { return e.clock.Now().Add(-refresh) }
	if n, err := e.svc.RefreshOperators(ctx, before(), 10); err != nil || n != 0 {
		t.Fatalf("refresh with nothing due = %d, %v", n, err)
	}
	// b had its last event 6h and 1s ago; a went online 1h after its
	// creation, and c was created then.
	e.clock.Advance(5*time.Hour + time.Second)
	if n, err := e.svc.RefreshOperators(ctx, before(), 10); err != nil || n != 1 {
		t.Fatalf("refresh at 6h = %d, %v; want b only", n, err)
	}
	wantTypes(t, e.opEvents(b), "operator_updated,operator_updated")
	wantOp(t, e.opEvents(b)[1], event.TypeOperatorUpdated, 2, 4, event.PresenceOffline, "", "ja")
	if n, _ := e.svc.RefreshOperators(ctx, before(), 10); n != 0 {
		t.Fatalf("refresh again = %d", n)
	}

	// One hour later a and c are due. With a batch of 1 each call takes
	// one operator, oldest first.
	e.clock.Advance(time.Hour)
	if n, _ := e.svc.RefreshOperators(ctx, before(), 1); n != 1 {
		t.Fatalf("first batch = %d", n)
	}
	if n, _ := e.svc.RefreshOperators(ctx, before(), 1); n != 1 {
		t.Fatalf("second batch = %d", n)
	}
	if n, _ := e.svc.RefreshOperators(ctx, before(), 1); n != 0 {
		t.Fatalf("third batch = %d", n)
	}
	evs := e.opEvents(a)
	wantTypes(t, evs, "operator_updated,operator_online,operator_updated")
	wantOp(t, evs[2], event.TypeOperatorUpdated, 3, 4, event.PresenceOnline, "", "en")

	// An operator that another transaction holds is skipped and stays due:
	// the refresh never waits for it.
	e.clock.Advance(refresh + time.Second)
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.WithoutCancel(ctx)) })
	if _, err := tx.Exec(ctx, `SELECT 1 FROM operators WHERE id = $1 FOR UPDATE`, b); err != nil {
		t.Fatal(err)
	}
	if n, err := e.svc.RefreshOperators(ctx, before(), 10); err != nil || n != 2 {
		t.Fatalf("refresh with b locked = %d, %v; want a and c", n, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := e.svc.RefreshOperators(ctx, before(), 10); err != nil || n != 1 {
		t.Fatalf("refresh after the lock = %d, %v; want b", n, err)
	}
	wantTypes(t, e.opEvents(b), "operator_updated,operator_updated,operator_updated")
}

func testLegacyOperator(t *testing.T, e *env) {
	ctx := t.Context()
	// Operators created before migration 00003: no operator event, op_seq
	// 0 and no event time.
	for _, id := range []string{operator(0), operator(1)} {
		if _, err := e.pool.Exec(ctx, `INSERT INTO operators (id, capacity) VALUES ($1, 3)`, id); err != nil {
			t.Fatal(err)
		}
		if _, err := e.pool.Exec(ctx, `INSERT INTO operator_languages (operator_id, lang) VALUES ($1, 'en')`, id); err != nil {
			t.Fatal(err)
		}
	}
	// The first event of an operator is operator_updated, also when the
	// change is a presence change.
	e.online(operator(0))
	evs := e.opEvents(operator(0))
	wantTypes(t, evs, "operator_updated")
	wantOp(t, evs[0], event.TypeOperatorUpdated, 1, 3, event.PresenceOnline, "", "en")
	e.online(operator(1))
	if _, err := e.svc.Offline(ctx, operator(1)); err != nil {
		t.Fatal(err)
	}
	wantTypes(t, e.opEvents(operator(1)), "operator_updated,operator_offline")

	// An operator with no event at all is refreshed at once.
	if _, err := e.pool.Exec(ctx, `INSERT INTO operators (id, capacity) VALUES ($1, 2)`, operator(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `INSERT INTO operator_languages (operator_id, lang) VALUES ($1, 'ja')`, operator(2)); err != nil {
		t.Fatal(err)
	}
	if n, err := e.svc.RefreshOperators(ctx, e.clock.Now().Add(-6*time.Hour), 10); err != nil || n != 1 {
		t.Fatalf("refresh = %d, %v", n, err)
	}
	wantOp(t, e.opEvents(operator(2))[0], event.TypeOperatorUpdated, 1, 2, event.PresenceOffline, "", "ja")
}

// wantPrevious checks the previous pair of a queued event.
func wantPrevious(t *testing.T, ev event.Routing, reason event.QueueReason, operatorID, offerID string) {
	t.Helper()
	if ev.Type != event.TypeQueued || ev.QueueReason != reason ||
		ev.PreviousOperatorID != operatorID || ev.PreviousOfferID != offerID {
		t.Errorf("event = %s(%s) previous %q %q, want queued(%s) previous %q %q", ev.Type, ev.QueueReason,
			ev.PreviousOperatorID, ev.PreviousOfferID, reason, operatorID, offerID)
	}
	if reason.HasPreviousOperator() != (operatorID != "") {
		t.Fatalf("test case for %s is wrong", reason)
	}
}

func testPreviousPair(t *testing.T, e *env) {
	ctx := t.Context()
	a, b := operator(0), operator(1)
	for _, id := range []string{a, b} {
		e.putOperator(id, 0, "en")
		e.online(id)
	}
	last := func(s *convStream) event.Routing {
		evs := e.events(s.key())
		return evs[len(evs)-1]
	}

	// new, then offer_expired.
	s1 := newStream(company(0), customer(1), "en")
	e.apply(s1.opened(), false)
	wantPrevious(t, last(s1), event.QueueNew, "", "")
	e.assign()
	o1, op1, _ := e.liveOffer(s1)
	e.clock.Advance(20 * time.Second)
	if n, _ := e.svc.ExpireOffers(ctx, 10); n != 1 {
		t.Fatal("no expiry")
	}
	wantPrevious(t, last(s1), event.QueueOfferExpired, op1, o1)

	// offer_refused.
	e.assign()
	o2, op2, _ := e.liveOffer(s1)
	if _, err := e.svc.Refuse(ctx, op2, o2); err != nil {
		t.Fatal(err)
	}
	wantPrevious(t, last(s1), event.QueueOfferRefused, op2, o2)

	// operator_offline of an offer and of an assignment.
	e.assign()
	o3, op3, _ := e.liveOffer(s1)
	if _, err := e.svc.Offline(ctx, op3); err != nil {
		t.Fatal(err)
	}
	wantPrevious(t, last(s1), event.QueueOperatorOffline, op3, o3)
	e.online(op3)
	e.assign()
	o4, op4, _ := e.liveOffer(s1)
	if _, err := e.svc.Accept(ctx, op4, o4); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Offline(ctx, op4); err != nil {
		t.Fatal(err)
	}
	wantPrevious(t, last(s1), event.QueueOperatorOffline, op4, o4)
	e.online(op4)

	// customer_message_after_resolve: the resolved assignment, also after
	// another assignment of an earlier session.
	e.assign()
	o5, op5, _ := e.liveOffer(s1)
	e.svc.Accept(ctx, op5, o5)
	e.svc.Resolve(ctx, op5, o5)
	e.apply(s1.message(event.FromCustomer), false)
	wantPrevious(t, last(s1), event.QueueCustomerMessage, op5, o5)

	// reopened after a close, and the reset of conv: no pair.
	e.assign()
	o6, op6, _ := e.liveOffer(s1)
	e.svc.Accept(ctx, op6, o6)
	e.svc.Resolve(ctx, op6, o6)
	e.apply(s1.closed(), false)
	e.apply(s1.opened(), false)
	wantPrevious(t, last(s1), event.QueueReopened, "", "")
	e.assign()
	o7, op7, _ := e.liveOffer(s1)
	e.svc.Accept(ctx, op7, o7)
	e.svc.Resolve(ctx, op7, o7)
	e.apply(s1.message(event.FromCustomer), false)
	wantPrevious(t, last(s1), event.QueueCustomerMessage, op7, o7)
	e.apply(s1.closed(), false)
	s1.forget()
	if r := e.apply(s1.opened(), false); !r.Reset {
		t.Fatalf("reset = %+v", r)
	}
	wantPrevious(t, last(s1), event.QueueNew, "", "")
	var lastOp, lastOffer *string
	err := e.pool.QueryRow(ctx, `SELECT last_operator_id, last_offer_id FROM conversations WHERE customer_id = $1`,
		s1.customer).Scan(&lastOp, &lastOffer)
	if err != nil || lastOp != nil || lastOffer != nil {
		t.Errorf("after the reset last operator %v offer %v, %v; want both cleared", lastOp, lastOffer, err)
	}

	// catch_up: no pair, also for a conversation that was resolved before.
	s2 := newStream(company(0), customer(2), "en")
	e.apply(s2.opened(), true)
	if _, err := e.svc.EndCatchUp(ctx, 10); err != nil {
		t.Fatal(err)
	}
	wantPrevious(t, last(s2), event.QueueCatchUp, "", "")
}

// numbers returns the stored session, sequence number and state.
func (e *env) numbers(s *convStream) (int64, int64, string) {
	e.t.Helper()
	var session, seq int64
	var st string
	err := e.pool.QueryRow(e.t.Context(),
		`SELECT conv_session, conv_seq, state FROM conversations WHERE company_id = $1 AND customer_id = $2`,
		s.company, s.customer).Scan(&session, &seq, &st)
	if err != nil {
		e.t.Fatal(err)
	}
	return session, seq, st
}

func testMessageRemoved(t *testing.T, e *env) {
	ctx := t.Context()
	a := operator(0)
	e.putOperator(a, 0, "en")
	e.online(a)
	n := 0
	// stream returns a conversation brought into one state.
	stream := func(state string) *convStream {
		n++
		s := newStream(company(0), customer(n), "en")
		switch state {
		case statePending:
			e.apply(s.opened(), true)
			return s
		case stateClosed:
			e.apply(s.opened(), false)
			e.apply(s.closed(), false)
			return s
		}
		e.apply(s.opened(), false)
		if state == stateWaiting {
			return s
		}
		e.assign()
		o, op, _ := e.liveOffer(s)
		if o == "" {
			t.Fatalf("%s: no offer", state)
		}
		if state == stateOffered {
			return s
		}
		e.svc.Accept(ctx, op, o)
		if state == stateResolved {
			e.svc.Resolve(ctx, op, o)
		}
		return s
	}

	for _, state := range []string{statePending, stateWaiting, stateOffered, stateAssigned, stateResolved, stateClosed} {
		s := stream(state)
		if got := e.convState(s); got != state {
			t.Fatalf("setup: state %s, want %s", got, state)
		}
		before := len(e.events(s.key()))
		session, _, _ := e.numbers(s)
		live, _, _ := e.liveOffer(s)

		ev := s.removed()
		r := e.apply(ev, false)
		if r.Skipped || r.Queued || r.Pending || r.Ended || r.Reset || r.Events != 0 {
			t.Errorf("%s: removed = %+v, want only the numbers moved", state, r)
		}
		if r := e.apply(ev, false); !r.Skipped {
			t.Errorf("%s: removed again = %+v, want skipped", state, r)
		}
		gotSession, gotSeq, gotState := e.numbers(s)
		if gotSession != session || gotSeq != int64(ev.Seq) || gotState != state {
			t.Errorf("%s: after removed %d/%d %s, want %d/%d %s", state, gotSession, gotSeq, gotState,
				session, ev.Seq, state)
		}
		if o, _, _ := e.liveOffer(s); o != live {
			t.Errorf("%s: live offer %q, want %q", state, o, live)
		}

		// A removal that names a later session (the router missed the
		// close and the reopen) moves only the sequence number.
		s.session++
		later := s.removed()
		if r := e.apply(later, false); r.Skipped || r.Queued || r.Ended || r.Events != 0 {
			t.Errorf("%s: later-session removed = %+v", state, r)
		}
		gotSession, gotSeq, gotState = e.numbers(s)
		if gotSession != session || gotSeq != int64(later.Seq) || gotState != state {
			t.Errorf("%s: after later-session removed %d/%d %s", state, gotSession, gotSeq, gotState)
		}
		if after := len(e.events(s.key())); after != before {
			t.Errorf("%s: %d routing events after the removals, want %d", state, after, before)
		}

		// The next real event of that session takes the lost-close path,
		// as without the removals.
		if state != statePending {
			r := e.apply(s.message(event.FromCustomer), false)
			if !r.Queued {
				t.Errorf("%s: next event of the later session = %+v, want a reopen", state, r)
			}
			evs := e.events(s.key())
			if q := evs[len(evs)-1]; q.QueueReason != event.QueueReopened || q.Session != uint64(session+1) {
				t.Errorf("%s: last event %s(%s) session %d", state, q.Type, q.QueueReason, q.Session)
			}
		}
	}

	// An unknown conversation: a closed row with the numbers of the event,
	// no event, nothing queued.
	u := newStream(company(1), customer(100), "en")
	u.session = 3
	u.seq = 20
	ev := u.removed()
	if r := e.apply(ev, false); r.Skipped || r.Queued || r.Events != 0 {
		t.Errorf("unknown: removed = %+v", r)
	}
	if session, seq, st := e.numbers(u); session != 3 || seq != 21 || st != stateClosed {
		t.Errorf("unknown: %d/%d %s, want 3/21 closed", session, seq, st)
	}
	if evs := e.events(u.key()); len(evs) != 0 {
		t.Errorf("unknown: events %s", types(evs))
	}
	// Its next session opens as a reopen.
	if r := e.apply(u.opened(), false); !r.Queued {
		t.Errorf("unknown: next session = %+v", r)
	}

	// The pending conversation is still pending and is queued at the end of
	// the catch-up like any other.
	if n, err := e.svc.EndCatchUp(ctx, 10); err != nil || n != 1 {
		t.Errorf("EndCatchUp = %d, %v; want the pending conversation", n, err)
	}
}

func testSettingsTargets(t *testing.T, e *env) {
	s := e.svc.Settings()
	if s.AssignmentTargetUS != 60_000_000 || s.FirstResponseTargetUS != 120_000_000 || s.Validate() != nil {
		t.Errorf("settings = %+v", s)
	}
}
