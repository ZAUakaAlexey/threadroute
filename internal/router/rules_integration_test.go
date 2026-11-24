//go:build integration

package router

import (
	"slices"
	"testing"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgdb"
)

// TestIntegrationRoutingRules covers the two rules of 2026-11-24 (decision
// 0009, "Changes after the first live run of the console"): the operator
// priority and the general queue for a language that no online operator
// of the shared pool has. Each scenario runs on an emptied database and
// checks the invariants after it.
func TestIntegrationRoutingRules(t *testing.T) {
	pool := startDB(t, 8)
	smallTeams := func(c *Config) {
		c.TeamFormLoad, c.TeamReleaseLoad, c.TeamMinSize, c.TeamMaxSize = 3, 1, 1, 1
	}
	tests := []struct {
		name string
		cfg  func(*Config)
		fn   func(t *testing.T, e *env)
	}{
		{name: "pick order: previous operator, load, priority, ID", fn: testPickOrder},
		{name: "a PUT stores the priority and a PUT without it resets it", fn: testPutPriority},
		{name: "general queue when no online operator has the language", fn: testGeneralQueue},
		{name: "general queue when the only operator with the language is in a team", cfg: smallTeams, fn: testGeneralQueueTeam},
		{name: "no general queue while an operator with the language is online but full", fn: testNoGeneralQueueWhenFull},
		{name: "und conversations as before", fn: testUndAsBefore},
		{name: "skips in the general queue", fn: testGeneralQueueSkips},
		{name: "the three queries agree on who can serve", fn: testQueriesAgree},
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

// putPriority creates or changes an operator with a priority.
func (e *env) putPriority(id string, capacity, priority int, langs ...string) opapi.OperatorResponse {
	e.t.Helper()
	resp, err := e.svc.PutOperator(e.t.Context(), id, opapi.OperatorRequest{Languages: langs, Capacity: capacity, Priority: priority})
	if err != nil {
		e.t.Fatalf("PutOperator(%s) error = %v", id, err)
	}
	return resp
}

// offerTo opens s, runs one assignment and checks who got the offer and
// how many offers were made under the general queue rule.
func (e *env) offerTo(s *convStream, want string, general int) string {
	e.t.Helper()
	e.apply(s.opened(), false)
	return e.assignTo(s, want, general)
}

// assignTo runs one assignment and checks the offer of s.
func (e *env) assignTo(s *convStream, want string, general int) string {
	e.t.Helper()
	r := e.assign()
	offer, op, _ := e.liveOffer(s)
	if op != want {
		e.t.Fatalf("%s offered to %q, want %q", s.customer, op, want)
	}
	if r.GeneralQueue != general {
		e.t.Fatalf("assign %+v: %d offers under the general queue rule, want %d", r, r.GeneralQueue, general)
	}
	return offer
}

func testPickOrder(t *testing.T, e *env) {
	ctx := t.Context()
	a, b, c, d := operator(0), operator(1), operator(2), operator(3)
	e.putPriority(a, 0, 0, "en")
	e.putPriority(b, 0, 3, "en")
	e.putPriority(c, 0, 3, "en")
	for _, id := range []string{a, b, c} {
		e.online(id)
	}

	// Equal lowest load (0): the higher priority wins over the lower ID
	// of a, and between b and c (same priority) the lower ID wins.
	e.offerTo(newStream(company(0), customer(1), "en"), b, 0)
	// a and c have load 0: the higher priority wins.
	e.offerTo(newStream(company(0), customer(2), "en"), c, 0)
	// a has load 0, b and c have 1: the lower load beats the priority.
	s3 := newStream(company(0), customer(3), "en")
	o3 := e.offerTo(s3, a, 0)
	if _, err := e.svc.Accept(ctx, a, o3); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Resolve(ctx, a, o3); err != nil {
		t.Fatal(err)
	}
	// a gets one more and has load 1 again; d comes with load 0 and the
	// highest priority.
	e.offerTo(newStream(company(0), customer(4), "en"), a, 0)
	e.putPriority(d, 0, opapi.MaxPriority, "en")
	e.online(d)
	// A customer message after the resolve prefers a, the previous
	// operator, over d with a lower load and a higher priority.
	if r := e.apply(s3.message(event.FromCustomer), false); !r.Queued {
		t.Fatalf("customer message after resolve: %+v", r)
	}
	e.assignTo(s3, a, 0)
	// The preference holds for one offer: the next conversation goes to d.
	e.offerTo(newStream(company(0), customer(5), "en"), d, 0)
}

func testPutPriority(t *testing.T, e *env) {
	ctx := t.Context()
	a := operator(0)
	resp := e.putPriority(a, 2, 5, "en", "und")
	if resp.Result != opapi.ResultDone || resp.Operator.Priority != 5 || resp.Operator.Capacity != 2 {
		t.Fatalf("create %+v", resp)
	}
	if resp := e.putPriority(a, 2, 5, "und", "en"); resp.Result != opapi.ResultAlreadyDone || resp.Operator.Priority != 5 {
		t.Fatalf("same again %+v", resp)
	}
	events := len(e.opEvents(a))
	// A PUT replaces every field: without a priority it is 0 again. The
	// change is done, but the operator event has no priority, so none is
	// written.
	resp, err := e.svc.PutOperator(ctx, a, opapi.OperatorRequest{Languages: []string{"en", "und"}, Capacity: 2})
	if err != nil || resp.Result != opapi.ResultDone || resp.Operator.Priority != 0 {
		t.Fatalf("put without priority %+v, %v", resp, err)
	}
	if op, err := e.svc.GetOperator(ctx, a); err != nil || op.Priority != 0 {
		t.Fatalf("GetOperator %+v, %v", op, err)
	}
	if n := len(e.opEvents(a)); n != events {
		t.Errorf("%d operator events after a change of the priority alone, want %d", n, events)
	}
	if resp := e.putPriority(a, 2, 0, "en", "und"); resp.Result != opapi.ResultAlreadyDone {
		t.Fatalf("explicit 0 after the reset %+v", resp)
	}
	// A change of the priority together with the capacity writes the
	// event of the capacity.
	if resp := e.putPriority(a, 3, opapi.MaxPriority, "en", "und"); resp.Result != opapi.ResultDone || resp.Operator.Priority != opapi.MaxPriority {
		t.Fatalf("priority 9 %+v", resp)
	}
	if n := len(e.opEvents(a)); n != events+1 {
		t.Errorf("%d operator events after a change of the capacity, want %d", n, events+1)
	}
	_, err = e.svc.PutOperator(ctx, a, opapi.OperatorRequest{Languages: []string{"en"}, Priority: opapi.MaxPriority + 1})
	wantCode(t, "priority 10", err, opapi.CodeInvalidRequest)
	// The database has the same range as the API.
	if _, err := e.pool.Exec(ctx, `UPDATE operators SET priority = 10 WHERE id = $1`, a); err == nil {
		t.Error("priority 10 in the database: no error, want a check violation")
	}
}

func testGeneralQueue(t *testing.T, e *env) {
	ctx := t.Context()
	u, p, en := operator(0), operator(1), operator(2)
	e.putOperator(u, 0, "en", "und")
	e.putOperator(p, 0, "pl")
	e.putOperator(en, 0, "en")
	e.online(u)
	e.online(en)

	// The only operator with "pl" exists but is offline: an operator of
	// the general queue takes it. The operator with only "en" does not.
	s1 := newStream(company(0), customer(1), "pl")
	t0 := e.clock.Now()
	e.offerTo(s1, u, 1)
	evs := e.events(s1.key())
	wantTypes(t, evs, "queued(new),offered")
	if evs[1].QueuedAt.IsZero() || !evs[1].QueuedAt.Equal(t0) {
		t.Errorf("queued at %s, want %s", evs[1].QueuedAt, t0)
	}
	var lang string
	var priority int
	if err := e.pool.QueryRow(ctx, `SELECT lang, priority FROM conversations WHERE customer_id = $1`, s1.customer).
		Scan(&lang, &priority); err != nil || lang != "pl" || priority != 0 {
		t.Fatalf("conversation lang %q priority %d, %v; want pl and 0", lang, priority, err)
	}

	// The first operator with "pl" comes online: the next one goes to them.
	e.online(p)
	e.offerTo(newStream(company(0), customer(2), "pl"), p, 0)

	// The last operator with "pl" goes offline while a conversation waits
	// for them: the next pick uses the general queue. p has capacity 4 and
	// holds one offer; it gets one more, then goes offline with both.
	e.offerTo(newStream(company(0), customer(3), "pl"), p, 0)
	if _, err := e.svc.Offline(ctx, p); err != nil {
		t.Fatal(err)
	}
	r := e.assign()
	if r.Offered != 2 || r.GeneralQueue != 2 {
		t.Fatalf("assign after the last pl operator went offline = %+v, want 2 offers, both general", r)
	}
	for i := 2; i <= 3; i++ {
		if _, op, _ := e.liveOffer(newStream(company(0), customer(i), "pl")); op != u {
			t.Errorf("conversation %d offered to %q, want %s", i, op, u)
		}
	}
}

func testGeneralQueueTeam(t *testing.T, e *env) {
	ctx := t.Context()
	tm, u := operator(0), operator(1)
	e.putOperator(tm, 0, "pl")
	e.putOperator(u, 0, "en", "und")
	e.online(tm)
	e.online(u)
	// Three "pl" conversations of company 0 form a team of one: tm.
	var team []*convStream
	for i := range 3 {
		s := newStream(company(0), customer(i), "pl")
		e.apply(s.opened(), false)
		team = append(team, s)
	}
	if r, err := e.svc.CheckTeams(ctx); err != nil || r.Formed != 1 {
		t.Fatalf("CheckTeams = %+v, %v", r, err)
	}
	if m := e.members(company(0)); !slices.Equal(m, []string{tm}) {
		t.Fatalf("team %v, want %s", m, tm)
	}
	// The only operator with "pl" is in a team, so a "pl" conversation of
	// another company goes to the general queue of the shared pool. The
	// team queue is served by its member as before.
	other := newStream(company(1), customer(10), "pl")
	e.apply(other.opened(), false)
	r := e.assign()
	if r.Offered != 4 || r.GeneralQueue != 1 {
		t.Fatalf("assign = %+v, want 3 team offers and 1 general", r)
	}
	if _, op, _ := e.liveOffer(other); op != u {
		t.Fatalf("other company offered to %q, want %s", op, u)
	}
	for _, s := range team {
		if _, op, _ := e.liveOffer(s); op != tm {
			t.Fatalf("team conversation offered to %q, want %s", op, tm)
		}
	}
	// A member never serves through the general queue, but the shared pool
	// takes the overflow of the team by its own rules: with the only member
	// full, the fifth conversation goes to u through the general queue
	// (decision 0009, "Overflow from a team to the shared pool").
	for i := 3; i <= 4; i++ {
		e.apply(newStream(company(0), customer(i), "pl").opened(), false)
	}
	r = e.assign()
	if r.Offered != 2 || r.GeneralQueue != 1 || r.Overflow != 1 {
		t.Fatalf("assign = %+v, want 1 team offer and 1 overflow under the general queue rule", r)
	}
	if _, op, _ := e.liveOffer(newStream(company(0), customer(3), "pl")); op != tm {
		t.Fatalf("fourth team conversation offered to %q, want %s", op, tm)
	}
	if _, op, _ := e.liveOffer(newStream(company(0), customer(4), "pl")); op != u {
		t.Fatalf("fifth team conversation offered to %q, want %s", op, u)
	}
}

func testNoGeneralQueueWhenFull(t *testing.T, e *env) {
	ctx := t.Context()
	p, u := operator(0), operator(1)
	e.putOperator(p, 1, "pl")
	e.putOperator(u, 0, "en", "und")
	e.online(p)
	e.online(u)
	s1 := newStream(company(0), customer(1), "pl")
	e.offerTo(s1, p, 0)
	// p is full. The conversation waits for p, also with u free, and the
	// scan does not even list it.
	s2 := newStream(company(0), customer(2), "pl")
	e.apply(s2.opened(), false)
	if r := e.assign(); r.Offered != 0 || r.Scanned != 0 {
		t.Fatalf("assign with the pl operator full = %+v, want nothing", r)
	}
	// p goes offline while s2 waits: now nobody online has "pl", and the
	// general queue takes both.
	if _, err := e.svc.Offline(ctx, p); err != nil {
		t.Fatal(err)
	}
	if r := e.assign(); r.Offered != 2 || r.GeneralQueue != 2 {
		t.Fatalf("assign after offline = %+v, want 2 general offers", r)
	}
	if _, op, _ := e.liveOffer(s2); op != u {
		t.Fatalf("s2 offered to %q, want %s", op, u)
	}
}

func testUndAsBefore(t *testing.T, e *env) {
	u, en := operator(0), operator(1)
	e.putOperator(u, 0, "en", "und")
	e.putOperator(en, 0, "en")
	e.online(en)
	// No operator with "und" online: an "und" conversation waits.
	s1 := newStream(company(0), customer(1), "und")
	e.apply(s1.opened(), false)
	if r := e.assign(); r.Offered != 0 {
		t.Fatalf("und without und operators = %+v", r)
	}
	// With one, it is served as before and does not count as general.
	e.online(u)
	e.assignTo(s1, u, 0)
	// An "en" conversation goes to the operator with the lower load, as
	// before: u has "en" too.
	e.offerTo(newStream(company(0), customer(2), "en"), en, 0)
}

func testGeneralQueueSkips(t *testing.T, e *env) {
	ctx := t.Context()
	en, u1, u2, p := operator(0), operator(1), operator(2), operator(3)
	e.putOperator(en, 0, "en")
	e.putOperator(u1, 0, "en", "und")
	e.putOperator(u2, 0, "und")
	e.putOperator(p, 0, "pl")
	for _, id := range []string{en, u1, u2} {
		e.online(id)
	}
	s := newStream(company(0), customer(1), "pl")
	o1 := e.offerTo(s, u1, 1)
	// u1 refuses: skipped, the other operator of the general queue gets it.
	if _, err := e.svc.Refuse(ctx, u1, o1); err != nil {
		t.Fatal(err)
	}
	o2 := e.assignTo(s, u2, 1)
	// u2 lets it time out. Every operator who could take it (u1 and u2;
	// en cannot) is skipped now, so the skips are cleared and u1 gets it
	// again.
	e.clock.Advance(e.svc.cfg.machine(e.svc.cfg.AcceptTimeout))
	if n, err := e.svc.ExpireOffers(ctx, 10); err != nil || n != 1 {
		t.Fatalf("expire = %d, %v", n, err)
	}
	o3 := e.assignTo(s, u1, 1)
	if o3 == o1 || o3 == o2 {
		t.Fatalf("third offer %s is not new", o3)
	}
	var skips int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM offer_skips WHERE customer_id = $1`, s.customer).Scan(&skips); err != nil || skips != 0 {
		t.Fatalf("skips after the clear = %d, %v", skips, err)
	}
	// u1 refuses again. An operator with "pl" comes online: the general
	// queue rule no longer applies, and the operator with the language
	// gets it, although u2 is not skipped.
	if _, err := e.svc.Refuse(ctx, u1, o3); err != nil {
		t.Fatal(err)
	}
	e.online(p)
	e.assignTo(s, p, 0)
}

// testQueriesAgree runs the three queries that decide "who can serve this
// conversation" on the same states and checks that they agree. When every
// operator has a free slot and nobody is skipped, a conversation is listed
// by WaitingConversations exactly when PickOperator finds an operator and
// UnskippedOperatorExists says one exists. When the only operator with the
// language is full, the first two say no and the third says yes (it
// ignores the load), so the skips are not cleared and the general queue is
// not used.
func testQueriesAgree(t *testing.T, e *env) {
	type state struct {
		name string
		// ops are operators by their languages; offline ones end in "!".
		ops       [][]string
		lang      string
		fullFirst bool // the first operator is full
		want      [3]bool
	}
	states := []state{
		{name: "only the general queue online", ops: [][]string{{"en", "und"}}, lang: "pl", want: [3]bool{true, true, true}},
		{name: "operator with the language online", ops: [][]string{{"pl"}, {"en", "und"}}, lang: "pl", want: [3]bool{true, true, true}},
		{name: "operator with the language offline", ops: [][]string{{"pl", "!"}, {"en", "und"}}, lang: "pl", want: [3]bool{true, true, true}},
		{name: "operator with the language full", ops: [][]string{{"pl"}, {"en", "und"}}, lang: "pl", fullFirst: true,
			want: [3]bool{false, false, true}},
		{name: "no general queue online", ops: [][]string{{"en"}, {"und", "!"}}, lang: "pl", want: [3]bool{false, false, false}},
		{name: "und without und operators", ops: [][]string{{"en"}}, lang: "und", want: [3]bool{false, false, false}},
		{name: "und with an und operator", ops: [][]string{{"und"}}, lang: "und", want: [3]bool{true, true, true}},
	}
	for i, st := range states {
		resetDB(t, e.pool)
		for j, langs := range st.ops {
			id := operator(j)
			offline := langs[len(langs)-1] == "!"
			if offline {
				langs = langs[:len(langs)-1]
			}
			capacity := 0
			if j == 0 && st.fullFirst {
				capacity = 1
			}
			e.putOperator(id, capacity, langs...)
			if !offline {
				e.online(id)
			}
		}
		if st.fullFirst {
			e.offerTo(newStream(company(1), customer(100+i), st.lang), operator(0), 0)
		}
		s := newStream(company(0), customer(i), st.lang)
		e.apply(s.opened(), false)
		got := e.whoCanServe(s)
		if got != st.want {
			t.Errorf("%s: listed, picked, unskipped = %v, want %v", st.name, got, st.want)
		}
	}
}

// whoCanServe asks the three queries about the waiting conversation s,
// without skips. PickOperator runs in a transaction that is rolled back.
func (e *env) whoCanServe(s *convStream) [3]bool {
	e.t.Helper()
	ctx := e.t.Context()
	q := pgdb.New(e.pool)
	var out [3]bool
	rows, err := q.WaitingConversations(ctx, pgdb.WaitingConversationsParams{First: true, MaxRows: 100})
	if err != nil {
		e.t.Fatal(err)
	}
	for _, r := range rows {
		if r.CustomerID == s.customer {
			out[0] = true
		}
	}
	c, err := q.GetConversation(ctx, pgdb.GetConversationParams{CompanyID: s.company, CustomerID: s.customer})
	if err != nil {
		e.t.Fatal(err)
	}
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var team *string
	if c.TeamQueue {
		team = &c.CompanyID
	}
	_, err = q.WithTx(tx).PickOperator(ctx, pgdb.PickOperatorParams{
		Lang: c.Lang, TeamCompanyID: team, CompanyID: c.CompanyID, CustomerID: c.CustomerID, Session: c.ConvSession,
		Exclude: []string{},
	})
	switch {
	case err == nil:
		out[1] = true
	case !isNoRows(err):
		e.t.Fatal(err)
	}
	out[2], err = q.UnskippedOperatorExists(ctx, pgdb.UnskippedOperatorExistsParams{
		Lang: c.Lang, TeamCompanyID: team, CompanyID: c.CompanyID, CustomerID: c.CustomerID, Session: c.ConvSession,
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return out
}
