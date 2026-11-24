//go:build integration

package router

import (
	"slices"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgdb"
)

// TestIntegrationTeamOverflow covers the overflow from a team to the shared
// pool (decision 0009, "Overflow from a team to the shared pool"): a
// conversation in a team queue goes to a member first, and to an operator
// of the shared pool, by the rules of the shared queue, when no member can
// take it. Unless a scenario says otherwise, teams have one member (form
// at 3 active conversations), the cap is off, and every operator has the
// default capacity of 4. Each scenario runs on an emptied database and
// checks the invariants after it.
func TestIntegrationTeamOverflow(t *testing.T) {
	pool := startDB(t, 8)
	oneMember := func(c *Config) {
		c.TeamFormLoad, c.TeamReleaseLoad, c.TeamMinSize, c.TeamMaxSize = 3, 1, 1, 1
		c.TeamMaxShare = 1
	}
	tests := []struct {
		name string
		cfg  func(*Config)
		fn   func(t *testing.T, e *env)
	}{
		{name: "a free member wins over a pool operator with a lower load and a higher priority", cfg: oneMember,
			fn: testOverflowMemberFirst},
		{name: "a full team: a pool operator with the language takes it", cfg: oneMember, fn: testOverflowFullTeam},
		{name: "a full team and no pool operator with the language: the general queue", cfg: oneMember,
			fn: testOverflowGeneralQueue},
		{name: "a free member that is skipped: the pool takes it", cfg: oneMember, fn: testOverflowSkippedMember},
		{name: "a previous operator from the pool still wins", cfg: oneMember, fn: testOverflowPreviousOperator},
		{name: "never to a member of another team", cfg: oneMember, fn: testOverflowOtherTeam},
		{name: "re-validation rejects an operator who joined another team", cfg: oneMember, fn: testOverflowRevalidation},
		{name: "skips of the team and the pool are cleared once", cfg: oneMember, fn: testOverflowSkips},
		{name: "a team at its minimum under the cap: the pool serves the rest", fn: testOverflowCapAtMinimum},
		{name: "the three queries agree for a team queue", cfg: oneMember, fn: testOverflowQueriesAgree},
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

// teamOf opens n conversations of a company in one language and runs the
// team check, which must form a team with exactly the given members. The
// caller makes sure that they are the only candidates: the online
// operators of the shared pool with the language. The customers are
// first, first+1 and so on.
func (e *env) teamOf(co, lang string, first, n int, members ...string) []*convStream {
	e.t.Helper()
	var out []*convStream
	for i := range n {
		s := newStream(co, customer(first+i), lang)
		e.apply(s.opened(), false)
		out = append(out, s)
	}
	r, err := e.svc.CheckTeams(e.t.Context())
	if err != nil || r.Formed != 1 {
		e.t.Fatalf("CheckTeams = %+v, %v; want one team formed", r, err)
	}
	if got := e.members(co); !slices.Equal(got, members) {
		e.t.Fatalf("team of %s = %v, want %v", co, got, members)
	}
	return out
}

// wantOffer checks the operator of the live offer of s.
func (e *env) wantOffer(s *convStream, want string) string {
	e.t.Helper()
	offer, op, _ := e.liveOffer(s)
	if op != want {
		e.t.Fatalf("%s offered to %q, want %q", s.customer, op, want)
	}
	return offer
}

// wantAssign runs one assignment and checks its counts.
func (e *env) wantAssign(offered, general, overflow int) AssignResult {
	e.t.Helper()
	r := e.assign()
	if r.Offered != offered || r.GeneralQueue != general || r.Overflow != overflow {
		e.t.Fatalf("assign = %+v, want %d offers, %d general, %d overflow", r, offered, general, overflow)
	}
	return r
}

// acceptResolve accepts and resolves the live offer of s.
func (e *env) acceptResolve(s *convStream) {
	e.t.Helper()
	offer, op, _ := e.liveOffer(s)
	if _, err := e.svc.Accept(e.t.Context(), op, offer); err != nil {
		e.t.Fatal(err)
	}
	if _, err := e.svc.Resolve(e.t.Context(), op, offer); err != nil {
		e.t.Fatal(err)
	}
}

func testOverflowMemberFirst(t *testing.T, e *env) {
	m, p := operator(0), operator(1)
	e.putPriority(m, 0, 0, "en")
	e.online(m)
	convs := e.teamOf(company(0), "en", 0, 3, m)
	// p is idle and has the highest priority. The member gets all three,
	// also with a higher load than p.
	e.putPriority(p, 0, opapi.MaxPriority, "en")
	e.online(p)
	e.wantAssign(3, 0, 0)
	for _, s := range convs {
		e.wantOffer(s, m)
	}
}

func testOverflowFullTeam(t *testing.T, e *env) {
	ctx := t.Context()
	m, p := operator(0), operator(1)
	e.putOperator(m, 1, "en")
	e.online(m)
	convs := e.teamOf(company(0), "en", 0, 3, m)
	e.putOperator(p, 0, "en")
	e.online(p)
	e.wantAssign(3, 0, 2)
	e.wantOffer(convs[0], m)
	for _, s := range convs[1:] {
		e.wantOffer(s, p)
		// The conversation keeps its queue, its priority and its queue
		// time. The offered event says so too.
		var teamQueue bool
		var priority int
		var queuedAt time.Time
		err := e.pool.QueryRow(ctx, `SELECT team_queue, priority, queued_at FROM conversations WHERE customer_id = $1`, s.customer).
			Scan(&teamQueue, &priority, &queuedAt)
		if err != nil || !teamQueue || priority != 0 {
			t.Fatalf("%s: team_queue %v, priority %d, %v", s.customer, teamQueue, priority, err)
		}
		evs := e.events(s.key())
		last := evs[len(evs)-1]
		if last.OperatorID != p || !last.TeamQueue || !last.QueuedAt.Equal(queuedAt) || !queuedAt.Equal(evs[0].QueuedAt) {
			t.Errorf("%s: last event %+v, queued at %s, first queued at %s", s.customer, last, queuedAt, evs[0].QueuedAt)
		}
	}
}

func testOverflowGeneralQueue(t *testing.T, e *env) {
	m, u, q := operator(0), operator(1), operator(2)
	e.putOperator(m, 1, "pl")
	e.online(m)
	convs := e.teamOf(company(0), "pl", 0, 3, m)
	e.putOperator(u, 0, "en", "und")
	e.online(u)
	// q has "pl" but is offline: no online pool operator has the language,
	// so the overflow goes to the general queue. Both rules count.
	e.putOperator(q, 0, "pl")
	e.wantAssign(3, 2, 2)
	e.wantOffer(convs[0], m)
	e.wantOffer(convs[1], u)
	e.wantOffer(convs[2], u)
	// With q online the overflow goes to q, without the general queue.
	e.online(q)
	s := newStream(company(0), customer(3), "pl")
	e.apply(s.opened(), false)
	e.wantAssign(1, 0, 1)
	e.wantOffer(s, q)
}

func testOverflowSkippedMember(t *testing.T, e *env) {
	ctx := t.Context()
	m, p := operator(0), operator(1)
	e.putOperator(m, 0, "en")
	e.online(m)
	convs := e.teamOf(company(0), "en", 0, 3, m)
	e.putOperator(p, 0, "en")
	e.online(p)
	e.wantAssign(3, 0, 0)
	// m refuses the first one and still has two free slots, but it is
	// skipped for this conversation: the pool takes it.
	offer := e.wantOffer(convs[0], m)
	if _, err := e.svc.Refuse(ctx, m, offer); err != nil {
		t.Fatal(err)
	}
	e.wantAssign(1, 0, 1)
	e.wantOffer(convs[0], p)
	// A new conversation of the company goes to m again.
	s := newStream(company(0), customer(3), "en")
	e.apply(s.opened(), false)
	e.wantAssign(1, 0, 0)
	e.wantOffer(s, m)
}

// testOverflowPreviousOperator: the preference for the previous operator
// holds for a pool operator that serves the overflow, over a pool operator
// with a lower load and a higher priority. It does not take the
// conversation from a free member: the previous operator must pass every
// rule (decision 0009, "Previous operator"), and a pool operator passes
// the rule of a team queue only while no member can take it.
func testOverflowPreviousOperator(t *testing.T, e *env) {
	m, p, q := operator(0), operator(1), operator(2)
	e.putOperator(m, 1, "en")
	e.online(m)
	convs := e.teamOf(company(0), "en", 0, 3, m)
	e.putPriority(p, 0, 0, "en")
	e.online(p)
	e.wantAssign(3, 0, 2)
	c := convs[1]
	e.wantOffer(c, p)
	e.wantOffer(convs[2], p)
	e.acceptResolve(c)
	// p holds one, q holds none and has a higher priority. A customer
	// message brings c back with p preferred; m is still full.
	e.putPriority(q, 0, opapi.MaxPriority, "en")
	e.online(q)
	if r := e.apply(c.message(event.FromCustomer), false); !r.Queued {
		t.Fatalf("customer message after resolve: %+v", r)
	}
	e.wantAssign(1, 0, 1)
	e.wantOffer(c, p)
	// Now m is free when c comes back: the member gets it, not p.
	e.acceptResolve(c)
	e.acceptResolve(convs[0])
	if r := e.apply(c.message(event.FromCustomer), false); !r.Queued {
		t.Fatalf("second customer message after resolve: %+v", r)
	}
	e.wantAssign(1, 0, 0)
	e.wantOffer(c, m)
}

func testOverflowOtherTeam(t *testing.T, e *env) {
	a, b := company(1), company(2)
	ma, mb, p := operator(0), operator(1), operator(2)
	e.putOperator(ma, 2, "en")
	e.online(ma)
	convsA := e.teamOf(a, "en", 0, 4, ma)
	// mb is idle with the highest priority, in the team of b.
	e.putPriority(mb, 0, opapi.MaxPriority, "en")
	e.online(mb)
	e.clock.Advance(time.Second)
	convsB := e.teamOf(b, "en", 10, 3, mb)
	e.putPriority(p, 0, 0, "en")
	e.online(p)
	e.wantAssign(7, 0, 2)
	// a: two to its member, the overflow to the pool, none to mb.
	e.wantOffer(convsA[0], ma)
	e.wantOffer(convsA[1], ma)
	e.wantOffer(convsA[2], p)
	e.wantOffer(convsA[3], p)
	// b: its member has room for all three, so p gets none.
	for _, s := range convsB {
		e.wantOffer(s, mb)
	}
}

// testOverflowRevalidation: the pick and the row lock are one statement,
// so a join between them cannot be made from outside. The test runs the
// steps of pickOperator by hand in one transaction and moves the picked
// pool operator into another team after the pick, while the transaction
// holds its row: the re-read row then fails fitsQueue, and a new pick in
// the same state finds nobody. The transaction is rolled back.
func testOverflowRevalidation(t *testing.T, e *env) {
	ctx := t.Context()
	m, p := operator(0), operator(1)
	e.putOperator(m, 1, "en")
	e.online(m)
	convs := e.teamOf(company(0), "en", 0, 3, m)
	e.wantAssign(1, 0, 0)
	e.putOperator(p, 0, "en")
	e.online(p)
	c := convs[1]

	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := pgdb.New(tx)
	row, err := q.TryLockWaitingConversation(ctx, pgdb.TryLockWaitingConversationParams{CompanyID: c.company, CustomerID: c.customer})
	if err != nil {
		t.Fatal(err)
	}
	team := &row.CompanyID
	pick, err := q.PickOperator(ctx, pgdb.PickOperatorParams{
		TeamCompanyID: team, Lang: row.Lang, UseSkips: true, CompanyID: row.CompanyID, CustomerID: row.CustomerID,
		Session: row.ConvSession, Exclude: []string{},
	})
	if err != nil || pick.ID != p || pick.GeneralQueue {
		t.Fatalf("pick = %+v, %v; want %s", pick, err, p)
	}
	slots, err := q.OperatorSlots(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if ok, overflow := fitsQueue(slots, team, pick.GeneralQueue); !ok || !overflow {
		t.Fatalf("fitsQueue before the join = %v, %v; want an overflow", ok, overflow)
	}
	if _, err := tx.Exec(ctx, `UPDATE operators SET dedicated_company_id = $1 WHERE id = $2`, company(5), p); err != nil {
		t.Fatal(err)
	}
	slots, err = q.OperatorSlots(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := fitsQueue(slots, team, pick.GeneralQueue); ok {
		t.Fatal("fitsQueue accepted an operator of another team")
	}
	if got, err := e.svc.pickOperator(ctx, q, &row); err != nil || got.id != "" {
		t.Fatalf("pickOperator after the join = %+v, %v; want nobody", got, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// Without the join, p takes it.
	e.wantAssign(2, 0, 2)
	e.wantOffer(c, p)
}

func testOverflowSkips(t *testing.T, e *env) {
	ctx := t.Context()
	m, p := operator(0), operator(1)
	e.putOperator(m, 0, "en")
	e.online(m)
	convs := e.teamOf(company(0), "en", 0, 3, m)
	e.putOperator(p, 1, "en")
	e.online(p)
	e.wantAssign(3, 0, 0)
	// p holds a conversation of another company and is full.
	x := newStream(company(9), customer(90), "en")
	e.apply(x.opened(), false)
	e.wantAssign(1, 0, 0)
	e.wantOffer(x, p)
	skips := func() int {
		var n int
		if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM offer_skips WHERE customer_id = $1`, convs[0].customer).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// m refuses. The only member is skipped, and p is full but not
	// skipped, so the skips stay and the conversation waits for p instead
	// of going back to m.
	c := convs[0]
	o1 := e.wantOffer(c, m)
	if _, err := e.svc.Refuse(ctx, m, o1); err != nil {
		t.Fatal(err)
	}
	e.wantAssign(0, 0, 0)
	if n := skips(); n != 1 || e.convState(c) != stateWaiting {
		t.Fatalf("skips %d, state %s; want 1 and waiting", n, e.convState(c))
	}
	// p is free again and takes it.
	e.acceptResolve(x)
	e.wantAssign(1, 0, 1)
	o2 := e.wantOffer(c, p)
	// p refuses too. Every member and every pool operator who could take
	// it is skipped now: the skips are cleared once, and the member gets
	// it again first.
	if _, err := e.svc.Refuse(ctx, p, o2); err != nil {
		t.Fatal(err)
	}
	e.wantAssign(1, 0, 0)
	if o3 := e.wantOffer(c, m); o3 == o1 {
		t.Fatal("the third offer is not new")
	}
	if n := skips(); n != 0 {
		t.Fatalf("skips after the clear = %d", n)
	}
}

// testOverflowCapAtMinimum is the case of the live check of the cap: a cap
// that keeps a team at its minimum size, more conversations than the team
// can hold, and free pool operators. Every conversation gets an operator
// and the team stays inside the cap.
func testOverflowCapAtMinimum(t *testing.T, e *env) {
	ctx := t.Context()
	// 5 online: the cap of one half is 2, the minimum team size. The
	// members are the two lowest IDs; the pool operators have more room.
	for i := range 5 {
		capacity := 4
		if i >= 2 {
			capacity = 6
		}
		e.putOperator(operator(i), capacity, "en")
		e.online(operator(i))
	}
	co := company(1)
	var convs []*convStream
	for i := range 20 {
		s := newStream(co, customer(i), "en")
		e.apply(s.opened(), false)
		convs = append(convs, s)
	}
	if r, err := e.svc.CheckTeams(ctx); err != nil || r != (TeamResult{Formed: 1, Moved: 20, CapGrow: 1}) {
		t.Fatalf("CheckTeams = %+v, %v", r, err)
	}
	members := e.members(co)
	if !slices.Equal(members, []string{operator(0), operator(1)}) {
		t.Fatalf("members = %v", members)
	}
	// Strict teams served 8 of 20 here. Now the pool takes the other 12
	// and keeps 6 free slots.
	e.wantAssign(20, 0, 12)
	held := map[string]int{}
	for _, s := range convs {
		_, op, _ := e.liveOffer(s)
		held[op]++
	}
	if held[operator(0)] != 4 || held[operator(1)] != 4 {
		t.Errorf("held per operator %v, want 4 for each member", held)
	}
	// The team still cannot grow, and stays inside the cap.
	if r, err := e.svc.CheckTeams(ctx); err != nil || r != (TeamResult{CapGrow: 1}) {
		t.Fatalf("second CheckTeams = %+v, %v", r, err)
	}
	c, err := e.svc.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c.TeamOnline != 2 || c.TeamCap != 2 || c.Conversations[stateWaiting] != 0 || c.LiveSlots != 20 {
		t.Errorf("counts: team online %d, cap %d, waiting %d, live slots %d; want 2, 2, 0, 20",
			c.TeamOnline, c.TeamCap, c.Conversations[stateWaiting], c.LiveSlots)
	}
}

// testOverflowQueriesAgree is testQueriesAgree for a conversation of a
// team queue. Every state has a team of one member with "pl" and a waiting
// conversation of that company in "pl".
func testOverflowQueriesAgree(t *testing.T, e *env) {
	type state struct {
		name string
		// memberFull fills the member (capacity 1) before the pool comes.
		memberFull bool
		// before runs before the team forms, after runs after it.
		before, after func(i int)
		want          [3]bool
	}
	co := company(0)
	pl := func(id string, capacity int) {
		e.putOperator(id, capacity, "pl")
		e.online(id)
	}
	states := []state{
		{name: "member free", want: [3]bool{true, true, true}},
		{name: "member full, a pool operator with the language", memberFull: true,
			after: func(int) { pl(operator(1), 0) }, want: [3]bool{true, true, true}},
		{name: "member full, the pool operator with the language full, a general queue operator free", memberFull: true,
			before: func(i int) {
				// operator(1) takes a shared conversation first, so the team
				// gets operator(0) with the lower load.
				e.putPriority(operator(1), 1, opapi.MaxPriority, "pl")
				e.online(operator(1))
				e.offerTo(newStream(company(7), customer(700+i), "pl"), operator(1), 0)
			},
			after: func(int) {
				e.putOperator(operator(2), 0, "und")
				e.online(operator(2))
			},
			want: [3]bool{false, false, true}},
		{name: "member full, no pool operator with the language, a general queue operator", memberFull: true,
			after: func(int) {
				e.putOperator(operator(2), 0, "en", "und")
				e.online(operator(2))
			},
			want: [3]bool{true, true, true}},
		{name: "member full, no pool operator", memberFull: true, want: [3]bool{false, false, true}},
		{name: "member full, a free member of another team", memberFull: true,
			after: func(i int) {
				e.clock.Advance(time.Second)
				pl(operator(3), 0)
				e.teamOf(company(5), "pl", 500+10*i, 3, operator(3))
			},
			want: [3]bool{false, false, true}},
	}
	for i, st := range states {
		resetDB(t, e.pool)
		capacity := 0
		if st.memberFull {
			capacity = 1
		}
		e.putOperator(operator(0), capacity, "pl")
		e.online(operator(0))
		if st.before != nil {
			st.before(i)
		}
		convs := e.teamOf(co, "pl", 10*i, 3, operator(0))
		if st.memberFull {
			e.wantAssign(1, 0, 0)
		}
		if st.after != nil {
			st.after(i)
		}
		got := e.whoCanServe(convs[2])
		if got != st.want {
			t.Errorf("%s: listed, picked, unskipped = %v, want %v", st.name, got, st.want)
		}
	}
}
