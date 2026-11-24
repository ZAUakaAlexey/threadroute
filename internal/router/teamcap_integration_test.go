//go:build integration

package router

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

// TestIntegrationTeamCap covers the cap on the operators that teams hold
// (decision 0009, "A cap on the operators that teams hold"). Every scenario
// uses the default rules (capacity 4, a team forms at 20 active
// conversations, size between 2 and 20) and the default share of one half,
// unless it says otherwise, on an emptied database, and checks the
// invariants after it.
func TestIntegrationTeamCap(t *testing.T) {
	pool := startDB(t, 8)
	tests := []struct {
		name string
		cfg  func(*Config)
		fn   func(t *testing.T, e *env)
	}{
		{name: "a team forms below the cap with its full size", fn: testCapFullSize},
		{name: "a growing team gets the rest and a new team without the minimum does not form", fn: testCapRest},
		{name: "more operators online let the company form later", fn: testCapMoreOnline},
		{name: "a shrink and a release let the company form later", fn: testCapAfterRelease},
		{name: "the highest active load gets the last places", fn: testCapOrder},
		{name: "pool operators go offline: nobody leaves, no team grows", fn: testCapPoolOffline},
		{name: "share 1 turns the cap off", cfg: func(c *Config) { c.TeamMaxShare = 1 }, fn: testCapOff},
		{name: "two routers at once stay within the cap", fn: func(t *testing.T, e *env) { testCapTwoRouters(t, e, pool) }},
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

// capEnv adds helpers for the cap scenarios to env.
type capEnv struct {
	*env
	streams map[string][]*convStream
	next    int
}

func newCapEnv(e *env) *capEnv { return &capEnv{env: e, streams: map[string][]*convStream{}} }

// onlineOps creates en operators from..to-1 and puts them online.
func (c *capEnv) onlineOps(from, to int) {
	c.t.Helper()
	for i := from; i < to; i++ {
		c.putOperator(operator(i), 0, "en")
		c.online(operator(i))
	}
}

// open opens n en conversations of a company.
func (c *capEnv) open(co string, n int) {
	c.t.Helper()
	for range n {
		s := newStream(co, customer(c.next), "en")
		c.next++
		c.streams[co] = append(c.streams[co], s)
		c.apply(s.opened(), false)
	}
}

// close closes the newest n open conversations of a company.
func (c *capEnv) close(co string, n int) {
	c.t.Helper()
	for i := len(c.streams[co]) - 1; i >= 0 && n > 0; i-- {
		s := c.streams[co][i]
		if s.open {
			c.apply(s.closed(), false)
			n--
		}
	}
}

func (c *capEnv) check() TeamResult {
	c.t.Helper()
	r, err := c.svc.CheckTeams(c.t.Context())
	if err != nil {
		c.t.Fatalf("CheckTeams() error = %v", err)
	}
	return r
}

// wantCounts checks the online team members and the cap as the metrics
// see them.
func (c *capEnv) wantCounts(inTeam, teamCap int64) {
	c.t.Helper()
	got, err := c.svc.Counts(c.t.Context())
	if err != nil {
		c.t.Fatal(err)
	}
	if got.TeamOnline != inTeam || got.TeamCap != teamCap {
		c.t.Errorf("counts: team online %d, cap %d; want %d, %d", got.TeamOnline, got.TeamCap, inTeam, teamCap)
	}
}

func (c *capEnv) teamActive(co string) bool {
	c.t.Helper()
	var active bool
	err := c.pool.QueryRow(c.t.Context(), `SELECT COALESCE((SELECT team_active FROM companies WHERE company_id = $1), false)`, co).
		Scan(&active)
	if err != nil {
		c.t.Fatal(err)
	}
	return active
}

func wantTeam(t *testing.T, r, want TeamResult) {
	t.Helper()
	if r != want {
		t.Fatalf("CheckTeams = %+v, want %+v", r, want)
	}
}

func testCapFullSize(t *testing.T, e *env) {
	c := newCapEnv(e)
	c.onlineOps(0, 20) // cap 10
	c.open(company(1), 20)
	wantTeam(t, c.check(), TeamResult{Formed: 1, Moved: 20})
	if got := c.members(company(1)); len(got) != 5 {
		t.Fatalf("members = %v, want 5", got)
	}
	c.wantCounts(5, 10)
}

func testCapRest(t *testing.T, e *env) {
	ctx := t.Context()
	c := newCapEnv(e)
	a, b := company(1), company(2)
	c.onlineOps(0, 12) // cap 6
	c.open(a, 20)
	wantTeam(t, c.check(), TeamResult{Formed: 1, Moved: 20})
	if got := c.members(a); len(got) != 5 {
		t.Fatalf("members = %v, want 5", got)
	}
	// a grows to 28 active, size 7: one place is left under the cap.
	c.open(a, 8)
	wantTeam(t, c.check(), TeamResult{Resized: 1, CapGrow: 1})
	if got := c.members(a); len(got) != 6 {
		t.Fatalf("members after growth = %v, want 6", got)
	}
	c.wantCounts(6, 6)
	teamEvs := e.events(event.CompanyKey(a))
	wantTypes(t, teamEvs, "team_formed,team_resized")
	if len(teamEvs[1].Members) != 6 {
		t.Errorf("team_resized = %+v", teamEvs[1])
	}

	// b reaches the threshold, but the cap leaves no room for two members.
	c.open(b, 20)
	wantTeam(t, c.check(), TeamResult{CapForm: 1, CapGrow: 1})
	if c.teamActive(b) || len(c.members(b)) != 0 {
		t.Fatalf("company b got a team: %v", c.members(b))
	}
	if evs := e.events(event.CompanyKey(b)); len(evs) != 0 {
		t.Fatalf("team events of b = %s", types(evs))
	}
	// b's conversations stay in the shared queue and the shared pool serves
	// them: six pool operators with 4 slots each take all 20.
	members := c.members(a)
	r := c.assign()
	if r.Offered < 20 {
		t.Fatalf("assign = %+v", r)
	}
	for _, s := range c.streams[b] {
		_, op, st := c.liveOffer(s)
		if st != offerOffered || slices.Contains(members, op) {
			t.Errorf("%s of b offered to %q (%s), want a shared pool operator", s.customer, op, st)
		}
		if evs := e.events(s.key()); len(evs) == 0 || evs[0].TeamQueue {
			t.Errorf("%s of b events %s, want the shared queue", s.customer, types(evs))
		}
	}
	if _, err := e.svc.CheckTeams(ctx); err != nil {
		t.Fatal(err)
	}
	if c.teamActive(b) {
		t.Fatal("company b got a team at a second check without room")
	}
}

func testCapMoreOnline(t *testing.T, e *env) {
	c := newCapEnv(e)
	a, b := company(1), company(2)
	c.onlineOps(0, 10) // cap 5
	c.open(a, 24)
	c.open(b, 20)
	// a (24, size 6) first: it gets 5. b: no room.
	wantTeam(t, c.check(), TeamResult{Formed: 1, Moved: 24, CapForm: 1, CapGrow: 1})
	if len(c.members(a)) != 5 || c.teamActive(b) {
		t.Fatalf("members a %v b %v", c.members(a), c.members(b))
	}
	// Four more online: 14, cap 7, room 2. a is checked first, wants one
	// more and gets it; b then has room for 1, less than the minimum 2.
	c.onlineOps(10, 14)
	wantTeam(t, c.check(), TeamResult{Resized: 1, CapForm: 1})
	if len(c.members(a)) != 6 || c.teamActive(b) {
		t.Fatalf("members a %v b %v", c.members(a), c.members(b))
	}
	// Two more: 16, cap 8, room 2. b forms with 2 of its 5.
	c.onlineOps(14, 16)
	wantTeam(t, c.check(), TeamResult{Formed: 1, Moved: 20, CapGrow: 1})
	if got := c.members(b); len(got) != 2 {
		t.Fatalf("members b = %v, want 2", got)
	}
	c.wantCounts(8, 8)
	if evs := e.events(event.CompanyKey(b)); len(evs) != 1 || evs[0].Type != event.TypeTeamFormed || len(evs[0].Members) != 2 {
		t.Errorf("team events of b = %+v", evs)
	}
}

func testCapAfterRelease(t *testing.T, e *env) {
	c := newCapEnv(e)
	a, b := company(1), company(2)
	c.onlineOps(0, 10) // cap 5
	c.open(a, 20)
	wantTeam(t, c.check(), TeamResult{Formed: 1, Moved: 20})
	c.open(b, 20)
	wantTeam(t, c.check(), TeamResult{CapForm: 1})

	// a drops to 7 active: below the release load, size 2. b (20) is
	// checked before a (7) and still finds no room; a shrinks to 2.
	c.close(a, 13)
	wantTeam(t, c.check(), TeamResult{Resized: 1, CapForm: 1})
	if got := c.members(a); len(got) != 2 {
		t.Fatalf("members a after shrink = %v, want 2", got)
	}
	// The next check: room 3 for b, size 5.
	wantTeam(t, c.check(), TeamResult{Formed: 1, Moved: 20, CapGrow: 1})
	if got := c.members(b); len(got) != 3 {
		t.Fatalf("members b = %v, want 3", got)
	}
	c.wantCounts(5, 5)

	// a is released after 15 minutes below 8, and its 7 waiting
	// conversations go to the shared queue. b grows into the room at the
	// check after that.
	c.clock.Advance(15 * time.Minute)
	wantTeam(t, c.check(), TeamResult{Released: 1, Moved: 7, CapGrow: 1})
	if len(c.members(a)) != 0 || len(c.members(b)) != 3 {
		t.Fatalf("members a %v b %v", c.members(a), c.members(b))
	}
	wantTeam(t, c.check(), TeamResult{Resized: 1})
	if got := c.members(b); len(got) != 5 {
		t.Fatalf("members b after the release of a = %v, want 5", got)
	}
}

func testCapOrder(t *testing.T, e *env) {
	c := newCapEnv(e)
	// The lower company ID has the lower load, so an order by ID would give
	// it the places.
	low, high := company(1), company(2)
	c.onlineOps(0, 8) // cap 4
	c.open(low, 20)
	c.open(high, 24)
	wantTeam(t, c.check(), TeamResult{Formed: 1, Moved: 24, CapForm: 1, CapGrow: 1})
	if !c.teamActive(high) || c.teamActive(low) {
		t.Fatalf("teams: high %v low %v", c.teamActive(high), c.teamActive(low))
	}
	if got := c.members(high); len(got) != 4 {
		t.Fatalf("members of the busier company = %v, want 4", got)
	}

	// Equal loads: the lower company ID first.
	e2 := newCapEnv(newEnv(t, e.pool, e.svc.config()))
	e2.onlineOps(0, 4) // cap 2
	e2.open(company(4), 20)
	e2.open(company(3), 20)
	wantTeam(t, e2.check(), TeamResult{Formed: 1, Moved: 20, CapForm: 1, CapGrow: 1})
	if !e2.teamActive(company(3)) || e2.teamActive(company(4)) {
		t.Fatal("with equal loads the lower company ID did not come first")
	}
}

func testCapPoolOffline(t *testing.T, e *env) {
	ctx := t.Context()
	c := newCapEnv(e)
	a := company(1)
	c.onlineOps(0, 10) // cap 5
	c.open(a, 20)
	wantTeam(t, c.check(), TeamResult{Formed: 1, Moved: 20})
	members := c.members(a)
	// Four pool operators go offline: 6 online, cap 3, the team holds 5.
	var gone int
	for i := range 10 {
		if gone == 4 || slices.Contains(members, operator(i)) {
			continue
		}
		if _, err := e.svc.Offline(ctx, operator(i)); err != nil {
			t.Fatal(err)
		}
		gone++
	}
	c.wantCounts(5, 3)
	// a should grow to 7. Nobody is taken out and nobody joins.
	c.open(a, 8)
	wantTeam(t, c.check(), TeamResult{CapGrow: 1})
	if got := c.members(a); !slices.Equal(got, members) {
		t.Fatalf("members = %v, want %v", got, members)
	}
	// Shrinking works as before: 12 active, size 3.
	c.close(a, 16)
	wantTeam(t, c.check(), TeamResult{Resized: 1})
	if got := c.members(a); len(got) != 3 {
		t.Fatalf("members after shrink = %v, want 3", got)
	}
	c.wantCounts(3, 3)
}

func testCapOff(t *testing.T, e *env) {
	c := newCapEnv(e)
	a, b := company(1), company(2)
	c.onlineOps(0, 6)
	c.open(a, 20)
	c.open(b, 20)
	// Without the cap the first team takes 5 of 6 and the second gets the
	// last one, below the minimum size, as before the cap.
	wantTeam(t, c.check(), TeamResult{Formed: 2, Moved: 40})
	if len(c.members(a)) != 5 || len(c.members(b)) != 1 {
		t.Fatalf("members a %v b %v", c.members(a), c.members(b))
	}
	c.wantCounts(6, 6)
}

// testCapTwoRouters runs the team check of two routers at once on many
// companies. The team lock and the cap read after it keep the teams within
// the cap: 20 online, cap 10, so exactly two teams of 5 form.
func testCapTwoRouters(t *testing.T, e *env, pool DB) {
	c := newCapEnv(e)
	c.onlineOps(0, 20)
	for i := range 8 {
		c.open(company(i), 20)
	}
	other, err := New(pool, e.svc.config(), e.clock, nil)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make([]TeamResult, 6)
	errs := make([]error, 6)
	for i := range 6 {
		svc := e.svc
		if i%2 == 1 {
			svc = other
		}
		wg.Go(func() { results[i], errs[i] = svc.CheckTeams(t.Context()) })
	}
	wg.Wait()
	var formed int
	for i := range 6 {
		if errs[i] != nil {
			t.Fatalf("CheckTeams %d: %v", i, errs[i])
		}
		formed += results[i].Formed
	}
	if formed != 2 {
		t.Errorf("teams formed = %d, want 2", formed)
	}
	c.wantCounts(10, 10)
}
