//go:build integration

package router

import (
	"slices"
	"sync"
	"testing"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// TestIntegrationTeamTrim covers the trim after a lowered share (decision
// 0009, "A trim after the cap was lowered") on a real database. Every
// scenario runs with the stand settings on (the defaults of the code:
// capacity 4, a team forms at 20 active conversations, size 2 to 20, share
// one half) and changes them only with PutSettings, within the bounds of
// opapi, unless it says otherwise. Each scenario starts on an emptied
// database and checks the invariants after it.
func TestIntegrationTeamTrim(t *testing.T) {
	pool := startDB(t, 16)
	tests := []struct {
		name   string
		cfg    func(*Config)
		noLoad bool
		fn     func(t *testing.T, e *standEnv)
	}{
		{name: "a lowered share trims at the next check in load order", fn: testTrimOrder},
		{name: "the request survives a restart", fn: testTrimRestart},
		{name: "two routers trim once", fn: testTrimTwoRouters},
		{name: "offline members keep their place, and coming back is no trigger", fn: testTrimOffline},
		{name: "reset to defaults that lowers the share trims", fn: testTrimReset},
		{name: "a request with nothing to trim is cleared", fn: testTrimNothing},
		{name: "pool operators offline, other fields and a higher share: no trim", fn: testTrimNoTrigger},
		{name: "the seed of the defaults does not trim", cfg: func(c *Config) { c.TeamMaxShare = 1 }, noLoad: true,
			fn: testTrimSeed},
		{name: "a router without stand settings never trims", fn: testTrimSettingsOff},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			if tt.cfg != nil {
				tt.cfg(&cfg)
			}
			e := &standEnv{capEnv: newCapEnv(newEnv(t, pool, cfg))}
			if !tt.noLoad {
				if err := e.svc.LoadSettings(t.Context()); err != nil {
					t.Fatalf("LoadSettings() error = %v", err)
				}
			}
			tt.fn(t, e)
			requireInvariants(t, pool)
		})
	}
}

// share returns the whole default set with another team share.
func share(v float64) opapi.StandSettings {
	return full(teams(func(t *opapi.TeamSettings) { t.MaxShare = v }))
}

// trimPending returns the version of the stored trim request, or nil.
func (e *standEnv) trimPending() *int64 {
	e.t.Helper()
	var v *int64
	if err := e.pool.QueryRow(e.t.Context(), `SELECT trim_version FROM stand_settings`).Scan(&v); err != nil {
		e.t.Fatal(err)
	}
	return v
}

// wantNoTrimRequest fails when a trim request is stored.
func (e *standEnv) wantNoTrimRequest(what string) {
	e.t.Helper()
	if v := e.trimPending(); v != nil {
		e.t.Fatalf("%s: trim request for version %d, want none", what, *v)
	}
}

// waitingInTeamQueue counts the waiting conversations of a company in its
// team queue.
func (e *standEnv) waitingInTeamQueue(co string) int {
	e.t.Helper()
	var n int
	err := e.pool.QueryRow(e.t.Context(),
		`SELECT count(*) FROM conversations WHERE company_id = $1 AND state = 'waiting' AND team_queue`, co).Scan(&n)
	if err != nil {
		e.t.Fatal(err)
	}
	return n
}

// liveOffers maps each conversation of the companies that has a live offer
// to its operator.
func (e *standEnv) liveOffers(cos ...string) map[string]string {
	e.t.Helper()
	out := map[string]string{}
	for _, co := range cos {
		for _, s := range e.streams[co] {
			if _, op, st := e.liveOffer(s); st != "" {
				out[s.customer] = op
			}
		}
	}
	return out
}

// twoTeams forms two teams of 5 on 20 online operators (cap 10): a and b
// with 20 active conversations each, a first by company ID.
func (e *standEnv) twoTeams() (a, b string) {
	e.t.Helper()
	a, b = company(1), company(2)
	e.onlineOps(0, 20)
	e.open(a, 20)
	e.open(b, 20)
	wantTeam(e.t, e.check(), TeamResult{Formed: 2, Moved: 40})
	e.wantCounts(10, 10)
	return a, b
}

func testTrimOrder(t *testing.T, e *standEnv) {
	a, b, c := company(1), company(2), company(3)
	e.onlineOps(0, 30) // cap 15
	e.open(a, 28)      // size 7
	e.open(b, 24)      // size 6
	e.open(c, 20)      // size 5, gets the 2 left under the cap
	wantTeam(t, e.check(), TeamResult{Formed: 3, Moved: 72, CapGrow: 1})
	if r := e.assign(); r.Offered != 72 {
		t.Fatalf("assign = %+v, want 72 offers", r)
	}
	// Three more conversations of c wait in its team queue.
	e.open(c, 3)
	if n := e.waitingInTeamQueue(c); n != 3 {
		t.Fatalf("waiting in the team queue of c = %d, want 3", n)
	}
	membersB, membersC := e.members(b), e.members(c)
	offers := e.liveOffers(a, b, c)
	if len(offers) != 72 {
		t.Fatalf("live offers = %d, want 72", len(offers))
	}
	// The members that will leave accept their offers, so they hold
	// conversations. An accepted offer keeps its slot, so the loads and
	// the choice of who leaves do not change.
	leaving := append(slices.Clone(membersB[:4]), membersC...)
	var accepted []*convStream
	for _, co := range []string{b, c} {
		for _, s := range e.streams[co] {
			id, op, _ := e.liveOffer(s)
			if !slices.Contains(leaving, op) {
				continue
			}
			if res, err := e.svc.Accept(t.Context(), op, id); err != nil || res != opapi.ResultDone {
				t.Fatalf("Accept(%s, %s) = %v, %v", op, id, res, err)
			}
			accepted = append(accepted, s)
		}
	}
	if len(accepted) != 6*4 {
		t.Fatalf("accepted = %d, want 24", len(accepted))
	}

	st := e.putDone(e.svc, 1, share(0.3), opapi.ResultDone) // cap 9
	if v := e.trimPending(); v == nil || *v != st.Version {
		t.Fatalf("trim request = %v, want version %d", v, st.Version)
	}
	// The trim: a (28) keeps its 7, b (24) keeps the 2 the room allows,
	// c (23) is released. Then the normal check: b cannot grow, c cannot
	// form again, and its 3 waiting conversations go to the shared queue.
	wantTeam(t, e.check(), TeamResult{
		Resized: 1, Released: 1, Moved: 3, CapForm: 1, CapGrow: 1,
		Trims: 1, TrimOperators: 6, TrimReleased: 1,
	})
	e.wantNoTrimRequest("after the trim")
	e.wantCounts(9, 9)
	if got := e.members(a); len(got) != 7 {
		t.Errorf("members of a = %v, want 7", got)
	}
	// All six members of b hold 4 offers, so the lowest IDs leave first.
	if got := e.members(b); !slices.Equal(got, membersB[4:]) {
		t.Errorf("members of b = %v, want %v", got, membersB[4:])
	}
	if got := e.members(c); len(got) != 0 || e.teamActive(c) {
		t.Errorf("c: active %v, members %v; want released", e.teamActive(c), got)
	}

	// The events: team_resized for b with the members that stay, and
	// team_released for c with the new reason.
	evB := e.events(event.CompanyKey(b))
	wantTypes(t, evB, "team_formed,team_resized")
	if got := evB[1].Members; !slices.Equal(got, membersB[4:]) || evB[1].ActiveLoad != 24 {
		t.Errorf("team_resized of b = %+v", evB[1])
	}
	evC := e.events(event.CompanyKey(c))
	wantTypes(t, evC, "team_formed,team_released")
	if rel := evC[1]; rel.ReleaseReason != event.TeamCapLowered || !slices.Equal(rel.Members, membersC) || rel.ActiveLoad != 23 {
		t.Errorf("team_released of c = %+v", rel)
	}
	for _, op := range leaving {
		evs := e.opEvents(op)
		if last := evs[len(evs)-1]; last.Type != event.TypeOperatorUpdated || last.DedicatedCompanyID != "" {
			t.Errorf("last operator event of %s = %+v, want operator_updated without a company", op, last)
		}
	}

	// Operators keep their offers and assignments, and the waiting
	// conversations are in the shared queue now.
	if got := e.liveOffers(a, b, c); len(got) != len(offers) {
		t.Errorf("live offers after the trim = %d, want %d", len(got), len(offers))
	} else {
		for cu, op := range offers {
			if got[cu] != op {
				t.Errorf("offer of %s moved from %s to %s", cu, op, got[cu])
			}
		}
	}
	// The operators who left keep their conversations.
	for _, s := range accepted {
		if _, op, st := e.liveOffer(s); st != offerAccepted || op != offers[s.customer] || e.convState(s) != stateAssigned {
			t.Errorf("%s: offer %s of %s, conversation %s; want still assigned to %s", s.customer, st, op, e.convState(s), offers[s.customer])
		}
	}
	if n := e.waitingInTeamQueue(c); n != 0 {
		t.Errorf("waiting in the team queue of c after the check = %d, want 0", n)
	}
	for _, s := range e.streams[c][20:] {
		evs := e.events(s.key())
		if last := evs[len(evs)-1]; last.Type != event.TypeQueued || last.QueueReason != event.QueueTeamReleased || last.TeamQueue {
			t.Errorf("last event of %s = %s", s.customer, types(evs))
		}
	}

	// A second check trims nothing more.
	wantTeam(t, e.check(), TeamResult{CapForm: 1, CapGrow: 1})
	e.wantCounts(9, 9)
}

func testTrimRestart(t *testing.T, e *standEnv) {
	a, b := e.twoTeams()
	e.putDone(e.svc, 1, share(0.25), opapi.ResultDone) // cap 5

	// A new router on the same database runs the check: the request is
	// stored, not held in memory.
	again := e.router(DefaultConfig())
	r, err := again.CheckTeams(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// a and b have the same load: a comes first by company ID.
	wantTeam(t, r, TeamResult{Released: 1, Moved: 20, CapForm: 1, Trims: 1, TrimOperators: 5, TrimReleased: 1})
	if len(e.members(a)) != 5 || e.teamActive(b) {
		t.Errorf("members a %v, b active %v", e.members(a), e.teamActive(b))
	}
	e.wantNoTrimRequest("after the trim")
	// The first router finds nothing more to trim.
	wantTeam(t, e.check(), TeamResult{CapForm: 1})
}

func testTrimTwoRouters(t *testing.T, e *standEnv) {
	e.twoTeams()
	e.putDone(e.svc, 1, share(0.25), opapi.ResultDone)
	other := e.router(DefaultConfig())
	const n = 6
	results := make([]TeamResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		svc := e.svc
		if i%2 == 1 {
			svc = other
		}
		wg.Go(func() { results[i], errs[i] = svc.CheckTeams(t.Context()) })
	}
	wg.Wait()
	var sum TeamResult
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("CheckTeams %d: %v", i, errs[i])
		}
		sum.Trims += results[i].Trims
		sum.TrimOperators += results[i].TrimOperators
		sum.TrimReleased += results[i].TrimReleased
		sum.Released += results[i].Released
	}
	if want := (TeamResult{Released: 1, Trims: 1, TrimOperators: 5, TrimReleased: 1}); sum != want {
		t.Errorf("sum of the checks = %+v, want %+v", sum, want)
	}
	e.wantNoTrimRequest("after the checks")
	e.wantCounts(5, 5)
}

func testTrimOffline(t *testing.T, e *standEnv) {
	ctx := t.Context()
	a, b := company(1), company(2)
	e.onlineOps(0, 20) // cap 10
	e.open(a, 28)      // size 7
	e.open(b, 20)      // gets the 3 left
	wantTeam(t, e.check(), TeamResult{Formed: 2, Moved: 48, CapGrow: 1})
	membersA, membersB := e.members(a), e.members(b)
	// The member of a with the lowest ID goes offline. With every load at
	// 0, it is the one pickLeave would choose first among all members.
	offA, offB := membersA[0], membersB[0]
	for _, op := range []string{offA, offB} {
		if _, err := e.svc.Offline(ctx, op); err != nil {
			t.Fatal(err)
		}
	}
	// 18 online, a has 6 online members and b 2. Cap 4 after the change.
	e.putDone(e.svc, 1, share(0.25), opapi.ResultDone)
	wantTeam(t, e.check(), TeamResult{
		Resized: 1, Released: 1, Moved: 20, CapForm: 1, CapGrow: 1,
		Trims: 1, TrimOperators: 4, TrimReleased: 1,
	})
	// Two online members of a leave: the lowest IDs among the online ones.
	// The offline member keeps its place.
	want := append([]string{offA}, membersA[3:]...)
	if got := e.members(a); !slices.Equal(got, want) {
		t.Errorf("members of a = %v, want %v", got, want)
	}
	// The released team loses its offline member too.
	if got := e.members(b); len(got) != 0 {
		t.Errorf("members of b = %v, want none", got)
	}
	if rel := e.events(event.CompanyKey(b)); rel[len(rel)-1].ReleaseReason != event.TeamCapLowered ||
		!slices.Equal(rel[len(rel)-1].Members, membersB) {
		t.Errorf("team_released of b = %+v", rel[len(rel)-1])
	}
	e.wantCounts(4, 4)

	// The offline member comes back: the teams hold 5 of a cap of 4. That
	// is no trigger; the teams only stop growing.
	e.online(offA)
	e.wantCounts(5, 4)
	wantTeam(t, e.check(), TeamResult{CapForm: 1, CapGrow: 1})
	if got := e.members(a); !slices.Equal(got, want) {
		t.Errorf("members of a after the return = %v, want %v", got, want)
	}
}

func testTrimReset(t *testing.T, e *standEnv) {
	a, b := company(1), company(2)
	e.putDone(e.svc, 1, share(1), opapi.ResultDone) // the cap is off
	e.wantNoTrimRequest("after a higher share")
	e.onlineOps(0, 20)
	e.open(a, 40)
	e.open(b, 40)
	wantTeam(t, e.check(), TeamResult{Formed: 2, Moved: 80})
	e.wantCounts(20, 20)

	// "Reset to defaults" sends the defaults of the answer back: the share
	// goes from 1 to one half, cap 10.
	e.clock.Advance(opapi.SettingsChangeInterval)
	st := e.read(e.svc, false)
	e.putDone(e.svc, st.Version, st.Defaults, opapi.ResultDone)
	if e.trimPending() == nil {
		t.Fatal("no trim request after a reset that lowered the share")
	}
	wantTeam(t, e.check(), TeamResult{Released: 1, Moved: 40, CapForm: 1, Trims: 1, TrimOperators: 10, TrimReleased: 1})
	if len(e.members(a)) != 10 || e.teamActive(b) || e.waitingInTeamQueue(b) != 0 {
		t.Errorf("members a %v, b active %v", e.members(a), e.teamActive(b))
	}
}

func testTrimNothing(t *testing.T, e *standEnv) {
	a := company(1)
	e.onlineOps(0, 20)
	e.open(a, 20)
	wantTeam(t, e.check(), TeamResult{Formed: 1, Moved: 20})
	e.putDone(e.svc, 1, share(0.3), opapi.ResultDone) // cap 6, the team holds 5
	if e.trimPending() == nil {
		t.Fatal("no trim request after a lower share")
	}
	wantTeam(t, e.check(), TeamResult{})
	e.wantNoTrimRequest("after the check")
	if got := e.members(a); len(got) != 5 {
		t.Errorf("members = %v, want 5", got)
	}
}

func testTrimNoTrigger(t *testing.T, e *standEnv) {
	ctx := t.Context()
	a := company(1)
	e.onlineOps(0, 20) // cap 10
	e.open(a, 40)      // size 10
	wantTeam(t, e.check(), TeamResult{Formed: 1, Moved: 40})
	members := e.members(a)
	// Four pool operators go offline: 16 online, cap 8, the team holds 10.
	var gone int
	for i := 0; gone < 4; i++ {
		if slices.Contains(members, operator(i)) {
			continue
		}
		if _, err := e.svc.Offline(ctx, operator(i)); err != nil {
			t.Fatal(err)
		}
		gone++
	}
	e.wantCounts(10, 8)
	wantTeam(t, e.check(), TeamResult{})
	e.wantNoTrimRequest("after pool operators went offline")

	// A change of another field keeps the share.
	e.putDone(e.svc, 1, full(teams(func(t *opapi.TeamSettings) { t.FormLoad = 25 })), opapi.ResultDone)
	e.wantNoTrimRequest("after a change of the form load")
	wantTeam(t, e.check(), TeamResult{})

	// A higher share is still below the teams: cap 9.
	e.clock.Advance(opapi.SettingsChangeInterval)
	e.putDone(e.svc, 2, full(teams(func(t *opapi.TeamSettings) { t.FormLoad = 25; t.MaxShare = 0.6 })), opapi.ResultDone)
	e.wantNoTrimRequest("after a higher share")
	e.wantCounts(10, 9)
	wantTeam(t, e.check(), TeamResult{})
	if got := e.members(a); !slices.Equal(got, members) {
		t.Errorf("members = %v, want %v", got, members)
	}
}

// testTrimSeed forms teams with a router that has no stand settings and
// the cap off, then starts a router that seeds the defaults (share one
// half) and runs its check.
func testTrimSeed(t *testing.T, e *standEnv) {
	a, b := company(1), company(2)
	e.onlineOps(0, 20)
	e.open(a, 40)
	e.open(b, 40)
	wantTeam(t, e.check(), TeamResult{Formed: 2, Moved: 80})
	if s, _ := e.rows(); s != 0 {
		t.Fatalf("settings rows = %d before the seed, want 0", s)
	}
	seeded := e.router(DefaultConfig())
	if snap := seeded.StandSnapshot(); snap.Version != 1 || snap.Current.Teams.MaxShare != 0.5 {
		t.Fatalf("snapshot after the seed = %+v", snap)
	}
	e.wantNoTrimRequest("after the seed")
	r, err := seeded.CheckTeams(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	wantTeam(t, r, TeamResult{})
	got, err := seeded.Counts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got.TeamOnline != 20 || got.TeamCap != 10 {
		t.Errorf("counts: team online %d, cap %d; want 20 above the cap 10", got.TeamOnline, got.TeamCap)
	}
}

func testTrimSettingsOff(t *testing.T, e *standEnv) {
	a, b := e.twoTeams()
	e.putDone(e.svc, 1, share(0.25), opapi.ResultDone)
	off, err := New(e.pool, DefaultConfig(), e.clock, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := off.CheckTeams(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// The router without stand settings runs with share one half: no trim,
	// and nothing changes.
	wantTeam(t, r, TeamResult{})
	if e.trimPending() == nil || len(e.members(a)) != 5 || len(e.members(b)) != 5 {
		t.Fatalf("request %v, members a %v b %v; want the request kept and both teams", e.trimPending(), e.members(a), e.members(b))
	}
	// The router with stand settings trims.
	wantTeam(t, e.check(), TeamResult{Released: 1, Moved: 20, CapForm: 1, Trims: 1, TrimOperators: 5, TrimReleased: 1})
}
