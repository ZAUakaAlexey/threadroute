//go:build integration

package router

import (
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// TestIntegrationStandSettings covers the stand settings of decision 0014
// on a real database: the seed of the defaults at start, every outcome of a
// change in the order of the contract, two routers at once, the interval
// with the test clock, and the team rules with changed numbers. Every
// scenario starts on an emptied database. before runs on it before
// LoadSettings; with noLoad the scenario loads the settings itself. Each
// scenario checks the invariants after it.
func TestIntegrationStandSettings(t *testing.T) {
	pool := startDB(t, 16)
	tests := []struct {
		name   string
		before func(t *testing.T, e *standEnv)
		noLoad bool
		fn     func(t *testing.T, e *standEnv)
	}{
		{name: "a start on an empty database stores the defaults", fn: testStandSeed},
		{name: "two routers that start together seed once", noLoad: true, fn: testStandSeedConcurrent},
		{name: "a stored set from before is kept", before: storeOldSet, fn: testStandKeepStored},
		{name: "a stored set without a sim part gets the sim defaults", before: storeTeamsOnly, fn: testStandAddSim},
		{name: "change outcomes in the order of the contract", fn: testStandOrder},
		{name: "reset to defaults is an ordinary change", fn: testStandReset},
		{name: "two routers change at once: exactly one applies", fn: testStandConcurrent},
		{name: "one change per interval, by the test clock", fn: testStandInterval},
		{name: "a restart keeps the stored set", fn: testStandRestart},
		{name: "a second router converges at its next team check", fn: testStandConverge},
		{name: "the change log keeps every entry and answers the newest", fn: testStandLog},
		{name: "a stored row outside the bounds: off", fn: testStandOffStored},
		{name: "lower share: a trim at the next check, then no new team", fn: testStandLowerShare},
		{name: "higher share: growth at the next check", fn: testStandHigherShare},
		{name: "lower form load forms a team", fn: testStandLowerFormLoad},
		{name: "higher form load forms no team and releases none", fn: testStandHigherFormLoad},
		{name: "lower max size shrinks by the normal size check", fn: testStandLowerMaxSize},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &standEnv{capEnv: newCapEnv(newEnv(t, pool, DefaultConfig()))}
			if tt.before != nil {
				tt.before(t, e)
			}
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

// standEnv adds helpers for the stand settings to capEnv.
type standEnv struct {
	*capEnv
}

const testBy = "test-user-0001"

// router returns a second Service on the same database and clock, with
// stand settings loaded.
func (e *standEnv) router(cfg Config) *Service {
	e.t.Helper()
	s, err := New(e.pool, cfg, e.clock, nil)
	if err != nil {
		e.t.Fatal(err)
	}
	if err := s.LoadSettings(e.t.Context()); err != nil {
		e.t.Fatalf("LoadSettings() error = %v", err)
	}
	return s
}

func (e *standEnv) read(s *Service, withChanges bool) opapi.StandState {
	e.t.Helper()
	got, err := s.ReadSettings(e.t.Context(), withChanges)
	if err != nil {
		e.t.Fatalf("ReadSettings() error = %v", err)
	}
	if err := got.Validate(); err != nil {
		e.t.Fatalf("settings %+v: %v", got, err)
	}
	if got.Stand == nil {
		e.t.Fatal("no stand block")
	}
	return *got.Stand
}

func (e *standEnv) put(s *Service, base int64, set opapi.StandSettings) (opapi.SettingsChangeResponse, error) {
	return s.PutSettings(e.t.Context(), opapi.SettingsChangeRequest{Settings: set, BaseVersion: base, By: testBy})
}

// putDone changes the set and requires the result.
func (e *standEnv) putDone(s *Service, base int64, set opapi.StandSettings, want opapi.Result) opapi.StandState {
	e.t.Helper()
	resp, err := e.put(s, base, set)
	if err != nil {
		e.t.Fatalf("PutSettings(base %d) error = %v", base, err)
	}
	if resp.Result != want {
		e.t.Fatalf("PutSettings(base %d) = %s, want %s", base, resp.Result, want)
	}
	if err := resp.Settings.Validate(); err != nil || resp.Settings.Stand == nil {
		e.t.Fatalf("answer %+v: %v", resp.Settings, err)
	}
	return *resp.Settings.Stand
}

// full returns a whole set: the teams part and the sim defaults.
func full(t opapi.TeamSettings) opapi.StandSettings {
	sim := opapi.DefaultSimSettings()
	return opapi.StandSettings{Teams: t, Sim: &sim}
}

// teams returns the default team values with fn applied.
func teams(fn func(*opapi.TeamSettings)) opapi.TeamSettings {
	t := defaultTeams()
	if fn != nil {
		fn(&t)
	}
	return t
}

// wantErr requires an *opapi.SettingsError with the code and returns it.
func wantErr(t *testing.T, err error, code opapi.ErrorCode) *opapi.SettingsError {
	t.Helper()
	se, ok := errors.AsType[*opapi.SettingsError](err)
	if !ok {
		t.Fatalf("error = %v, want a settings error %s", err, code)
	}
	if se.Code != code || se.Status != code.Status() {
		t.Fatalf("error = %+v, want code %s", se, code)
	}
	if err := se.Validate(); err != nil {
		t.Fatalf("error answer %+v: %v", se.SettingsErrorResponse, err)
	}
	return se
}

// rows counts the rows of the settings tables.
func (e *standEnv) rows() (settings, changes int) {
	e.t.Helper()
	err := e.pool.QueryRow(e.t.Context(),
		`SELECT (SELECT count(*) FROM stand_settings), (SELECT count(*) FROM stand_settings_changes)`).Scan(&settings, &changes)
	if err != nil {
		e.t.Fatal(err)
	}
	return settings, changes
}

// insertRow writes the row of stand_settings by hand, as an older router
// left it, and the given log entries.
func (e *standEnv) insertRow(version int64, set opapi.StandSettings, changes ...opapi.SettingsChange) {
	e.t.Helper()
	b, err := json.Marshal(set)
	if err != nil {
		e.t.Fatal(err)
	}
	_, err = e.pool.Exec(e.t.Context(), `INSERT INTO stand_settings (version, current_set, updated_at)
		VALUES ($1, $2, $3)`, version, b, e.clock.Now())
	if err != nil {
		e.t.Fatal(err)
	}
	for _, c := range changes {
		f, err := json.Marshal(c.Fields)
		if err != nil {
			e.t.Fatal(err)
		}
		_, err = e.pool.Exec(e.t.Context(), `INSERT INTO stand_settings_changes (version, changed_at, changed_by, fields)
			VALUES ($1, $2, $3, $4)`, c.Version, time.UnixMicro(c.ChangedAtUS), c.By, f)
		if err != nil {
			e.t.Fatal(err)
		}
	}
}

// oldSet is the set of storeOldSet: every number differs from the
// defaults of the code.
func oldSet() opapi.StandSettings {
	return opapi.StandSettings{
		Teams: opapi.TeamSettings{FormLoad: 30, ReleaseLoad: 5, ReleaseAfterUS: 600_000_000, MinSize: 3, MaxSize: 9, MaxShare: 0.25},
		Sim: &opapi.SimSettings{
			Operators: 120, Capacity: 6, HandlingMedianUS: 30_000_000, ResolveMedianUS: 120_000_000, MissedOfferShare: 0.1,
		},
	}
}

// storeOldSet stores a set at version 2 the way the first version of
// decision 0014 left it: a simulator report at version 1, then a change
// from the console. Its old column defaults_sim is gone since migration
// 00006.
func storeOldSet(t *testing.T, e *standEnv) {
	set := oldSet()
	at := e.clock.Now().Add(-time.Hour).UnixMicro()
	reported := opapi.StandSettings{Teams: set.Teams, Sim: &opapi.SimSettings{
		Operators: 200, Capacity: 4, HandlingMedianUS: 45_000_000, ResolveMedianUS: 180_000_000, MissedOfferShare: 0.03,
	}}
	e.insertRow(2, set,
		opapi.SettingsChange{ChangedAtUS: at, Version: 1, By: opapi.BySimulator,
			Fields: opapi.Diff(opapi.StandSettings{Teams: set.Teams}, reported)},
		opapi.SettingsChange{ChangedAtUS: at + 1, Version: 2, By: testBy, Fields: opapi.Diff(reported, set)},
	)
}

// storeTeamsOnly stores a set at version 1 without a sim part: a change from
// the console before any simulator reported.
func storeTeamsOnly(t *testing.T, e *standEnv) {
	set := opapi.StandSettings{Teams: oldSet().Teams}
	e.insertRow(1, set, opapi.SettingsChange{
		ChangedAtUS: e.clock.Now().Add(-time.Hour).UnixMicro(), Version: 1, By: testBy,
		Fields: opapi.Diff(opapi.StandSettings{Teams: defaultTeams()}, set),
	})
}

// requireSeed checks the stand of a fresh database after the seed.
func requireSeed(t *testing.T, st opapi.StandState) {
	t.Helper()
	def := opapi.DefaultStandSettings()
	if st.Version != 1 || !st.Current.Equal(def) || !st.Defaults.Equal(def) || len(st.Changes) != 1 {
		t.Fatalf("stand = %+v, want the defaults at version 1 with one change", st)
	}
	c := st.Changes[0]
	names := make([]string, len(c.Fields))
	for i, f := range c.Fields {
		names[i] = f.Field
		if f.Old != nil {
			t.Errorf("seed field %s has an old value", f.Field)
		}
	}
	if c.Version != 1 || c.By != opapi.ByDefaults || !slices.Equal(names, opapi.SettingsFields) {
		t.Errorf("seed change = %+v", c)
	}
}

func testStandSeed(t *testing.T, e *standEnv) {
	requireSeed(t, e.read(e.svc, true))
	if s, c := e.rows(); s != 1 || c != 1 {
		t.Errorf("rows = %d, %d; want 1, 1", s, c)
	}
	var lastPut *time.Time
	if err := e.pool.QueryRow(t.Context(), `SELECT last_put_at FROM stand_settings`).Scan(&lastPut); err != nil || lastPut != nil {
		t.Errorf("last_put_at = %v, %v; the seed must not start the interval", lastPut, err)
	}
	snap := e.svc.StandSnapshot()
	if !snap.Enabled || snap.Version != 1 || snap.DefaultsChanges != 1 || snap.ConsoleChanges != 0 ||
		!snap.Current.Equal(opapi.DefaultStandSettings()) {
		t.Errorf("snapshot = %+v", snap)
	}
	if _, err := e.svc.CheckTeams(t.Context()); err != nil {
		t.Fatal(err)
	}

	// A second start changes nothing.
	e.clock.Advance(time.Minute)
	again := e.router(DefaultConfig())
	requireSeed(t, e.read(again, true))
	if s, c := e.rows(); s != 1 || c != 1 {
		t.Errorf("rows after a second start = %d, %d; want 1, 1", s, c)
	}
	if snap := again.StandSnapshot(); snap.DefaultsChanges != 0 || snap.Version != 1 {
		t.Errorf("snapshot of the second router = %+v", snap)
	}
}

func testStandSeedConcurrent(t *testing.T, e *standEnv) {
	const n = 6
	routers := make([]*Service, n)
	var wg sync.WaitGroup
	for i := range routers {
		s, err := New(e.pool, DefaultConfig(), e.clock, nil)
		if err != nil {
			t.Fatal(err)
		}
		routers[i] = s
		wg.Go(func() {
			if err := s.LoadSettings(t.Context()); err != nil {
				t.Errorf("LoadSettings() error = %v", err)
			}
		})
	}
	wg.Wait()
	if s, c := e.rows(); s != 1 || c != 1 {
		t.Errorf("rows = %d, %d; want 1, 1", s, c)
	}
	var seeded int64
	for _, s := range routers {
		snap := s.StandSnapshot()
		seeded += snap.DefaultsChanges
		if !snap.Enabled || snap.Version != 1 {
			t.Errorf("snapshot = %+v", snap)
		}
	}
	if seeded != 1 {
		t.Errorf("seeds counted = %d, want 1", seeded)
	}
	requireSeed(t, e.read(routers[0], true))
}

func testStandKeepStored(t *testing.T, e *standEnv) {
	st := e.read(e.svc, true)
	set := oldSet()
	if st.Version != 2 || !st.Current.Equal(set) || !st.Defaults.Equal(opapi.DefaultStandSettings()) || len(st.Changes) != 2 {
		t.Fatalf("stand = %+v, want the stored set at version 2", st)
	}
	// The old log row of the simulator still reads.
	if st.Changes[1].By != opapi.BySimulator {
		t.Errorf("old change = %+v", st.Changes[1])
	}
	if s, c := e.rows(); s != 1 || c != 2 {
		t.Errorf("rows = %d, %d; want 1, 2", s, c)
	}
	snap := e.svc.StandSnapshot()
	if snap.DefaultsChanges != 0 || snap.Version != 2 || e.svc.config().TeamFormLoad != 30 || e.svc.config().TeamMaxShare != 0.25 {
		t.Errorf("snapshot = %+v, config %+v", snap, e.svc.config())
	}
	// A change goes on from the stored version, and the answers give the
	// defaults of the code.
	e.clock.Advance(opapi.SettingsChangeInterval)
	next := full(set.Teams)
	st = e.putDone(e.svc, 2, next, opapi.ResultDone)
	if st.Version != 3 || !st.Current.Equal(next) || !st.Defaults.Equal(opapi.DefaultStandSettings()) {
		t.Errorf("stand after a change = %+v", st)
	}
}

func testStandAddSim(t *testing.T, e *standEnv) {
	st := e.read(e.svc, true)
	if st.Version != 2 || st.Current.Teams != oldSet().Teams || st.Current.Sim == nil ||
		*st.Current.Sim != opapi.DefaultSimSettings() || !st.Defaults.Equal(opapi.DefaultStandSettings()) || len(st.Changes) != 2 {
		t.Fatalf("stand = %+v, want the teams part kept and the sim defaults added at version 2", st)
	}
	c := st.Changes[0]
	if c.Version != 2 || c.By != opapi.ByDefaults || len(c.Fields) != 5 || c.Fields[0].Field != opapi.FieldSimOperators ||
		c.Fields[0].Old != nil || c.Fields[0].New != opapi.DefaultSimOperators {
		t.Errorf("change = %+v", c)
	}
	if snap := e.svc.StandSnapshot(); snap.DefaultsChanges != 1 || snap.Version != 2 {
		t.Errorf("snapshot = %+v", snap)
	}
	// A second start changes nothing.
	e.router(DefaultConfig())
	if s, c := e.rows(); s != 1 || c != 2 {
		t.Errorf("rows = %d, %d; want 1, 2", s, c)
	}
}

func testStandOrder(t *testing.T, e *standEnv) {
	form30 := full(teams(func(t *opapi.TeamSettings) { t.FormLoad = 30 }))

	// 3 before 4: invalid fields with a wrong base version.
	bad := full(teams(func(t *opapi.TeamSettings) { t.FormLoad = 1; t.MaxShare = 2 }))
	_, err := e.put(e.svc, 5, bad)
	se := wantErr(t, err, opapi.CodeInvalidRequest)
	if len(se.Fields) != 2 || se.Fields[0].Field != opapi.FieldTeamFormLoad || se.Fields[1].Field != opapi.FieldTeamMaxShare {
		t.Errorf("fields = %+v", se.Fields)
	}
	// The label is checked with the fields, and the labels of the router
	// are reserved.
	for _, by := range []string{opapi.ByDefaults, opapi.BySimulator} {
		_, err = e.svc.PutSettings(t.Context(), opapi.SettingsChangeRequest{Settings: form30, BaseVersion: 1, By: by})
		if se := wantErr(t, err, opapi.CodeInvalidRequest); len(se.Fields) != 1 || se.Fields[0].Field != opapi.FieldBy {
			t.Errorf("by %q: fields = %+v", by, se.Fields)
		}
	}

	// 4 before 5: a wrong base version with a sim part that is missing.
	_, err = e.put(e.svc, 0, opapi.StandSettings{Teams: form30.Teams})
	se = wantErr(t, err, opapi.CodeSettingsConflict)
	if se.Settings == nil || se.Settings.Stand.Version != 1 {
		t.Errorf("conflict settings = %+v", se.Settings)
	}

	// 5 before 6: the stored set has a sim part, so the change needs one.
	_, err = e.put(e.svc, 1, opapi.StandSettings{Teams: defaultTeams()})
	if se := wantErr(t, err, opapi.CodeInvalidRequest); len(se.Fields) != 1 || se.Fields[0].Field != opapi.FieldSim {
		t.Errorf("fields = %+v", se.Fields)
	}

	// 6: the defaults are the current set. Nothing is logged.
	if st := e.putDone(e.svc, 1, opapi.DefaultStandSettings(), opapi.ResultAlreadyDone); st.Version != 1 {
		t.Errorf("version = %d", st.Version)
	}
	if s, c := e.rows(); c != 1 || s != 1 {
		t.Errorf("rows = %d, %d after an unchanged set", s, c)
	}

	// 8: applied.
	st := e.putDone(e.svc, 1, form30, opapi.ResultDone)
	if st.Version != 2 || st.Current.Teams.FormLoad != 30 || st.Defaults.Teams.FormLoad != 20 || len(st.Changes) != 2 {
		t.Fatalf("stand = %+v", st)
	}
	c := st.Changes[0]
	if c.Version != 2 || c.By != testBy || c.ChangedAtUS != e.clock.Now().UnixMicro() || len(c.Fields) != 1 ||
		c.Fields[0].Field != opapi.FieldTeamFormLoad || c.Fields[0].Old == nil || *c.Fields[0].Old != 20 || c.Fields[0].New != 30 {
		t.Errorf("change = %+v", c)
	}
	if got := e.svc.config().TeamFormLoad; got != 30 {
		t.Errorf("form load in use = %d, want 30", got)
	}

	// 6 before 7: the same set again inside the interval.
	e.putDone(e.svc, 2, form30, opapi.ResultAlreadyDone)

	// 4 before 7: a repeat of the applied request is a conflict, and its
	// settings show the set the caller wanted.
	_, err = e.put(e.svc, 1, form30)
	if se := wantErr(t, err, opapi.CodeSettingsConflict); se.Settings.Stand.Version != 2 ||
		!se.Settings.Stand.Current.Equal(form30) || len(se.Settings.Stand.Changes) != 2 {
		t.Errorf("conflict settings = %+v", se.Settings.Stand)
	}

	// 7: too soon.
	form40 := full(teams(func(t *opapi.TeamSettings) { t.FormLoad = 40 }))
	_, err = e.put(e.svc, 2, form40)
	if se := wantErr(t, err, opapi.CodeSettingsTooSoon); se.RetryAfterMS != 10_000 {
		t.Errorf("retry after = %d ms, want 10000", se.RetryAfterMS)
	}
	e.clock.Advance(opapi.SettingsChangeInterval)
	if st := e.putDone(e.svc, 2, form40, opapi.ResultDone); st.Version != 3 || len(st.Changes) != 3 || st.Changes[0].Version != 3 {
		t.Errorf("stand = %+v", st)
	}
	if snap := e.svc.StandSnapshot(); snap.ConsoleChanges != 2 || snap.DefaultsChanges != 1 || snap.Version != 3 {
		t.Errorf("snapshot = %+v", snap)
	}
}

// testStandReset changes both parts, then sends the defaults of the answer
// back as an ordinary change, as "Reset to defaults" of the console does.
func testStandReset(t *testing.T, e *standEnv) {
	changed := oldSet()
	st := e.putDone(e.svc, 1, changed, opapi.ResultDone)
	if st.Version != 2 || !st.Current.Equal(changed) {
		t.Fatalf("stand = %+v", st)
	}
	e.clock.Advance(opapi.SettingsChangeInterval)
	st = e.putDone(e.svc, st.Version, st.Defaults, opapi.ResultDone)
	def := opapi.DefaultStandSettings()
	if st.Version != 3 || !st.Current.Equal(def) || !st.Defaults.Equal(def) {
		t.Errorf("stand after the reset = %+v, want the defaults of the code", st)
	}
	if c := st.Changes[0]; c.By != testBy || len(c.Fields) != len(opapi.SettingsFields) {
		t.Errorf("reset change = %+v", c)
	}
	if got := e.svc.config(); teamsOf(got) != opapi.DefaultTeamSettings() {
		t.Errorf("team numbers in use = %+v", teamsOf(got))
	}
}

func testStandConcurrent(t *testing.T, e *standEnv) {
	cfg := DefaultConfig()
	routers := []*Service{e.svc, e.router(cfg), e.router(cfg)}
	const rounds, perRouter = 6, 3
	for round := range rounds {
		e.clock.Advance(opapi.SettingsChangeInterval)
		base := int64(round) + 1
		var (
			mu       sync.Mutex
			done     int
			conflict int
			wg       sync.WaitGroup
		)
		for i, s := range routers {
			for j := range perRouter {
				set := full(teams(func(t *opapi.TeamSettings) {
					t.FormLoad = 100 + round*10 + i*perRouter + j
				}))
				wg.Go(func() {
					_, err := e.put(s, base, set)
					mu.Lock()
					defer mu.Unlock()
					switch se, ok := errors.AsType[*opapi.SettingsError](err); {
					case err == nil:
						done++
					case ok && se.Code == opapi.CodeSettingsConflict:
						conflict++
					default:
						t.Errorf("round %d: PutSettings error = %v", round, err)
					}
				})
			}
		}
		wg.Wait()
		if done != 1 || conflict != len(routers)*perRouter-1 {
			t.Fatalf("round %d: %d applied, %d conflicts; want exactly one applied", round, done, conflict)
		}
	}
	if s, c := e.rows(); s != 1 || c != rounds+1 {
		t.Errorf("rows = %d, %d; want 1, %d", s, c, rounds+1)
	}
	var console int64
	for _, s := range routers {
		console += s.StandSnapshot().ConsoleChanges
	}
	if console != rounds {
		t.Errorf("console changes counted = %d, want %d", console, rounds)
	}
	st := e.read(e.svc, true)
	if st.Version != rounds+1 || len(st.Changes) != rounds+1 {
		t.Errorf("stand = %+v", st)
	}
}

func testStandInterval(t *testing.T, e *standEnv) {
	set := func(n int) opapi.StandSettings {
		return full(teams(func(t *opapi.TeamSettings) { t.FormLoad = n }))
	}
	// The seed did not start the interval.
	e.putDone(e.svc, 1, set(30), opapi.ResultDone)
	e.clock.Advance(opapi.SettingsChangeInterval - time.Millisecond)
	_, err := e.put(e.svc, 2, set(31))
	if se := wantErr(t, err, opapi.CodeSettingsTooSoon); se.RetryAfterMS != 1 {
		t.Errorf("retry after = %d ms, want 1", se.RetryAfterMS)
	}
	e.clock.Advance(time.Millisecond)
	e.putDone(e.svc, 2, set(31), opapi.ResultDone)
	// The interval counts from that change again.
	e.clock.Advance(4 * time.Second)
	_, err = e.put(e.svc, 3, set(32))
	if se := wantErr(t, err, opapi.CodeSettingsTooSoon); se.RetryAfterMS != 6000 {
		t.Errorf("retry after = %d ms, want 6000", se.RetryAfterMS)
	}
}

func testStandRestart(t *testing.T, e *standEnv) {
	set := full(teams(func(t *opapi.TeamSettings) { t.FormLoad = 30; t.MaxShare = 0.25 }))
	e.putDone(e.svc, 1, set, opapi.ResultDone)

	// The new process has other team values in its Config: the stored set
	// wins, and the defaults are the ones of the code.
	cfg := DefaultConfig()
	cfg.TeamFormLoad = 25
	again := e.router(cfg)
	if got := again.config(); got.TeamFormLoad != 30 || got.TeamMaxShare != 0.25 {
		t.Errorf("config after the restart = %+v, want the stored set", got)
	}
	st := e.read(again, false)
	if st.Version != 2 || !st.Current.Equal(set) || st.Defaults.Teams.FormLoad != 20 {
		t.Errorf("stand after the restart = %+v", st)
	}
	if c := again.Config().TeamCap(100); c != 25 {
		t.Errorf("team cap of 100 online = %d, want 25", c)
	}
}

func testStandConverge(t *testing.T, e *standEnv) {
	other := e.router(DefaultConfig())
	set := full(teams(func(t *opapi.TeamSettings) { t.MinSize = 3; t.MaxSize = 9 }))
	e.putDone(e.svc, 1, set, opapi.ResultDone)
	if got := other.config(); got.TeamMinSize != 2 || got.TeamMaxSize != 20 {
		t.Fatalf("the other router changed before its team check: %+v", got)
	}
	if _, err := other.CheckTeams(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := other.StandSnapshot(); got.Version != 2 || got.Current.Teams != set.Teams || other.config().TeamMaxSize != 9 {
		t.Errorf("the other router after its team check: %+v", got)
	}
	// The version never goes back: an older row does not replace it.
	other.apply(t.Context(), standRow{version: 1, current: opapi.DefaultStandSettings()})
	if other.config().TeamMaxSize != 9 {
		t.Error("an older version replaced the snapshot")
	}
}

func testStandLog(t *testing.T, e *standEnv) {
	const n = opapi.MaxSettingsChanges + 5
	for i := range n {
		e.clock.Advance(opapi.SettingsChangeInterval)
		set := full(teams(func(t *opapi.TeamSettings) { t.FormLoad = 30 + i }))
		e.putDone(e.svc, int64(i)+1, set, opapi.ResultDone)
	}
	st := e.read(e.svc, true)
	if st.Version != n+1 || len(st.Changes) != opapi.MaxSettingsChanges || st.Changes[0].Version != n+1 ||
		st.Changes[len(st.Changes)-1].Version != n+1-opapi.MaxSettingsChanges+1 {
		t.Errorf("stand: version %d, %d changes from %d", st.Version, len(st.Changes), st.Changes[0].Version)
	}
	if st := e.read(e.svc, false); st.Changes != nil {
		t.Errorf("the plain answer has %d changes", len(st.Changes))
	}
	if _, c := e.rows(); c != n+1 {
		t.Errorf("log rows = %d, want %d", c, n+1)
	}
}

func testStandOffStored(t *testing.T, e *standEnv) {
	ctx := t.Context()
	_, err := e.pool.Exec(ctx, `UPDATE stand_settings SET version = 2, current_set = '{"teams":{"form_load":1}}'`)
	if err != nil {
		t.Fatal(err)
	}
	// A router that starts now does not offer stand settings and runs with
	// the team numbers of its Config, the defaults of the code.
	other := e.router(DefaultConfig())
	if snap := other.StandSnapshot(); snap.Enabled || snap.Version != 0 || other.config().TeamFormLoad != 20 {
		t.Errorf("snapshot = %+v", snap)
	}
	got, err := other.ReadSettings(ctx, true)
	if err != nil || got.Stand != nil {
		t.Fatalf("ReadSettings() = %+v, %v; want no stand block", got.Stand, err)
	}
	_, err = e.put(other, 2, full(defaultTeams()))
	wantErr(t, err, opapi.CodeSettingsOff)
	// Off comes before the fields.
	_, err = e.put(other, 2, opapi.StandSettings{})
	wantErr(t, err, opapi.CodeSettingsOff)
	if _, err := other.CheckTeams(ctx); err != nil {
		t.Fatal(err)
	}
	// Nothing was seeded over the row.
	if s, c := e.rows(); s != 1 || c != 1 {
		t.Errorf("rows = %d, %d", s, c)
	}
	// The running router keeps its set and its team check goes on.
	if _, err := e.svc.CheckTeams(ctx); err != nil {
		t.Fatal(err)
	}
	if e.svc.config().TeamFormLoad != 20 {
		t.Error("the running router took the stored row")
	}
}

func testStandLowerShare(t *testing.T, e *standEnv) {
	a, b := company(1), company(2)
	e.onlineOps(0, 20) // cap 10
	e.open(a, 20)
	wantTeam(t, e.check(), TeamResult{Formed: 1, Moved: 20})
	before := e.members(a)
	if len(before) != 5 {
		t.Fatalf("members = %v, want 5", before)
	}
	// Cap 1: the team holds more than the cap.
	e.putDone(e.svc, 1, full(teams(func(t *opapi.TeamSettings) { t.MaxShare = 0.05 })), opapi.ResultDone)
	e.wantCounts(5, 1)
	// The load would give 10 members, and a second company reaches the
	// threshold. The trim (decision 0009, "A trim after the cap was
	// lowered") has a room of 1, below the min size, so it releases the
	// team; then neither company can form one, and the 40 waiting
	// conversations of a go to the shared queue.
	e.open(a, 20)
	e.open(b, 20)
	wantTeam(t, e.check(), TeamResult{Released: 1, Moved: 40, CapForm: 2, Trims: 1, TrimOperators: 5, TrimReleased: 1})
	if got := e.members(a); len(got) != 0 || e.teamActive(a) {
		t.Errorf("members = %v, want the team released", got)
	}
	if e.teamActive(b) {
		t.Error("company b got a team above the cap")
	}
	e.wantCounts(0, 1)
}

func testStandHigherShare(t *testing.T, e *standEnv) {
	a := company(1)
	e.putDone(e.svc, 1, full(teams(func(t *opapi.TeamSettings) { t.MaxShare = 0.1 })), opapi.ResultDone)
	e.onlineOps(0, 20) // cap 2
	e.open(a, 40)      // size 10
	wantTeam(t, e.check(), TeamResult{Formed: 1, Moved: 40, CapGrow: 1})
	if got := e.members(a); len(got) != 2 {
		t.Fatalf("members = %v, want 2", got)
	}
	e.clock.Advance(opapi.SettingsChangeInterval)
	e.putDone(e.svc, 2, full(teams(nil)), opapi.ResultDone) // share 0.5, cap 10
	if got := e.members(a); len(got) != 2 {
		t.Fatalf("members before the next check = %v", got)
	}
	wantTeam(t, e.check(), TeamResult{Resized: 1})
	if got := e.members(a); len(got) != 10 {
		t.Errorf("members after the check = %v, want 10", got)
	}
}

func testStandLowerFormLoad(t *testing.T, e *standEnv) {
	a := company(1)
	e.onlineOps(0, 20)
	e.open(a, 10)
	wantTeam(t, e.check(), TeamResult{})
	e.putDone(e.svc, 1, full(teams(func(t *opapi.TeamSettings) {
		t.FormLoad = 10
		t.ReleaseLoad = 4
	})), opapi.ResultDone)
	wantTeam(t, e.check(), TeamResult{Formed: 1, Moved: 10})
	if got := e.members(a); len(got) != 3 {
		t.Errorf("members = %v, want 3", got)
	}
}

func testStandHigherFormLoad(t *testing.T, e *standEnv) {
	a, b := company(1), company(2)
	e.onlineOps(0, 40)
	e.open(a, 25)
	wantTeam(t, e.check(), TeamResult{Formed: 1, Moved: 25})
	e.putDone(e.svc, 1, full(teams(func(t *opapi.TeamSettings) { t.FormLoad = 30 })), opapi.ResultDone)
	e.open(b, 25)
	wantTeam(t, e.check(), TeamResult{})
	if e.teamActive(b) {
		t.Error("company b got a team below the new form load")
	}
	// The release rule is unchanged: company a keeps its team.
	if !e.teamActive(a) || len(e.members(a)) != 7 {
		t.Errorf("company a: active %v, members %v", e.teamActive(a), e.members(a))
	}
}

func testStandLowerMaxSize(t *testing.T, e *standEnv) {
	a := company(1)
	e.onlineOps(0, 20)
	e.open(a, 20)
	wantTeam(t, e.check(), TeamResult{Formed: 1, Moved: 20})
	e.putDone(e.svc, 1, full(teams(func(t *opapi.TeamSettings) { t.MaxSize = 3 })), opapi.ResultDone)
	wantTeam(t, e.check(), TeamResult{Resized: 1})
	if got := e.members(a); len(got) != 3 {
		t.Errorf("members = %v, want 3", got)
	}
}
