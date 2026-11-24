//go:build integration

package router

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// TestIntegrationStandSettings covers the stand settings of decision 0014
// on a real database: the start, every outcome of a change in the order of
// the contract, two routers at once, the interval with the test clock, the
// simulator report, and the team rules with changed numbers. Every
// scenario starts on an emptied database with the default rules, unless it
// says otherwise, and checks the invariants after it.
func TestIntegrationStandSettings(t *testing.T) {
	pool := startDB(t, 16)
	tests := []struct {
		name string
		cfg  func(*Config)
		fn   func(t *testing.T, e *standEnv)
	}{
		{name: "start without a stored set writes nothing", fn: testStandStart},
		{name: "change outcomes in the order of the contract", fn: testStandOrder},
		{name: "two routers change at once: exactly one applies", fn: testStandConcurrent},
		{name: "one change per interval, by the test clock", fn: testStandInterval},
		{name: "a restart keeps the stored set", fn: testStandRestart},
		{name: "a restart at version 0 takes the new environment", fn: testStandRestartVersion0},
		{name: "simulator reports", fn: testStandSimDefaults},
		{name: "a second router converges at its next team check", fn: testStandConverge},
		{name: "the change log keeps every entry and answers the newest", fn: testStandLog},
		{name: "environment outside the bounds: off", cfg: func(c *Config) { c.TeamMaxShare = 0.01 }, fn: testStandOffEnv},
		{name: "a stored row outside the bounds: off", fn: testStandOffStored},
		{name: "lower share: no growth and no forced exit", fn: testStandLowerShare},
		{name: "higher share: growth at the next check", cfg: func(c *Config) { c.TeamMaxShare = 0.1 }, fn: testStandHigherShare},
		{name: "lower form load forms a team", fn: testStandLowerFormLoad},
		{name: "higher form load forms no team and releases none", fn: testStandHigherFormLoad},
		{name: "lower max size shrinks by the normal size check", fn: testStandLowerMaxSize},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			if tt.cfg != nil {
				tt.cfg(&cfg)
			}
			e := &standEnv{capEnv: newCapEnv(newEnv(t, pool, cfg))}
			if err := e.svc.LoadSettings(t.Context()); err != nil {
				t.Fatalf("LoadSettings() error = %v", err)
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

func testStandStart(t *testing.T, e *standEnv) {
	st := e.read(e.svc, true)
	env := defaultTeams()
	if st.Version != 0 || st.Current.Teams != env || st.Current.Sim != nil || st.Defaults.Teams != env ||
		st.Defaults.Sim != nil || st.Changes != nil {
		t.Errorf("stand = %+v", st)
	}
	if _, err := e.svc.CheckTeams(t.Context()); err != nil {
		t.Fatal(err)
	}
	if s, c := e.rows(); s != 0 || c != 0 {
		t.Errorf("rows = %d, %d; want none", s, c)
	}
	snap := e.svc.StandSnapshot()
	if !snap.Enabled || snap.Version != 0 || snap.Current.Teams != env {
		t.Errorf("snapshot = %+v", snap)
	}
}

func testStandOrder(t *testing.T, e *standEnv) {
	sim := testSim()
	form30 := opapi.StandSettings{Teams: teams(func(t *opapi.TeamSettings) { t.FormLoad = 30 })}

	// 3 before 4: invalid fields with a wrong base version.
	bad := opapi.StandSettings{Teams: teams(func(t *opapi.TeamSettings) { t.FormLoad = 1; t.MaxShare = 2 })}
	_, err := e.put(e.svc, 5, bad)
	se := wantErr(t, err, opapi.CodeInvalidRequest)
	if len(se.Fields) != 2 || se.Fields[0].Field != opapi.FieldTeamFormLoad || se.Fields[1].Field != opapi.FieldTeamMaxShare {
		t.Errorf("fields = %+v", se.Fields)
	}
	// The label is checked with the fields, and "simulator" is reserved.
	_, err = e.svc.PutSettings(t.Context(), opapi.SettingsChangeRequest{Settings: form30, By: opapi.BySimulator})
	if se := wantErr(t, err, opapi.CodeInvalidRequest); len(se.Fields) != 1 || se.Fields[0].Field != opapi.FieldBy {
		t.Errorf("fields = %+v", se.Fields)
	}

	// 4 before 5: a wrong base version with a sim part that must be absent.
	_, err = e.put(e.svc, 1, opapi.StandSettings{Teams: form30.Teams, Sim: &sim})
	se = wantErr(t, err, opapi.CodeSettingsConflict)
	if se.Settings == nil || se.Settings.Stand.Version != 0 {
		t.Errorf("conflict settings = %+v", se.Settings)
	}

	// 5 before 6: the sim part must be absent while none is stored.
	_, err = e.put(e.svc, 0, opapi.StandSettings{Teams: defaultTeams(), Sim: &sim})
	if se := wantErr(t, err, opapi.CodeInvalidRequest); len(se.Fields) != 1 || se.Fields[0].Field != opapi.FieldSim {
		t.Errorf("fields = %+v", se.Fields)
	}

	// 6: the environment set at version 0 is the current set. Nothing is
	// stored.
	if st := e.putDone(e.svc, 0, opapi.StandSettings{Teams: defaultTeams()}, opapi.ResultAlreadyDone); st.Version != 0 {
		t.Errorf("version = %d", st.Version)
	}
	if s, c := e.rows(); c != 0 || s > 1 {
		t.Errorf("rows = %d, %d after an unchanged set", s, c)
	}

	// 8: applied.
	st := e.putDone(e.svc, 0, form30, opapi.ResultDone)
	if st.Version != 1 || st.Current.Teams.FormLoad != 30 || st.Defaults.Teams.FormLoad != 20 || len(st.Changes) != 1 {
		t.Fatalf("stand = %+v", st)
	}
	c := st.Changes[0]
	if c.Version != 1 || c.By != testBy || c.ChangedAtUS != e.clock.Now().UnixMicro() || len(c.Fields) != 1 ||
		c.Fields[0].Field != opapi.FieldTeamFormLoad || c.Fields[0].Old == nil || *c.Fields[0].Old != 20 || c.Fields[0].New != 30 {
		t.Errorf("change = %+v", c)
	}
	if got := e.svc.config().TeamFormLoad; got != 30 {
		t.Errorf("form load in use = %d, want 30", got)
	}

	// 6 before 7: the same set again inside the interval.
	e.putDone(e.svc, 1, form30, opapi.ResultAlreadyDone)

	// 4 before 7: a repeat of the applied request is a conflict, and its
	// settings show the set the caller wanted.
	_, err = e.put(e.svc, 0, form30)
	if se := wantErr(t, err, opapi.CodeSettingsConflict); se.Settings.Stand.Version != 1 ||
		!se.Settings.Stand.Current.Equal(form30) || len(se.Settings.Stand.Changes) != 1 {
		t.Errorf("conflict settings = %+v", se.Settings.Stand)
	}

	// 7: too soon.
	form40 := opapi.StandSettings{Teams: teams(func(t *opapi.TeamSettings) { t.FormLoad = 40 })}
	_, err = e.put(e.svc, 1, form40)
	if se := wantErr(t, err, opapi.CodeSettingsTooSoon); se.RetryAfterMS != 10_000 {
		t.Errorf("retry after = %d ms, want 10000", se.RetryAfterMS)
	}
	e.clock.Advance(opapi.SettingsChangeInterval)
	if st := e.putDone(e.svc, 1, form40, opapi.ResultDone); st.Version != 2 || len(st.Changes) != 2 || st.Changes[0].Version != 2 {
		t.Errorf("stand = %+v", st)
	}
	if snap := e.svc.StandSnapshot(); snap.ConsoleChanges != 2 || snap.SimulatorChanges != 0 || snap.Version != 2 {
		t.Errorf("snapshot = %+v", snap)
	}
}

func testStandConcurrent(t *testing.T, e *standEnv) {
	cfg := DefaultConfig()
	routers := []*Service{e.svc, e.router(cfg), e.router(cfg)}
	const rounds, perRouter = 6, 3
	for round := range rounds {
		e.clock.Advance(opapi.SettingsChangeInterval)
		base := int64(round)
		var (
			mu       sync.Mutex
			done     int
			conflict int
			wg       sync.WaitGroup
		)
		for i, s := range routers {
			for j := range perRouter {
				set := opapi.StandSettings{Teams: teams(func(t *opapi.TeamSettings) {
					t.FormLoad = 100 + round*10 + i*perRouter + j
				})}
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
	if s, c := e.rows(); s != 1 || c != rounds {
		t.Errorf("rows = %d, %d; want 1, %d", s, c, rounds)
	}
	var console int64
	for _, s := range routers {
		console += s.StandSnapshot().ConsoleChanges
	}
	if console != rounds {
		t.Errorf("console changes counted = %d, want %d", console, rounds)
	}
	st := e.read(e.svc, true)
	if st.Version != rounds || len(st.Changes) != rounds {
		t.Errorf("stand = %+v", st)
	}
}

func testStandInterval(t *testing.T, e *standEnv) {
	set := func(n int) opapi.StandSettings {
		return opapi.StandSettings{Teams: teams(func(t *opapi.TeamSettings) { t.FormLoad = n })}
	}
	e.putDone(e.svc, 0, set(30), opapi.ResultDone)
	e.clock.Advance(opapi.SettingsChangeInterval - time.Millisecond)
	_, err := e.put(e.svc, 1, set(31))
	if se := wantErr(t, err, opapi.CodeSettingsTooSoon); se.RetryAfterMS != 1 {
		t.Errorf("retry after = %d ms, want 1", se.RetryAfterMS)
	}
	e.clock.Advance(time.Millisecond)
	e.putDone(e.svc, 1, set(31), opapi.ResultDone)

	// A simulator report is not limited and does not start the interval.
	e.clock.Advance(opapi.SettingsChangeInterval)
	sim := testSim()
	resp, err := e.svc.PutSimDefaults(t.Context(), opapi.SimDefaultsRequest{Sim: sim})
	if err != nil || resp.Result != opapi.ResultDone || resp.Settings.Stand.Version != 3 {
		t.Fatalf("PutSimDefaults() = %+v, %v", resp, err)
	}
	st := e.putDone(e.svc, 3, opapi.StandSettings{Teams: set(32).Teams, Sim: &sim}, opapi.ResultDone)
	if st.Version != 4 {
		t.Errorf("version = %d", st.Version)
	}
	// The interval counts from that change again.
	e.clock.Advance(4 * time.Second)
	_, err = e.put(e.svc, 4, opapi.StandSettings{Teams: set(33).Teams, Sim: &sim})
	if se := wantErr(t, err, opapi.CodeSettingsTooSoon); se.RetryAfterMS != 6000 {
		t.Errorf("retry after = %d ms, want 6000", se.RetryAfterMS)
	}
}

func testStandRestart(t *testing.T, e *standEnv) {
	set := opapi.StandSettings{Teams: teams(func(t *opapi.TeamSettings) { t.FormLoad = 30; t.MaxShare = 0.25 })}
	e.putDone(e.svc, 0, set, opapi.ResultDone)

	// The new process has other team values in its environment.
	cfg := DefaultConfig()
	cfg.TeamFormLoad = 25
	again := e.router(cfg)
	if got := again.config(); got.TeamFormLoad != 30 || got.TeamMaxShare != 0.25 {
		t.Errorf("config after the restart = %+v, want the stored set", got)
	}
	st := e.read(again, false)
	if st.Version != 1 || !st.Current.Equal(set) || st.Defaults.Teams.FormLoad != 25 {
		t.Errorf("stand after the restart = %+v", st)
	}
	if c := again.Config().TeamCap(100); c != 25 {
		t.Errorf("team cap of 100 online = %d, want 25", c)
	}
}

func testStandRestartVersion0(t *testing.T, e *standEnv) {
	cfg := DefaultConfig()
	cfg.TeamFormLoad = 25
	again := e.router(cfg)
	st := e.read(again, false)
	if st.Version != 0 || st.Current.Teams.FormLoad != 25 || again.config().TeamFormLoad != 25 {
		t.Errorf("stand = %+v", st)
	}
}

func testStandSimDefaults(t *testing.T, e *standEnv) {
	ctx := t.Context()
	sim := testSim()

	// Values outside the bounds: nothing is stored.
	bad := sim
	bad.Operators = 5
	_, err := e.svc.PutSimDefaults(ctx, opapi.SimDefaultsRequest{Sim: bad})
	if se := wantErr(t, err, opapi.CodeInvalidRequest); len(se.Fields) != 1 || se.Fields[0].Field != opapi.FieldSimOperators {
		t.Errorf("fields = %+v", se.Fields)
	}
	if s, c := e.rows(); s != 0 || c != 0 {
		t.Errorf("rows = %d, %d after an invalid report", s, c)
	}

	// The first report becomes the current sim part: a change by the
	// simulator.
	resp, err := e.svc.PutSimDefaults(ctx, opapi.SimDefaultsRequest{Sim: sim})
	if err != nil || resp.Result != opapi.ResultDone {
		t.Fatalf("PutSimDefaults() = %+v, %v", resp, err)
	}
	if st := resp.Settings.Stand; st.Version != 1 || *st.Current.Sim != sim || *st.Defaults.Sim != sim || st.Changes != nil {
		t.Errorf("stand = %+v", st)
	}
	st := e.read(e.svc, true)
	c := st.Changes[0]
	if c.By != opapi.BySimulator || len(c.Fields) != 5 || c.Fields[0].Old != nil || c.Fields[0].Field != opapi.FieldSimOperators {
		t.Errorf("change = %+v", c)
	}

	// The same report again.
	resp, err = e.svc.PutSimDefaults(ctx, opapi.SimDefaultsRequest{Sim: sim})
	if err != nil || resp.Result != opapi.ResultAlreadyDone || resp.Settings.Stand.Version != 1 {
		t.Fatalf("PutSimDefaults() again = %+v, %v", resp, err)
	}

	// Now a change must carry the sim part.
	_, err = e.put(e.svc, 1, opapi.StandSettings{Teams: defaultTeams()})
	if se := wantErr(t, err, opapi.CodeInvalidRequest); se.Fields[0].Field != opapi.FieldSim {
		t.Errorf("fields = %+v", se.Fields)
	}
	console := sim
	console.Operators = 300
	e.putDone(e.svc, 1, opapi.StandSettings{Teams: defaultTeams(), Sim: &console}, opapi.ResultDone)

	// Another report changes only the defaults, without a version or a log
	// entry.
	other := sim
	other.Capacity = 6
	resp, err = e.svc.PutSimDefaults(ctx, opapi.SimDefaultsRequest{Sim: other})
	if err != nil || resp.Result != opapi.ResultDone {
		t.Fatalf("PutSimDefaults(other) = %+v, %v", resp, err)
	}
	st = *resp.Settings.Stand
	if st.Version != 2 || *st.Current.Sim != console || *st.Defaults.Sim != other {
		t.Errorf("stand = %+v", st)
	}
	if _, c := e.rows(); c != 2 {
		t.Errorf("log entries = %d, want 2", c)
	}
	if snap := e.svc.StandSnapshot(); snap.SimulatorChanges != 1 || snap.ConsoleChanges != 1 || *snap.Current.Sim != console {
		t.Errorf("snapshot = %+v", snap)
	}
}

func testStandConverge(t *testing.T, e *standEnv) {
	other := e.router(DefaultConfig())
	set := opapi.StandSettings{Teams: teams(func(t *opapi.TeamSettings) { t.MinSize = 3; t.MaxSize = 9 })}
	e.putDone(e.svc, 0, set, opapi.ResultDone)
	if got := other.config(); got.TeamMinSize != 2 || got.TeamMaxSize != 20 {
		t.Fatalf("the other router changed before its team check: %+v", got)
	}
	if _, err := other.CheckTeams(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := other.StandSnapshot(); got.Version != 1 || got.Current.Teams != set.Teams || other.config().TeamMaxSize != 9 {
		t.Errorf("the other router after its team check: %+v", got)
	}
	// The version never goes back: an older row does not replace it.
	other.apply(t.Context(), standRow{version: 0, current: opapi.StandSettings{Teams: defaultTeams()}})
	if other.config().TeamMaxSize != 9 {
		t.Error("an older version replaced the snapshot")
	}
}

func testStandLog(t *testing.T, e *standEnv) {
	const n = opapi.MaxSettingsChanges + 5
	for i := range n {
		e.clock.Advance(opapi.SettingsChangeInterval)
		set := opapi.StandSettings{Teams: teams(func(t *opapi.TeamSettings) { t.FormLoad = 30 + i })}
		e.putDone(e.svc, int64(i), set, opapi.ResultDone)
	}
	st := e.read(e.svc, true)
	if st.Version != n || len(st.Changes) != opapi.MaxSettingsChanges || st.Changes[0].Version != n ||
		st.Changes[len(st.Changes)-1].Version != n-opapi.MaxSettingsChanges+1 {
		t.Errorf("stand: version %d, %d changes from %d", st.Version, len(st.Changes), st.Changes[0].Version)
	}
	if st := e.read(e.svc, false); st.Changes != nil {
		t.Errorf("the plain answer has %d changes", len(st.Changes))
	}
	if _, c := e.rows(); c != n {
		t.Errorf("log rows = %d, want %d", c, n)
	}
}

func testStandOffEnv(t *testing.T, e *standEnv) {
	ctx := t.Context()
	got, err := e.svc.ReadSettings(ctx, true)
	if err != nil || got.Stand != nil {
		t.Fatalf("ReadSettings() = %+v, %v; want no stand block", got.Stand, err)
	}
	_, err = e.put(e.svc, 0, opapi.StandSettings{Teams: defaultTeams()})
	wantErr(t, err, opapi.CodeSettingsOff)
	// Off comes before the fields.
	_, err = e.put(e.svc, 0, opapi.StandSettings{})
	wantErr(t, err, opapi.CodeSettingsOff)
	_, err = e.svc.PutSimDefaults(ctx, opapi.SimDefaultsRequest{Sim: testSim()})
	wantErr(t, err, opapi.CodeSettingsOff)
	if _, err := e.svc.CheckTeams(ctx); err != nil {
		t.Fatal(err)
	}
	if s, c := e.rows(); s != 0 || c != 0 {
		t.Errorf("rows = %d, %d", s, c)
	}
	if snap := e.svc.StandSnapshot(); snap.Enabled || snap.Current.Teams.MaxShare != 0.01 {
		t.Errorf("snapshot = %+v", snap)
	}
	// The environment value is in use.
	if c := e.svc.Config().TeamCap(1000); c != 10 {
		t.Errorf("team cap of 1000 online = %d, want 10", c)
	}
}

func testStandOffStored(t *testing.T, e *standEnv) {
	_, err := e.pool.Exec(t.Context(), `INSERT INTO stand_settings (version, current_set, updated_at)
		VALUES (1, '{"teams":{"form_load":1}}', now())`)
	if err != nil {
		t.Fatal(err)
	}
	other := e.router(DefaultConfig())
	if snap := other.StandSnapshot(); snap.Enabled || other.config().TeamFormLoad != 20 {
		t.Errorf("snapshot = %+v", snap)
	}
	// The running router keeps its set and its team check goes on.
	if _, err := e.svc.CheckTeams(t.Context()); err != nil {
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
	e.putDone(e.svc, 0, opapi.StandSettings{Teams: teams(func(t *opapi.TeamSettings) { t.MaxShare = 0.05 })}, opapi.ResultDone)
	e.wantCounts(5, 1)
	// The load would give 10 members, and a second company reaches the
	// threshold.
	e.open(a, 20)
	e.open(b, 20)
	wantTeam(t, e.check(), TeamResult{CapForm: 1, CapGrow: 1})
	if got := e.members(a); len(got) != 5 {
		t.Errorf("members = %v, want the same 5", got)
	}
	if e.teamActive(b) {
		t.Error("company b got a team above the cap")
	}
}

func testStandHigherShare(t *testing.T, e *standEnv) {
	a := company(1)
	e.onlineOps(0, 20) // cap 2
	e.open(a, 40)      // size 10
	wantTeam(t, e.check(), TeamResult{Formed: 1, Moved: 40, CapGrow: 1})
	if got := e.members(a); len(got) != 2 {
		t.Fatalf("members = %v, want 2", got)
	}
	e.putDone(e.svc, 0, opapi.StandSettings{Teams: teams(nil)}, opapi.ResultDone) // share 0.5, cap 10
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
	e.putDone(e.svc, 0, opapi.StandSettings{Teams: teams(func(t *opapi.TeamSettings) {
		t.FormLoad = 10
		t.ReleaseLoad = 4
	})}, opapi.ResultDone)
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
	e.putDone(e.svc, 0, opapi.StandSettings{Teams: teams(func(t *opapi.TeamSettings) { t.FormLoad = 30 })}, opapi.ResultDone)
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
	e.putDone(e.svc, 0, opapi.StandSettings{Teams: teams(func(t *opapi.TeamSettings) { t.MaxSize = 3 })}, opapi.ResultDone)
	wantTeam(t, e.check(), TeamResult{Resized: 1})
	if got := e.members(a); len(got) != 3 {
		t.Errorf("members = %v, want 3", got)
	}
}
