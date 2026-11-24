package router

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgdb"
)

func defaultTeams() opapi.TeamSettings { return teamsOf(DefaultConfig()) }

func testSim() opapi.SimSettings {
	return opapi.SimSettings{
		Operators: 200, Capacity: 4, HandlingMedianUS: 45_000_000, ResolveMedianUS: 180_000_000, MissedOfferShare: 0.03,
	}
}

func TestTeamsOfAndWithTeams(t *testing.T) {
	cfg := DefaultConfig()
	if err := defaultTeams().Validate(); err != nil {
		t.Fatalf("default team values are outside the bounds: %v", err)
	}
	want := opapi.TeamSettings{
		FormLoad: 30, ReleaseLoad: 5, ReleaseAfterUS: 90_000_000, MinSize: 3, MaxSize: 7, MaxShare: 0.25,
	}
	got := cfg.withTeams(want)
	if teamsOf(got) != want {
		t.Errorf("teamsOf(withTeams(x)) = %+v, want %+v", teamsOf(got), want)
	}
	if got.TeamReleaseAfter != 90*time.Second {
		t.Errorf("release after = %s, want 1m30s", got.TeamReleaseAfter)
	}
	// The fields outside the teams part stay.
	got = got.withTeams(teamsOf(cfg))
	if got != cfg {
		t.Errorf("withTeams changed other fields: %+v, want %+v", got, cfg)
	}
}

func TestSettingsValues(t *testing.T) {
	sim := testSim()
	full := SettingsValues(opapi.StandSettings{Teams: defaultTeams(), Sim: &sim})
	names := make([]string, len(full))
	for i, v := range full {
		names[i] = v.Field
	}
	if !slices.Equal(names, opapi.SettingsFields) {
		t.Errorf("names = %v, want %v", names, opapi.SettingsFields)
	}
	byName := map[string]float64{}
	for _, v := range full {
		byName[v.Field] = v.Value
	}
	if byName[opapi.FieldTeamMaxShare] != 0.5 || byName[opapi.FieldSimHandlingMedian] != 45_000_000 ||
		byName[opapi.FieldSimMissedOfferShare] != 0.03 || byName[opapi.FieldTeamFormLoad] != 20 {
		t.Errorf("values = %v", byName)
	}
	teamsOnly := SettingsValues(opapi.StandSettings{Teams: defaultTeams()})
	if len(teamsOnly) != 6 {
		t.Errorf("without a sim part: %d values, want 6", len(teamsOnly))
	}
}

func TestCloneSettings(t *testing.T) {
	sim := testSim()
	a := opapi.StandSettings{Teams: defaultTeams(), Sim: &sim}
	b := cloneSettings(a)
	b.Sim.Operators = 11
	if a.Sim.Operators != 200 {
		t.Error("the clone shares the sim part")
	}
	if c := cloneSettings(opapi.StandSettings{Teams: defaultTeams()}); c.Sim != nil {
		t.Error("the clone of a set without a sim part has one")
	}
}

func TestRetryAfterMS(t *testing.T) {
	tests := []struct {
		elapsed time.Duration
		wait    int64
		ok      bool
	}{
		{elapsed: 10 * time.Second, ok: true},
		{elapsed: time.Hour, ok: true},
		{elapsed: 0, wait: 10_000},
		{elapsed: -time.Second, wait: 10_000},
		{elapsed: 9 * time.Second, wait: 1000},
		{elapsed: 9*time.Second + time.Microsecond, wait: 1000},
		{elapsed: 9*time.Second + 999*time.Millisecond + time.Microsecond, wait: 1},
		{elapsed: 10*time.Second - time.Nanosecond, wait: 1},
		{elapsed: 2500 * time.Millisecond, wait: 7500},
	}
	for _, tt := range tests {
		wait, ok := retryAfterMS(tt.elapsed)
		if wait != tt.wait || ok != tt.ok {
			t.Errorf("retryAfterMS(%s) = %d, %v; want %d, %v", tt.elapsed, wait, ok, tt.wait, tt.ok)
		}
	}
}

// TestCheckChange covers steps 4 to 7 of opapi.PatternPutSettings: each
// case passes every earlier step and fails exactly one, so the order is
// checked too.
func TestCheckChange(t *testing.T) {
	now := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	sim := testSim()
	teams := defaultTeams()
	other := teams
	other.FormLoad = 40
	withSim := standRow{version: 3, current: opapi.StandSettings{Teams: teams, Sim: &sim}, lastPutAt: ptr(now.Add(-time.Minute))}
	noSim := standRow{current: opapi.StandSettings{Teams: teams}}
	recent := withSim
	recent.lastPutAt = ptr(now.Add(-4 * time.Second))
	req := func(base int64, set opapi.StandSettings) opapi.SettingsChangeRequest {
		return opapi.SettingsChangeRequest{Settings: set, BaseVersion: base, By: "test-user"}
	}
	tests := []struct {
		name  string
		st    standRow
		req   opapi.SettingsChangeRequest
		step  changeStep
		field string
		wait  int64
	}{
		{"old base version", withSim, req(2, opapi.StandSettings{Teams: other, Sim: &sim}), stepConflict, "", 0},
		{"newer base version", withSim, req(4, opapi.StandSettings{Teams: other, Sim: &sim}), stepConflict, "", 0},
		{"conflict before the sim part", withSim, req(2, opapi.StandSettings{Teams: other}), stepConflict, "", 0},
		{"conflict before too soon", recent, req(2, opapi.StandSettings{Teams: other, Sim: &sim}), stepConflict, "", 0},
		{"sim part missing", withSim, req(3, opapi.StandSettings{Teams: other}), stepSimPart, opapi.FieldSim, 0},
		{"sim part before anything reported", noSim, req(0, opapi.StandSettings{Teams: other, Sim: &sim}), stepSimPart, opapi.FieldSim, 0},
		{"same set", withSim, req(3, opapi.StandSettings{Teams: teams, Sim: &sim}), stepSame, "", 0},
		{"same set before too soon", recent, req(3, opapi.StandSettings{Teams: teams, Sim: &sim}), stepSame, "", 0},
		{"same set at version 0", noSim, req(0, opapi.StandSettings{Teams: teams}), stepSame, "", 0},
		{"too soon", recent, req(3, opapi.StandSettings{Teams: other, Sim: &sim}), stepTooSoon, "", 6000},
		{"apply", withSim, req(3, opapi.StandSettings{Teams: other, Sim: &sim}), stepApply, "", 0},
		{"apply at version 0", noSim, req(0, opapi.StandSettings{Teams: other}), stepApply, "", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step, fe, wait := checkChange(tt.st, tt.req, now)
			if step != tt.step || fe.Field != tt.field || wait != tt.wait {
				t.Errorf("checkChange = %d, %+v, %d; want %d, field %q, %d", step, fe, wait, tt.step, tt.field, tt.wait)
			}
			if fe.Field != "" && fe.Reason == "" {
				t.Error("field error without a reason")
			}
		})
	}
}

func TestPlanSimDefaults(t *testing.T) {
	sim := testSim()
	changed := sim
	changed.Operators = 50
	teams := defaultTeams()

	t.Run("first report becomes the current sim part", func(t *testing.T) {
		st := standRow{version: 2, current: opapi.StandSettings{Teams: teams}}
		next, fields, ok := planSimDefaults(st, sim)
		if !ok || next.version != 3 || next.current.Sim == nil || *next.current.Sim != sim || *next.simDefaults != sim {
			t.Fatalf("next = %+v, changed %v", next, ok)
		}
		if len(fields) != 5 || fields[0].Field != opapi.FieldSimOperators || fields[0].Old != nil || fields[0].New != 200 {
			t.Errorf("fields = %+v", fields)
		}
		if st.current.Sim != nil {
			t.Error("the plan changed its input")
		}
		if next.state(teams, nil).Validate() != nil {
			t.Errorf("state after the first report is not valid: %v", next.state(teams, nil).Validate())
		}
	})
	t.Run("the same report again", func(t *testing.T) {
		st := standRow{version: 3, current: opapi.StandSettings{Teams: teams, Sim: &sim}, simDefaults: &sim}
		next, fields, ok := planSimDefaults(st, sim)
		if ok || fields != nil || next.version != 3 {
			t.Errorf("changed %v, fields %v, version %d", ok, fields, next.version)
		}
	})
	t.Run("other values change only the defaults", func(t *testing.T) {
		current := sim
		current.Capacity = 9
		st := standRow{version: 7, current: opapi.StandSettings{Teams: teams, Sim: &current}, simDefaults: &sim}
		next, fields, ok := planSimDefaults(st, changed)
		if !ok || fields != nil || next.version != 7 || *next.simDefaults != changed || *next.current.Sim != current {
			t.Errorf("next = %+v, fields %v, changed %v", next, fields, ok)
		}
	})
}

func TestDecodeStored(t *testing.T) {
	env := defaultTeams()
	sim := testSim()
	good := standRow{version: 4, current: opapi.StandSettings{Teams: env, Sim: &sim}, simDefaults: &sim}
	cur, def, err := good.encode()
	if err != nil {
		t.Fatal(err)
	}
	t.Run("round trip", func(t *testing.T) {
		got, err := decodeStored(pgdb.StandSetting{Version: 4, CurrentSet: cur, DefaultsSim: def}, env)
		if err != nil {
			t.Fatal(err)
		}
		if got.version != 4 || !got.current.Equal(good.current) || *got.simDefaults != sim {
			t.Errorf("decoded %+v", got)
		}
	})
	t.Run("version 0 is the environment", func(t *testing.T) {
		got, err := decodeStored(pgdb.StandSetting{}, env)
		if err != nil {
			t.Fatal(err)
		}
		if got.version != 0 || got.current.Teams != env || got.current.Sim != nil || got.simDefaults != nil {
			t.Errorf("decoded %+v", got)
		}
		c, d, err := got.encode()
		if err != nil || c != nil || d != nil {
			t.Errorf("encode at version 0 = %s, %s, %v; want NULL columns", c, d, err)
		}
	})
	bad := opapi.StandSettings{Teams: env}
	bad.Teams.MaxShare = 0.01
	badJSON, _, _ := standRow{version: 1, current: bad}.encode()
	for name, row := range map[string]pgdb.StandSetting{
		"not JSON":             {Version: 1, CurrentSet: []byte(`{`)},
		"a missing field":      {Version: 1, CurrentSet: []byte(`{"teams":{"form_load":30}}`)},
		"outside the bounds":   {Version: 1, CurrentSet: badJSON},
		"sim only in current":  {Version: 4, CurrentSet: cur},
		"sim defaults not set": {Version: 1, CurrentSet: []byte(`{"teams":{}}`), DefaultsSim: []byte(`[`)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeStored(row, env); !errors.Is(err, errBadStored) {
				t.Errorf("error = %v, want errBadStored", err)
			}
		})
	}
}

func TestSettingsErrors(t *testing.T) {
	e := settingsErr(opapi.CodeSettingsTooSoon, "wait")
	e.RetryAfterMS = 5
	if e.Status != 429 || e.Validate() != nil {
		t.Errorf("too soon = %+v, %v", e, e.Validate())
	}
	if off := settingsOff(); off.Status != 501 || off.Validate() != nil {
		t.Errorf("off = %+v", off)
	}
	inv := invalidFields([]opapi.FieldError{{Field: opapi.FieldSim, Reason: "is not set"}})
	if inv.Status != 400 || len(inv.Fields) != 1 || inv.Validate() != nil {
		t.Errorf("invalid = %+v", inv)
	}
}
