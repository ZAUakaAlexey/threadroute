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

func TestPlanSeed(t *testing.T) {
	sim := testSim()
	teams := defaultTeams()
	other := teams
	other.FormLoad = 40
	put := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)

	t.Run("nothing stored gets the whole set as version 1", func(t *testing.T) {
		st := standRow{current: opapi.StandSettings{Teams: teams}}
		next, fields, ok := planSeed(st)
		if !ok || next.version != 1 || !next.current.Equal(opapi.DefaultStandSettings()) || next.lastPutAt != nil {
			t.Fatalf("next = %+v, changed %v", next, ok)
		}
		names := make([]string, len(fields))
		for i, f := range fields {
			names[i] = f.Field
			if f.Old != nil {
				t.Errorf("field %s has an old value %v, want none", f.Field, *f.Old)
			}
		}
		if !slices.Equal(names, opapi.SettingsFields) {
			t.Errorf("fields = %v, want every field", names)
		}
		if fields[0].New != 20 || fields[len(fields)-1].New != 0.03 {
			t.Errorf("fields = %+v", fields)
		}
		state := next.state([]opapi.SettingsChange{{ChangedAtUS: 1, Version: 1, By: opapi.ByDefaults, Fields: fields}})
		if err := state.Validate(); err != nil {
			t.Errorf("state after the seed is not valid: %v", err)
		}
	})
	t.Run("a stored set without a sim part gets the sim defaults", func(t *testing.T) {
		st := standRow{version: 4, current: opapi.StandSettings{Teams: other}, lastPutAt: &put}
		next, fields, ok := planSeed(st)
		if !ok || next.version != 5 || next.current.Teams != other || next.current.Sim == nil ||
			*next.current.Sim != opapi.DefaultSimSettings() || next.lastPutAt != &put {
			t.Fatalf("next = %+v, changed %v", next, ok)
		}
		if len(fields) != 5 || fields[0].Field != opapi.FieldSimOperators || fields[0].Old != nil {
			t.Errorf("fields = %+v", fields)
		}
		if st.current.Sim != nil {
			t.Error("the plan changed its input")
		}
	})
	t.Run("a stored set with both parts stays", func(t *testing.T) {
		changed := sim
		changed.Capacity = 9
		st := standRow{version: 7, current: opapi.StandSettings{Teams: other, Sim: &changed}}
		next, fields, ok := planSeed(st)
		if ok || fields != nil || next.version != 7 || !next.current.Equal(st.current) {
			t.Errorf("next = %+v, fields %v, changed %v", next, fields, ok)
		}
	})
}

func TestStateDefaults(t *testing.T) {
	sim := testSim()
	sim.Operators = 50
	withSim := standRow{version: 3, current: opapi.StandSettings{Teams: defaultTeams(), Sim: &sim}}.state(nil)
	if !withSim.Defaults.Equal(opapi.DefaultStandSettings()) {
		t.Errorf("defaults = %+v, want the defaults of the code", withSim.Defaults)
	}
	noSim := standRow{current: opapi.StandSettings{Teams: defaultTeams()}}.state(nil)
	if noSim.Defaults.Sim != nil || noSim.Defaults.Teams != opapi.DefaultTeamSettings() {
		t.Errorf("defaults without a sim part = %+v", noSim.Defaults)
	}
	if err := noSim.Validate(); err != nil {
		t.Errorf("state at version 0 is not valid: %v", err)
	}
}

func TestDecodeStored(t *testing.T) {
	env := defaultTeams()
	sim := testSim()
	good := standRow{version: 4, current: opapi.StandSettings{Teams: env, Sim: &sim}}
	cur, err := good.encode()
	if err != nil {
		t.Fatal(err)
	}
	t.Run("round trip", func(t *testing.T) {
		got, err := decodeStored(pgdb.StandSetting{Version: 4, CurrentSet: cur})
		if err != nil {
			t.Fatal(err)
		}
		if got.version != 4 || !got.current.Equal(good.current) {
			t.Errorf("decoded %+v", got)
		}
	})
	t.Run("a trim request", func(t *testing.T) {
		got, err := decodeStored(pgdb.StandSetting{Version: 4, CurrentSet: cur, TrimVersion: ptr(int64(3))})
		if err != nil || got.trimVersion == nil || *got.trimVersion != 3 {
			t.Errorf("decoded %+v, %v", got, err)
		}
		got, err = decodeStored(pgdb.StandSetting{Version: 4, CurrentSet: cur})
		if err != nil || got.trimVersion != nil {
			t.Errorf("decoded without a request %+v, %v", got, err)
		}
	})
	t.Run("a stored set without a sim part", func(t *testing.T) {
		c, err := standRow{version: 2, current: opapi.StandSettings{Teams: env}}.encode()
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeStored(pgdb.StandSetting{Version: 2, CurrentSet: c})
		if err != nil || got.version != 2 || got.current.Sim != nil {
			t.Errorf("decoded %+v, %v", got, err)
		}
	})
	t.Run("version 0 is the team defaults", func(t *testing.T) {
		got, err := decodeStored(pgdb.StandSetting{})
		if err != nil {
			t.Fatal(err)
		}
		if got.version != 0 || got.current.Teams != opapi.DefaultTeamSettings() || got.current.Sim != nil {
			t.Errorf("decoded %+v", got)
		}
		c, err := got.encode()
		if err != nil || c != nil {
			t.Errorf("encode at version 0 = %s, %v; want NULL", c, err)
		}
	})
	bad := opapi.StandSettings{Teams: env}
	bad.Teams.MaxShare = 0.01
	badJSON, _ := standRow{version: 1, current: bad}.encode()
	for name, row := range map[string]pgdb.StandSetting{
		"not JSON":           {Version: 1, CurrentSet: []byte(`{`)},
		"a missing field":    {Version: 1, CurrentSet: []byte(`{"teams":{"form_load":30}}`)},
		"outside the bounds": {Version: 1, CurrentSet: badJSON},
		"sim at version 0":   {Version: 0, CurrentSet: cur},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeStored(row); !errors.Is(err, errBadStored) {
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
