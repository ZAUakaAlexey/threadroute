package opapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/defaults"
)

// defaultSet returns the starting values of decisions 0009 and 0010 as a
// stand set, with the sim part.
func defaultSet() StandSettings {
	return StandSettings{
		Teams: TeamSettings{
			FormLoad:       defaults.TeamFormLoad,
			ReleaseLoad:    defaults.TeamReleaseLoad,
			ReleaseAfterUS: defaults.TeamReleaseAfter.Microseconds(),
			MinSize:        defaults.TeamMinSize,
			MaxSize:        defaults.TeamMaxSize,
			MaxShare:       defaults.TeamMaxShare,
		},
		Sim: &SimSettings{
			Operators:        defaults.SimOperators,
			Capacity:         defaults.OperatorCapacity,
			HandlingMedianUS: defaults.SimHandlingMedian.Microseconds(),
			ResolveMedianUS:  defaults.SimResolveMedian.Microseconds(),
			MissedOfferShare: defaults.SimMissedOfferShare,
		},
	}
}

// TestBounds checks the bounds against the table of decision 0014, written
// out here a second time on purpose.
func TestBounds(t *testing.T) {
	b := Bounds()
	if err := b.Validate(); err != nil {
		t.Fatalf("Bounds().Validate() error = %v", err)
	}
	want := map[string]Bound{
		FieldTeamFormLoad:        {Min: 2, Max: 500, Integer: true},
		FieldTeamReleaseLoad:     {Min: 1, Max: 499, Integer: true, Below: FieldTeamFormLoad},
		FieldTeamReleaseAfter:    {Min: 60_000_000, Max: 7_200_000_000, Integer: true},
		FieldTeamMinSize:         {Min: 1, Max: 50, Integer: true, NotAbove: FieldTeamMaxSize},
		FieldTeamMaxSize:         {Min: 1, Max: 50, Integer: true},
		FieldTeamMaxShare:        {Min: 0.05, Max: 1},
		FieldSimOperators:        {Min: 10, Max: 500, Integer: true},
		FieldSimCapacity:         {Min: 1, Max: 20, Integer: true},
		FieldSimHandlingMedian:   {Min: 5_000_000, Max: 600_000_000, Integer: true},
		FieldSimResolveMedian:    {Min: 10_000_000, Max: 1_800_000_000, Integer: true},
		FieldSimMissedOfferShare: {Min: 0, Max: 0.5},
	}
	got := b.fields()
	if len(got) != len(SettingsFields) || len(got) != len(want) {
		t.Fatalf("%d bounds, %d fields, want %d", len(got), len(SettingsFields), len(want))
	}
	for i, f := range got {
		if f.name != SettingsFields[i] {
			t.Errorf("bound %d is %s, SettingsFields has %s", i, f.name, SettingsFields[i])
		}
		if f.b != want[f.name] {
			t.Errorf("%s: bound %+v, want %+v", f.name, f.b, want[f.name])
		}
	}

	bad := []Bound{{Min: 2, Max: 1}, {Min: math.NaN(), Max: 1}, {Min: 0, Max: math.Inf(1)}, {Min: 0.5, Max: 2, Integer: true}}
	for _, bd := range bad {
		if bd.Validate() == nil {
			t.Errorf("Bound%+v.Validate() error = nil", bd)
		}
	}
}

func TestBoundsJSON(t *testing.T) {
	b, err := json.Marshal(Bounds())
	if err != nil {
		t.Fatal(err)
	}
	want := `{"teams":{` +
		`"form_load":{"min":2,"max":500,"integer":true},` +
		`"release_load":{"min":1,"max":499,"integer":true,"below":"teams.form_load"},` +
		`"release_after_us":{"min":60000000,"max":7200000000,"integer":true},` +
		`"min_size":{"min":1,"max":50,"integer":true,"not_above":"teams.max_size"},` +
		`"max_size":{"min":1,"max":50,"integer":true},` +
		`"max_share":{"min":0.05,"max":1,"integer":false}},` +
		`"sim":{` +
		`"operators":{"min":10,"max":500,"integer":true},` +
		`"capacity":{"min":1,"max":20,"integer":true},` +
		`"handling_median_us":{"min":5000000,"max":600000000,"integer":true},` +
		`"resolve_median_us":{"min":10000000,"max":1800000000,"integer":true},` +
		`"missed_offer_share":{"min":0,"max":0.5,"integer":false}}}`
	if string(b) != want {
		t.Errorf("json =\n%s\nwant\n%s", b, want)
	}
}

func TestDefaultsAreWithinBounds(t *testing.T) {
	if err := defaultSet().Validate(); err != nil {
		t.Errorf("the defaults of decisions 0009 and 0010 break the bounds: %v", err)
	}
}

func TestStandSettingsValidate(t *testing.T) {
	type change func(s *StandSettings)
	tests := []struct {
		name   string
		change change
		fields []string
	}{
		{name: "defaults", change: func(*StandSettings) {}},
		{name: "no sim part", change: func(s *StandSettings) { s.Sim = nil }},
		{name: "every lower bound", change: func(s *StandSettings) {
			s.Teams = TeamSettings{FormLoad: 2, ReleaseLoad: 1, ReleaseAfterUS: 60_000_000, MinSize: 1, MaxSize: 1, MaxShare: 0.05}
			*s.Sim = SimSettings{Operators: 10, Capacity: 1, HandlingMedianUS: 5_000_000, ResolveMedianUS: 10_000_000}
		}},
		{name: "every upper bound", change: func(s *StandSettings) {
			s.Teams = TeamSettings{FormLoad: 500, ReleaseLoad: 499, ReleaseAfterUS: 7_200_000_000, MinSize: 50, MaxSize: 50, MaxShare: 1}
			*s.Sim = SimSettings{Operators: 500, Capacity: 20, HandlingMedianUS: 600_000_000, ResolveMedianUS: 1_800_000_000, MissedOfferShare: 0.5}
		}},
		{name: "zero set", change: func(s *StandSettings) { *s = StandSettings{Sim: &SimSettings{}} },
			fields: []string{FieldTeamFormLoad, FieldTeamReleaseLoad, FieldTeamReleaseAfter, FieldTeamMinSize, FieldTeamMaxSize,
				FieldTeamMaxShare, FieldSimOperators, FieldSimCapacity, FieldSimHandlingMedian, FieldSimResolveMedian}},
		{name: "below the lower bounds", change: func(s *StandSettings) {
			s.Teams = TeamSettings{FormLoad: 1, ReleaseLoad: 0, ReleaseAfterUS: 59_999_999, MinSize: 0, MaxSize: 0, MaxShare: 0.0499}
			*s.Sim = SimSettings{Operators: 9, Capacity: 0, HandlingMedianUS: 4_999_999, ResolveMedianUS: 9_999_999, MissedOfferShare: -0.01}
		}, fields: SettingsFields},
		{name: "above the upper bounds", change: func(s *StandSettings) {
			s.Teams = TeamSettings{FormLoad: 501, ReleaseLoad: 500, ReleaseAfterUS: 7_200_000_001, MinSize: 51, MaxSize: 51, MaxShare: 1.0001}
			*s.Sim = SimSettings{Operators: 501, Capacity: 21, HandlingMedianUS: 600_000_001, ResolveMedianUS: 1_800_000_001, MissedOfferShare: 0.5001}
		}, fields: SettingsFields},
		{name: "large numbers without exponent", change: func(s *StandSettings) { s.Teams.ReleaseAfterUS = 59_999_999 },
			fields: []string{FieldTeamReleaseAfter + ": 59999999 is not between 60000000 and 7200000000"}},
		{name: "release load equal to form load", change: func(s *StandSettings) { s.Teams.ReleaseLoad = s.Teams.FormLoad },
			fields: []string{FieldTeamReleaseLoad + ": 20 is not below teams.form_load (20)"}},
		{name: "min size above max size", change: func(s *StandSettings) { s.Teams.MinSize, s.Teams.MaxSize = 6, 5 },
			fields: []string{FieldTeamMinSize + ": 6 is above teams.max_size (5)"}},
		{name: "NaN and infinity", change: func(s *StandSettings) { s.Teams.MaxShare, s.Sim.MissedOfferShare = math.NaN(), math.Inf(-1) },
			fields: []string{FieldTeamMaxShare, FieldSimMissedOfferShare}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := defaultSet()
			tt.change(&s)
			fes := s.FieldErrors()
			if len(fes) != len(tt.fields) {
				t.Errorf("FieldErrors() = %v, want %d errors", fes, len(tt.fields))
			}
			assertErrors(t, s.Validate(), tt.fields...)
		})
	}
}

// TestCrossRuleNeedsValidFields checks that a value out of its own bounds
// gives one error, not a second one from a rule between two fields.
func TestCrossRuleNeedsValidFields(t *testing.T) {
	s := defaultSet()
	s.Teams.FormLoad = 1000 // out of bounds, and above the release load
	s.Teams.ReleaseLoad = 600
	fes := s.FieldErrors()
	if len(fes) != 2 || fes[0].Field != FieldTeamFormLoad || fes[1].Field != FieldTeamReleaseLoad ||
		strings.Contains(fes[1].Reason, "below") {
		t.Errorf("FieldErrors() = %v", fes)
	}
}

func TestPartsValidateAlone(t *testing.T) {
	s := defaultSet()
	if err := s.Teams.Validate(); err != nil {
		t.Errorf("Teams.Validate() error = %v", err)
	}
	if err := s.Sim.Validate(); err != nil {
		t.Errorf("Sim.Validate() error = %v", err)
	}
	s.Sim.Capacity = 21
	if fes := s.Sim.FieldErrors(); len(fes) != 1 || fes[0].Field != FieldSimCapacity {
		t.Errorf("Sim.FieldErrors() = %v", fes)
	}
	s.Teams.MaxSize = 0
	if fes := s.Teams.FieldErrors(); len(fes) != 1 || fes[0].Field != FieldTeamMaxSize {
		t.Errorf("Teams.FieldErrors() = %v", fes)
	}
}

func TestStandSettingsJSON(t *testing.T) {
	s := defaultSet()
	s.Sim.MissedOfferShare = 0
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"teams":{"form_load":20,"release_load":8,"release_after_us":900000000,"min_size":2,"max_size":20,"max_share":0.5},` +
		`"sim":{"operators":200,"capacity":4,"handling_median_us":45000000,"resolve_median_us":180000000,"missed_offer_share":0}}`
	if string(b) != want {
		t.Errorf("json = %s, want %s", b, want)
	}
	s.Sim = nil
	if b, _ = json.Marshal(s); strings.Contains(string(b), "sim") {
		t.Errorf("json without sim part = %s", b)
	}
	var back StandSettings
	if err := json.Unmarshal([]byte(want), &back); err != nil || !back.Equal(defaultSetWithShare(0)) {
		t.Errorf("decode = %+v, %v", back, err)
	}
}

func defaultSetWithShare(share float64) StandSettings {
	s := defaultSet()
	s.Sim.MissedOfferShare = share
	return s
}

// TestNumbersOnTheWire checks how JSON numbers decode: integers only in
// integer fields, shares compared as doubles with the bounds included.
func TestNumbersOnTheWire(t *testing.T) {
	tests := []struct {
		body      string
		decodeErr bool
		invalid   bool
	}{
		{body: `{"form_load":20}`},
		{body: `{"form_load":20.0}`, decodeErr: true},
		{body: `{"form_load":2e1}`, decodeErr: true},
		{body: `{"form_load":"20"}`, decodeErr: true},
		{body: `{"release_after_us":7200000000}`},
		{body: `{"release_after_us":7200000000.5}`, decodeErr: true},
		{body: `{"max_share":0.05}`},
		{body: `{"max_share":5e-2}`},
		{body: `{"max_share":0.04999999}`, invalid: true},
		{body: `{"max_share":1}`},
		{body: `{"max_share":1.0000001}`, invalid: true},
		{body: `{"max_share":1e999}`, decodeErr: true},
	}
	for _, tt := range tests {
		team := defaultSet().Teams
		err := json.Unmarshal([]byte(tt.body), &team)
		if (err != nil) != tt.decodeErr {
			t.Errorf("%s: decode error = %v, want error %v", tt.body, err, tt.decodeErr)
			continue
		}
		if err == nil && (team.Validate() != nil) != tt.invalid {
			t.Errorf("%s: Validate() = %v, want invalid %v", tt.body, team.Validate(), tt.invalid)
		}
	}
}

func TestEqualAndDiff(t *testing.T) {
	a, b := defaultSet(), defaultSet()
	if !a.Equal(b) {
		t.Error("Equal() of two default sets = false")
	}
	b.Teams.MaxShare, b.Sim.Operators = 0.3, 300
	if a.Equal(b) {
		t.Error("Equal() of different sets = true")
	}
	got := Diff(a, b)
	if len(got) != 2 || got[0].Field != FieldTeamMaxShare || *got[0].Old != 0.5 || got[0].New != 0.3 ||
		got[1].Field != FieldSimOperators || *got[1].Old != 200 || got[1].New != 300 {
		t.Errorf("Diff() = %+v", got)
	}
	if d := Diff(a, a); len(d) != 0 {
		t.Errorf("Diff() of equal sets = %+v", d)
	}

	noSim := a
	noSim.Sim = nil
	if a.Equal(noSim) || noSim.Equal(a) {
		t.Error("Equal() with and without sim part = true")
	}
	added := Diff(noSim, a)
	if len(added) != 5 {
		t.Fatalf("Diff() that adds the sim part = %+v", added)
	}
	for i, c := range added {
		if c.Field != SettingsFields[6+i] || c.Old != nil {
			t.Errorf("Diff()[%d] = %+v", i, c)
		}
	}
	b2, err := json.Marshal(added[0])
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"field":"sim.operators","new":200}`; string(b2) != want {
		t.Errorf("json = %s, want %s", b2, want)
	}
}

func TestCheckBy(t *testing.T) {
	for _, s := range []string{"simulator", "guest", "user:42", "a b ~", strings.Repeat("x", MaxByLen)} {
		if err := CheckBy(s); err != nil {
			t.Errorf("CheckBy(%q) error = %v", s, err)
		}
	}
	for _, s := range []string{"", strings.Repeat("x", MaxByLen+1), "a\nb", "tab\t", "del\x7f", "caf\xc3\xa9"} {
		if err := CheckBy(s); err == nil {
			t.Errorf("CheckBy(%q) error = nil", s)
		}
	}
}

func change(version int64, by string, fields ...FieldChange) SettingsChange {
	return SettingsChange{ChangedAtUS: 1_767_323_100_000_000 + version, Version: version, By: by, Fields: fields}
}

func ptr(f float64) *float64 { return &f }

func TestValidateChanges(t *testing.T) {
	one := FieldChange{Field: FieldTeamMaxShare, Old: ptr(0.5), New: 0.3}
	tests := []struct {
		name    string
		changes []SettingsChange
		version int64
		withBy  bool
		errs    []string
	}{
		{name: "none", version: 0, withBy: true},
		{name: "valid", version: 3, withBy: true, changes: []SettingsChange{
			change(3, "guest", one), change(1, BySimulator, FieldChange{Field: FieldSimOperators, New: 200}),
		}},
		{name: "hidden by", version: 3, changes: []SettingsChange{change(3, "", one)}},
		{name: "by shown to a reader without the right", version: 3, changes: []SettingsChange{change(3, "guest", one)},
			errs: []string{"changes[0].by: must be empty"}},
		{name: "by missing", version: 3, withBy: true, changes: []SettingsChange{change(3, "", one)},
			errs: []string{"changes[0].by: is not set"}},
		{name: "newest is not the current version", version: 4, withBy: true, changes: []SettingsChange{change(3, "guest", one)},
			errs: []string{"changes[0].version"}},
		{name: "oldest first", version: 3, withBy: true, changes: []SettingsChange{change(3, "guest", one), change(3, "guest", one)},
			errs: []string{"changes[1].version"}},
		{name: "no fields and no time", version: 1, withBy: true, changes: []SettingsChange{{Version: 1, By: "guest"}},
			errs: []string{"changes[0].changed_at_us", "changes[0].fields is not set"}},
		{name: "bad fields", version: 1, withBy: true, changes: []SettingsChange{change(1, "guest",
			FieldChange{Field: FieldSimCapacity, New: 5},
			FieldChange{Field: FieldTeamFormLoad, New: 30},
			FieldChange{Field: "teams.speed", New: 1},
			FieldChange{Field: FieldSimResolveMedian, New: 1.5},
			FieldChange{Field: FieldSimMissedOfferShare, Old: ptr(math.NaN()), New: 0},
		)}, errs: []string{"fields[1].field", "fields[2].field", "fields[3]: old and new must be whole", "fields[4]: old and new must be finite"}},
		{name: "too many", version: 21, withBy: true, changes: func() []SettingsChange {
			var out []SettingsChange
			for v := int64(21); v > 0; v-- {
				out = append(out, change(v, "guest", one))
			}
			return out
		}(), errs: []string{"21 are more than 20"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertErrors(t, ValidateChanges(tt.changes, tt.version, tt.withBy), tt.errs...)
		})
	}
}

func TestStandStateValidate(t *testing.T) {
	noSim := defaultSet()
	noSim.Sim = nil
	tests := []struct {
		name string
		in   StandState
		errs []string
	}{
		{name: "nothing stored", in: StandState{Current: noSim, Defaults: noSim}},
		{name: "after the first report", in: StandState{Version: 1, Current: defaultSet(), Defaults: defaultSet(),
			Changes: []SettingsChange{change(1, BySimulator, Diff(noSim, defaultSet())...)}}},
		{name: "sim part only in current", in: StandState{Version: 1, Current: defaultSet(), Defaults: noSim},
			errs: []string{"current.sim and defaults.sim"}},
		{name: "sim part at version 0", in: StandState{Current: defaultSet(), Defaults: defaultSet()},
			errs: []string{"current.sim: must be absent at version 0"}},
		{name: "negative version and bad sets", in: StandState{Version: -1, Current: StandSettings{}, Defaults: noSim},
			errs: []string{"version: -1", "current.teams.form_load"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertErrors(t, tt.in.Validate(), tt.errs...)
			withStand := Settings{SpeedFactor: 1, AcceptTimeoutUS: 1, CapacityDefault: 4, Stand: &tt.in}
			if (withStand.Validate() != nil) != (len(tt.errs) > 0) {
				t.Errorf("Settings.Validate() = %v", withStand.Validate())
			}
		})
	}
}

func TestSettingsChangeRequest(t *testing.T) {
	valid := SettingsChangeRequest{Settings: defaultSet(), BaseVersion: 3, By: "guest"}
	if err := valid.Validate(); err != nil {
		t.Errorf("Validate() error = %v", err)
	}
	b, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"settings":{"teams":{"form_load":20,"release_load":8,"release_after_us":900000000,"min_size":2,"max_size":20,"max_share":0.5},` +
		`"sim":{"operators":200,"capacity":4,"handling_median_us":45000000,"resolve_median_us":180000000,"missed_offer_share":0.03}},` +
		`"base_version":3,"by":"guest"}`
	if string(b) != want {
		t.Errorf("json = %s, want %s", b, want)
	}
	if len(b) > MaxBodyBytes {
		t.Errorf("request of %d bytes is larger than MaxBodyBytes", len(b))
	}

	bad := valid
	bad.BaseVersion, bad.By, bad.Settings.Teams.FormLoad = -1, BySimulator, 1
	fes := bad.FieldErrors()
	got := make([]string, len(fes))
	for i, fe := range fes {
		got[i] = fe.Field
	}
	if strings.Join(got, ",") != "teams.form_load,base_version,by" {
		t.Errorf("FieldErrors() fields = %v", got)
	}
	bad.By = "a\x00b"
	assertErrors(t, bad.Validate(), "by: byte 1 is not printable ASCII")

	sim := SimDefaultsRequest{Sim: *defaultSet().Sim}
	if err := sim.Validate(); err != nil {
		t.Errorf("SimDefaultsRequest.Validate() error = %v", err)
	}
	sim.Sim.Operators = 9999
	assertErrors(t, sim.Validate(), "sim.operators: 9999 is not between 10 and 500")
}

func testSettings(changes int) Settings {
	st := &StandState{Version: int64(changes) + 1, Current: defaultSet(), Defaults: defaultSet()}
	for i := range changes {
		st.Changes = append(st.Changes, change(st.Version-int64(i), "guest", FieldChange{Field: FieldTeamFormLoad, Old: ptr(20), New: 30}))
	}
	return Settings{SpeedFactor: 1, AcceptTimeoutUS: 20_000_000, CapacityDefault: 4,
		AssignmentTargetUS: 60_000_000, FirstResponseTargetUS: 120_000_000, Stand: st}
}

func TestSettingsError(t *testing.T) {
	resp := SettingsErrorResponse{
		ErrorResponse: ErrorResponse{Code: CodeInvalidRequest, Message: "invalid settings"},
		Fields:        []FieldError{{Field: FieldSimCapacity, Reason: "25 is not between 1 and 20"}},
	}
	err := fmt.Errorf("put settings: %w", &SettingsError{Status: 400, SettingsErrorResponse: resp})
	var se *SettingsError
	if !errors.As(err, &se) || se.Code != CodeInvalidRequest || len(se.Fields) != 1 {
		t.Fatalf("errors.As(%v) failed", err)
	}
	want := "put settings: operator API: 400 invalid_request: invalid settings: sim.capacity: 25 is not between 1 and 20"
	if err.Error() != want {
		t.Errorf("Error() = %q, want %q", err.Error(), want)
	}
}

func TestSettingsErrorResponse(t *testing.T) {
	cur := testSettings(1)
	tests := []struct {
		name string
		in   SettingsErrorResponse
		want string
		errs []string
	}{
		{name: "invalid fields", in: SettingsErrorResponse{
			ErrorResponse: ErrorResponse{Code: CodeInvalidRequest, Message: "invalid settings"},
			Fields:        []FieldError{{Field: FieldTeamReleaseLoad, Reason: "20 is not below teams.form_load (20)"}},
		}, want: `{"code":"invalid_request","message":"invalid settings",` +
			`"fields":[{"field":"teams.release_load","reason":"20 is not below teams.form_load (20)"}]}`},
		{name: "too soon", in: SettingsErrorResponse{
			ErrorResponse: ErrorResponse{Code: CodeSettingsTooSoon, Message: "the last change was 3.2 s ago"},
			RetryAfterMS:  6800,
		}, want: `{"code":"settings_too_soon","message":"the last change was 3.2 s ago","retry_after_ms":6800}`},
		{name: "off", in: SettingsErrorResponse{ErrorResponse: ErrorResponse{Code: CodeSettingsOff, Message: "stand settings are off"}},
			want: `{"code":"settings_off","message":"stand settings are off"}`},
		{name: "conflict", in: SettingsErrorResponse{
			ErrorResponse: ErrorResponse{Code: CodeSettingsConflict, Message: "base version 1 is not the current version 2"},
			Settings:      &cur,
		}},
		{name: "conflict without settings", in: SettingsErrorResponse{ErrorResponse: ErrorResponse{Code: CodeSettingsConflict}},
			errs: []string{"settings is not set"}},
		{name: "conflict without stand", in: SettingsErrorResponse{ErrorResponse: ErrorResponse{Code: CodeSettingsConflict},
			Settings: &Settings{SpeedFactor: 1, AcceptTimeoutUS: 1, CapacityDefault: 1}}, errs: []string{"settings.stand is not set"}},
		{name: "details with the wrong codes", in: SettingsErrorResponse{
			ErrorResponse: ErrorResponse{Code: CodeUnavailable},
			Fields:        []FieldError{{Field: FieldBy, Reason: "x"}}, Settings: &cur, RetryAfterMS: 5,
		}, errs: []string{"fields: only with", "settings: only with", "retry_after_ms: only with"}},
		{name: "wait too long", in: SettingsErrorResponse{ErrorResponse: ErrorResponse{Code: CodeSettingsTooSoon}, RetryAfterMS: 10_001},
			errs: []string{"retry_after_ms: 10001 is not between 1 and 10000"}},
		{name: "unknown code", in: SettingsErrorResponse{ErrorResponse: ErrorResponse{Code: "nope"}}, errs: []string{"code"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertErrors(t, tt.in.Validate(), tt.errs...)
			if tt.want == "" {
				return
			}
			b, err := json.Marshal(tt.in)
			if err != nil {
				t.Fatal(err)
			}
			if string(b) != tt.want {
				t.Errorf("json = %s, want %s", b, tt.want)
			}
			// A client that knows only ErrorResponse reads the code.
			var plain ErrorResponse
			if err := json.Unmarshal(b, &plain); err != nil || plain.Code != tt.in.Code {
				t.Errorf("ErrorResponse decode = %+v, %v", plain, err)
			}
		})
	}
}

// TestSettingsSizes checks the body limits with the largest valid answers:
// without the change log the answer fits MaxBodyBytes, so a client from
// before decision 0014 can read it; with a full log it fits half of
// MaxSettingsBodyBytes.
func TestSettingsSizes(t *testing.T) {
	big := testSettings(0)
	big.SpeedFactor = 1.2345678901234567
	worst := func(s *StandSettings) {
		s.Teams = TeamSettings{FormLoad: 499, ReleaseLoad: 498, ReleaseAfterUS: 7_199_999_999, MinSize: 49, MaxSize: 50, MaxShare: 0.123456789012345}
		*s.Sim = SimSettings{Operators: 499, Capacity: 19, HandlingMedianUS: 599_999_999, ResolveMedianUS: 1_799_999_999, MissedOfferShare: 0.123456789012345}
	}
	worst(&big.Stand.Current)
	worst(&big.Stand.Defaults)
	big.Stand.Version = 1 << 40
	if err := big.Validate(); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(big)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > MaxBodyBytes {
		t.Errorf("settings without changes: %d bytes, more than MaxBodyBytes", len(b))
	}

	other := defaultSet()
	other.Teams = TeamSettings{FormLoad: 2, ReleaseLoad: 1, ReleaseAfterUS: 60_000_000, MinSize: 1, MaxSize: 1, MaxShare: 0.987654321098765}
	*other.Sim = SimSettings{Operators: 10, Capacity: 1, HandlingMedianUS: 5_000_000, ResolveMedianUS: 10_000_000, MissedOfferShare: 0.487654321098765}
	every := Diff(big.Stand.Current, other)
	if len(every) != len(SettingsFields) {
		t.Fatalf("Diff() has %d fields, want every field", len(every))
	}
	for i := range MaxSettingsChanges {
		c := change(big.Stand.Version-int64(i), strings.Repeat("u", MaxByLen), every...)
		c.ChangedAtUS = math.MaxInt64 / 1000
		big.Stand.Changes = append(big.Stand.Changes, c)
	}
	if err := big.Validate(); err != nil {
		t.Fatal(err)
	}
	resp := SettingsChangeResponse{Result: ResultDone, Settings: big}
	if b, err = json.Marshal(resp); err != nil {
		t.Fatal(err)
	}
	if len(b) > MaxSettingsBodyBytes/2 {
		t.Errorf("answer with a full log: %d bytes, more than half of MaxSettingsBodyBytes", len(b))
	}
	t.Logf("largest answer without changes: %d bytes; with a full log: %d bytes", len(mustJSON(t, testSettingsWithout(big))), len(b))
}

func testSettingsWithout(s Settings) Settings {
	st := *s.Stand
	st.Changes = nil
	s.Stand = &st
	return s
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestOldClientReadsNewSettings decodes a new answer into the Settings of a
// client from before decision 0014, which knows only the first fields.
func TestOldClientReadsNewSettings(t *testing.T) {
	var old struct {
		SpeedFactor     float64 `json:"speed_factor"`
		AcceptTimeoutUS int64   `json:"accept_timeout_us"`
		CapacityDefault int     `json:"capacity_default"`
	}
	b := mustJSON(t, testSettings(1))
	if err := json.Unmarshal(b, &old); err != nil || old.SpeedFactor != 1 || old.CapacityDefault != 4 {
		t.Errorf("old decode = %+v, %v", old, err)
	}
}

func TestSettingsAnswersFormat(t *testing.T) {
	noSim := defaultSet()
	noSim.Sim = nil
	s := Settings{SpeedFactor: 1, AcceptTimeoutUS: 20_000_000, CapacityDefault: 4, Stand: &StandState{
		Version: 1, Current: defaultSet(), Defaults: defaultSet(),
	}}
	got := string(mustJSON(t, SimDefaultsResponse{Result: ResultDone, Settings: s}))
	set := `{"teams":{"form_load":20,"release_load":8,"release_after_us":900000000,"min_size":2,"max_size":20,"max_share":0.5},` +
		`"sim":{"operators":200,"capacity":4,"handling_median_us":45000000,"resolve_median_us":180000000,"missed_offer_share":0.03}}`
	want := `{"result":"done","settings":{"speed_factor":1,"accept_timeout_us":20000000,"capacity_default":4,` +
		`"stand":{"version":1,"current":` + set + `,"defaults":` + set + `}}}`
	if got != want {
		t.Errorf("json =\n%s\nwant\n%s", got, want)
	}
	if err := s.Validate(); err != nil {
		t.Error(err)
	}
	if got := SimDefaultsPath(); got != "/v1/settings/sim-defaults" {
		t.Errorf("SimDefaultsPath() = %s", got)
	}
	if got := SettingsWithChangesPath(); got != "/v1/settings?changes=1" {
		t.Errorf("SettingsWithChangesPath() = %s", got)
	}
	if SettingsChangeInterval != 10*time.Second || MaxSettingsChanges != 20 {
		t.Error("the limits of decision 0014 changed")
	}
}
