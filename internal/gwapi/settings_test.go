package gwapi

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

func settingsFixture[T ServerMessage](t *testing.T, name string) (T, fixture) {
	t.Helper()
	for _, f := range loadIndexIn(t, settingsFixtureDir) {
		if f.File != name {
			continue
		}
		m, err := DecodeServer(f.read(t), f.audience())
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		v, ok := m.(T)
		if !ok {
			t.Fatalf("%s: got %T", name, m)
		}
		return v, f
	}
	t.Fatalf("no fixture %s", name)
	var zero T
	return zero, fixture{}
}

// TestByFollowsTheRight checks the rule of decision 0014: the "by" label of
// a change goes only to a connection that may change settings, at any
// level, and a message built for one right is refused for the other.
func TestByFollowsTheRight(t *testing.T) {
	for _, f := range loadIndexIn(t, settingsFixtureDir) {
		if f.Direction != "server" || (f.Type != string(TypeWelcome) && f.Type != string(TypeSettings)) {
			continue
		}
		t.Run(f.File, func(t *testing.T) {
			raw := f.read(t)
			if hasBy := bytes.Contains(raw, []byte(`"by"`)); !f.MayChangeSettings && hasBy {
				t.Errorf("holds a by label for a connection without the right")
			}
			other := f.audience()
			other.MayChangeSettings = !other.MayChangeSettings
			_, err := DecodeServer(raw, other)
			if err == nil || !strings.Contains(err.Error(), "may_change") {
				t.Errorf("DecodeServer() for the other right error = %v, want a may_change error", err)
			}
		})
	}

	// A message with by labels and a wrong flag is still refused for an
	// audience without the right: the rule reads the audience.
	u, _ := settingsFixture[SettingsUpdate](t, "server_settings_update_may_change_demo.json")
	u.Settings.MayChange = false
	err := u.Validate(Audience{Level: LevelFull, View: ViewSupervisor})
	if err == nil || !strings.Contains(err.Error(), "settings.changes[0].by: must be empty") {
		t.Errorf("Validate() error = %v, want a by error", err)
	}
	if _, err := EncodeServer(u, Audience{Level: LevelFull, View: ViewSupervisor}); err == nil {
		t.Error("EncodeServer() with a by label for a connection without the right error = nil")
	}
}

func TestSettingsValidate(t *testing.T) {
	demoSup := Audience{Level: LevelDemo, View: ViewSupervisor}
	mayChange := Audience{Level: LevelDemo, View: ViewSupervisor, MayChangeSettings: true}
	update := func() SettingsUpdate {
		u, _ := settingsFixture[SettingsUpdate](t, "server_settings_update_demo.json")
		return u
	}
	welcome := func() Welcome {
		w, _ := settingsFixture[Welcome](t, "server_welcome_supervisor_settings_demo.json")
		return w
	}
	tests := []struct {
		name   string
		a      Audience
		msg    func() ServerMessage
		fields []string
	}{
		{name: "null changes", a: demoSup, msg: func() ServerMessage {
			u := update()
			u.Settings.Changes = nil
			return u
		}, fields: []string{"settings.changes is not set"}},
		{name: "by missing for a connection with the right", a: mayChange, msg: func() ServerMessage {
			u := update()
			u.Settings.MayChange = true
			return u
		}, fields: []string{"settings.changes[0].by: is not set"}},
		{name: "current out of bounds", a: demoSup, msg: func() ServerMessage {
			u := update()
			u.Settings.Current.Teams.MaxSize = 51
			return u
		}, fields: []string{"settings.current.teams.max_size"}},
		{name: "sim part only in defaults", a: demoSup, msg: func() ServerMessage {
			u := update()
			u.Settings.Current.Sim = nil
			return u
		}, fields: []string{"current.sim and defaults.sim"}},
		{name: "broken bounds", a: demoSup, msg: func() ServerMessage {
			u := update()
			u.Settings.Bounds.Sim.Capacity.Min = 30
			return u
		}, fields: []string{"settings.bounds"}},
		{name: "newest change is not the version", a: demoSup, msg: func() ServerMessage {
			u := update()
			u.Settings.Version = 5
			return u
		}, fields: []string{"settings.changes[0].version"}},
		{name: "trend points out of order", a: demoSup, msg: func() ServerMessage {
			w := welcome()
			w.Trend.Points[1].SampledAtUS = w.Trend.Points[0].SampledAtUS
			w.Trend.Points[2].LongestWaitUS = -1
			return w
		}, fields: []string{"trend.points: are not ordered", "trend.points[2].longest_wait_us"}},
		{name: "too many trend points", a: demoSup, msg: func() ServerMessage {
			w := welcome()
			w.Trend.Points = make([]TrendPoint, MaxTrendPoints+1)
			for i := range w.Trend.Points {
				w.Trend.Points[i].SampledAtUS = int64(i + 1)
			}
			return w
		}, fields: []string{"trend.points: 361 are more than 360"}},
		{name: "trend without points", a: demoSup, msg: func() ServerMessage {
			w := welcome()
			w.Trend.Points, w.Trend.IntervalMS = nil, 0
			return w
		}, fields: []string{"trend.points is not set", "trend.interval_ms"}},
		{name: "trend point without time", a: demoSup, msg: func() ServerMessage { return TrendUpdate{} },
			fields: []string{"point.sampled_at_us"}},
		{name: "fields with another code", a: demoSup, msg: func() ServerMessage {
			return Result{RequestID: "r1", Outcome: OutcomeRejected, Code: CodeNotAllowed,
				Fields: []opapi.FieldError{{Field: opapi.FieldSimCapacity, Reason: "x"}}}
		}, fields: []string{"fields: only with"}},
		{name: "empty field error", a: demoSup, msg: func() ServerMessage {
			return Result{RequestID: "r1", Outcome: OutcomeRejected, Code: CodeInvalidRequest, Fields: []opapi.FieldError{{}}}
		}, fields: []string{"fields[0]"}},
		{name: "too soon without wait", a: demoSup, msg: func() ServerMessage {
			return Result{RequestID: "r1", Outcome: OutcomeRejected, Code: CodeSettingsTooSoon}
		}, fields: []string{"retry_after_ms: 0 is not between 1 and 10000"}},
		{name: "wait with another code", a: demoSup, msg: func() ServerMessage {
			return Result{RequestID: "r1", Outcome: OutcomeRejected, Code: CodeRateLimited, RetryAfterMS: 100}
		}, fields: []string{"retry_after_ms: only with"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := tt.msg()
			err := m.Validate(tt.a)
			if err == nil {
				t.Fatal("Validate() error = nil, want an error")
			}
			for _, f := range tt.fields {
				if !strings.Contains(err.Error(), f) {
					t.Errorf("error %q does not name %s", err, f)
				}
			}
			if _, err := EncodeServer(m, tt.a); err == nil {
				t.Error("EncodeServer() error = nil, want an error")
			}
		})
	}
}

// TestFixturesUseThePackageBounds checks that the examples carry the bounds
// of opapi, so the console tests see the real numbers.
func TestFixturesUseThePackageBounds(t *testing.T) {
	for _, name := range []string{"server_welcome_supervisor_settings_demo.json", "server_welcome_operator_settings_demo.json"} {
		w, _ := settingsFixture[Welcome](t, name)
		if w.Settings == nil || w.Settings.Bounds != opapi.Bounds() {
			t.Errorf("%s: bounds are not opapi.Bounds()", name)
		}
		if w.Trend != nil && w.Trend.IntervalMS != int(TrendInterval.Milliseconds()) {
			t.Errorf("%s: interval_ms %d, want %d", name, w.Trend.IntervalMS, TrendInterval.Milliseconds())
		}
	}
	if MaxTrendPoints != 360 {
		t.Errorf("MaxTrendPoints = %d, want 360", MaxTrendPoints)
	}
}

// TestTrendIsCountsOnly fails when a field that could hold a key, an ID or
// a pseudonym is added to a trend point: it must stay valid for every
// audience.
func TestTrendIsCountsOnly(t *testing.T) {
	for f := range reflect.TypeFor[TrendPoint]().Fields() {
		switch f.Type.Kind() {
		case reflect.Int64, reflect.Uint64:
		default:
			t.Errorf("trend point field %s has kind %s", f.Name, f.Type.Kind())
		}
	}
	u, _ := settingsFixture[TrendUpdate](t, "server_trend_demo.json")
	for _, a := range []Audience{
		{Level: LevelDemo, View: ViewSupervisor},
		{Level: LevelFull, View: ViewSupervisor},
		{Level: LevelDemo, View: ViewOperator},
		{Level: LevelFull, View: ViewOperator, MayChangeSettings: true},
	} {
		if err := u.Validate(a); err != nil {
			t.Errorf("Validate(%+v) error = %v", a, err)
		}
	}
}

// TestChangeSettingsDecode checks that values out of the bounds do not break
// the message (the server answers them with a Result), while a wrong shape
// does.
func TestChangeSettingsDecode(t *testing.T) {
	m, err := DecodeClient([]byte(`{"type":"change_settings","request_id":"r1","base_version":2,` +
		`"settings":{"teams":{"form_load":9999,"release_load":0,"release_after_us":1,"min_size":0,"max_size":0,"max_share":7}}}`))
	if err != nil {
		t.Fatalf("DecodeClient() error = %v", err)
	}
	c, ok := m.(ChangeSettings)
	if !ok || c.BaseVersion != 2 || c.Settings.Sim != nil || len(c.Settings.FieldErrors()) != 6 {
		t.Errorf("DecodeClient() = %#v", m)
	}

	tests := []struct {
		name   string
		in     string
		fields []string
	}{
		{name: "no request ID", in: `{"type":"change_settings","base_version":1,"settings":{"teams":{}}}`, fields: []string{"request_id"}},
		{name: "negative base version", in: `{"type":"change_settings","request_id":"r1","base_version":-1,"settings":{"teams":{}}}`,
			fields: []string{"base_version"}},
		{name: "fraction in a count", in: `{"type":"change_settings","request_id":"r1","settings":{"teams":{"form_load":20.5}}}`,
			fields: []string{"form_load"}},
		{name: "share as a string", in: `{"type":"change_settings","request_id":"r1","settings":{"teams":{"max_share":"0.5"}}}`,
			fields: []string{"max_share"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeClient([]byte(tt.in))
			if err == nil {
				t.Fatal("DecodeClient() error = nil")
			}
			for _, f := range tt.fields {
				if !strings.Contains(err.Error(), f) {
					t.Errorf("error %q does not name %s", err, f)
				}
			}
		})
	}

	b, err := EncodeClient(ChangeSettings{RequestID: "r1", BaseVersion: 0, Settings: opapi.StandSettings{}})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"type":"change_settings","request_id":"r1","base_version":0,` +
		`"settings":{"teams":{"form_load":0,"release_load":0,"release_after_us":0,"min_size":0,"max_size":0,"max_share":0}}}`
	if string(b) != want {
		t.Errorf("EncodeClient() = %s, want %s", b, want)
	}
}

// TestOlderPartsIgnoreTheAdditions checks the two directions of
// compatibility that the Go side can see: a Welcome without the new blocks
// is still valid, and an unknown server type is skipped.
func TestOlderPartsIgnoreTheAdditions(t *testing.T) {
	w := decodeFixture[Welcome](t, "server_welcome_supervisor_full.json", fullSupervisor)
	if w.Settings != nil || w.Trend != nil {
		t.Errorf("an older Welcome has settings or trend: %+v", w)
	}
	_, err := DecodeServer([]byte(`{"type":"settings_v2"}`), demoSupervisor)
	if !errors.Is(err, ErrUnknownType) {
		t.Errorf("DecodeServer() error = %v, want ErrUnknownType", err)
	}
}

func TestSettingsMessageSizes(t *testing.T) {
	// The largest client message: a whole set with long numbers.
	c := ChangeSettings{RequestID: strings.Repeat("r", MaxRequestIDLen), BaseVersion: 1 << 50, Settings: opapi.StandSettings{
		Teams: opapi.TeamSettings{FormLoad: 499, ReleaseLoad: 498, ReleaseAfterUS: 7_199_999_999, MinSize: 49, MaxSize: 50, MaxShare: 0.123456789012345},
		Sim: &opapi.SimSettings{Operators: 499, Capacity: 19, HandlingMedianUS: 599_999_999, ResolveMedianUS: 1_799_999_999,
			MissedOfferShare: 0.123456789012345},
	}}
	b, err := EncodeClient(c)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > MaxClientMessageBytes/4 {
		t.Errorf("change_settings of %d bytes is close to MaxClientMessageBytes", len(b))
	}
}
