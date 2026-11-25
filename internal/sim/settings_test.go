package sim

import (
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// testTeams is a valid teams part.
func testTeams() opapi.TeamSettings {
	return opapi.TeamSettings{
		FormLoad: 20, ReleaseLoad: 8, ReleaseAfterUS: (15 * time.Minute).Microseconds(),
		MinSize: 2, MaxSize: 20, MaxShare: 0.5,
	}
}

// fakeStand is the stand settings store of a router, as the simulator sees
// it through PatternSettings. It is safe for concurrent use.
type fakeStand struct {
	mu      sync.Mutex
	version int64
	teams   opapi.TeamSettings
	current *opapi.SimSettings
}

// newFakeStand returns a store at version 0, without a sim part, as an
// older router that has stored nothing yet.
func newFakeStand() *fakeStand { return &fakeStand{teams: testTeams()} }

// storedStand returns a store that holds sim at version 1.
func storedStand(sim opapi.SimSettings) *fakeStand {
	fs := newFakeStand()
	fs.set(sim)
	return fs
}

// state returns the stand block. The defaults are the defaults of the code,
// with a sim part exactly when the current set has one.
func (f *fakeStand) state() *opapi.StandState {
	f.mu.Lock()
	defer f.mu.Unlock()
	st := &opapi.StandState{Version: f.version}
	st.Current.Teams, st.Defaults.Teams = f.teams, opapi.DefaultTeamSettings()
	if f.current != nil {
		cur, def := *f.current, opapi.DefaultSimSettings()
		st.Current.Sim, st.Defaults.Sim = &cur, &def
	}
	return st
}

// settings returns the answer of PatternSettings.
func (f *fakeStand) settings() opapi.Settings {
	s := testSettings()
	s.Stand = f.state()
	return s
}

// set stores a new sim part, as a change from the console.
func (f *fakeStand) set(sim opapi.SimSettings) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.current = &sim
	f.version++
}

// changeTeams changes only the teams part.
func (f *fakeStand) changeTeams() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.teams.FormLoad++
	f.version++
}

// standAPI connects a fake API to a fake stand.
func standAPI(api *fakeAPI, fs *fakeStand) {
	api.mu.Lock()
	defer api.mu.Unlock()
	api.settingsFn = func() (opapi.Settings, error) { return fs.settings(), nil }
}

// standService returns a service for the stand tests, without Kafka.
func standService(t *testing.T, b Behaviour, api *fakeAPI, clock Clock, m *metrics) (*Service, *lockedBuffer) {
	t.Helper()
	if m == nil {
		var err error
		if m, err = newMetrics(prometheus.NewRegistry()); err != nil {
			t.Fatal(err)
		}
	}
	logs := &lockedBuffer{}
	cfg := testConfig([]string{"127.0.0.1:1"})
	cfg.Behaviour = b
	cfg.StartWait = 3 * time.Second
	cfg.OutageLimit = time.Minute
	return &Service{
		cfg: cfg, api: api, clock: clock, m: m, pollEvery: opapi.SettingsPollInterval,
		logger: slog.New(slog.NewJSONHandler(logs, nil)), capacityDefault: testSettings().CapacityDefault,
	}, logs
}

// standBehaviour has 10 operators of en and ja, in the bounds of opapi.
func standBehaviour() Behaviour {
	b := testBehaviour()
	b.Operators = 10
	b.Languages = Spread{{Lang: "en", Count: 2}, {Lang: "ja", Count: 1}}
	b.Capacity = 4
	return b
}

func envSim(b Behaviour) opapi.SimSettings { return simOf(b, testSettings().CapacityDefault) }

// TestStartStand: the mode at start comes from the settings read at start,
// and the simulator calls nothing else for it.
func TestStartStand(t *testing.T) {
	stored := opapi.SimSettings{
		Operators: 30, Capacity: 7, HandlingMedianUS: (time.Minute).Microseconds(),
		ResolveMedianUS: (5 * time.Minute).Microseconds(), MissedOfferShare: 0.1,
	}
	tests := []struct {
		name string
		// b is the behaviour of the config; standBehaviour when nil.
		b func() Behaviour
		// stand is the stand block of the settings; nil leaves it out.
		stand *fakeStand
		mode  standMode
		want  opapi.SimSettings
		log   string
	}{
		{name: "no stand block", mode: standOff, log: "offers no stand settings"},
		{name: "no stand block runs with the defaults of the code", b: DefaultBehaviour, mode: standOff,
			log: "offers no stand settings"},
		{name: "stand block without a sim part", stand: newFakeStand(), mode: standOff, log: "offers no stand settings"},
		{name: "the stored set wins over the defaults", b: DefaultBehaviour, stand: storedStand(stored),
			mode: standLive, want: stored},
		{name: "the stored set wins over the config", stand: storedStand(stored), mode: standLive, want: stored},
		{name: "the stored set equal to the config", stand: storedStand(envSim(standBehaviour())), mode: standLive,
			want: envSim(standBehaviour())},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := standBehaviour()
			if tt.b != nil {
				b = tt.b()
			}
			api := &fakeAPI{clock: newFakeClock(), settings: testSettings()}
			if tt.stand != nil {
				standAPI(api, tt.stand)
			}
			svc, logs := standService(t, b, api, newFakeClock(), nil)
			settings, err := api.Settings(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			st := svc.startStand(settings)
			if st.mode != tt.mode {
				t.Errorf("mode %d, want %d", st.mode, tt.mode)
			}
			if tt.mode == standLive && (st.sim != tt.want || st.version != 1) {
				t.Errorf("runs with %+v at version %d, want %+v at 1", st.sim, st.version, tt.want)
			}
			if tt.log != "" {
				if n := strings.Count(logs.String(), tt.log); n != 1 || !strings.Contains(logs.String(), `"level":"WARN"`) {
					t.Errorf("log has %d warnings %q, want 1: %s", n, tt.log, logs.String())
				}
			}
			if n := len(api.callsOf("")); n != 0 {
				t.Errorf("%d API calls at the start of the stand settings, want none", n)
			}
			got := svc.startBehaviour(st)
			want := b
			if tt.mode == standLive {
				want.Operators, want.Capacity = tt.want.Operators, tt.want.Capacity
				want.HandlingMedian, want.ResolveMedian = tt.want.HandlingMedian(), tt.want.ResolveMedian()
				want.MissedOfferShare = tt.want.MissedOfferShare
			}
			if !behaviourEqual(got, want) {
				t.Errorf("start behaviour %+v, want %+v", got, want)
			}
			if tt.b != nil && tt.mode == standOff && simOf(got, 0) != opapi.DefaultSimSettings() {
				t.Errorf("off: runs with %+v, want the defaults of the code", simOf(got, 0))
			}
		})
	}
}

func behaviourEqual(a, b Behaviour) bool {
	return a.Operators == b.Operators && a.Capacity == b.Capacity && a.HandlingMedian == b.HandlingMedian &&
		a.ResolveMedian == b.ResolveMedian && a.MissedOfferShare == b.MissedOfferShare
}

// pollHarness is an engine with a service whose stand is live at version 1
// with the values in effect.
func pollHarness(t *testing.T) (*harness, *Service, *stand, *fakeStand, *lockedBuffer) {
	t.Helper()
	h := changeHarness(t, Spread{{Lang: "en", Count: 2}, {Lang: "ja", Count: 1}}, 10)
	fs := storedStand(envSim(h.e.b))
	standAPI(h.api, fs)
	svc, logs := standService(t, h.e.b, h.api, h.clock, h.m)
	st := svc.startStand(fs.settings())
	if st.mode != standLive || st.version != 1 {
		t.Fatalf("start: %+v", st)
	}
	return h, svc, st, fs, logs
}

func (h *harness) poll(svc *Service, st *stand) error {
	h.t.Helper()
	err := svc.pollOnce(h.t.Context(), h.e, st)
	h.until(h.model() + time.Minute)
	return err
}

// TestPollApplies: a new version with another sim part is applied, with the
// metrics; a new version with the same sim part only moves the version.
func TestPollApplies(t *testing.T) {
	h, svc, st, fs, _ := pollHarness(t)
	before := len(h.api.callsOf(""))
	if err := h.poll(svc, st); err != nil {
		t.Fatal(err)
	}
	if n := len(h.api.callsOf("")) - before; n != 0 {
		t.Errorf("%d calls without a change", n)
	}
	next := st.sim
	next.Operators, next.Capacity, next.MissedOfferShare = 13, 6, 0.25
	fs.set(next)
	if err := h.poll(svc, st); err != nil {
		t.Fatal(err)
	}
	h.checkState(13)
	if got := h.e.inEffect(); got != next {
		t.Errorf("in effect %+v, want %+v", got, next)
	}
	for _, m := range []struct {
		name string
		c    prometheus.Collector
		want float64
	}{
		{"version", h.m.settingsVersion, 2},
		{"changes", h.m.settingsChanges, 1},
		{"operators", h.m.settingsValue.WithLabelValues(opapi.FieldSimOperators), 13},
		{"capacity", h.m.settingsValue.WithLabelValues(opapi.FieldSimCapacity), 6},
		{"handling", h.m.settingsValue.WithLabelValues(opapi.FieldSimHandlingMedian), float64(next.HandlingMedianUS)},
		{"resolve", h.m.settingsValue.WithLabelValues(opapi.FieldSimResolveMedian), float64(next.ResolveMedianUS)},
		{"missed", h.m.settingsValue.WithLabelValues(opapi.FieldSimMissedOfferShare), 0.25},
	} {
		if got := testutil.ToFloat64(m.c); got != m.want {
			t.Errorf("%s metric %v, want %v", m.name, got, m.want)
		}
	}
	fs.changeTeams()
	before = len(h.api.callsOf(""))
	if err := h.poll(svc, st); err != nil {
		t.Fatal(err)
	}
	if n := len(h.api.callsOf("")) - before; n != 0 {
		t.Errorf("%d calls after a change of the teams part", n)
	}
	if got := testutil.ToFloat64(h.m.settingsVersion); got != 3 {
		t.Errorf("version %v, want 3", got)
	}
	if got := testutil.ToFloat64(h.m.settingsChanges); got != 1 {
		t.Errorf("changes %v, want still 1", got)
	}
}

// TestPollFailed: a failed poll never changes the applied values; no answer
// for the outage limit stops the simulator; an error answer does not count
// on the outage clock.
func TestPollFailed(t *testing.T) {
	h, svc, st, fs, logs := pollHarness(t)
	applied := h.e.inEffect()
	next := st.sim
	next.Operators = 20
	fs.set(next)
	h.api.settingsFn = nil
	for _, err := range []error{apiError(opapi.CodeInternal), errNoAnswer, apiError(opapi.CodeUnavailable)} {
		h.api.setSettings(opapi.Settings{}, err)
		if perr := svc.pollOnce(t.Context(), h.e, st); perr != nil {
			t.Fatalf("poll with %v: %v", err, perr)
		}
		h.until(h.model() + 10*time.Second)
		if got := h.e.inEffect(); got != applied {
			t.Errorf("after %v: in effect %+v, want %+v", err, got, applied)
		}
	}
	if strings.Count(logs.String(), "settings poll got no answer") != 1 {
		t.Errorf("the missing answer was not logged once: %s", logs.String())
	}
	// The outage limit is a minute from the first failure.
	h.until(h.model() + time.Minute)
	err := svc.pollOnce(t.Context(), h.e, st)
	if !errors.Is(err, ErrOutage) {
		t.Fatalf("poll after the outage limit: %v, want ErrOutage", err)
	}
	// An answer resets the outage clock and applies the new set.
	standAPI(h.api, fs)
	if err := h.poll(svc, st); err != nil {
		t.Fatal(err)
	}
	if st.outage.since.Load() != nil {
		t.Error("the outage clock of the poll runs after an answer")
	}
	h.checkState(20)
	if !strings.Contains(logs.String(), "poll is fine again") {
		t.Errorf("no log line when the poll recovered: %s", logs.String())
	}
}

// TestPollStandCameOn: in the off mode the simulator runs with its config
// and keeps polling; when a stand block with a sim part appears, it applies
// the stored values. Nothing is reported to the router.
func TestPollStandCameOn(t *testing.T) {
	h := changeHarness(t, Spread{{Lang: "en", Count: 2}, {Lang: "ja", Count: 1}}, 10)
	h.api.setSettings(testSettings(), nil)
	svc, logs := standService(t, h.e.b, h.api, h.clock, h.m)
	st := svc.startStand(testSettings())
	if st.mode != standOff {
		t.Fatalf("start: %+v", st)
	}
	applied := h.e.inEffect()
	before := len(h.api.callsOf(""))
	for range 3 {
		if err := h.poll(svc, st); err != nil {
			t.Fatal(err)
		}
	}
	// A stand block without a sim part is still off.
	h.api.setSettings(newFakeStand().settings(), nil)
	if err := h.poll(svc, st); err != nil {
		t.Fatal(err)
	}
	if st.mode != standOff || h.e.inEffect() != applied || len(h.api.callsOf(""))-before != 0 {
		t.Fatalf("mode %d, in effect %+v, %d calls while off", st.mode, h.e.inEffect(), len(h.api.callsOf(""))-before)
	}
	if n := strings.Count(logs.String(), "offers no stand settings"); n != 1 {
		t.Errorf("the off state was logged %d times, want once: %s", n, logs.String())
	}
	stored := envSim(h.e.b)
	stored.Operators = 12
	fs := storedStand(stored)
	standAPI(h.api, fs)
	if err := h.poll(svc, st); err != nil {
		t.Fatal(err)
	}
	if st.mode != standLive || st.version != 1 {
		t.Fatalf("mode %d version %d after the stand came on", st.mode, st.version)
	}
	h.checkState(12)
	if got := testutil.ToFloat64(h.m.settingsVersion); got != 1 {
		t.Errorf("version %v, want 1", got)
	}
	if !strings.Contains(logs.String(), "stand settings came on") {
		t.Errorf("no log line when the stand came on: %s", logs.String())
	}
}

// TestPollStandGone: a stand block that goes away, or loses its sim part,
// keeps the applied values, with one warning; the next stored set applies.
func TestPollStandGone(t *testing.T) {
	h, svc, st, fs, logs := pollHarness(t)
	next := st.sim
	next.Operators = 15
	fs.set(next)
	if err := h.poll(svc, st); err != nil {
		t.Fatal(err)
	}
	h.checkState(15)
	for _, gone := range []opapi.Settings{testSettings(), newFakeStand().settings()} {
		h.api.settingsFn = func() (opapi.Settings, error) { return gone, nil }
		for range 2 {
			if err := h.poll(svc, st); err != nil {
				t.Fatal(err)
			}
		}
		h.checkState(15)
	}
	if n := strings.Count(logs.String(), "no stand settings now"); n != 1 {
		t.Errorf("logged %d times, want once: %s", n, logs.String())
	}
	next.Operators = 11
	fs.set(next)
	standAPI(h.api, fs)
	if err := h.poll(svc, st); err != nil {
		t.Fatal(err)
	}
	h.checkState(11)
}

// TestPollDoesNotHideOutage: polls that work do not reset the outage clock
// of the operator calls, so failing calls still stop the simulator.
func TestPollDoesNotHideOutage(t *testing.T) {
	h, svc, st, _, _ := pollHarness(t)
	h.api.answer = func(c call) (opapi.Result, error) {
		if c.action == actionAccept {
			return "", errNoAnswer
		}
		return "", nil
	}
	h.send(event.TypeOffered, customerID(1), op1, offerID(1))
	for h.fatal == nil && h.model() < 10*time.Minute {
		if err := svc.pollOnce(t.Context(), h.e, st); err != nil {
			t.Fatal(err)
		}
		h.until(h.model() + opapi.SettingsPollInterval)
	}
	if !errors.Is(h.fatal, ErrOutage) {
		t.Fatalf("fatal %v, want ErrOutage while the polls work", h.fatal)
	}
}
