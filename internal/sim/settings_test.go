package sim

import (
	"context"
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

// fakeStand is the stand settings store of a router, by the contract of
// opapi.PatternPutSimDefaults. It is safe for concurrent use.
type fakeStand struct {
	mu       sync.Mutex
	version  int64
	teams    opapi.TeamSettings
	current  *opapi.SimSettings
	defaults *opapi.SimSettings
}

func newFakeStand() *fakeStand { return &fakeStand{teams: testTeams()} }

// state returns the stand block.
func (f *fakeStand) state() *opapi.StandState {
	f.mu.Lock()
	defer f.mu.Unlock()
	st := &opapi.StandState{Version: f.version}
	st.Current.Teams, st.Defaults.Teams = f.teams, f.teams
	if f.current != nil {
		cur, def := *f.current, *f.defaults
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

// report is PatternPutSimDefaults.
func (f *fakeStand) report(req opapi.SimDefaultsRequest) (opapi.SimDefaultsResponse, error) {
	if fes := req.FieldErrors(); len(fes) > 0 {
		return opapi.SimDefaultsResponse{}, settingsError(opapi.CodeInvalidRequest, fes...)
	}
	f.mu.Lock()
	res := opapi.ResultAlreadyDone
	if f.defaults == nil || *f.defaults != req.Sim {
		res = opapi.ResultDone
	}
	d := req.Sim
	f.defaults = &d
	if f.current == nil {
		c := req.Sim
		f.current = &c
		f.version++
		res = opapi.ResultDone
	}
	f.mu.Unlock()
	return opapi.SimDefaultsResponse{Result: res, Settings: f.settings()}, nil
}

// set stores a new sim part, as a change from the console.
func (f *fakeStand) set(sim opapi.SimSettings) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.defaults == nil {
		f.defaults = &sim
	}
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

// wipe forgets everything, as a new database.
func (f *fakeStand) wipe() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.version, f.current, f.defaults = 0, nil, nil
}

// standAPI connects a fake API to a fake stand.
func standAPI(api *fakeAPI, fs *fakeStand) {
	api.mu.Lock()
	defer api.mu.Unlock()
	api.settingsFn = func() (opapi.Settings, error) { return fs.settings(), nil }
	api.simDefaults = fs.report
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

// TestStartStand: the outcomes of the report at start.
func TestStartStand(t *testing.T) {
	stored := opapi.SimSettings{
		Operators: 30, Capacity: 7, HandlingMedianUS: (time.Minute).Microseconds(),
		ResolveMedianUS: (5 * time.Minute).Microseconds(), MissedOfferShare: 0.1,
	}
	tests := []struct {
		name string
		b    func(b *Behaviour)
		// stand prepares the router; nil leaves the stand block out.
		stand   func(api *fakeAPI) bool
		mode    standMode
		reports int
		want    opapi.SimSettings
		log     string
	}{
		{name: "no stand block", mode: standOff, log: "offers no stand settings"},
		{name: "environment out of bounds", b: func(b *Behaviour) { b.Operators = 5 },
			stand: func(api *fakeAPI) bool { standAPI(api, newFakeStand()); return true },
			mode:  standInvalid, log: "sim.operators: 5 is not between 10 and 500"},
		{name: "settings off", stand: func(api *fakeAPI) bool {
			api.settingsFn = func() (opapi.Settings, error) { return newFakeStand().settings(), nil }
			return true
		}, mode: standOff, reports: 1, log: "does not offer stand settings"},
		{name: "first report", stand: func(api *fakeAPI) bool { standAPI(api, newFakeStand()); return true },
			mode: standLive, reports: 1, want: envSim(standBehaviour())},
		{name: "stored values win", stand: func(api *fakeAPI) bool {
			fs := newFakeStand()
			fs.set(stored)
			standAPI(api, fs)
			return true
		}, mode: standLive, reports: 1, want: stored},
		{name: "capacity 0 reports the router default", b: func(b *Behaviour) { b.Capacity = 0 },
			stand: func(api *fakeAPI) bool { standAPI(api, newFakeStand()); return true },
			mode:  standLive, reports: 1, want: envSim(standBehaviour())},
		{name: "router finds the values invalid", stand: func(api *fakeAPI) bool {
			standAPI(api, newFakeStand())
			api.simDefaults = func(opapi.SimDefaultsRequest) (opapi.SimDefaultsResponse, error) {
				return opapi.SimDefaultsResponse{}, settingsError(opapi.CodeInvalidRequest,
					opapi.FieldError{Field: opapi.FieldSimCapacity, Reason: "test reason"})
			}
			return true
		}, mode: standInvalid, reports: 1, log: "sim.capacity: test reason"},
		{name: "internal error", stand: func(api *fakeAPI) bool {
			standAPI(api, newFakeStand())
			api.simDefaults = func(opapi.SimDefaultsRequest) (opapi.SimDefaultsResponse, error) {
				return opapi.SimDefaultsResponse{}, settingsError(opapi.CodeInternal)
			}
			return true
		}, mode: standOff, reports: 1, log: "refused the report"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := standBehaviour()
			if tt.b != nil {
				tt.b(&b)
			}
			api := &fakeAPI{clock: newFakeClock(), settings: testSettings()}
			if tt.stand != nil {
				tt.stand(api)
			}
			svc, logs := standService(t, b, api, newFakeClock(), nil)
			settings, err := api.Settings(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			st, err := svc.startStand(t.Context(), settings)
			if err != nil {
				t.Fatalf("startStand() = %v", err)
			}
			if st.mode != tt.mode {
				t.Errorf("mode %d, want %d", st.mode, tt.mode)
			}
			if len(api.reports) != tt.reports {
				t.Errorf("%d reports, want %d", len(api.reports), tt.reports)
			}
			if tt.reports > 0 && api.reports[0] != envSim(standBehaviour()) && tt.b == nil {
				t.Errorf("reported %+v, want the environment %+v", api.reports[0], envSim(standBehaviour()))
			}
			if tt.mode == standLive && st.sim != tt.want {
				t.Errorf("runs with %+v, want %+v", st.sim, tt.want)
			}
			if tt.log != "" && !strings.Contains(logs.String(), tt.log) {
				t.Errorf("log has no %q: %s", tt.log, logs.String())
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
		})
	}
}

func behaviourEqual(a, b Behaviour) bool {
	return a.Operators == b.Operators && a.Capacity == b.Capacity && a.HandlingMedian == b.HandlingMedian &&
		a.ResolveMedian == b.ResolveMedian && a.MissedOfferShare == b.MissedOfferShare
}

// TestStartStandUnavailable: a report without an answer is retried like the
// settings at start, until StartWait.
func TestStartStandUnavailable(t *testing.T) {
	for _, recover := range []bool{true, false} {
		clock := newFakeClock()
		api := &fakeAPI{clock: clock}
		fs := newFakeStand()
		standAPI(api, fs)
		fails := 0
		api.simDefaults = func(req opapi.SimDefaultsRequest) (opapi.SimDefaultsResponse, error) {
			if !recover || fails < 2 {
				fails++
				if fails%2 == 0 {
					return opapi.SimDefaultsResponse{}, errNoAnswer
				}
				return opapi.SimDefaultsResponse{}, settingsError(opapi.CodeUnavailable)
			}
			return fs.report(req)
		}
		svc, _ := standService(t, standBehaviour(), api, clock, nil)
		type result struct {
			st  *stand
			err error
		}
		done := make(chan result, 1)
		go func() {
			st, err := svc.startStand(t.Context(), fs.settings())
			done <- result{st, err}
		}()
		// StartWait is 3 seconds and the pause 1 second: two pauses either
		// way.
		for range 2 {
			clock.waitWaiters(1)
			clock.Advance(startPause)
		}
		r := <-done
		switch {
		case recover && (r.err != nil || r.st.mode != standLive):
			t.Errorf("after two failures: %v, %+v, want the live mode", r.err, r.st)
		case !recover && r.err == nil:
			t.Errorf("no error after StartWait without an answer")
		}
	}
}

// pollHarness is an engine with a service whose stand is live at version 1
// with the values in effect.
func pollHarness(t *testing.T) (*harness, *Service, *stand, *fakeStand, *lockedBuffer) {
	t.Helper()
	h := changeHarness(t, Spread{{Lang: "en", Count: 2}, {Lang: "ja", Count: 1}}, 10)
	fs := newFakeStand()
	standAPI(h.api, fs)
	svc, logs := standService(t, h.e.b, h.api, h.clock, h.m)
	st, err := svc.startStand(t.Context(), fs.settings())
	if err != nil || st.mode != standLive || st.version != 1 {
		t.Fatalf("start: %v, %+v", err, st)
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

// TestPollStandCameOn: in the off mode the poll reports when the stand
// block appears and applies the stored values.
func TestPollStandCameOn(t *testing.T) {
	h := changeHarness(t, Spread{{Lang: "en", Count: 2}, {Lang: "ja", Count: 1}}, 10)
	h.api.setSettings(testSettings(), nil)
	svc, _ := standService(t, h.e.b, h.api, h.clock, h.m)
	st, err := svc.startStand(t.Context(), testSettings())
	if err != nil || st.mode != standOff {
		t.Fatalf("start: %v, %+v", err, st)
	}
	if err := h.poll(svc, st); err != nil || len(h.api.reports) != 0 {
		t.Fatalf("poll without a stand block: %v, %d reports", err, len(h.api.reports))
	}
	fs := newFakeStand()
	stored := envSim(h.e.b)
	stored.Operators = 12
	fs.set(stored)
	standAPI(h.api, fs)
	if err := h.poll(svc, st); err != nil {
		t.Fatal(err)
	}
	if st.mode != standLive || len(h.api.reports) != 1 {
		t.Fatalf("mode %d, %d reports after the stand came on", st.mode, len(h.api.reports))
	}
	h.checkState(12)
	if got := testutil.ToFloat64(h.m.settingsVersion); got != 1 {
		t.Errorf("version %v, want 1", got)
	}
}

// TestPollStandGoneAndWiped: a stand block that goes away keeps the applied
// values; a store without a sim part gets a new report.
func TestPollStandGoneAndWiped(t *testing.T) {
	h, svc, st, fs, logs := pollHarness(t)
	next := st.sim
	next.Operators = 15
	fs.set(next)
	if err := h.poll(svc, st); err != nil {
		t.Fatal(err)
	}
	h.checkState(15)
	h.api.settingsFn = func() (opapi.Settings, error) { return testSettings(), nil }
	for range 2 {
		if err := h.poll(svc, st); err != nil {
			t.Fatal(err)
		}
	}
	h.checkState(15)
	if n := strings.Count(logs.String(), "no stand settings now"); n != 1 {
		t.Errorf("logged %d times, want once: %s", n, logs.String())
	}
	fs.wipe()
	standAPI(h.api, fs)
	if err := h.poll(svc, st); err != nil {
		t.Fatal(err)
	}
	if len(h.api.reports) != 2 {
		t.Errorf("%d reports, want a second one for the empty store", len(h.api.reports))
	}
	h.checkState(10)
}

// TestPollReportInvalid: when the router finds the reported values invalid
// on a later report, the poll stops.
func TestPollReportInvalid(t *testing.T) {
	h := changeHarness(t, Spread{{Lang: "en", Count: 1}}, 10)
	svc, _ := standService(t, h.e.b, h.api, h.clock, h.m)
	st := &stand{mode: standOff, env: envSim(h.e.b)}
	h.api.setSettings(newFakeStand().settings(), nil)
	h.api.simDefaults = func(opapi.SimDefaultsRequest) (opapi.SimDefaultsResponse, error) {
		return opapi.SimDefaultsResponse{}, settingsError(opapi.CodeInvalidRequest)
	}
	svc.pollEvery = time.Second
	done := make(chan error, 1)
	go func() { done <- svc.pollStand(context.Background(), h.e, st) }()
	h.clock.waitWaiters(1)
	h.clock.Advance(time.Second)
	if err := <-done; err != nil || st.mode != standInvalid {
		t.Errorf("pollStand() = %v, mode %d, want the end of the poll in the invalid mode", err, st.mode)
	}
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
