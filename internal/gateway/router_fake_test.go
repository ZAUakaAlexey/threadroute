package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/router/opclient"
)

// routerKind is the kind of router a fakeRouter plays.
type routerKind int

const (
	// routerStand offers stand settings.
	routerStand routerKind = iota
	// routerOff knows decision 0014 but offers no stand settings: its GET
	// has no stand block and its PUT answers 501 settings_off.
	routerOff
	// routerOld is a router from before decision 0014: its GET has no
	// stand block and it has no PUT route at all, so a PUT gets a bare 405.
	routerOld
)

// fakeRouter serves the settings calls of the operator API over HTTP and
// follows the contract of opapi.PatternPutSettings, step by step. The
// gateway reaches it through the real opclient, so the decoding of every
// answer is the real one. It does not use the router package.
type fakeRouter struct {
	t    *testing.T
	kind routerKind
	now  func() time.Time

	mu    sync.Mutex
	base  opapi.Settings
	stand *opapi.StandState
	// lastChange is the time of the last change applied by PUT.
	lastChange time.Time
	// fail, when set, is the code of every PUT answer.
	fail opapi.ErrorCode
	// puts are the requests of every PUT.
	puts []opapi.SettingsChangeRequest

	srv    *httptest.Server
	client *opclient.Client
}

func newFakeRouter(t *testing.T, kind routerKind, base opapi.Settings, stand *opapi.StandState) *fakeRouter {
	t.Helper()
	r := &fakeRouter{t: t, kind: kind, now: time.Now, base: base, stand: stand}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/settings", r.get)
	if kind != routerOld {
		mux.HandleFunc("PUT /v1/settings", r.put)
	}
	r.srv = httptest.NewServer(mux)
	t.Cleanup(r.srv.Close)
	c, err := opclient.New(r.srv.URL, opclient.WithTimeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	r.client = c
	return r
}

// settings returns the answer settings, with the change log or without.
func (r *fakeRouter) settings(withChanges bool) opapi.Settings {
	s := r.base
	s.Stand = nil
	if r.kind == routerStand && r.stand != nil {
		st := *r.stand
		st.Changes = slices.Clone(st.Changes)
		if !withChanges {
			st.Changes = nil
		}
		s.Stand = &st
	}
	return s
}

func (r *fakeRouter) get(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	writeJSON(w, http.StatusOK, r.settings(req.URL.Query().Get(opapi.QueryChanges) == "1"))
}

func (r *fakeRouter) put(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fail := func(code opapi.ErrorCode, mod func(e *opapi.SettingsErrorResponse)) {
		e := opapi.SettingsErrorResponse{ErrorResponse: opapi.ErrorResponse{Code: code, Message: "fake router"}}
		if mod != nil {
			mod(&e)
		}
		writeJSON(w, code.Status(), e)
	}
	// 1. The body.
	var body opapi.SettingsChangeRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		fail(opapi.CodeInvalidRequest, nil)
		return
	}
	r.puts = append(r.puts, body)
	if r.fail != "" {
		fail(r.fail, nil)
		return
	}
	// 2. Stand settings offered.
	if r.kind != routerStand || r.stand == nil {
		fail(opapi.CodeSettingsOff, nil)
		return
	}
	// 3. The fields.
	if fes := body.FieldErrors(); len(fes) > 0 {
		fail(opapi.CodeInvalidRequest, func(e *opapi.SettingsErrorResponse) { e.Fields = fes })
		return
	}
	// 4. The version.
	st := r.stand
	if body.BaseVersion != st.Version {
		cur := r.settings(true)
		fail(opapi.CodeSettingsConflict, func(e *opapi.SettingsErrorResponse) { e.Settings = &cur })
		return
	}
	// 5. The sim part.
	if (st.Current.Sim == nil) != (body.Settings.Sim == nil) {
		fe := opapi.FieldError{Field: opapi.FieldSim, Reason: "does not match the stored set"}
		fail(opapi.CodeInvalidRequest, func(e *opapi.SettingsErrorResponse) { e.Fields = []opapi.FieldError{fe} })
		return
	}
	// 6. Nothing changes.
	if body.Settings.Equal(st.Current) {
		writeJSON(w, http.StatusOK, opapi.SettingsChangeResponse{Result: opapi.ResultAlreadyDone, Settings: r.settings(true)})
		return
	}
	// 7. The interval.
	now := r.now()
	if wait := r.lastChange.Add(opapi.SettingsChangeInterval).Sub(now); !r.lastChange.IsZero() && wait > 0 {
		ms := (wait + time.Millisecond - 1) / time.Millisecond
		fail(opapi.CodeSettingsTooSoon, func(e *opapi.SettingsErrorResponse) { e.RetryAfterMS = int64(ms) })
		return
	}
	// 8. Applied.
	r.applyLocked(body.Settings, body.By, now)
	writeJSON(w, http.StatusOK, opapi.SettingsChangeResponse{Result: opapi.ResultDone, Settings: r.settings(true)})
}

// applyLocked stores a new current set with the next version and logs it.
func (r *fakeRouter) applyLocked(next opapi.StandSettings, by string, now time.Time) {
	st := *r.stand
	ch := opapi.SettingsChange{ChangedAtUS: now.UnixMicro(), Version: st.Version + 1, By: by, Fields: opapi.Diff(st.Current, next)}
	st.Changes = append([]opapi.SettingsChange{ch}, st.Changes...)
	if len(st.Changes) > opapi.MaxSettingsChanges {
		st.Changes = st.Changes[:opapi.MaxSettingsChanges]
	}
	st.Version++
	st.Current = next
	r.stand = &st
	r.lastChange = now
}

// change applies a change as another gateway would, without the interval.
func (r *fakeRouter) change(mod func(s *opapi.StandSettings), by string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	next := cloneSet(r.stand.Current)
	mod(&next)
	r.applyLocked(next, by, r.now())
	r.lastChange = time.Time{}
}

// set replaces the stand state, as a router restart with other values.
func (r *fakeRouter) set(st *opapi.StandState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stand = st
}

func (r *fakeRouter) setFail(code opapi.ErrorCode) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fail = code
}

func (r *fakeRouter) setLastChange(at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastChange = at
}

func (r *fakeRouter) putRequests() []opapi.SettingsChangeRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.puts)
}

func (r *fakeRouter) version() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stand.Version
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", opapi.ContentType)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// cloneSet copies a set with its own sim part.
func cloneSet(s opapi.StandSettings) opapi.StandSettings {
	if s.Sim != nil {
		sim := *s.Sim
		s.Sim = &sim
	}
	return s
}

// Synthetic stand settings, in the shape of the protocol examples.
var (
	testTeams = opapi.TeamSettings{FormLoad: 20, ReleaseLoad: 8, ReleaseAfterUS: 900_000_000, MinSize: 2, MaxSize: 20, MaxShare: 0.5}
	testSim   = opapi.SimSettings{Operators: 200, Capacity: 4, HandlingMedianUS: 45_000_000, ResolveMedianUS: 180_000_000, MissedOfferShare: 0.03}
)

// testStand returns a stand at version 3: a simulator report, then two
// changes by user 9.
func testStand() *opapi.StandState {
	sim := testSim
	defaults := opapi.StandSettings{Teams: testTeams, Sim: &sim}
	v1 := cloneSet(defaults)
	v2 := cloneSet(v1)
	v2.Sim.Capacity = 6
	v3 := cloneSet(v2)
	v3.Teams.FormLoad = 30
	at := t0.UnixMicro()
	return &opapi.StandState{
		Version: 3, Current: v3, Defaults: defaults,
		Changes: []opapi.SettingsChange{
			{ChangedAtUS: at - 60_000_000, Version: 3, By: "user:9", Fields: opapi.Diff(v2, v3)},
			{ChangedAtUS: at - 600_000_000, Version: 2, By: "user:9", Fields: opapi.Diff(v1, v2)},
			{ChangedAtUS: at - 3_600_000_000, Version: 1, By: opapi.BySimulator, Fields: opapi.Diff(opapi.StandSettings{Teams: testTeams}, v1)},
		},
	}
}

// firstStartStand returns a stand at version 0: nothing stored, no
// simulator report.
func firstStartStand(teams opapi.TeamSettings) *opapi.StandState {
	return &opapi.StandState{Version: 0, Current: opapi.StandSettings{Teams: teams}, Defaults: opapi.StandSettings{Teams: teams}}
}

// newStandEnv is newEnv with a fake router of the kind and stand, whose
// settings the gateway has read once.
func newStandEnv(t *testing.T, kind routerKind, stand *opapi.StandState, mutate ...func(*Config)) (*env, *fakeRouter) {
	t.Helper()
	e := newEnv(t, mutate...)
	return e, attachRouter(t, e, kind, stand)
}

// attachRouter gives e a fake router and reads its settings once.
func attachRouter(t *testing.T, e *env, kind routerKind, stand *opapi.StandState) *fakeRouter {
	t.Helper()
	r := newFakeRouter(t, kind, e.api.settings, stand)
	e.api.mu.Lock()
	e.api.router = r
	e.api.mu.Unlock()
	if _, err := e.g.RefreshSettings(t.Context()); err != nil {
		t.Fatal(err)
	}
	return r
}
