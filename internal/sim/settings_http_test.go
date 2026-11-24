package sim

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// fakeRouter serves the operator API over HTTP by the contract of opapi,
// with the stand settings of a fakeStand. A nil stand is a router without
// stand settings: no stand block, and no route for the report.
type fakeRouter struct {
	stand *fakeStand

	mu  sync.Mutex
	ops map[string]*opapi.Operator
	// reports counts the requests to the path of PatternPutSimDefaults,
	// also when no route serves it.
	reports int
}

func (r *fakeRouter) standOf() *fakeStand {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stand
}

func newFakeRouter(stand *fakeStand) (*fakeRouter, *httptest.Server) {
	r := &fakeRouter{stand: stand, ops: map[string]*opapi.Operator{}}
	mux := http.NewServeMux()
	mux.HandleFunc(opapi.PatternSettings, func(w http.ResponseWriter, _ *http.Request) {
		s := testSettings()
		if fs := r.standOf(); fs != nil {
			s = fs.settings()
		}
		writeJSON(w, http.StatusOK, s)
	})
	if stand != nil {
		mux.HandleFunc(opapi.PatternPutSimDefaults, func(w http.ResponseWriter, req *http.Request) {
			var body opapi.SimDefaultsRequest
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				writeJSON(w, http.StatusBadRequest, opapi.ErrorResponse{Code: opapi.CodeInvalidRequest, Message: err.Error()})
				return
			}
			resp, err := r.standOf().report(body)
			if se, ok := errors.AsType[*opapi.SettingsError](err); ok {
				writeJSON(w, se.Status, se.SettingsErrorResponse)
				return
			}
			writeJSON(w, http.StatusOK, resp)
		})
	}
	mux.HandleFunc(opapi.PatternPutOperator, func(w http.ResponseWriter, req *http.Request) {
		var body opapi.OperatorRequest
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil || body.Validate() != nil {
			writeJSON(w, http.StatusBadRequest, opapi.ErrorResponse{Code: opapi.CodeInvalidRequest, Message: "bad body"})
			return
		}
		id := req.PathValue(opapi.ParamOperatorID)
		capacity := body.Capacity
		if capacity == 0 {
			capacity = testSettings().CapacityDefault
		}
		r.mu.Lock()
		op, ok := r.ops[id]
		if !ok {
			op = &opapi.Operator{ID: id, Presence: opapi.PresenceOffline}
			r.ops[id] = op
		}
		// A put replaces languages, capacity and priority, never presence.
		op.Languages, op.Capacity, op.Priority = body.Languages, capacity, body.Priority
		out := *op
		r.mu.Unlock()
		writeJSON(w, http.StatusOK, opapi.OperatorResponse{Result: opapi.ResultDone, Operator: out})
	})
	presence := func(p opapi.Presence) http.HandlerFunc {
		return func(w http.ResponseWriter, req *http.Request) {
			id := req.PathValue(opapi.ParamOperatorID)
			r.mu.Lock()
			op, ok := r.ops[id]
			if ok {
				op.Presence = p
			}
			r.mu.Unlock()
			if !ok {
				writeJSON(w, http.StatusNotFound, opapi.ErrorResponse{Code: opapi.CodeOperatorNotFound, Message: id})
				return
			}
			writeJSON(w, http.StatusOK, opapi.ActionResponse{Result: opapi.ResultDone})
		}
	}
	mux.HandleFunc(opapi.PatternOnline, presence(opapi.PresenceOnline))
	mux.HandleFunc(opapi.PatternOffline, presence(opapi.PresenceOffline))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == opapi.SimDefaultsPath() {
			r.mu.Lock()
			r.reports++
			r.mu.Unlock()
		}
		mux.ServeHTTP(w, req)
	}))
	return r, srv
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", opapi.ContentType)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// online returns the operators online and the capacity of each.
func (r *fakeRouter) online() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]int{}
	for id, op := range r.ops {
		if op.Presence == opapi.PresenceOnline {
			out[id] = op.Capacity
		}
	}
	return out
}

func (r *fakeRouter) reportCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reports
}

// onlineWith reports whether exactly n operators are online, each with
// the capacity.
func (r *fakeRouter) onlineWith(n, capacity int) bool {
	on := r.online()
	if len(on) != n {
		return false
	}
	for _, c := range on {
		if c != capacity {
			return false
		}
	}
	return true
}

// runHTTPSim runs the service against the fake router and a fake Kafka. It
// returns the registry and a stop function that waits for Run.
func runHTTPSim(t *testing.T, url string) (*Service, *prometheus.Registry, func()) {
	t.Helper()
	c := newCluster(t)
	cfg := testConfig(c.ListenAddrs())
	cfg.RouterURL = url
	cfg.Behaviour = standBehaviour()
	cfg.Behaviour.Capacity = 0
	reg := prometheus.NewRegistry()
	svc, err := New(Options{Config: cfg, Registerer: reg})
	if err != nil {
		t.Fatal(err)
	}
	svc.pollEvery = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	waitUntil(t, "ready", svc.Ready)
	return svc, reg, func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run() = %v", err)
		}
	}
}

// TestHTTPStandChange: the report at start and the poll over HTTP with the
// real client. The environment becomes the stored set, a change from the
// console is applied on the run, and a clean stop puts every operator
// offline, also the created ones.
func TestHTTPStandChange(t *testing.T) {
	fs := newFakeStand()
	router, srv := newFakeRouter(fs)
	defer srv.Close()
	svc, reg, stop := runHTTPSim(t, srv.URL)

	waitUntil(t, "10 operators online with the default capacity", func() bool { return router.onlineWith(10, 4) })
	st := fs.state()
	if st.Version != 1 || st.Current.Sim == nil || *st.Current.Sim != envSim(standBehaviour()) {
		t.Fatalf("stored after the report: %+v", st)
	}

	next := *st.Current.Sim
	next.Operators, next.Capacity = 12, 6
	fs.set(next)
	waitUntil(t, "12 operators online with capacity 6", func() bool { return router.onlineWith(12, 6) })
	waitUntil(t, "version metric", func() bool { return testutil.ToFloat64(svc.m.settingsVersion) == 2 })
	if got := testutil.ToFloat64(svc.m.settingsChanges); got != 1 {
		t.Errorf("changes metric %v, want 1", got)
	}
	if n, err := testutil.GatherAndCount(reg, "threadroute_sim_settings_value"); err != nil || n != 5 {
		t.Errorf("%d value series (%v), want 5", n, err)
	}

	next.Operators = 10
	fs.set(next)
	waitUntil(t, "10 operators online after the change back", func() bool { return router.onlineWith(10, 6) })
	if got := testutil.ToFloat64(svc.m.operatorsAside); got != 2 {
		t.Errorf("put aside metric %v, want 2", got)
	}
	stop()
	if on := router.online(); len(on) != 0 {
		t.Errorf("online after the stop: %v", on)
	}
	if n := router.reportCount(); n != 1 {
		t.Errorf("%d reports, want 1", n)
	}
}

// TestHTTPStandOff: a router without stand settings. The simulator runs
// from its environment and never reports, then reports once the stand
// block appears.
func TestHTTPStandOff(t *testing.T) {
	router, srv := newFakeRouter(nil)
	defer srv.Close()
	svc, _, stop := runHTTPSim(t, srv.URL)
	defer stop()
	waitUntil(t, "10 operators online", func() bool { return router.onlineWith(10, 4) })
	if got := testutil.ToFloat64(svc.m.settingsVersion); got != 0 {
		t.Errorf("version metric %v, want 0", got)
	}
	if got := testutil.ToFloat64(svc.m.settingsValue.WithLabelValues(opapi.FieldSimCapacity)); got != 4 {
		t.Errorf("capacity metric %v, want the router default 4", got)
	}
	// The router learns stand settings. The fake router has no route for
	// the report, so it answers 404 like a router from before decision
	// 0014, which the client reads as settings off.
	fs := newFakeStand()
	router.mu.Lock()
	router.stand = fs
	router.mu.Unlock()
	waitUntil(t, "two reports", func() bool { return router.reportCount() >= 2 })
	if got := testutil.ToFloat64(svc.m.settingsVersion); got != 0 {
		t.Errorf("version metric %v with a report route missing, want 0", got)
	}
	if !router.onlineWith(10, 4) {
		t.Errorf("online %v, want the 10 of the environment", router.online())
	}
}
