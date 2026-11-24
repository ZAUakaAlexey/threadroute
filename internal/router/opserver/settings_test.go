package opserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// fakeStand is fakeOps with the stand settings. It records what reached
// the core and answers with res and err.
type fakeStand struct {
	fakeOps
	mu          sync.Mutex
	withChanges []bool
	puts        []opapi.SettingsChangeRequest
	sims        []opapi.SimDefaultsRequest
	res         opapi.Result
	err         error
}

func testStand() *opapi.Settings {
	sim := opapi.SimSettings{Operators: 200, Capacity: 4, HandlingMedianUS: 45_000_000, ResolveMedianUS: 180_000_000}
	teams := opapi.TeamSettings{FormLoad: 20, ReleaseLoad: 8, ReleaseAfterUS: 900_000_000, MinSize: 2, MaxSize: 20, MaxShare: 0.5}
	set := opapi.StandSettings{Teams: teams, Sim: &sim}
	s := opapi.Settings{SpeedFactor: 50, AcceptTimeoutUS: 20_000_000, CapacityDefault: 4,
		Stand: &opapi.StandState{Version: 1, Current: set, Defaults: set}}
	return &s
}

func (f *fakeStand) ReadSettings(_ context.Context, withChanges bool) (opapi.Settings, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.withChanges = append(f.withChanges, withChanges)
	return *testStand(), f.err
}

func (f *fakeStand) PutSettings(_ context.Context, req opapi.SettingsChangeRequest) (opapi.SettingsChangeResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts = append(f.puts, req)
	return opapi.SettingsChangeResponse{Result: f.res, Settings: *testStand()}, f.err
}

func (f *fakeStand) PutSimDefaults(_ context.Context, req opapi.SimDefaultsRequest) (opapi.SimDefaultsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sims = append(f.sims, req)
	return opapi.SimDefaultsResponse{Result: f.res, Settings: *testStand()}, f.err
}

func (f *fakeStand) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.puts) + len(f.sims)
}

const (
	validPut = `{"settings":{"teams":{"form_load":30,"release_load":8,"release_after_us":900000000,` +
		`"min_size":2,"max_size":20,"max_share":0.5}},"base_version":0,"by":"test-user"}`
	validSim = `{"sim":{"operators":200,"capacity":4,"handling_median_us":45000000,` +
		`"resolve_median_us":180000000,"missed_offer_share":0.03}}`
)

func TestSettingsQuery(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{"", false},
		{"?changes=1", true},
		{"?changes=0", false},
		{"?changes=true", false},
		{"?changes=", false},
		{"?other=1", false},
	}
	for _, tt := range tests {
		st := &fakeStand{}
		h, _ := newTestHandler(t, st)
		rec := serve(h, http.MethodGet, opapi.SettingsPath()+tt.query, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("%q: status %d", tt.query, rec.Code)
		}
		if got := decode[opapi.Settings](t, rec); got.Stand == nil {
			t.Errorf("%q: no stand block", tt.query)
		}
		if len(st.withChanges) != 1 || st.withChanges[0] != tt.want {
			t.Errorf("%q: withChanges = %v, want [%v]", tt.query, st.withChanges, tt.want)
		}
	}
	if got := opapi.SettingsWithChangesPath(); !strings.HasSuffix(got, "?changes=1") {
		t.Errorf("SettingsWithChangesPath() = %q", got)
	}
}

func TestSettingsReadError(t *testing.T) {
	h, _ := newTestHandler(t, &fakeStand{err: &pgconn.PgError{Code: "08006", Message: "secret detail"}})
	rec := serve(h, http.MethodGet, opapi.SettingsPath(), "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", rec.Code)
	}
	if er := decode[opapi.ErrorResponse](t, rec); er.Code != opapi.CodeUnavailable || strings.Contains(er.Message, "secret") {
		t.Errorf("answer %+v", er)
	}
}

// TestStandOff: a core without Stand answers like a router that does not
// offer stand settings, after the body check.
func TestStandOff(t *testing.T) {
	h, _ := newTestHandler(t, &fakeOps{})
	rec := serve(h, http.MethodGet, opapi.SettingsWithChangesPath(), "")
	if s := decode[opapi.Settings](t, rec); rec.Code != http.StatusOK || s.Stand != nil {
		t.Errorf("GET: status %d, stand %+v", rec.Code, s.Stand)
	}
	for _, c := range []struct{ path, body string }{
		{opapi.SettingsPath(), validPut}, {opapi.SimDefaultsPath(), validSim},
	} {
		rec := serve(h, http.MethodPut, c.path, c.body)
		er := decode[opapi.SettingsErrorResponse](t, rec)
		if rec.Code != http.StatusNotImplemented || er.Code != opapi.CodeSettingsOff || er.Validate() != nil {
			t.Errorf("PUT %s: status %d, answer %+v", c.path, rec.Code, er)
		}
		// Step 1 comes before step 2.
		rec = serve(h, http.MethodPut, c.path, `{"settings":`)
		if er := decode[opapi.SettingsErrorResponse](t, rec); rec.Code != http.StatusBadRequest || er.Code != opapi.CodeInvalidRequest {
			t.Errorf("PUT %s with a broken body: status %d, answer %+v", c.path, rec.Code, er)
		}
	}
}

// TestSettingsBadBodies: step 1 of the contract. None reaches the core,
// and none has fields.
func TestSettingsBadBodies(t *testing.T) {
	long := strings.Repeat("a", opapi.MaxBodyBytes)
	put := func(teams string) string {
		return `{"settings":{"teams":{` + teams + `}},"base_version":0,"by":"test-user"}`
	}
	tests := []struct{ name, path, body string }{
		{"empty", opapi.SettingsPath(), ""},
		{"null", opapi.SettingsPath(), "null"},
		{"array", opapi.SettingsPath(), "[]"},
		{"number", opapi.SettingsPath(), "1"},
		{"not JSON", opapi.SettingsPath(), "form_load=20"},
		{"two objects", opapi.SettingsPath(), validPut + validPut},
		{"count with a fraction", opapi.SettingsPath(), put(`"form_load":20.0`)},
		{"count with an exponent", opapi.SettingsPath(), put(`"form_load":2e1`)},
		{"microseconds with a fraction", opapi.SettingsPath(), put(`"release_after_us":900000000.5`)},
		{"share as a string", opapi.SettingsPath(), put(`"max_share":"0.5"`)},
		{"base version as a string", opapi.SettingsPath(), `{"settings":{},"base_version":"1","by":"x"}`},
		{"oversized", opapi.SettingsPath(), `{"by":"` + long + `"}`},
		{"sim null", opapi.SimDefaultsPath(), "null"},
		{"sim count with a fraction", opapi.SimDefaultsPath(), `{"sim":{"operators":200.0}}`},
		{"sim oversized", opapi.SimDefaultsPath(), `{"sim":{},"x":"` + long + `"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &fakeStand{res: opapi.ResultDone}
			h, _ := newTestHandler(t, st)
			rec := serve(h, http.MethodPut, tt.path, tt.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status %d, want 400, body %s", rec.Code, rec.Body)
			}
			er := decode[opapi.SettingsErrorResponse](t, rec)
			if er.Code != opapi.CodeInvalidRequest || er.Message == "" || len(er.Fields) != 0 || er.Validate() != nil {
				t.Errorf("answer %+v", er)
			}
			if n := st.calls(); n != 0 {
				t.Errorf("%d calls reached the core", n)
			}
		})
	}
}

func TestSettingsPuts(t *testing.T) {
	for _, res := range []opapi.Result{opapi.ResultDone, opapi.ResultAlreadyDone} {
		st := &fakeStand{res: res}
		h, reg := newTestHandler(t, st)
		// Unknown fields are ignored, as on every route.
		rec := serve(h, http.MethodPut, opapi.SettingsPath(), strings.TrimSuffix(validPut, "}")+`,"extra":1}`)
		if resp := decode[opapi.SettingsChangeResponse](t, rec); rec.Code != http.StatusOK || resp.Result != res {
			t.Errorf("PUT settings: status %d, answer %+v", rec.Code, resp)
		}
		if len(st.puts) != 1 || st.puts[0].Settings.Teams.FormLoad != 30 || st.puts[0].By != "test-user" ||
			st.puts[0].Settings.Sim != nil {
			t.Errorf("core got %+v", st.puts)
		}
		rec = serve(h, http.MethodPut, opapi.SimDefaultsPath(), validSim)
		if resp := decode[opapi.SimDefaultsResponse](t, rec); rec.Code != http.StatusOK || resp.Result != res {
			t.Errorf("PUT sim defaults: status %d, answer %+v", rec.Code, resp)
		}
		if len(st.sims) != 1 || st.sims[0].Sim.MissedOfferShare != 0.03 {
			t.Errorf("core got %+v", st.sims)
		}
		for _, action := range []string{"put_settings", "put_sim_defaults"} {
			if n := counterValue(t, reg, "threadroute_router_operator_actions_total", action, string(res)); n != 1 {
				t.Errorf("%s %s counted %v times", action, res, n)
			}
		}
	}
	h, _ := newTestHandler(t, &fakeStand{res: "maybe"})
	if rec := serve(h, http.MethodPut, opapi.SettingsPath(), validPut); rec.Code != http.StatusInternalServerError {
		t.Errorf("unknown result: status %d, want 500", rec.Code)
	}
}

// TestSettingsErrorAnswers maps every error of the core on both PUT
// routes. Every body is a SettingsErrorResponse that passes its own
// Validate.
func TestSettingsErrorAnswers(t *testing.T) {
	coreErr := func(code opapi.ErrorCode) *opapi.SettingsError {
		return &opapi.SettingsError{Status: code.Status(), SettingsErrorResponse: opapi.SettingsErrorResponse{
			ErrorResponse: opapi.ErrorResponse{Code: code, Message: "test message"},
		}}
	}
	fields := coreErr(opapi.CodeInvalidRequest)
	fields.Fields = []opapi.FieldError{{Field: opapi.FieldTeamFormLoad, Reason: "1 is not between 2 and 500"}}
	conflict := coreErr(opapi.CodeSettingsConflict)
	conflict.Settings = testStand()
	tooSoon := coreErr(opapi.CodeSettingsTooSoon)
	tooSoon.RetryAfterMS = 2500
	tests := []struct {
		name   string
		err    error
		status int
		check  func(opapi.SettingsErrorResponse) bool
	}{
		{"fields", fields, 400, func(r opapi.SettingsErrorResponse) bool { return len(r.Fields) == 1 }},
		{"conflict", fmt.Errorf("wrapped: %w", conflict), 409,
			func(r opapi.SettingsErrorResponse) bool { return r.Settings != nil && r.Settings.Stand.Version == 1 }},
		{"too soon", tooSoon, 429, func(r opapi.SettingsErrorResponse) bool { return r.RetryAfterMS == 2500 }},
		{"off", coreErr(opapi.CodeSettingsOff), 501, nil},
		{"database down", &pgconn.PgError{Code: "08006", Message: "secret detail"}, 503,
			func(r opapi.SettingsErrorResponse) bool { return r.Code == opapi.CodeUnavailable }},
		{"other", errors.New("secret detail"), 500,
			func(r opapi.SettingsErrorResponse) bool { return r.Code == opapi.CodeInternal }},
	}
	for _, tt := range tests {
		for _, c := range []struct{ path, body string }{
			{opapi.SettingsPath(), validPut}, {opapi.SimDefaultsPath(), validSim},
		} {
			t.Run(tt.name+" "+c.path, func(t *testing.T) {
				h, _ := newTestHandler(t, &fakeStand{err: tt.err})
				rec := serve(h, http.MethodPut, c.path, c.body)
				if rec.Code != tt.status {
					t.Fatalf("status %d, want %d, body %s", rec.Code, tt.status, rec.Body)
				}
				r := decode[opapi.SettingsErrorResponse](t, rec)
				if err := r.Validate(); err != nil {
					t.Errorf("answer %+v: %v", r, err)
				}
				if r.Code.Status() != tt.status || strings.Contains(r.Message, "secret") {
					t.Errorf("answer %+v", r)
				}
				if tt.check != nil && !tt.check(r) {
					t.Errorf("answer %+v", r)
				}
			})
		}
	}
}

func TestSettingsRoutesRegistered(t *testing.T) {
	h, _ := newTestHandler(t, &fakeStand{res: opapi.ResultDone})
	if rec := serve(h, http.MethodPost, opapi.SettingsPath(), validPut); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST settings: status %d, want 405", rec.Code)
	}
	if rec := serve(h, http.MethodGet, opapi.SimDefaultsPath(), ""); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET sim defaults: status %d, want 405", rec.Code)
	}
}

// counterValue returns the value of a counter with the given labels.
func counterValue(t *testing.T, reg *prometheus.Registry, name string, labels ...string) float64 {
	t.Helper()
	return testutil.ToFloat64(must(t, reg, name, labels...))
}
