//go:build integration

package opserver

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgtest"
	"github.com/ZAUakaAlexey/threadroute/internal/router"
)

// stepClock is a router.Clock that moves only when the test moves it.
type stepClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *stepClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// TestIntegrationSettingsHTTP sends every outcome of PUT /v1/settings in
// the order of the contract through the handler to the router core on a
// real database, and checks the answers as a client reads them.
func TestIntegrationSettingsHTTP(t *testing.T) {
	pool := pgtest.MigratedPool(t)
	clock := &stepClock{t: time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)}
	core, err := router.New(pool, router.DefaultConfig(), clock, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := core.LoadSettings(t.Context()); err != nil {
		t.Fatal(err)
	}
	h, _ := newTestHandler(t, core)

	put := func(body string) (int, opapi.SettingsErrorResponse, opapi.SettingsChangeResponse) {
		t.Helper()
		rec := serve(h, http.MethodPut, opapi.SettingsPath(), body)
		var er opapi.SettingsErrorResponse
		var ok opapi.SettingsChangeResponse
		if rec.Code == http.StatusOK {
			ok = decode[opapi.SettingsChangeResponse](t, rec)
			if err := ok.Settings.Validate(); err != nil || ok.Settings.Stand == nil {
				t.Fatalf("answer %s: %v", rec.Body, err)
			}
			return rec.Code, er, ok
		}
		er = decode[opapi.SettingsErrorResponse](t, rec)
		if err := er.Validate(); err != nil {
			t.Fatalf("error answer %s: %v", rec.Body, err)
		}
		if er.Code.Status() != rec.Code {
			t.Fatalf("status %d for code %s", rec.Code, er.Code)
		}
		return rec.Code, er, ok
	}
	body := func(formLoad, base int, sim string) string {
		s := `{"settings":{"teams":{"form_load":` + strconv.Itoa(formLoad) + `,"release_load":8,"release_after_us":900000000,` +
			`"min_size":2,"max_size":20,"max_share":0.5}`
		if sim != "" {
			s += `,"sim":` + sim
		}
		return s + `},"base_version":` + strconv.Itoa(base) + `,"by":"test-user-0001"}`
	}
	const sim = `{"operators":200,"capacity":4,"handling_median_us":45000000,"resolve_median_us":180000000,"missed_offer_share":0.03}`

	steps := []struct {
		name   string
		body   string
		before func()
		status int
		code   opapi.ErrorCode
		check  func(opapi.SettingsErrorResponse, opapi.SettingsChangeResponse) bool
	}{
		{name: "1 body shape", body: strings.Replace(body(30, 1, sim), `"form_load":30`, `"form_load":30.0`, 1),
			status: 400, code: opapi.CodeInvalidRequest,
			check: func(e opapi.SettingsErrorResponse, _ opapi.SettingsChangeResponse) bool { return len(e.Fields) == 0 }},
		{name: "3 fields before 4", body: body(1, 7, sim), status: 400, code: opapi.CodeInvalidRequest,
			check: func(e opapi.SettingsErrorResponse, _ opapi.SettingsChangeResponse) bool {
				return len(e.Fields) == 1 && e.Fields[0].Field == opapi.FieldTeamFormLoad
			}},
		{name: "4 conflict before 5", body: body(30, 7, ""), status: 409, code: opapi.CodeSettingsConflict,
			check: func(e opapi.SettingsErrorResponse, _ opapi.SettingsChangeResponse) bool {
				return e.Settings.Stand.Version == 1
			}},
		{name: "5 sim part before 6", body: body(20, 1, ""), status: 400, code: opapi.CodeInvalidRequest,
			check: func(e opapi.SettingsErrorResponse, _ opapi.SettingsChangeResponse) bool {
				return len(e.Fields) == 1 && e.Fields[0].Field == opapi.FieldSim
			}},
		{name: "6 same set", body: body(20, 1, sim), status: 200,
			check: func(_ opapi.SettingsErrorResponse, r opapi.SettingsChangeResponse) bool {
				return r.Result == opapi.ResultAlreadyDone && r.Settings.Stand.Version == 1
			}},
		{name: "8 applied", body: body(30, 1, sim), status: 200,
			check: func(_ opapi.SettingsErrorResponse, r opapi.SettingsChangeResponse) bool {
				st := r.Settings.Stand
				return r.Result == opapi.ResultDone && st.Version == 2 && len(st.Changes) == 2 && st.Changes[0].By == "test-user-0001"
			}},
		{name: "6 same set before 7", body: body(30, 2, sim), status: 200,
			check: func(_ opapi.SettingsErrorResponse, r opapi.SettingsChangeResponse) bool {
				return r.Result == opapi.ResultAlreadyDone
			}},
		{name: "4 repeat of the applied request", body: body(30, 1, sim), status: 409, code: opapi.CodeSettingsConflict,
			check: func(e opapi.SettingsErrorResponse, _ opapi.SettingsChangeResponse) bool {
				return e.Settings.Stand.Current.Teams.FormLoad == 30 && len(e.Settings.Stand.Changes) == 2
			}},
		{name: "7 too soon", body: body(40, 2, sim), before: func() { clock.advance(2500 * time.Millisecond) },
			status: 429, code: opapi.CodeSettingsTooSoon,
			check: func(e opapi.SettingsErrorResponse, _ opapi.SettingsChangeResponse) bool {
				return e.RetryAfterMS == 7500
			}},
		{name: "8 applied after the interval", body: body(40, 2, sim), before: func() { clock.advance(7500 * time.Millisecond) },
			status: 200,
			check: func(_ opapi.SettingsErrorResponse, r opapi.SettingsChangeResponse) bool {
				return r.Result == opapi.ResultDone && r.Settings.Stand.Version == 3
			}},
	}
	for _, s := range steps {
		if s.before != nil {
			s.before()
		}
		status, er, ok := put(s.body)
		if status != s.status || er.Code != s.code {
			t.Fatalf("%s: status %d code %q, want %d %q (%+v)", s.name, status, er.Code, s.status, s.code, er)
		}
		if s.check != nil && !s.check(er, ok) {
			t.Fatalf("%s: answer %+v %+v", s.name, er, ok)
		}
	}

	// GET without and with the change log. The oldest entry is the seed of
	// the defaults.
	rec := serve(h, http.MethodGet, opapi.SettingsPath(), "")
	plain := decode[opapi.Settings](t, rec)
	if plain.Stand == nil || plain.Stand.Version != 3 || plain.Stand.Changes != nil || rec.Body.Len() > opapi.MaxBodyBytes {
		t.Errorf("GET: %s", rec.Body)
	}
	rec = serve(h, http.MethodGet, opapi.SettingsWithChangesPath(), "")
	full := decode[opapi.Settings](t, rec)
	if err := errors.Join(full.Validate(), full.ValidateStand()); err != nil || len(full.Stand.Changes) != 3 ||
		full.Stand.Changes[2].By != opapi.ByDefaults || !full.Stand.Defaults.Equal(opapi.DefaultStandSettings()) {
		t.Errorf("GET with changes: %s, %v", rec.Body, err)
	}
	// The report of the simulator is gone.
	if rec := serve(h, http.MethodPut, "/v1/settings/sim-defaults", `{"sim":`+sim+`}`); rec.Code != http.StatusNotFound {
		t.Errorf("PUT sim defaults: status %d, want 404", rec.Code)
	}
	// The answer reads like the JSON of the contract: integers stay
	// integers.
	var raw map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rec.Body.String(), `"form_load":40,`) {
		t.Errorf("GET body %s", rec.Body)
	}

	// A core that never loaded stand settings: the body check first, then
	// "off", and GET has no stand block.
	offCore, err := router.New(pool, router.DefaultConfig(), clock, nil)
	if err != nil {
		t.Fatal(err)
	}
	off, _ := newTestHandler(t, offCore)
	if rec := serve(off, http.MethodPut, opapi.SettingsPath(), `{"settings":`); rec.Code != http.StatusBadRequest {
		t.Errorf("off, broken body: status %d, want 400", rec.Code)
	}
	if rec := serve(off, http.MethodPut, opapi.SettingsPath(), body(1, 0, "")); rec.Code != http.StatusNotImplemented ||
		decode[opapi.SettingsErrorResponse](t, rec).Code != opapi.CodeSettingsOff {
		t.Errorf("off, bad fields: %d %s", rec.Code, rec.Body)
	}
	if rec := serve(off, http.MethodGet, opapi.SettingsWithChangesPath(), ""); decode[opapi.Settings](t, rec).Stand != nil {
		t.Errorf("off, GET: %s", rec.Body)
	}

	// Through a real server: the stand routes are served next to the
	// others.
	srv := httptest.NewServer(h)
	defer srv.Close()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, srv.URL+opapi.SettingsPath(), strings.NewReader(body(40, 3, sim)))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("PUT settings through a server: %d", resp.StatusCode)
	}
}
