package opclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// These tests use plain httptest handlers, so they check the client against
// the wire format of opapi alone.

// testSet returns a valid stand set with the sim part.
func testSet() opapi.StandSettings {
	return opapi.StandSettings{
		Teams: opapi.TeamSettings{FormLoad: 20, ReleaseLoad: 8, ReleaseAfterUS: 900_000_000, MinSize: 2, MaxSize: 20, MaxShare: 0.5},
		Sim: &opapi.SimSettings{Operators: 200, Capacity: 4, HandlingMedianUS: 45_000_000, ResolveMedianUS: 180_000_000,
			MissedOfferShare: 0.03},
	}
}

// testSettings returns valid settings with a stand block at version 30 and
// the given number of log entries, each with every field and a "by" label
// of byLen bytes.
func testSettings(changes, byLen int) opapi.Settings {
	st := &opapi.StandState{Version: 30, Current: testSet(), Defaults: testSet()}
	other := testSet()
	other.Teams = opapi.TeamSettings{FormLoad: 2, ReleaseLoad: 1, ReleaseAfterUS: 60_000_000, MinSize: 1, MaxSize: 1, MaxShare: 1}
	*other.Sim = opapi.SimSettings{Operators: 10, Capacity: 1, HandlingMedianUS: 5_000_000, ResolveMedianUS: 10_000_000}
	every := opapi.Diff(st.Current, other)
	for i := range changes {
		st.Changes = append(st.Changes, opapi.SettingsChange{
			ChangedAtUS: 1_790_000_000_000_000 - int64(i), Version: st.Version - int64(i),
			By: strings.Repeat("g", byLen), Fields: every,
		})
	}
	s := baseSettings()
	s.Stand = st
	return s
}

func baseSettings() opapi.Settings {
	return opapi.Settings{SpeedFactor: 20, AcceptTimeoutUS: 20_000_000, CapacityDefault: 4}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// answer is what the test server sends back, always with the JSON media
// type.
type answer struct {
	status int
	body   string
}

// request is what the test server got.
type request struct {
	method, path, query, contentType string
	body                             []byte
}

// recorder answers every request with one answer and keeps the last request.
type recorder struct {
	mu   sync.Mutex
	a    answer
	last request
}

func (r *recorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	b, _ := io.ReadAll(req.Body)
	r.mu.Lock()
	r.last = request{req.Method, req.URL.Path, req.URL.RawQuery, req.Header.Get("Content-Type"), b}
	a := r.a
	r.mu.Unlock()
	w.Header().Set("Content-Type", opapi.ContentType)
	w.WriteHeader(a.status)
	_, _ = io.WriteString(w, a.body)
}

func (r *recorder) request() request {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last
}

// serve starts a server with h and returns a client of it that logs into
// the returned buffer.
func serve(t *testing.T, h http.Handler) (*Client, *bytes.Buffer) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	var logs bytes.Buffer
	c, err := New(srv.URL, WithTimeout(5*time.Second), WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	if err != nil {
		t.Fatal(err)
	}
	return c, &logs
}

// bare answers with a status and a text body, like http.ServeMux or a proxy.
func bare(status int) answer {
	return answer{status, http.StatusText(status) + "\n"}
}

func errAnswer(t *testing.T, e opapi.SettingsErrorResponse) answer {
	return answer{e.Code.Status(), mustJSON(t, e)}
}

// settingsError returns the *opapi.SettingsError in err, or fails.
func settingsError(t *testing.T, err error, code opapi.ErrorCode, status int) *opapi.SettingsError {
	t.Helper()
	e, ok := errors.AsType[*opapi.SettingsError](err)
	if !ok {
		t.Fatalf("error %v is not *opapi.SettingsError", err)
	}
	if e.Code != code || e.Status != status || Code(err) != code {
		t.Fatalf("error %+v, want code %q status %d", e, code, status)
	}
	return e
}

// plainError fails unless err is an error that is not an answer of the
// router.
func plainError(t *testing.T, err error, contains string) {
	t.Helper()
	if err == nil || Code(err) != "" {
		t.Fatalf("error %v, want a plain error", err)
	}
	if !strings.Contains(err.Error(), contains) {
		t.Errorf("error %q does not contain %q", err, contains)
	}
}

func TestReadSettings(t *testing.T) {
	type call struct {
		name  string
		query string
		read  func(*Client) (opapi.Settings, error)
	}
	plain := call{"Settings", "", func(c *Client) (opapi.Settings, error) { return c.Settings(t.Context()) }}
	changes := call{"SettingsWithChanges", "changes=1", func(c *Client) (opapi.Settings, error) {
		return c.SettingsWithChanges(t.Context())
	}}

	withoutLog := testSettings(0, 0)
	withLog := testSettings(3, 5)
	brokenStand := baseSettings()
	brokenStand.Stand = &opapi.StandState{Version: -1, Current: testSet(), Defaults: testSet()}
	brokenBase := testSettings(0, 0)
	brokenBase.CapacityDefault = 0
	// A valid answer above MaxBodyBytes: the largest change log.
	large := testSettings(opapi.MaxSettingsChanges, opapi.MaxByLen)
	largeJSON := mustJSON(t, large)
	if len(largeJSON) <= opapi.MaxBodyBytes || len(largeJSON) > opapi.MaxSettingsBodyBytes {
		t.Fatalf("large answer has %d bytes, want between %d and %d", len(largeJSON), opapi.MaxBodyBytes, opapi.MaxSettingsBodyBytes)
	}
	// A valid JSON object above the limit of each call.
	pad := func(n int) string { return `{"speed_factor":1,"x":"` + strings.Repeat("a", n) + `"}` }

	tests := []struct {
		name  string
		calls []call
		a     answer
		// wantErr is "" for a call that succeeds, a code for an
		// *opapi.Error, or "plain" for any other error.
		wantErr opapi.ErrorCode
		// check checks a successful answer.
		check func(t *testing.T, s opapi.Settings)
		// warn reports that the call logs a broken stand block.
		warn bool
	}{
		{name: "router from before decision 0014", calls: []call{plain, changes},
			a: answer{200, mustJSON(t, baseSettings())}, check: func(t *testing.T, s opapi.Settings) {
				if s != baseSettings() {
					t.Errorf("settings %+v", s)
				}
			}},
		{name: "stand block without log", calls: []call{plain, changes},
			a: answer{200, mustJSON(t, withoutLog)}, check: func(t *testing.T, s opapi.Settings) {
				if s.Stand == nil || s.Stand.Version != 30 || !s.Stand.Current.Equal(testSet()) || s.CapacityDefault != 4 {
					t.Errorf("settings %+v", s)
				}
			}},
		{name: "stand block with log", calls: []call{plain, changes},
			a: answer{200, mustJSON(t, withLog)}, check: func(t *testing.T, s opapi.Settings) {
				if s.Stand == nil || len(s.Stand.Changes) != 3 || s.Stand.Changes[0].By != "ggggg" {
					t.Errorf("settings %+v", s.Stand)
				}
			}},
		{name: "stand null", calls: []call{plain, changes},
			a: answer{200, `{"speed_factor":20,"accept_timeout_us":20000000,"capacity_default":4,"stand":null}`},
			check: func(t *testing.T, s opapi.Settings) {
				if s != baseSettings() {
					t.Errorf("settings %+v", s)
				}
			}},
		{name: "stand block out of bounds", calls: []call{plain, changes},
			a: answer{200, mustJSON(t, brokenStand)}, warn: true, check: func(t *testing.T, s opapi.Settings) {
				if s != baseSettings() {
					t.Errorf("settings %+v, want the base without stand", s)
				}
			}},
		{name: "stand block of the wrong shape", calls: []call{plain, changes},
			a:    answer{200, `{"speed_factor":20,"accept_timeout_us":20000000,"capacity_default":4,"stand":{"version":"1"}}`},
			warn: true, check: func(t *testing.T, s opapi.Settings) {
				if s != baseSettings() {
					t.Errorf("settings %+v, want the base without stand", s)
				}
			}},
		{name: "stand block that is a string", calls: []call{plain, changes},
			a:    answer{200, `{"speed_factor":20,"accept_timeout_us":20000000,"capacity_default":4,"stand":"x"}`},
			warn: true, check: func(t *testing.T, s opapi.Settings) {
				if s != baseSettings() {
					t.Errorf("settings %+v, want the base without stand", s)
				}
			}},
		{name: "broken base with a valid stand block", calls: []call{plain, changes},
			a: answer{200, mustJSON(t, brokenBase)}, wantErr: "plain"},
		{name: "base of the wrong shape", calls: []call{plain, changes},
			a: answer{200, `{"speed_factor":"fast"}`}, wantErr: "plain"},
		{name: "not JSON", calls: []call{plain, changes}, a: answer{200, `ok`}, wantErr: "plain"},
		{name: "bare 404", calls: []call{plain, changes}, a: bare(404), wantErr: opapi.CodeInternal},
		{name: "bare 405", calls: []call{plain, changes}, a: bare(405), wantErr: opapi.CodeInternal},
		{name: "unavailable", calls: []call{plain, changes},
			a: answer{503, `{"code":"unavailable","message":"database not reachable"}`}, wantErr: opapi.CodeUnavailable},
		{name: "proxy 502", calls: []call{plain, changes}, a: bare(502), wantErr: opapi.CodeUnavailable},
		{name: "full log above the small limit", calls: []call{changes},
			a: answer{200, largeJSON}, check: func(t *testing.T, s opapi.Settings) {
				if s.Stand == nil || len(s.Stand.Changes) != opapi.MaxSettingsChanges {
					t.Errorf("settings %+v", s.Stand)
				}
			}},
		{name: "full log above the small limit", calls: []call{plain}, a: answer{200, largeJSON}, wantErr: "plain"},
		{name: "above the small limit", calls: []call{plain}, a: answer{200, pad(opapi.MaxBodyBytes)}, wantErr: "plain"},
		{name: "above the large limit", calls: []call{changes}, a: answer{200, pad(opapi.MaxSettingsBodyBytes)}, wantErr: "plain"},
	}
	for _, tt := range tests {
		for _, cl := range tt.calls {
			t.Run(cl.name+" "+tt.name, func(t *testing.T) {
				rec := &recorder{a: tt.a}
				c, logs := serve(t, rec)
				s, err := cl.read(c)
				if got := rec.request(); got.method != http.MethodGet || got.path != "/v1/settings" || got.query != cl.query ||
					len(got.body) != 0 {
					t.Errorf("request %+v", got)
				}
				switch tt.wantErr {
				case "":
					if err != nil {
						t.Fatal(err)
					}
					tt.check(t, s)
				case "plain":
					plainError(t, err, "")
				default:
					e, ok := errors.AsType[*opapi.Error](err)
					if !ok || e.Code != tt.wantErr || e.Status != tt.a.status || Code(err) != tt.wantErr {
						t.Fatalf("error %v, want *opapi.Error with code %q", err, tt.wantErr)
					}
				}
				if got := strings.Contains(logs.String(), "level=WARN"); got != tt.warn {
					t.Errorf("warning logged = %v, want %v; log:\n%s", got, tt.warn, logs)
				}
			})
		}
	}
}

// TestBrokenStandLog checks that a client that polls logs a broken stand
// block once per path and reason, and once when the block is fine again.
func TestBrokenStandLog(t *testing.T) {
	broken := func(version int64) string {
		s := baseSettings()
		s.Stand = &opapi.StandState{Version: version, Current: testSet(), Defaults: testSet()}
		return mustJSON(t, s)
	}
	rec := &recorder{}
	c, logs := serve(t, rec)
	steps := []struct {
		body  string
		read  func() (opapi.Settings, error)
		warns int
		infos int
	}{
		{broken(-1), func() (opapi.Settings, error) { return c.Settings(t.Context()) }, 1, 0},
		{broken(-1), func() (opapi.Settings, error) { return c.Settings(t.Context()) }, 1, 0},
		// Another path has its own state.
		{broken(-1), func() (opapi.Settings, error) { return c.SettingsWithChanges(t.Context()) }, 2, 0},
		// Another reason is logged again.
		{broken(-2), func() (opapi.Settings, error) { return c.Settings(t.Context()) }, 3, 0},
		{mustJSON(t, testSettings(0, 0)), func() (opapi.Settings, error) { return c.Settings(t.Context()) }, 3, 1},
		{mustJSON(t, testSettings(0, 0)), func() (opapi.Settings, error) { return c.Settings(t.Context()) }, 3, 1},
		// An absent block is not broken either.
		{mustJSON(t, baseSettings()), func() (opapi.Settings, error) { return c.SettingsWithChanges(t.Context()) }, 3, 2},
		{broken(-1), func() (opapi.Settings, error) { return c.Settings(t.Context()) }, 4, 2},
	}
	for i, st := range steps {
		rec.mu.Lock()
		rec.a = answer{200, st.body}
		rec.mu.Unlock()
		if _, err := st.read(); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		warns, infos := strings.Count(logs.String(), "level=WARN"), strings.Count(logs.String(), "level=INFO")
		if warns != st.warns || infos != st.infos {
			t.Fatalf("step %d: %d warnings and %d infos, want %d and %d; log:\n%s", i, warns, infos, st.warns, st.infos, logs)
		}
	}
	if !strings.Contains(logs.String(), "stand: version: -2 is negative") {
		t.Errorf("log does not name the reason:\n%s", logs)
	}
}

func TestPutStandSettings(t *testing.T) {
	type call struct {
		name string
		path string
		// body is the request that put sends, as JSON.
		body string
		put  func(*Client) (opapi.Result, opapi.Settings, error)
	}
	changeReq := opapi.SettingsChangeRequest{Settings: testSet(), BaseVersion: 30, By: "guest"}
	simReq := opapi.SimDefaultsRequest{Sim: *testSet().Sim}
	putSettings := call{"PutSettings", "/v1/settings", mustJSON(t, changeReq),
		func(c *Client) (opapi.Result, opapi.Settings, error) {
			resp, err := c.PutSettings(t.Context(), changeReq)
			return resp.Result, resp.Settings, err
		}}
	putSim := call{"PutSimDefaults", "/v1/settings/sim-defaults", mustJSON(t, simReq),
		func(c *Client) (opapi.Result, opapi.Settings, error) {
			resp, err := c.PutSimDefaults(t.Context(), simReq)
			return resp.Result, resp.Settings, err
		}}
	both := []call{putSettings, putSim}

	ok := func(r opapi.Result, s opapi.Settings) string {
		return mustJSON(t, opapi.SettingsChangeResponse{Result: r, Settings: s})
	}
	current := testSettings(2, 5)
	noStand := baseSettings()
	brokenStand := baseSettings()
	brokenStand.Stand = &opapi.StandState{Version: -1, Current: testSet(), Defaults: testSet()}
	brokenBase := testSettings(0, 0)
	brokenBase.SpeedFactor = 0
	// A valid set at version 30 that no simulator has reported to yet.
	noSim := testSettings(0, 0)
	noSim.Stand.Current.Sim, noSim.Stand.Defaults.Sim = nil, nil
	large := testSettings(opapi.MaxSettingsChanges, opapi.MaxByLen)
	if n := len(ok(opapi.ResultDone, large)); n <= opapi.MaxBodyBytes {
		t.Fatalf("large answer has %d bytes, want more than %d", n, opapi.MaxBodyBytes)
	}
	pad := func(n int) string { return `{"result":"done","x":"` + strings.Repeat("a", n) + `"}` }

	tests := []struct {
		name  string
		calls []call
		a     answer
		// want is the result of a call that succeeds.
		want opapi.Result
		// code and status are those of an *opapi.SettingsError.
		code   opapi.ErrorCode
		status int
		// plain is a part of a plain error.
		plain string
		// check checks the *opapi.SettingsError.
		check func(t *testing.T, e *opapi.SettingsError)
	}{
		{name: "done", calls: both, a: answer{200, ok(opapi.ResultDone, current)}, want: opapi.ResultDone},
		{name: "already done", calls: both, a: answer{200, ok(opapi.ResultAlreadyDone, current)}, want: opapi.ResultAlreadyDone},
		{name: "full log above the small limit", calls: []call{putSettings},
			a: answer{200, ok(opapi.ResultDone, large)}, want: opapi.ResultDone},
		{name: "full log above the small limit", calls: []call{putSim},
			a: answer{200, ok(opapi.ResultDone, large)}, plain: "larger than 4096"},
		{name: "invalid with fields", calls: both, a: errAnswer(t, opapi.SettingsErrorResponse{
			ErrorResponse: opapi.ErrorResponse{Code: opapi.CodeInvalidRequest, Message: "invalid settings"},
			Fields: []opapi.FieldError{
				{Field: opapi.FieldTeamReleaseLoad, Reason: "20 is not below teams.form_load (20)"},
				{Field: opapi.FieldSimCapacity, Reason: "25 is not between 1 and 20"},
			},
		}), code: opapi.CodeInvalidRequest, status: 400, check: func(t *testing.T, e *opapi.SettingsError) {
			if len(e.Fields) != 2 || e.Fields[1].Field != opapi.FieldSimCapacity || e.Message != "invalid settings" {
				t.Errorf("error %+v", e)
			}
		}},
		{name: "invalid without fields", calls: both,
			a:    answer{400, `{"code":"invalid_request","message":"body is not a JSON object"}`},
			code: opapi.CodeInvalidRequest, status: 400, check: func(t *testing.T, e *opapi.SettingsError) {
				if len(e.Fields) != 0 {
					t.Errorf("fields %+v", e.Fields)
				}
			}},
		{name: "conflict with settings", calls: []call{putSettings}, a: errAnswer(t, opapi.SettingsErrorResponse{
			ErrorResponse: opapi.ErrorResponse{Code: opapi.CodeSettingsConflict, Message: "base version 29 is not 30"},
			Settings:      &current,
		}), code: opapi.CodeSettingsConflict, status: 409, check: func(t *testing.T, e *opapi.SettingsError) {
			if e.Settings == nil || e.Settings.Stand == nil || e.Settings.Stand.Version != 30 || len(e.Settings.Stand.Changes) != 2 {
				t.Errorf("settings %+v", e.Settings)
			}
			if !e.Code.Rejected() {
				t.Error("a conflict is not rejected")
			}
		}},
		{name: "conflict with a full log above the small limit", calls: []call{putSettings}, a: errAnswer(t, opapi.SettingsErrorResponse{
			ErrorResponse: opapi.ErrorResponse{Code: opapi.CodeSettingsConflict, Message: "m"},
			Settings:      &large,
		}), code: opapi.CodeSettingsConflict, status: 409, check: func(t *testing.T, e *opapi.SettingsError) {
			if e.Settings == nil || e.Settings.Stand == nil || len(e.Settings.Stand.Changes) != opapi.MaxSettingsChanges {
				t.Errorf("settings %+v", e.Settings)
			}
		}},
		{name: "no sim part", calls: []call{putSim}, a: answer{200, ok(opapi.ResultDone, noSim)}, plain: "no sim part"},
		{name: "no sim part", calls: []call{putSettings}, a: answer{200, ok(opapi.ResultDone, noSim)}, want: opapi.ResultDone},
		{name: "conflict with a broken stand block", calls: []call{putSettings}, a: answer{409, mustJSON(t, opapi.SettingsErrorResponse{
			ErrorResponse: opapi.ErrorResponse{Code: opapi.CodeSettingsConflict, Message: "m"},
			Settings:      &brokenStand,
		})}, plain: "not valid"},
		{name: "conflict with a stand block of the wrong shape", calls: []call{putSettings},
			a: answer{409, `{"code":"settings_conflict","message":"m","settings":{"speed_factor":1,"accept_timeout_us":1,` +
				`"capacity_default":1,"stand":"x"}}`}, plain: "decode error answer"},
		{name: "conflict without settings", calls: []call{putSettings},
			a: answer{409, `{"code":"settings_conflict","message":"m"}`}, plain: "settings is not set"},
		{name: "too soon with the wait", calls: []call{putSettings}, a: errAnswer(t, opapi.SettingsErrorResponse{
			ErrorResponse: opapi.ErrorResponse{Code: opapi.CodeSettingsTooSoon, Message: "the last change was 3.2 s ago"},
			RetryAfterMS:  6800,
		}), code: opapi.CodeSettingsTooSoon, status: 429, check: func(t *testing.T, e *opapi.SettingsError) {
			if e.RetryAfterMS != 6800 {
				t.Errorf("wait %d", e.RetryAfterMS)
			}
		}},
		{name: "too soon without the wait", calls: []call{putSettings},
			a: answer{429, `{"code":"settings_too_soon","message":"m"}`}, plain: "retry_after_ms"},
		{name: "off", calls: both, a: errAnswer(t, opapi.SettingsErrorResponse{
			ErrorResponse: opapi.ErrorResponse{Code: opapi.CodeSettingsOff, Message: "stand settings are off"},
		}), code: opapi.CodeSettingsOff, status: 501},
		{name: "bare 404", calls: both, a: bare(404), code: opapi.CodeSettingsOff, status: 404},
		{name: "bare 405", calls: both, a: bare(405), code: opapi.CodeSettingsOff, status: 405},
		{name: "404 with an error answer", calls: both,
			a:    answer{404, `{"code":"operator_not_found","message":"m"}`},
			code: opapi.CodeOperatorNotFound, status: 404},
		{name: "unavailable", calls: both,
			a:    answer{503, `{"code":"unavailable","message":"database not reachable"}`},
			code: opapi.CodeUnavailable, status: 503},
		{name: "proxy 502", calls: both, a: bare(502), code: opapi.CodeUnavailable, status: 502},
		{name: "bare 500", calls: both, a: bare(500), code: opapi.CodeInternal, status: 500},
		{name: "unknown code", calls: both, a: answer{500, `{"code":"nope","message":"m"}`}, plain: "not a known code"},
		{name: "above the small limit", calls: []call{putSim}, a: answer{200, pad(opapi.MaxBodyBytes)}, plain: "larger than 4096"},
		{name: "error above the small limit", calls: []call{putSim},
			a:     answer{400, `{"code":"invalid_request","message":"` + strings.Repeat("a", opapi.MaxBodyBytes) + `"}`},
			plain: "larger than 4096"},
		{name: "above the large limit", calls: []call{putSettings},
			a: answer{200, pad(opapi.MaxSettingsBodyBytes)}, plain: "larger than 32768"},
		{name: "broken stand block", calls: both, a: answer{200, ok(opapi.ResultDone, brokenStand)}, plain: "stand: version: -1"},
		{name: "stand block of the wrong shape", calls: both,
			a:     answer{200, `{"result":"done","settings":{"speed_factor":1,"accept_timeout_us":1,"capacity_default":1,"stand":"x"}}`},
			plain: "decode answer"},
		{name: "no stand block", calls: both, a: answer{200, ok(opapi.ResultDone, noStand)}, plain: "no stand block"},
		{name: "broken base", calls: both, a: answer{200, ok(opapi.ResultDone, brokenBase)}, plain: "speed_factor"},
		{name: "unknown result", calls: both, a: answer{200, ok("maybe", current)}, plain: "unknown result"},
		{name: "not JSON", calls: both, a: answer{200, `ok`}, plain: "decode answer"},
	}
	for _, tt := range tests {
		for _, cl := range tt.calls {
			t.Run(cl.name+" "+tt.name, func(t *testing.T) {
				rec := &recorder{a: tt.a}
				c, _ := serve(t, rec)
				res, s, err := cl.put(c)
				got := rec.request()
				if got.method != http.MethodPut || got.path != cl.path || got.query != "" ||
					got.contentType != opapi.ContentType || string(got.body) != cl.body {
					t.Errorf("request %+v, want PUT %s with %s", got, cl.path, cl.body)
				}
				switch {
				case tt.want != "":
					if err != nil || res != tt.want {
						t.Fatalf("= %q, %v; want %q", res, err, tt.want)
					}
					if s.Stand == nil || s.Stand.Version != 30 || s.Validate() != nil {
						t.Errorf("settings %+v", s)
					}
				case tt.code != "":
					e := settingsError(t, err, tt.code, tt.status)
					if _, isPlain := errors.AsType[*opapi.Error](err); isPlain {
						t.Error("error is also an *opapi.Error")
					}
					if tt.check != nil {
						tt.check(t, e)
					}
				default:
					plainError(t, err, tt.plain)
					if res != "" || s.Stand != nil {
						t.Errorf("failed call returned %q, %+v", res, s)
					}
				}
			})
		}
	}
}

// TestPutOldRouter runs both PUT calls against a mux like the one of a
// router from before decision 0014, which serves only GET /v1/settings of
// the settings paths.
func TestPutOldRouter(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc(opapi.PatternSettings, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, mustJSON(t, baseSettings()))
	})
	c, _ := serve(t, mux)

	_, err := c.PutSettings(t.Context(), opapi.SettingsChangeRequest{Settings: testSet(), By: "guest"})
	settingsError(t, err, opapi.CodeSettingsOff, http.StatusMethodNotAllowed)
	_, err = c.PutSimDefaults(t.Context(), opapi.SimDefaultsRequest{Sim: *testSet().Sim})
	settingsError(t, err, opapi.CodeSettingsOff, http.StatusNotFound)

	for _, read := range []func() (opapi.Settings, error){
		func() (opapi.Settings, error) { return c.Settings(t.Context()) },
		func() (opapi.Settings, error) { return c.SettingsWithChanges(t.Context()) },
	} {
		s, err := read()
		if err != nil || s != baseSettings() {
			t.Errorf("settings = %+v, %v; want the base without stand", s, err)
		}
	}
}

func TestPutUnreachable(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	url := dead.URL
	dead.Close()
	c, err := New(url)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.PutSettings(t.Context(), opapi.SettingsChangeRequest{Settings: testSet(), By: "guest"}); err == nil || Code(err) != "" {
		t.Errorf("PutSettings: error %v, want a plain error", err)
	}
	if _, err := c.PutSimDefaults(t.Context(), opapi.SimDefaultsRequest{}); err == nil || Code(err) != "" {
		t.Errorf("PutSimDefaults: error %v, want a plain error", err)
	}
}
