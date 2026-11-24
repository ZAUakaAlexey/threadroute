package opserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/router"
)

// Synthetic IDs.
const (
	testOperator = "sim-op-0001"
	testOffer    = "offer-000001"
)

// call is one call that reached the fake.
type call struct {
	method          string
	operator, offer string
	replyID         string
	req             opapi.OperatorRequest
}

// fakeOps records the calls and answers with res and err.
type fakeOps struct {
	mu    sync.Mutex
	calls []call
	res   opapi.Result
	err   error
}

func (f *fakeOps) record(c call) (opapi.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, c)
	return f.res, f.err
}

func (f *fakeOps) last(t *testing.T) call {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		t.Fatal("no call reached the core")
	}
	return f.calls[len(f.calls)-1]
}

func (f *fakeOps) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeOps) PutOperator(_ context.Context, id string, req opapi.OperatorRequest) (opapi.OperatorResponse, error) {
	res, err := f.record(call{method: "put", operator: id, req: req})
	if err != nil {
		return opapi.OperatorResponse{}, err
	}
	return opapi.OperatorResponse{Result: res, Operator: opapi.Operator{
		ID: id, Languages: req.Languages, Capacity: max(req.Capacity, 4), Presence: opapi.PresenceOffline,
	}}, nil
}

func (f *fakeOps) Online(_ context.Context, id string) (opapi.Result, error) {
	return f.record(call{method: "online", operator: id})
}

func (f *fakeOps) Offline(_ context.Context, id string) (opapi.Result, error) {
	return f.record(call{method: "offline", operator: id})
}

func (f *fakeOps) Accept(_ context.Context, op, offer string) (opapi.Result, error) {
	return f.record(call{method: "accept", operator: op, offer: offer})
}

func (f *fakeOps) Refuse(_ context.Context, op, offer string) (opapi.Result, error) {
	return f.record(call{method: "refuse", operator: op, offer: offer})
}

func (f *fakeOps) Reply(_ context.Context, op, offer, replyID string) (opapi.Result, error) {
	return f.record(call{method: "reply", operator: op, offer: offer, replyID: replyID})
}

func (f *fakeOps) Resolve(_ context.Context, op, offer string) (opapi.Result, error) {
	return f.record(call{method: "resolve", operator: op, offer: offer})
}

func (f *fakeOps) Settings() opapi.Settings {
	return opapi.Settings{SpeedFactor: 50, AcceptTimeoutUS: 20_000_000, CapacityDefault: 4}
}

func newTestHandler(t *testing.T, ops Operators) (http.Handler, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	h, err := NewHandler(ops, Options{Registerer: reg})
	if err != nil {
		t.Fatal(err)
	}
	return h, reg
}

func serve(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, rd))
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != opapi.ContentType {
		t.Errorf("Content-Type = %q, want %q", ct, opapi.ContentType)
	}
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("answer %q: %v", rec.Body.String(), err)
	}
	return v
}

// route is one route of the API with a valid request.
type route struct {
	name, method, path, body string
	want                     call
}

func routes() []route {
	return []route{
		{"put operator", http.MethodPut, opapi.OperatorPath(testOperator), `{"languages":["en","und"],"capacity":3}`,
			call{method: "put", operator: testOperator, req: opapi.OperatorRequest{Languages: []string{"en", "und"}, Capacity: 3}}},
		{"put operator with priority 9", http.MethodPut, opapi.OperatorPath(testOperator), `{"languages":["en"],"priority":9}`,
			call{method: "put", operator: testOperator, req: opapi.OperatorRequest{Languages: []string{"en"}, Priority: 9}}},
		{"online", http.MethodPost, opapi.OnlinePath(testOperator), "", call{method: "online", operator: testOperator}},
		{"offline", http.MethodPost, opapi.OfflinePath(testOperator), "", call{method: "offline", operator: testOperator}},
		{"accept", http.MethodPost, opapi.AcceptPath(testOperator, testOffer), "",
			call{method: "accept", operator: testOperator, offer: testOffer}},
		{"refuse", http.MethodPost, opapi.RefusePath(testOperator, testOffer), "",
			call{method: "refuse", operator: testOperator, offer: testOffer}},
		{"reply", http.MethodPost, opapi.ReplyPath(testOperator, testOffer), `{"reply_id":"r-1"}`,
			call{method: "reply", operator: testOperator, offer: testOffer, replyID: "r-1"}},
		{"resolve", http.MethodPost, opapi.ResolvePath(testOperator, testOffer), "",
			call{method: "resolve", operator: testOperator, offer: testOffer}},
	}
}

// TestRoutesResults sends every action route with each successful result
// of the core.
func TestRoutesResults(t *testing.T) {
	for _, rt := range routes() {
		for _, res := range []opapi.Result{opapi.ResultDone, opapi.ResultAlreadyDone} {
			t.Run(rt.name+" "+string(res), func(t *testing.T) {
				ops := &fakeOps{res: res}
				h, _ := newTestHandler(t, ops)
				rec := serve(h, rt.method, rt.path, rt.body)
				if rec.Code != http.StatusOK {
					t.Fatalf("status %d, body %s", rec.Code, rec.Body)
				}
				got := ops.last(t)
				if fmt.Sprint(got) != fmt.Sprint(rt.want) {
					t.Errorf("core got %+v, want %+v", got, rt.want)
				}
				if rt.want.method == "put" {
					resp := decode[opapi.OperatorResponse](t, rec)
					if resp.Result != res || resp.Operator.ID != testOperator {
						t.Errorf("answer %+v", resp)
					}
					return
				}
				if resp := decode[opapi.ActionResponse](t, rec); resp.Result != res {
					t.Errorf("result %q, want %q", resp.Result, res)
				}
			})
		}
	}
}

// TestRoutesErrors maps every error kind of the core on every action route.
func TestRoutesErrors(t *testing.T) {
	type errCase struct {
		name   string
		err    error
		code   opapi.ErrorCode
		status int
	}
	var cases []errCase
	for _, c := range opapi.Codes {
		cases = append(cases, errCase{"action error " + string(c), &router.ActionError{Code: c, Message: "test message"}, c, c.Status()})
	}
	cases = append(cases,
		errCase{"wrapped action error", fmt.Errorf("outer: %w", &router.ActionError{Code: opapi.CodeOfferExpired, Message: "m"}),
			opapi.CodeOfferExpired, http.StatusConflict},
		errCase{"timeout", fmt.Errorf("db: %w", context.DeadlineExceeded), opapi.CodeUnavailable, http.StatusServiceUnavailable},
		errCase{"connection exception", &pgconn.PgError{Code: "08006", Message: "secret detail"}, opapi.CodeUnavailable, http.StatusServiceUnavailable},
		errCase{"deadlock", &pgconn.PgError{Code: "40P01"}, opapi.CodeUnavailable, http.StatusServiceUnavailable},
		errCase{"shutdown", &pgconn.PgError{Code: "57P01"}, opapi.CodeUnavailable, http.StatusServiceUnavailable},
		errCase{"constraint", &pgconn.PgError{Code: "23505", Message: "secret detail"}, opapi.CodeInternal, http.StatusInternalServerError},
		errCase{"plain", errors.New("secret detail"), opapi.CodeInternal, http.StatusInternalServerError},
	)
	for _, rt := range routes() {
		for _, ec := range cases {
			t.Run(rt.name+" "+ec.name, func(t *testing.T) {
				h, _ := newTestHandler(t, &fakeOps{err: ec.err})
				rec := serve(h, rt.method, rt.path, rt.body)
				if rec.Code != ec.status {
					t.Fatalf("status %d, want %d, body %s", rec.Code, ec.status, rec.Body)
				}
				er := decode[opapi.ErrorResponse](t, rec)
				if er.Code != ec.code {
					t.Errorf("code %q, want %q", er.Code, ec.code)
				}
				if strings.Contains(er.Message, "secret") {
					t.Errorf("message %q leaks the database error", er.Message)
				}
			})
		}
	}
}

func TestUnknownResult(t *testing.T) {
	h, _ := newTestHandler(t, &fakeOps{res: "maybe"})
	rec := serve(h, http.MethodPost, opapi.OnlinePath(testOperator), "")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", rec.Code)
	}
}

// TestBadRequests checks that invalid paths and bodies get 400 and never
// reach the core.
func TestBadRequests(t *testing.T) {
	long := strings.Repeat("a", opapi.MaxBodyBytes)
	tests := []struct {
		name, method, path, body string
	}{
		{"operator ID with a bad character", http.MethodPost, "/v1/operators/bad%21id/online", ""},
		{"operator ID too long", http.MethodPost, opapi.OnlinePath(strings.Repeat("a", 65)), ""},
		{"offer ID with a bad character", http.MethodPost, "/v1/operators/" + testOperator + "/offers/x%2Ay/accept", ""},
		{"put without body", http.MethodPut, opapi.OperatorPath(testOperator), ""},
		{"put not JSON", http.MethodPut, opapi.OperatorPath(testOperator), "languages=en"},
		{"put wrong type", http.MethodPut, opapi.OperatorPath(testOperator), `{"languages":"en"}`},
		{"put two objects", http.MethodPut, opapi.OperatorPath(testOperator), `{"languages":["en"]}{"languages":["de"]}`},
		{"put no languages", http.MethodPut, opapi.OperatorPath(testOperator), `{"languages":[]}`},
		{"put bad language", http.MethodPut, opapi.OperatorPath(testOperator), `{"languages":["English"]}`},
		{"put duplicate language", http.MethodPut, opapi.OperatorPath(testOperator), `{"languages":["en","en"]}`},
		{"put capacity too big", http.MethodPut, opapi.OperatorPath(testOperator), `{"languages":["en"],"capacity":101}`},
		{"put negative capacity", http.MethodPut, opapi.OperatorPath(testOperator), `{"languages":["en"],"capacity":-1}`},
		{"put priority too big", http.MethodPut, opapi.OperatorPath(testOperator), `{"languages":["en"],"priority":10}`},
		{"put negative priority", http.MethodPut, opapi.OperatorPath(testOperator), `{"languages":["en"],"priority":-1}`},
		{"put priority not a whole number", http.MethodPut, opapi.OperatorPath(testOperator), `{"languages":["en"],"priority":1.5}`},
		{"put oversized", http.MethodPut, opapi.OperatorPath(testOperator), `{"languages":["en"],"x":"` + long + `"}`},
		{"put oversized after object", http.MethodPut, opapi.OperatorPath(testOperator), `{"languages":["en"]}` + strings.Repeat(" ", opapi.MaxBodyBytes)},
		{"reply without body", http.MethodPost, opapi.ReplyPath(testOperator, testOffer), ""},
		{"reply empty ID", http.MethodPost, opapi.ReplyPath(testOperator, testOffer), `{"reply_id":""}`},
		{"reply bad ID", http.MethodPost, opapi.ReplyPath(testOperator, testOffer), `{"reply_id":"a b"}`},
		{"reply ID too long", http.MethodPost, opapi.ReplyPath(testOperator, testOffer), `{"reply_id":"` + strings.Repeat("r", 65) + `"}`},
		{"reply oversized", http.MethodPost, opapi.ReplyPath(testOperator, testOffer), `{"reply_id":"` + long + `"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ops := &fakeOps{res: opapi.ResultDone}
			h, _ := newTestHandler(t, ops)
			rec := serve(h, tt.method, tt.path, tt.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status %d, want 400, body %s", rec.Code, rec.Body)
			}
			if er := decode[opapi.ErrorResponse](t, rec); er.Code != opapi.CodeInvalidRequest || er.Message == "" {
				t.Errorf("answer %+v", er)
			}
			if n := ops.count(); n != 0 {
				t.Errorf("%d calls reached the core", n)
			}
		})
	}
}

func TestUnknownFieldsAndNoBodyRoutes(t *testing.T) {
	ops := &fakeOps{res: opapi.ResultDone}
	h, _ := newTestHandler(t, ops)
	if rec := serve(h, http.MethodPut, opapi.OperatorPath(testOperator), `{"languages":["en"],"team":"x"}`); rec.Code != http.StatusOK {
		t.Errorf("unknown field: status %d, want 200", rec.Code)
	}
	// A body on a route without one is not read.
	if rec := serve(h, http.MethodPost, opapi.AcceptPath(testOperator, testOffer), `ignored`); rec.Code != http.StatusOK {
		t.Errorf("body on accept: status %d, want 200", rec.Code)
	}
}

func TestSettingsAndMux(t *testing.T) {
	h, reg := newTestHandler(t, &fakeOps{})
	rec := serve(h, http.MethodGet, opapi.SettingsPath(), "")
	if rec.Code != http.StatusOK {
		t.Fatalf("settings: status %d", rec.Code)
	}
	if s := decode[opapi.Settings](t, rec); s.SpeedFactor != 50 || s.CapacityDefault != 4 || s.Validate() != nil {
		t.Errorf("settings %+v", s)
	}
	if rec := serve(h, http.MethodGet, opapi.AcceptPath(testOperator, testOffer), ""); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET accept: status %d, want 405", rec.Code)
	}
	if rec := serve(h, http.MethodGet, "/v1/nothing", ""); rec.Code != http.StatusNotFound {
		t.Errorf("unknown path: status %d, want 404", rec.Code)
	}
	n := testutil.ToFloat64(must(t, reg, "threadroute_router_http_requests_total", opapi.PatternSettings, "200"))
	if n != 1 {
		t.Errorf("requests counted for settings = %v, want 1", n)
	}
}

func TestActionMetrics(t *testing.T) {
	ops := &fakeOps{res: opapi.ResultAlreadyDone}
	h, reg := newTestHandler(t, ops)
	serve(h, http.MethodPost, opapi.AcceptPath(testOperator, testOffer), "")
	ops.res, ops.err = "", &router.ActionError{Code: opapi.CodeOfferAccepted}
	serve(h, http.MethodPost, opapi.RefusePath(testOperator, testOffer), "")
	got, err := testutil.GatherAndCount(reg, "threadroute_router_operator_actions_total")
	if err != nil || got != 2 {
		t.Fatalf("operator_actions_total series = %d, %v; want 2", got, err)
	}
}

func TestNewHandlerNil(t *testing.T) {
	if _, err := NewHandler(nil, Options{}); err == nil {
		t.Error("NewHandler(nil) = nil error")
	}
}

// must returns the counter of a metric family with the given labels.
func must(t *testing.T, reg *prometheus.Registry, name string, labels ...string) prometheus.Collector {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			match := true
			for i, l := range m.GetLabel() {
				if i >= len(labels) || l.GetValue() != labels[i] {
					match = false
				}
			}
			if match {
				c := prometheus.NewCounter(prometheus.CounterOpts{Name: "copy"})
				c.Add(m.GetCounter().GetValue())
				return c
			}
		}
	}
	t.Fatalf("metric %s %v not found", name, labels)
	return nil
}
