package opclient

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/router"
	"github.com/ZAUakaAlexey/threadroute/internal/router/opserver"
)

// Synthetic IDs.
const (
	testOperator = "sim-op-0001"
	testOffer    = "offer-000001"
)

// fakeOps answers every action with res and err and records the last call.
type fakeOps struct {
	res     opapi.Result
	err     error
	last    string
	replyID string
	req     opapi.OperatorRequest
}

func (f *fakeOps) PutOperator(_ context.Context, id string, req opapi.OperatorRequest) (opapi.OperatorResponse, error) {
	f.last, f.req = "put "+id, req
	if f.err != nil {
		return opapi.OperatorResponse{}, f.err
	}
	return opapi.OperatorResponse{Result: f.res, Operator: opapi.Operator{
		ID: id, Languages: req.Languages, Capacity: 4, Presence: opapi.PresenceOffline,
	}}, nil
}

func (f *fakeOps) Online(_ context.Context, id string) (opapi.Result, error) {
	f.last = "online " + id
	return f.res, f.err
}

func (f *fakeOps) Offline(_ context.Context, id string) (opapi.Result, error) {
	f.last = "offline " + id
	return f.res, f.err
}

func (f *fakeOps) Accept(_ context.Context, op, offer string) (opapi.Result, error) {
	f.last = "accept " + op + " " + offer
	return f.res, f.err
}

func (f *fakeOps) Refuse(_ context.Context, op, offer string) (opapi.Result, error) {
	f.last = "refuse " + op + " " + offer
	return f.res, f.err
}

func (f *fakeOps) Reply(_ context.Context, op, offer, replyID string) (opapi.Result, error) {
	f.last, f.replyID = "reply "+op+" "+offer, replyID
	return f.res, f.err
}

func (f *fakeOps) Resolve(_ context.Context, op, offer string) (opapi.Result, error) {
	f.last = "resolve " + op + " " + offer
	return f.res, f.err
}

func (f *fakeOps) Settings() opapi.Settings {
	return opapi.Settings{SpeedFactor: 20, AcceptTimeoutUS: 20_000_000, CapacityDefault: 4}
}

func newPair(t *testing.T, ops opserver.Operators) *Client {
	t.Helper()
	h, err := opserver.NewHandler(ops, opserver.Options{})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := New(srv.URL+"/", WithTimeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// actions calls every action method once.
var actions = []struct {
	name string
	want string
	call func(context.Context, *Client) (opapi.Result, error)
}{
	{"online", "online " + testOperator, func(ctx context.Context, c *Client) (opapi.Result, error) {
		return c.Online(ctx, testOperator)
	}},
	{"offline", "offline " + testOperator, func(ctx context.Context, c *Client) (opapi.Result, error) {
		return c.Offline(ctx, testOperator)
	}},
	{"accept", "accept " + testOperator + " " + testOffer, func(ctx context.Context, c *Client) (opapi.Result, error) {
		return c.Accept(ctx, testOperator, testOffer)
	}},
	{"refuse", "refuse " + testOperator + " " + testOffer, func(ctx context.Context, c *Client) (opapi.Result, error) {
		return c.Refuse(ctx, testOperator, testOffer)
	}},
	{"reply", "reply " + testOperator + " " + testOffer, func(ctx context.Context, c *Client) (opapi.Result, error) {
		return c.Reply(ctx, testOperator, testOffer, "r-1")
	}},
	{"resolve", "resolve " + testOperator + " " + testOffer, func(ctx context.Context, c *Client) (opapi.Result, error) {
		return c.Resolve(ctx, testOperator, testOffer)
	}},
	{"put", "put " + testOperator, func(ctx context.Context, c *Client) (opapi.Result, error) {
		resp, err := c.PutOperator(ctx, testOperator, opapi.OperatorRequest{Languages: []string{"en"}, Capacity: 2})
		return resp.Result, err
	}},
}

func TestActionsResults(t *testing.T) {
	for _, a := range actions {
		for _, res := range []opapi.Result{opapi.ResultDone, opapi.ResultAlreadyDone} {
			t.Run(a.name+" "+string(res), func(t *testing.T) {
				ops := &fakeOps{res: res}
				c := newPair(t, ops)
				got, err := a.call(t.Context(), c)
				if err != nil || got != res {
					t.Fatalf("= %q, %v; want %q", got, err, res)
				}
				if ops.last != a.want {
					t.Errorf("core got %q, want %q", ops.last, a.want)
				}
			})
		}
	}
}

func TestActionsErrors(t *testing.T) {
	for _, a := range actions {
		for _, code := range []opapi.ErrorCode{
			opapi.CodeOperatorNotFound, opapi.CodeOfferExpired, opapi.CodeAssignmentEnded, opapi.CodeInternal,
		} {
			t.Run(a.name+" "+string(code), func(t *testing.T) {
				c := newPair(t, &fakeOps{err: &router.ActionError{Code: code, Message: "m"}})
				_, err := a.call(t.Context(), c)
				e, ok := errors.AsType[*opapi.Error](err)
				if !ok {
					t.Fatalf("error %v is not *opapi.Error", err)
				}
				if e.Code != code || e.Status != code.Status() || Code(err) != code {
					t.Errorf("error %+v, want code %q status %d", e, code, code.Status())
				}
				if e.Code.Rejected() != (code.Status() == http.StatusConflict) {
					t.Errorf("Rejected() = %v", e.Code.Rejected())
				}
			})
		}
	}
}

func TestReplyAndPutBodies(t *testing.T) {
	ops := &fakeOps{res: opapi.ResultDone}
	c := newPair(t, ops)
	if _, err := c.Reply(t.Context(), testOperator, testOffer, "reply-42"); err != nil {
		t.Fatal(err)
	}
	if ops.replyID != "reply-42" {
		t.Errorf("reply ID %q", ops.replyID)
	}
	resp, err := c.PutOperator(t.Context(), testOperator, opapi.OperatorRequest{Languages: []string{"de", "und"}, Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	if ops.req.Capacity != 2 || len(ops.req.Languages) != 2 || resp.Operator.ID != testOperator {
		t.Errorf("core got %+v, answer %+v", ops.req, resp)
	}
	// A bad reply ID is answered by the server with 400.
	_, err = c.Reply(t.Context(), testOperator, testOffer, "not valid")
	if Code(err) != opapi.CodeInvalidRequest {
		t.Errorf("bad reply ID: error %v", err)
	}
	// An ID with a slash is escaped, reaches the server as one segment and
	// is rejected there.
	_, err = c.Online(t.Context(), "a/b")
	if Code(err) != opapi.CodeInvalidRequest {
		t.Errorf("ID with slash: error %v", err)
	}
}

func TestSettings(t *testing.T) {
	c := newPair(t, &fakeOps{})
	s, err := c.Settings(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if s.SpeedFactor != 20 || s.AcceptTimeout() != 20*time.Second || s.CapacityDefault != 4 {
		t.Errorf("settings %+v", s)
	}
}

func TestBrokenAnswers(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		check   func(t *testing.T, err error)
	}{
		{"proxy 502 without JSON", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "bad gateway", http.StatusBadGateway)
		}, func(t *testing.T, err error) {
			if Code(err) != opapi.CodeUnavailable {
				t.Errorf("error %v, want code unavailable", err)
			}
		}},
		{"500 without JSON", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "oops", http.StatusInternalServerError)
		}, func(t *testing.T, err error) {
			if Code(err) != opapi.CodeInternal {
				t.Errorf("error %v, want code internal", err)
			}
		}},
		{"200 with an unknown result", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"result":"maybe"}`))
		}, func(t *testing.T, err error) {
			if err == nil || Code(err) != "" {
				t.Errorf("error %v, want a plain error", err)
			}
		}},
		{"200 not JSON", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`ok`))
		}, func(t *testing.T, err error) {
			if err == nil || Code(err) != "" {
				t.Errorf("error %v, want a plain error", err)
			}
		}},
		{"answer too large", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"result":"done","x":"` + strings.Repeat("a", opapi.MaxBodyBytes) + `"}`))
		}, func(t *testing.T, err error) {
			if err == nil || !strings.Contains(err.Error(), "larger") {
				t.Errorf("error %v, want too large", err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()
			c, err := New(srv.URL)
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.Online(t.Context(), testOperator)
			tt.check(t, err)
		})
	}
}

func TestTimeoutAndUnreachable(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-block:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(block)
	c, err := New(srv.URL, WithTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Accept(t.Context(), testOperator, testOffer)
	if !errors.Is(err, context.DeadlineExceeded) || Code(err) != "" {
		t.Errorf("slow server: error %v, want a deadline error", err)
	}

	dead := httptest.NewServer(http.NotFoundHandler())
	url := dead.URL
	dead.Close()
	c, err = New(url)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Online(t.Context(), testOperator); err == nil || Code(err) != "" {
		t.Errorf("closed server: error %v", err)
	}
}

func TestNew(t *testing.T) {
	for _, u := range []string{"", "localhost:9470", "ftp://h", "http://", "http://h?x=1", "://bad"} {
		if _, err := New(u); err == nil {
			t.Errorf("New(%q) = nil error", u)
		}
	}
	if _, err := New("http://127.0.0.1:9470", WithTimeout(0)); err == nil {
		t.Error("New with timeout 0 = nil error")
	}
	if _, err := New("http://127.0.0.1:9470/prefix"); err != nil {
		t.Errorf("New with a path prefix: %v", err)
	}
}
