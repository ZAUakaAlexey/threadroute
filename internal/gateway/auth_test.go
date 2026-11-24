package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
)

const siteToken = "synthetic.site.token"

func TestSiteChecker(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		want    Identity
		outcome CheckOutcome
	}{
		{"role in the list", answer(200, `{"user":{"id":42,"name":"x"},"role":{"name":"reviewer"},"extra":[1]}`),
			Identity{Level: gwapi.LevelFull, UserID: 42}, CheckFull},
		{"role not in the list", answer(200, `{"user":{"id":43},"role":{"name":"user"}}`),
			Identity{Level: gwapi.LevelDemo, UserID: 43}, CheckSignedIn},
		{"rejected", answer(401, `{"error":"x"}`), Anonymous, CheckRejected},
		{"forbidden", answer(403, ``), Anonymous, CheckRejected},
		{"server error", answer(500, ``), Anonymous, CheckUnavailable},
		{"malformed json", answer(200, `{"user":`), Anonymous, CheckMalformed},
		{"no role", answer(200, `{"user":{"id":1}}`), Anonymous, CheckMalformed},
		{"id not a number", answer(200, `{"user":{"id":"abc"},"role":{"name":"reviewer"}}`), Anonymous, CheckMalformed},
		{"id zero", answer(200, `{"user":{"id":0},"role":{"name":"reviewer"}}`), Anonymous, CheckMalformed},
		{"redirect", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "http://127.0.0.1:1/x", http.StatusFound)
		}, Anonymous, CheckMalformed},
		{"timeout", func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-time.After(2 * time.Second):
			}
		}, Anonymous, CheckUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var auth atomic.Value
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				auth.Store(r.Header.Get("Authorization"))
				tt.handler(w, r)
			}))
			defer srv.Close()
			c := NewSiteChecker(srv.URL+"/api/v1/users/me", []string{"reviewer", "admin"}, 200*time.Millisecond, time.Minute, nil, nil)
			id, outcome := c.Check(t.Context(), siteToken)
			if id != tt.want || outcome != tt.outcome {
				t.Errorf("Check() = %+v, %s; want %+v, %s", id, outcome, tt.want, tt.outcome)
			}
			if got, _ := auth.Load().(string); got != "Bearer "+siteToken {
				t.Errorf("Authorization %q", got)
			}
		})
	}
}

func answer(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

func TestSiteCheckerCache(t *testing.T) {
	var calls atomic.Int64
	status := atomic.Int64{}
	status.Store(200)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		answer(int(status.Load()), `{"user":{"id":5},"role":{"name":"reviewer"}}`)(w, r)
	}))
	defer srv.Close()
	now := t0
	c := NewSiteChecker(srv.URL, []string{"reviewer"}, time.Second, time.Minute, nil, func() time.Time { return now })
	for range 3 {
		if id, _ := c.Check(t.Context(), siteToken); id.Level != gwapi.LevelFull {
			t.Fatal("not full")
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("%d calls, want 1 with the cache", calls.Load())
	}
	// The cache key is a hash, never the token.
	c.mu.Lock()
	for k := range c.cache {
		if strings.Contains(string(k[:]), siteToken) {
			t.Error("token in the cache key")
		}
	}
	c.mu.Unlock()
	// After the cache time the site is asked again, and a failure is
	// never cached.
	now = now.Add(2 * time.Minute)
	status.Store(503)
	if id, o := c.Check(t.Context(), siteToken); id.Level != gwapi.LevelDemo || o != CheckUnavailable {
		t.Fatalf("after expiry %+v %s", id, o)
	}
	c.Check(t.Context(), siteToken)
	if calls.Load() != 3 {
		t.Fatalf("%d calls, want 3", calls.Load())
	}
	// Another token is another entry.
	status.Store(200)
	if _, o := c.Check(t.Context(), "other.token"); o != CheckFull {
		t.Fatalf("other token %s", o)
	}
}

func TestSiteCheckerWithoutURL(t *testing.T) {
	c := NewSiteChecker("", []string{"admin"}, time.Second, time.Minute, nil, nil)
	if id, o := c.Check(t.Context(), siteToken); id != Anonymous || o != CheckDisabled {
		t.Fatalf("%+v %s", id, o)
	}
	if id, o := c.Check(t.Context(), ""); id != Anonymous || o != CheckNoToken {
		t.Fatalf("%+v %s", id, o)
	}
}
