package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, slog.LevelWarn)
	logger.Info("hidden")
	logger.Warn("shown", "n", 1)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1: %q", len(lines), buf.String())
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &got); err != nil {
		t.Fatalf("line is not JSON: %v", err)
	}
	if got["msg"] != "shown" || got["level"] != "WARN" || got["n"] != float64(1) {
		t.Errorf("unexpected line: %v", got)
	}
}

func TestMux(t *testing.T) {
	reg := prometheus.NewRegistry()
	if err := registerDefaultCollectors(reg); err != nil {
		t.Fatal(err)
	}
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_things_total", Help: "Things."})
	reg.MustRegister(c)
	c.Add(3)

	var ready atomic.Bool
	srv := httptest.NewServer(newMux(reg, ready.Load, slog.New(slog.DiscardHandler)))
	t.Cleanup(srv.Close)

	tests := []struct {
		name     string
		method   string
		path     string
		setReady bool
		wantCode int
		wantBody string
	}{
		{name: "metrics has own counter", method: http.MethodGet, path: "/metrics", wantCode: http.StatusOK, wantBody: "test_things_total 3"},
		{name: "metrics has runtime collector", method: http.MethodGet, path: "/metrics", wantCode: http.StatusOK, wantBody: "go_goroutines"},
		{name: "health not ready", method: http.MethodGet, path: "/healthz", wantCode: http.StatusServiceUnavailable, wantBody: "not ready"},
		{name: "health ready", method: http.MethodGet, path: "/healthz", setReady: true, wantCode: http.StatusOK, wantBody: "ok"},
		{name: "pprof index", method: http.MethodGet, path: "/debug/pprof/", wantCode: http.StatusOK, wantBody: "goroutine"},
		{name: "pprof named profile", method: http.MethodGet, path: "/debug/pprof/goroutine?debug=1", wantCode: http.StatusOK, wantBody: "goroutine profile"},
		{name: "unknown path", method: http.MethodGet, path: "/nope", wantCode: http.StatusNotFound},
		{name: "wrong method", method: http.MethodPost, path: "/metrics", wantCode: http.StatusMethodNotAllowed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ready.Store(tt.setReady)
			req, err := http.NewRequestWithContext(t.Context(), tt.method, srv.URL+tt.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != tt.wantCode {
				t.Errorf("status %d, want %d", resp.StatusCode, tt.wantCode)
			}
			if !strings.Contains(string(body), tt.wantBody) {
				t.Errorf("body does not contain %q", tt.wantBody)
			}
		})
	}
}

func TestServerStartAndShutdown(t *testing.T) {
	reg := prometheus.NewRegistry()
	s, err := NewServer(t.Context(), "127.0.0.1:0", reg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	wait := s.Start(ctx)

	resp, err := http.Get("http://" + s.Addr() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200 with a nil ready func", resp.StatusCode)
	}

	cancel()
	errc := make(chan error, 1)
	go func() { errc <- wait() }()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(ShutdownTimeout + time.Second):
		t.Fatal("server did not stop")
	}
	if _, err := http.Get("http://" + s.Addr() + "/healthz"); err == nil {
		t.Error("server still answers after shutdown")
	}
}

func TestNewServerErrors(t *testing.T) {
	if _, err := NewServer(t.Context(), "127.0.0.1:0", nil, nil, nil); err == nil {
		t.Error("nil registry: want an error")
	}

	reg := prometheus.NewRegistry()
	first, err := NewServer(t.Context(), "127.0.0.1:0", reg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	wait := first.Start(ctx)
	t.Cleanup(func() {
		cancel()
		_ = wait()
	})

	// A second server on the same registry is fine, the same port is not.
	if _, err := NewServer(t.Context(), first.Addr(), reg, nil, nil); err == nil {
		t.Error("busy port: want an error")
	}
}
