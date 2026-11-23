// Package obs holds the observability code that every service shares
// (decision 0005): a JSON logger and one HTTP server per service for
// Prometheus metrics, pprof and a health check.
package obs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewLogger returns a logger that writes JSON lines to w at the given
// minimum level.
func NewLogger(w io.Writer, level slog.Leveler) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}

// Timeouts of the HTTP server.
const (
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 10 * time.Second
	// writeTimeout leaves room for /debug/pprof/profile and
	// /debug/pprof/trace, which stream for 30 seconds by default.
	writeTimeout = 60 * time.Second
	idleTimeout  = 2 * time.Minute
	// ShutdownTimeout is how long a stopping server waits for open
	// requests.
	ShutdownTimeout = 5 * time.Second
)

// ReadyFunc reports whether the service is ready. It is called on every
// health request and must be fast and safe for concurrent use.
type ReadyFunc func() bool

// Server serves /metrics, /debug/pprof/ and /healthz on one address.
type Server struct {
	srv    *http.Server
	ln     net.Listener
	logger *slog.Logger
}

// NewServer registers the Go runtime and process collectors on reg, builds
// the routes and starts listening on addr. It does not serve until Start.
// Listening here makes a busy port a start error, and lets tests use port 0.
//
// ready decides the answer of /healthz: 200 "ok" when it returns true, 503
// "not ready" otherwise. A nil ready is always ready.
func NewServer(ctx context.Context, addr string, reg *prometheus.Registry, ready ReadyFunc, logger *slog.Logger) (*Server, error) {
	if reg == nil {
		return nil, errors.New("obs: registry is nil")
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if ready == nil {
		ready = func() bool { return true }
	}
	if err := registerDefaultCollectors(reg); err != nil {
		return nil, err
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("obs: listen on %s: %w", addr, err)
	}
	return &Server{
		srv: &http.Server{
			Handler:           newMux(reg, ready, logger),
			ReadHeaderTimeout: readHeaderTimeout,
			ReadTimeout:       readTimeout,
			WriteTimeout:      writeTimeout,
			IdleTimeout:       idleTimeout,
			ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
		},
		ln:     ln,
		logger: logger,
	}, nil
}

func registerDefaultCollectors(reg *prometheus.Registry) error {
	for _, c := range []prometheus.Collector{
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	} {
		if err := reg.Register(c); err != nil {
			var are prometheus.AlreadyRegisteredError
			if errors.As(err, &are) {
				continue
			}
			return fmt.Errorf("obs: register collector: %w", err)
		}
	}
	return nil
}

func newMux(reg *prometheus.Registry, ready ReadyFunc, logger *slog.Logger) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		ErrorLog: slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
		Registry: reg,
	}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		if !ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "not ready\n")
			return
		}
		_, _ = io.WriteString(w, "ok\n")
	})
	// pprof.Index also serves the named profiles, for example
	// /debug/pprof/heap and /debug/pprof/goroutine.
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("POST /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	return mux
}

// Addr returns the address the server listens on.
func (s *Server) Addr() string {
	return s.ln.Addr().String()
}

// Start serves in a new goroutine until ctx ends, then shuts the server down
// and waits up to ShutdownTimeout for open requests. The returned function
// waits for the goroutine to finish and returns its error. It returns nil
// after a clean shutdown.
func (s *Server) Start(ctx context.Context) (wait func() error) {
	done := make(chan error, 1)
	go func() {
		serveErr := make(chan error, 1)
		go func() { serveErr <- s.srv.Serve(s.ln) }()

		select {
		case err := <-serveErr:
			done <- fmt.Errorf("obs: serve: %w", err)
			return
		case <-ctx.Done():
		}

		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ShutdownTimeout)
		defer cancel()
		err := s.srv.Shutdown(sctx)
		if err != nil {
			// Open requests did not finish in time. Drop them.
			_ = s.srv.Close()
		}
		if serr := <-serveErr; !errors.Is(serr, http.ErrServerClosed) && err == nil {
			err = serr
		}
		if err != nil {
			done <- fmt.Errorf("obs: shutdown: %w", err)
			return
		}
		s.logger.Debug("metrics server stopped", "addr", s.Addr())
		done <- nil
	}()
	return func() error { return <-done }
}
