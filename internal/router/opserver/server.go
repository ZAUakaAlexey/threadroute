package opserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Timeouts of the HTTP server. Bodies are at most opapi.MaxBodyBytes, so
// short limits are enough.
const (
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 10 * time.Second
	writeTimeout      = DefaultTimeout + 5*time.Second
	idleTimeout       = 2 * time.Minute
	// ShutdownTimeout is how long a stopping server waits for open
	// requests.
	ShutdownTimeout = 5 * time.Second
)

type metrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	actions  *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer) (*metrics, error) {
	m := &metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "threadroute", Subsystem: "router", Name: "http_requests_total",
			Help: "Operator API requests by route pattern and HTTP status.",
		}, []string{"route", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "threadroute", Subsystem: "router", Name: "http_request_seconds",
			Help: "Operator API request duration by route pattern.",
			// 0.5 ms to about 8 s.
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 15),
		}, []string{"route"}),
		actions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "threadroute", Subsystem: "router", Name: "operator_actions_total",
			Help: "Operator actions by action and result: done, already_done, or the error code.",
		}, []string{"action", "result"}),
	}
	for _, c := range []prometheus.Collector{m.requests, m.duration, m.actions} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("register operator API metrics: %w", err)
		}
	}
	return m, nil
}

// Server serves the operator API on one address.
type Server struct {
	srv    *http.Server
	ln     net.Listener
	logger *slog.Logger
}

// Listen starts listening on addr and returns a Server that serves h after
// Start. Listening here makes a busy port a start error, and lets tests use
// port 0.
func Listen(ctx context.Context, addr string, h http.Handler, logger *slog.Logger) (*Server, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("operator API: listen on %s: %w", addr, err)
	}
	return &Server{
		srv: &http.Server{
			Handler:           h,
			ReadHeaderTimeout: readHeaderTimeout,
			ReadTimeout:       readTimeout,
			WriteTimeout:      writeTimeout,
			IdleTimeout:       idleTimeout,
			MaxHeaderBytes:    16 << 10,
			ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
		},
		ln:     ln,
		logger: logger,
	}, nil
}

// Addr returns the address the server listens on.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Close stops listening without serving. Use it when Start is never
// called.
func (s *Server) Close() error { return s.ln.Close() }

// Serve serves until ctx ends, then shuts down and waits up to
// ShutdownTimeout for open requests. It returns nil after a clean
// shutdown.
func (s *Server) Serve(ctx context.Context) error {
	serveErr := make(chan error, 1)
	go func() { serveErr <- s.srv.Serve(s.ln) }()

	select {
	case err := <-serveErr:
		return fmt.Errorf("operator API: serve: %w", err)
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
		return fmt.Errorf("operator API: shutdown: %w", err)
	}
	s.logger.Debug("operator API stopped", "addr", s.Addr())
	return nil
}
