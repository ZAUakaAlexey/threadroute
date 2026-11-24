package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// HTTP server timeouts of the WebSocket and snapshot server. A WebSocket
// clears them after the upgrade.
const (
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 10 * time.Second
	writeTimeout      = 10 * time.Second
	idleTimeout       = 2 * time.Minute
	shutdownTimeout   = 5 * time.Second
	// stopWait bounds the close of the connections and putting the
	// operators offline at stop. Together they stay under the stop grace
	// period of a container.
	stopWait = 10 * time.Second
	// settingsPause is the wait between two reads of the router settings
	// before the first success; after it, opapi.SettingsPollInterval.
	settingsPause = 2 * time.Second
	// orphanSweepEvery is how often online human operators without a
	// session are put offline.
	orphanSweepEvery = time.Minute
)

// ServiceOptions are the dependencies of a Service.
type ServiceOptions struct {
	Config Config
	API    OperatorAPI
	Auth   TokenChecker
	Text   TextSource
	// Clock is SystemClock when nil.
	Clock      Clock
	Logger     *slog.Logger
	Registerer prometheus.Registerer
	// ConversationsTopic and RoutingTopic are the bus names when empty.
	ConversationsTopic string
	RoutingTopic       string
	// ConsumerOpts are applied last to the Kafka client, for tests.
	ConsumerOpts []kgo.Opt
}

// Service is the gateway process: the source, the fan-out, the HTTP
// server and the background loops.
type Service struct {
	cfg    Config
	g      *Gateway
	src    *Source
	pub    *PublicSnapshots
	logger *slog.Logger
	ln     net.Listener
	sweep  chan struct{}
	// trendEvery is gwapi.TrendInterval; tests make it shorter.
	trendEvery time.Duration
}

// NewService builds the service and listens on Config.HTTPAddr, so a busy
// port is a start error and tests can use port 0. Nothing else starts
// before Run.
func NewService(ctx context.Context, o ServiceOptions) (*Service, error) {
	g, err := New(Deps{
		Config: o.Config, API: o.API, Auth: o.Auth, Text: o.Text, Clock: o.Clock,
		Logger: o.Logger, Registerer: o.Registerer,
	})
	if err != nil {
		return nil, fmt.Errorf("new gateway service: %w", err)
	}
	s := &Service{cfg: o.Config, g: g, logger: g.logger, sweep: make(chan struct{}, 1), trendEvery: gwapi.TrendInterval}
	s.src = g.NewSource(SourceOptions{
		Brokers: o.Config.KafkaBrokers, ConversationsTopic: o.ConversationsTopic, RoutingTopic: o.RoutingTopic,
		ConsumerOpts: o.ConsumerOpts,
		OnCaughtUp: func() {
			select {
			case s.sweep <- struct{}{}:
			default:
			}
		},
	})
	s.pub = g.NewPublicSnapshots()
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", o.Config.HTTPAddr)
	if err != nil {
		return nil, fmt.Errorf("new gateway service: listen on %s: %w", o.Config.HTTPAddr, err)
	}
	s.ln = ln
	return s, nil
}

// Addr returns the address of the HTTP server.
func (s *Service) Addr() string { return s.ln.Addr().String() }

// Ready reports whether the view caught up and the data is fresh.
func (s *Service) Ready() bool { return s.src.Ready() }

// Gateway returns the gateway of the service, for tests.
func (s *Service) Gateway() *Gateway { return s.g }

// Run serves until ctx ends and returns nil after a clean stop. An outage
// of Kafka, Redis, the router or the site API never stops it: the status
// and the answers say so.
func (s *Service) Run(ctx context.Context) error {
	srv := &http.Server{
		Handler:           s.g.Handler(ctx, s.pub),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		ErrorLog:          slog.NewLogLogger(s.logger.Handler(), slog.LevelWarn),
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(s.ln) }()

	var wg sync.WaitGroup
	wg.Go(func() {
		if err := s.src.Run(ctx); err != nil {
			s.logger.Error("topic source stopped", "error", err)
		}
	})
	wg.Go(func() { s.settingsLoop(ctx) })
	wg.Go(func() { s.tickLoop(ctx) })
	wg.Go(func() { s.trendLoop(ctx) })
	wg.Go(func() { s.pub.Run(ctx) })
	wg.Go(func() { s.orphanLoop(ctx) })

	var err error
	select {
	case <-ctx.Done():
	case err = <-serveErr:
		err = fmt.Errorf("gateway: serve: %w", err)
	}
	s.stop(srv)
	wg.Wait()
	return err
}

// stop closes the server, every connection with "going away", and puts
// the operators of this gateway offline.
func (s *Service) stop(srv *http.Server) {
	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	if err := srv.Shutdown(sctx); err != nil {
		_ = srv.Close()
	}
	cancel()
	s.g.hub.closeAll(gwapi.CloseGoingAway, reasonGoingAway)
	done := make(chan struct{})
	go func() {
		s.g.conns.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(stopWait):
		s.logger.Warn("connections did not close in time")
	}
	octx, cancel := context.WithTimeout(context.Background(), stopWait)
	defer cancel()
	s.g.ops.Shutdown(octx)
	s.logger.Info("gateway stopped")
}

// settingsLoop reads the router settings with the change log every
// opapi.SettingsPollInterval, and every settingsPause until the first
// read worked.
func (s *Service) settingsLoop(ctx context.Context) {
	pause := settingsPause
	failing := false
	for {
		actx, cancel := context.WithTimeout(ctx, s.cfg.RouterTimeout)
		_, err := s.g.RefreshSettings(actx)
		cancel()
		switch {
		case err == nil:
			if failing {
				s.logger.Info("router settings read again")
			}
			pause, failing = opapi.SettingsPollInterval, false
		case ctx.Err() == nil && !failing:
			// Logged once per outage; new hellos get 1013 until the first
			// read.
			s.logger.Warn("router settings not read, retrying", "error", err)
			failing = true
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(pause):
		}
	}
}

// trendLoop takes a trend point every trendEvery. It runs apart from the
// tick loop, so a point never delays a state message.
func (s *Service) trendLoop(ctx context.Context) {
	t := time.NewTicker(s.trendEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.g.RecordTrend(s.g.clock.Now())
		}
	}
}

func (s *Service) tickLoop(ctx context.Context) {
	t := time.NewTicker(s.cfg.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.g.Tick(s.g.clock.Now())
		}
	}
}

// orphanLoop puts offline the human operators that are online in the
// view but have no session here: after a restart of the gateway every one
// of them, and later the ones whose offline call failed.
func (s *Service) orphanLoop(ctx context.Context) {
	t := time.NewTicker(orphanSweepEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.sweep:
		case <-t.C:
		}
		if !s.src.Ready() {
			continue
		}
		var online []string
		s.g.view.Read(func(rd Reader) { online = rd.OnlineHumans() })
		s.g.ops.SweepOrphans(ctx, online)
	}
}
