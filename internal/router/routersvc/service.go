// Package routersvc runs the router service around the routing core of
// internal/router (decisions 0009 and 0010): the consumer of the
// conversations topic with its catch-up, the outbox publisher to the
// routing topic, the operator API, and the background loops of assignment,
// offer expiry, SLA, teams, the refresh of operator snapshots (decision
// 0011), outbox pruning and state sampling.
//
// # Start-up order
//
// New reads the stored stand settings (decision 0014) before anything
// routes, so a set changed from the console survives a restart.
//
// Run waits for the conversations topic (conv creates it), ensures the
// routing topic, and starts the outbox publisher, the refresh, prune and
// sampling loops and the operator API. Then it catches up: it reads the end offsets
// of the conversations topic and applies everything up to them with
// catchUp=true, so no offer is made in that phase. It then calls
// EndCatchUp, which puts the conversations that are still open into their
// queues with the reason catch_up. Only then is the service ready
// (/healthz answers 200) and only then do the assignment workers and the
// expiry, SLA and team loops start. After that the consumer applies new
// records as they come.
//
// The operator API serves during the catch-up. Every action is checked
// against the database, and no offer is made until the catch-up ends.
//
// # One consumer process
//
// The router consumes as one process. The assignment workers inside it run
// at the same time, and every database operation of the core is safe for
// several processes, but the catch-up assumes one consumer process: it
// waits until this process has read every partition up to the end offsets,
// which only works when the group gives it every partition. A second
// router process in the same group would keep partitions from the first,
// and EndCatchUp could run while another process still applies backlog.
//
// # Outages
//
// A failing loop step, a failing database call of the consumer, or a run of
// fetch errors is retried with a backoff. When it has failed without a
// break for Config.OutageLimit, Run returns an error and the process exits,
// so the restart policy starts it again, like the other services.
package routersvc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/router"
	"github.com/ZAUakaAlexey/threadroute/internal/router/opserver"
)

// Batch sizes of the loops.
const (
	// assignBatch is the most waiting conversations one Assign call tries.
	assignBatch = 100
	// timerBatch is the most offers or conversations one expiry or SLA
	// call handles.
	timerBatch = 500
	// pruneBatch is the most rows one prune call deletes.
	pruneBatch = 1000
	// clientCloseTimeout bounds the close of a Kafka client at the end.
	clientCloseTimeout = 10 * time.Second
)

// Options are the dependencies of a Service.
type Options struct {
	// Config is the service configuration. It must be valid.
	Config Config
	// Pool is a pool to a migrated database. Required. The Service does
	// not close it.
	Pool *pgxpool.Pool
	// Clock is the clock of the routing core. router.SystemClock when nil.
	Clock router.Clock
	// Logger may be nil.
	Logger *slog.Logger
	// Registerer gets the metrics. Required.
	Registerer prometheus.Registerer

	// ConversationsTopic and RoutingTopic are bus.TopicConversations and
	// bus.TopicRouting when empty. Tests set them, so several scenarios
	// can share one broker.
	ConversationsTopic string
	RoutingTopic       string
	// ConsumerOpts and ProducerOpts are applied last to the Kafka clients,
	// for tests.
	ConsumerOpts []kgo.Opt
	ProducerOpts []kgo.Opt
}

// Service is the router service.
type Service struct {
	cfg          Config
	core         *router.Service
	pool         *pgxpool.Pool
	clock        router.Clock
	logger       *slog.Logger
	m            *metrics
	runner       *runner
	api          *opserver.Server
	convTopic    string
	routingTopic string
	consumerOpts []kgo.Opt
	producerOpts []kgo.Opt

	// assignKick wakes the assignment workers. Its channels are created in
	// New, before any goroutine can kick.
	assignKick  kicker
	assignWakes []<-chan struct{}

	ready     atomic.Bool
	publisher atomic.Pointer[Publisher]
}

// New builds the Service and starts listening on the API address, so a
// busy port is a start error. Nothing is served or consumed until Run.
func New(ctx context.Context, o Options) (*Service, error) {
	if err := o.Config.Validate(); err != nil {
		return nil, fmt.Errorf("new router service: %w", err)
	}
	if o.Pool == nil || o.Registerer == nil {
		return nil, errors.New("new router service: pool and registerer are required")
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.Clock == nil {
		o.Clock = router.SystemClock{}
	}
	if o.ConversationsTopic == "" {
		o.ConversationsTopic = bus.TopicConversations
	}
	if o.RoutingTopic == "" {
		o.RoutingTopic = bus.TopicRouting
	}
	core, err := router.New(o.Pool, o.Config.Router, o.Clock, o.Logger)
	if err != nil {
		return nil, fmt.Errorf("new router service: %w", err)
	}
	// The stored stand settings (decision 0014) win over the team values of
	// the environment, so they are read before anything routes.
	if err := core.LoadSettings(ctx); err != nil {
		return nil, fmt.Errorf("new router service: %w", err)
	}
	m, err := newMetrics(o.Registerer)
	if err != nil {
		return nil, fmt.Errorf("new router service: %w", err)
	}
	if err := o.Registerer.Register(newSettingsCollector(core)); err != nil {
		return nil, fmt.Errorf("new router service: register stand settings metrics: %w", err)
	}
	s := &Service{
		cfg:          o.Config,
		core:         core,
		pool:         o.Pool,
		clock:        o.Clock,
		logger:       o.Logger,
		m:            m,
		convTopic:    o.ConversationsTopic,
		routingTopic: o.RoutingTopic,
		consumerOpts: o.ConsumerOpts,
		producerOpts: o.ProducerOpts,
	}
	s.runner = &runner{
		timer:       realTimer{},
		outageLimit: o.Config.OutageLimit,
		logger:      o.Logger,
		failed:      func(loop string) { m.loopErrors.WithLabelValues(loop).Inc() },
	}
	for range o.Config.AssignWorkers {
		s.assignWakes = append(s.assignWakes, s.assignKick.add())
	}

	h, err := opserver.NewHandler(kickingOps{Service: core, kick: s.assignKick.kick}, opserver.Options{
		Logger: o.Logger, Registerer: o.Registerer,
	})
	if err != nil {
		return nil, fmt.Errorf("new router service: %w", err)
	}
	api, err := opserver.Listen(ctx, o.Config.APIAddr, h, o.Logger)
	if err != nil {
		return nil, fmt.Errorf("new router service: %w", err)
	}
	s.api = api
	if !isLoopback(o.Config.APIAddr) {
		o.Logger.Warn("the operator API has no authentication and listens beyond localhost",
			"api_addr", o.Config.APIAddr)
	}
	return s, nil
}

// Ready reports whether the catch-up has ended and the service routes. It
// is safe for concurrent use.
func (s *Service) Ready() bool { return s.ready.Load() }

// APIAddr returns the address the operator API listens on.
func (s *Service) APIAddr() string { return s.api.Addr() }

// Core returns the routing core.
func (s *Service) Core() *router.Service { return s.core }

// Run runs the service until ctx ends or a fatal error happens. It returns
// nil after a clean stop, also when ctx ends during start. Run can be
// called once.
func (s *Service) Run(ctx context.Context) error {
	served := false
	defer func() {
		s.ready.Store(false)
		if !served {
			_ = s.api.Close()
		}
	}()

	admin, err := kgo.NewClient(kgo.SeedBrokers(s.cfg.KafkaBrokers...))
	if err != nil {
		return fmt.Errorf("router: kafka admin client: %w", err)
	}
	err = s.prepareTopics(ctx, admin)
	admin.Close()
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("router: %w", err)
	}

	prod, err := s.newProducer()
	if err != nil {
		return err
	}
	defer closeClient(prod, s.logger, "producer")
	cons, err := s.newConsumer()
	if err != nil {
		return err
	}
	defer func() {
		cons.AllowRebalance()
		// Close leaves the group, so a restart gets the partitions at
		// once. Every applied batch committed its own offsets.
		closeClient(cons, s.logger, "consumer")
	}()

	pub := &Publisher{
		store: s.core, producer: prod, lock: newPGLocker(s.pool, PublisherLockKey),
		topic: s.routingTopic, batch: s.cfg.PublishBatch, interval: s.cfg.PublishInterval,
		runner: s.runner, m: s.m, logger: s.logger,
	}
	s.publisher.Store(pub)
	c := &consumer{
		client: cons, admin: kadm.NewClient(cons), apply: s.core, topic: s.convTopic,
		group: s.cfg.GroupID, runner: s.runner, m: s.m, logger: s.logger, timer: realTimer{},
		applied: func(queued bool) {
			if queued {
				s.assignKick.kick()
			}
		},
	}

	g, gctx := errgroup.WithContext(ctx)
	served = true
	g.Go(func() error { return s.api.Serve(gctx) })
	s.logger.Info("operator API listening", "addr", s.api.Addr())
	g.Go(func() error { return pub.Run(gctx) })
	g.Go(func() error { return s.runner.loop(gctx, s.refreshLoop()) })
	g.Go(func() error { return s.runner.loop(gctx, s.pruneLoop()) })
	g.Go(func() error { return s.runner.loop(gctx, s.countsLoop()) })
	g.Go(func() error {
		if err := s.catchUp(gctx, c); err != nil || gctx.Err() != nil {
			return err
		}
		for _, l := range s.routingLoops() {
			g.Go(func() error { return s.runner.loop(gctx, l) })
		}
		s.assignKick.kick()
		return c.live(gctx)
	})
	err = g.Wait()
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("router: %w", err)
	}
	s.logger.Info("router stopped")
	return nil
}

// prepareTopics waits for the input topic and ensures the routing topic,
// which the router owns.
func (s *Service) prepareTopics(ctx context.Context, admin *kgo.Client) error {
	if err := waitForTopic(ctx, admin, s.convTopic, s.cfg.InputTopicWait, s.logger); err != nil {
		return err
	}
	spec := bus.RoutingTopicSpec()
	spec.Name = s.routingTopic
	return bus.EnsureTopics(ctx, admin, s.logger, spec)
}

// catchUp runs the catch-up and EndCatchUp, then marks the service ready.
func (s *Service) catchUp(ctx context.Context, c *consumer) error {
	start := time.Now()
	s.m.catchUpActive.Set(1)
	if err := c.catchUp(ctx); err != nil || ctx.Err() != nil {
		return err
	}
	n, err := c.endCatchUp(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	took := time.Since(start)
	s.m.catchUpActive.Set(0)
	s.m.catchUpSeconds.Set(took.Seconds())
	s.m.catchUpQueued.Set(float64(n))
	s.ready.Store(true)
	s.logger.Info("catch-up ended, routing", "queued", n, "took", took.Round(time.Millisecond).String())
	return nil
}

// routingLoops are the loops that start after the catch-up.
func (s *Service) routingLoops() []loopSpec {
	speed := s.cfg.Router.SpeedFactor
	loops := make([]loopSpec, 0, len(s.assignWakes)+3)
	for _, wake := range s.assignWakes {
		loops = append(loops, loopSpec{
			name: "assign", interval: s.cfg.AssignInterval, kick: wake,
			step: func(ctx context.Context) (bool, error) {
				r, err := s.core.Assign(ctx, assignBatch)
				s.m.offers.Add(float64(r.Offered))
				s.m.generalQueue.Add(float64(r.GeneralQueue))
				s.m.overflow.Add(float64(r.Overflow))
				return r.Scanned >= assignBatch && r.Offered > 0, err
			},
		})
	}
	return append(loops,
		loopSpec{
			name: "expire", interval: modelInterval(s.cfg.ExpireInterval, speed),
			step: func(ctx context.Context) (bool, error) {
				n, err := s.core.ExpireOffers(ctx, timerBatch)
				s.m.expired.Add(float64(n))
				if n > 0 {
					s.assignKick.kick()
				}
				return n >= timerBatch, err
			},
		},
		loopSpec{
			name: "sla", interval: modelInterval(s.cfg.SLAInterval, speed),
			step: func(ctx context.Context) (bool, error) {
				n, err := s.core.CheckSLA(ctx, timerBatch)
				s.m.slaMarked.Add(float64(n))
				return false, err
			},
		},
		loopSpec{
			name: "teams", interval: modelInterval(s.cfg.TeamInterval, speed),
			step: func(ctx context.Context) (bool, error) {
				r, err := s.core.CheckTeams(ctx)
				s.m.observeTeams(r)
				if r.Formed+r.Resized+r.Released+r.Moved > 0 {
					s.assignKick.kick()
				}
				return false, err
			},
		},
	)
}

// refreshLoop publishes the snapshot of every operator again when its last
// operator event is older than Config.OperatorRefresh (decision 0011,
// "Operators stay in the view"). The time of the last event is stored per
// operator, so a restart does not reset it, and the refresh is safe when
// two processes run it.
func (s *Service) refreshLoop() loopSpec {
	return loopSpec{
		name: "refresh", interval: refreshCheck(s.cfg.OperatorRefresh),
		step: func(ctx context.Context) (bool, error) {
			n, err := s.core.RefreshOperators(ctx, s.clock.Now().Add(-s.cfg.OperatorRefresh), refreshBatch)
			s.m.refreshed.Add(float64(n))
			return n >= refreshBatch, err
		},
	}
}

// refreshCheck returns the pause of the refresh loop for a refresh
// interval: a sixtieth of it, between minInterval and maxRefreshCheck.
func refreshCheck(refresh time.Duration) time.Duration {
	return min(max(refresh/60, minInterval), maxRefreshCheck)
}

func (s *Service) pruneLoop() loopSpec {
	return loopSpec{
		name: "prune", interval: s.cfg.PruneInterval,
		step: func(ctx context.Context) (bool, error) {
			n, err := s.core.PruneOutbox(ctx, s.clock.Now().Add(-s.cfg.OutboxKeep), pruneBatch)
			s.m.pruned.Add(float64(n))
			return n >= pruneBatch, err
		},
	}
}

func (s *Service) countsLoop() loopSpec {
	return loopSpec{
		name: "counts", interval: s.cfg.CountsInterval,
		step: func(ctx context.Context) (bool, error) {
			c, err := s.core.Counts(ctx)
			if err != nil {
				return false, err
			}
			s.m.observeCounts(c, s.clock.Now())
			return false, nil
		},
	}
}

func (s *Service) newProducer() (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(s.cfg.KafkaBrokers...),
		kgo.DefaultProduceTopic(s.routingTopic),
		// Idempotent writes are the franz-go default. They keep the order
		// of the records of one partition across retries.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
		kgo.ProducerLinger(0),
	}
	cl, err := kgo.NewClient(append(opts, s.producerOpts...)...)
	if err != nil {
		return nil, fmt.Errorf("router: kafka producer: %w", err)
	}
	return cl, nil
}

// newConsumer returns the consumer of the conversations topic: a group that
// starts a new member at the oldest record, manual commits, and rebalances
// only between batches.
func (s *Service) newConsumer() (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(s.cfg.KafkaBrokers...),
		kgo.ConsumerGroup(s.cfg.GroupID),
		kgo.ConsumeTopics(s.convTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			s.logger.Info("partitions assigned", "partitions", assigned[s.convTopic])
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
			s.logger.Info("partitions revoked", "partitions", revoked[s.convTopic])
		}),
	}
	cl, err := kgo.NewClient(append(opts, s.consumerOpts...)...)
	if err != nil {
		return nil, fmt.Errorf("router: kafka consumer: %w", err)
	}
	return cl, nil
}

// closeClient closes a Kafka client, bounded, so a stop never hangs on a
// broker that is gone.
func closeClient(cl *kgo.Client, logger *slog.Logger, name string) {
	done := make(chan struct{})
	go func() {
		cl.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(clientCloseTimeout):
		logger.Warn("kafka client did not close in time", "client", name)
	}
}

// kickingOps serves the stand settings through the core (decision 0014). A
// change of the team numbers wakes nobody: it takes effect at the next team
// check.
var _ opserver.Stand = kickingOps{}

// kickingOps is the router core as the operator API sees it. An action
// that can free a slot or put a conversation back into a queue wakes the
// assignment workers.
type kickingOps struct {
	*router.Service
	kick func()
}

func (k kickingOps) after(res opapi.Result, err error) (opapi.Result, error) {
	if err == nil && res == opapi.ResultDone {
		k.kick()
	}
	return res, err
}

// Online implements opserver.Operators.
func (k kickingOps) Online(ctx context.Context, id string) (opapi.Result, error) {
	return k.after(k.Service.Online(ctx, id))
}

// Offline implements opserver.Operators.
func (k kickingOps) Offline(ctx context.Context, id string) (opapi.Result, error) {
	return k.after(k.Service.Offline(ctx, id))
}

// Refuse implements opserver.Operators.
func (k kickingOps) Refuse(ctx context.Context, operatorID, offerID string) (opapi.Result, error) {
	return k.after(k.Service.Refuse(ctx, operatorID, offerID))
}

// Resolve implements opserver.Operators.
func (k kickingOps) Resolve(ctx context.Context, operatorID, offerID string) (opapi.Result, error) {
	return k.after(k.Service.Resolve(ctx, operatorID, offerID))
}

// PutOperator implements opserver.Operators. A capacity change can free a
// slot.
func (k kickingOps) PutOperator(ctx context.Context, id string, req opapi.OperatorRequest) (opapi.OperatorResponse, error) {
	resp, err := k.Service.PutOperator(ctx, id, req)
	if err == nil && resp.Result == opapi.ResultDone {
		k.kick()
	}
	return resp, err
}
