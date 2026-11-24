package sim

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"golang.org/x/sync/errgroup"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/router/opclient"
)

const (
	// callWorkers is the number of workers that run API calls.
	callWorkers = 16
	// workerQueue is the buffer of each worker.
	workerQueue = 256
	// startParallel is how many operators are created or put offline at
	// once at start.
	startParallel = 16
	// startPause is the pause between two attempts to read the settings,
	// the topic or the end offsets at start.
	startPause = time.Second
	// startAttempt bounds one of those attempts.
	startAttempt = 5 * time.Second
	// maxPollRecords bounds one batch of routing records.
	maxPollRecords = 1000
	// assignPoll is the pause between two checks for the partitions of
	// the consumer at start.
	assignPoll = 20 * time.Millisecond
	// clientCloseTimeout bounds the close of the Kafka client at the end.
	clientCloseTimeout = 10 * time.Second
	// stopOfflineWait bounds putting every operator offline on a clean
	// stop. Together with clientCloseTimeout it stays under the stop grace
	// period of deploy/compose.yaml.
	stopOfflineWait = 10 * time.Second
)

// Options are the dependencies of a Service.
type Options struct {
	// Config is the service configuration. It must be valid.
	Config Config
	// API is the operator API. An *opclient.Client for Config.RouterURL
	// when nil.
	API API
	// Clock is SystemClock when nil.
	Clock Clock
	// Logger may be nil.
	Logger *slog.Logger
	// Registerer gets the metrics. Required.
	Registerer prometheus.Registerer
	// RoutingTopic is bus.TopicRouting when empty. Tests set it.
	RoutingTopic string
	// ConsumerOpts are applied last to the Kafka consumer, for tests.
	ConsumerOpts []kgo.Opt
}

// Service is the operator simulator.
type Service struct {
	cfg          Config
	api          API
	clock        Clock
	logger       *slog.Logger
	m            *metrics
	topic        string
	consumerOpts []kgo.Opt
	ids          []string
	langs        [][]string
	// stopWait is stopOfflineWait. Tests change it.
	stopWait time.Duration

	running  atomic.Bool
	assigned atomic.Bool
}

// New builds the Service. Nothing is called or consumed until Run.
func New(o Options) (*Service, error) {
	if err := o.Config.Validate(); err != nil {
		return nil, fmt.Errorf("new sim service: %w", err)
	}
	if o.Registerer == nil {
		return nil, errors.New("new sim service: registerer is required")
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.Clock == nil {
		o.Clock = SystemClock{}
	}
	if o.API == nil {
		// Every worker keeps an idle connection, so a busy run does not
		// open a new connection for each call.
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.MaxIdleConnsPerHost = callWorkers + startParallel
		c, err := opclient.New(o.Config.RouterURL, opclient.WithHTTPClient(&http.Client{Transport: tr}))
		if err != nil {
			return nil, fmt.Errorf("new sim service: %w", err)
		}
		o.API = c
	}
	if o.RoutingTopic == "" {
		o.RoutingTopic = bus.TopicRouting
	}
	b := o.Config.Behaviour
	scaled, err := b.Languages.Scale(b.Operators)
	if err != nil {
		return nil, fmt.Errorf("new sim service: languages: %w", err)
	}
	m, err := newMetrics(o.Registerer)
	if err != nil {
		return nil, fmt.Errorf("new sim service: %w", err)
	}
	s := &Service{
		cfg: o.Config, api: o.API, clock: o.Clock, logger: o.Logger, m: m,
		topic: o.RoutingTopic, consumerOpts: o.ConsumerOpts,
		langs: operatorLangs(scaled), stopWait: stopOfflineWait,
	}
	for i := range s.langs {
		s.ids = append(s.ids, operatorID(i+1))
	}
	o.Logger.Info("operator languages", "spread", scaled.String(), "operators", b.Operators)
	return s, nil
}

// Ready reports whether the operators exist and the consumer runs. It is
// safe for concurrent use.
func (s *Service) Ready() bool { return s.running.Load() && s.assigned.Load() }

// Run runs the simulator until ctx ends or a fatal error happens. It
// returns nil after a clean stop, also when ctx ends during start. Run can
// be called once. See the package documentation for the start-up order.
func (s *Service) Run(ctx context.Context) error {
	defer s.running.Store(false)
	err := s.run(ctx)
	if ctx.Err() != nil {
		return nil
	}
	if err != nil {
		return fmt.Errorf("sim: %w", err)
	}
	return nil
}

func (s *Service) run(ctx context.Context) error {
	settings, err := s.waitSettings(ctx)
	if err != nil {
		return err
	}
	b := s.cfg.Behaviour
	s.logger.Info("router settings read",
		"speed_factor", settings.SpeedFactor,
		"accept_timeout", settings.AcceptTimeout().String(),
		"capacity_default", settings.CapacityDefault)
	if b.AcceptDelayMax >= settings.AcceptTimeout() {
		s.logger.Warn("the longest accept delay is not shorter than the accept timeout, so some accepts come too late",
			"accept_delay_max", b.AcceptDelayMax.String(), "accept_timeout", settings.AcceptTimeout().String())
	}

	admin, err := kgo.NewClient(kgo.SeedBrokers(s.cfg.KafkaBrokers...))
	if err != nil {
		return fmt.Errorf("kafka admin client: %w", err)
	}
	defer admin.Close()
	if err := s.waitForTopic(ctx, admin); err != nil {
		return err
	}

	if err := s.forEachOperator(ctx, actionPut, func(ctx context.Context, i int) (opapi.Result, error) {
		resp, err := s.api.PutOperator(ctx, s.ids[i], opapi.OperatorRequest{Languages: s.langs[i], Capacity: b.Capacity})
		return resp.Result, err
	}); err != nil {
		return err
	}
	s.logger.Info("operators created or updated", "operators", len(s.ids))
	// From here the operators exist in the router. On a clean stop they go
	// offline, after the workers have returned (every path below waits for
	// the errgroup), so no late online call can undo it. See "Stop" in the
	// package documentation.
	defer func() {
		if ctx.Err() != nil {
			s.offlineAtStop(ctx)
		}
	}()

	ends, err := s.endOffsets(ctx, admin)
	if err != nil {
		return err
	}

	eng := newEngine(b, s.ids, settings.SpeedFactor, s.cfg.OutageLimit, s.clock, s.m, s.logger)
	cons, err := s.newConsumer(ends)
	if err != nil {
		return err
	}
	defer closeClient(cons, s.logger)

	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, gctx := errgroup.WithContext(rctx)
	g.Go(func() error { return s.consume(gctx, cons, eng) })

	if err := s.forEachOperator(gctx, actionOffline, func(ctx context.Context, i int) (opapi.Result, error) {
		return s.api.Offline(ctx, s.ids[i])
	}); err != nil {
		cancel()
		// An error of the consumer is the cause when it stopped first.
		if werr := g.Wait(); werr != nil {
			return werr
		}
		return err
	}
	// Offers are read only once the consumer has its partitions, so the
	// shifts start then. Otherwise the first offers could time out while
	// the group is still joining.
	if err := s.waitAssigned(gctx); err != nil {
		cancel()
		if werr := g.Wait(); werr != nil {
			return werr
		}
		return err
	}
	s.logger.Info("every operator is offline, starting the shifts")
	eng.start()

	queues := make([]chan *task, callWorkers)
	for i := range queues {
		q := make(chan *task, workerQueue)
		queues[i] = q
		g.Go(func() error { return s.work(gctx, eng, q) })
	}
	g.Go(func() error { return s.schedule(gctx, eng, queues) })
	s.running.Store(true)
	return g.Wait()
}

// waitSettings reads the router settings, retrying any error for up to
// StartWait (decision 0010: no start without them).
func (s *Service) waitSettings(ctx context.Context) (opapi.Settings, error) {
	deadline := s.clock.Now().Add(s.cfg.StartWait)
	for attempt := 1; ; attempt++ {
		actx, cancel := context.WithTimeout(ctx, startAttempt)
		st, err := s.api.Settings(actx)
		cancel()
		if err == nil {
			return st, nil
		}
		if ctx.Err() != nil {
			return opapi.Settings{}, ctx.Err()
		}
		if !s.clock.Now().Add(startPause).Before(deadline) {
			return opapi.Settings{}, fmt.Errorf("router settings not read within %s, after %d attempts: %w", s.cfg.StartWait, attempt, err)
		}
		if attempt == 1 {
			s.logger.Warn("waiting for the router API", "url", s.cfg.RouterURL, "wait", s.cfg.StartWait.String(), "error", err)
		}
		select {
		case <-ctx.Done():
			return opapi.Settings{}, ctx.Err()
		case <-s.clock.After(startPause):
		}
	}
}

// waitAssigned waits up to StartWait until the consumer has its
// partitions.
func (s *Service) waitAssigned(ctx context.Context) error {
	deadline := s.clock.Now().Add(s.cfg.StartWait)
	for !s.assigned.Load() {
		if s.clock.Now().After(deadline) {
			return fmt.Errorf("the consumer got no partitions of %s within %s", s.topic, s.cfg.StartWait)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.clock.After(assignPoll):
		}
	}
	return nil
}

// waitForTopic waits up to StartWait until the routing topic exists. The
// router creates it.
func (s *Service) waitForTopic(ctx context.Context, meta kmsg.Requestor) error {
	deadline := s.clock.Now().Add(s.cfg.StartWait)
	for attempt := 1; ; attempt++ {
		actx, cancel := context.WithTimeout(ctx, startAttempt)
		ok, err := topicExists(actx, meta, s.topic)
		cancel()
		if ok {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !s.clock.Now().Add(startPause).Before(deadline) {
			if err != nil {
				return fmt.Errorf("topic %s not found within %s, it is created by the router: %w", s.topic, s.cfg.StartWait, err)
			}
			return fmt.Errorf("topic %s not found within %s, it is created by the router", s.topic, s.cfg.StartWait)
		}
		if attempt == 1 {
			s.logger.Warn("waiting for the routing topic", "topic", s.topic, "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.clock.After(startPause):
		}
	}
}

// topicExists asks a broker directly whether the topic exists and has
// partitions.
func topicExists(ctx context.Context, meta kmsg.Requestor, topic string) (bool, error) {
	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = false
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, meta)
	if err != nil {
		return false, fmt.Errorf("metadata: %w", err)
	}
	for _, t := range resp.Topics {
		if t.Topic == nil || *t.Topic != topic {
			continue
		}
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			if errors.Is(err, kerr.UnknownTopicOrPartition) || errors.Is(err, kerr.LeaderNotAvailable) {
				return false, nil
			}
			return false, fmt.Errorf("metadata: %w", err)
		}
		return len(t.Partitions) > 0, nil
	}
	return false, nil
}

// endOffsets reads the end offset of every partition of the routing topic.
func (s *Service) endOffsets(ctx context.Context, admin *kgo.Client) (map[int32]int64, error) {
	adm := kadm.NewClient(admin)
	deadline := s.clock.Now().Add(s.cfg.StartWait)
	for {
		actx, cancel := context.WithTimeout(ctx, startAttempt)
		listed, err := adm.ListEndOffsets(actx, s.topic)
		cancel()
		if err == nil {
			err = listed.Error()
		}
		if err == nil {
			ends := map[int32]int64{}
			listed.Each(func(o kadm.ListedOffset) {
				if o.Topic == s.topic {
					ends[o.Partition] = o.Offset
				}
			})
			return ends, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !s.clock.Now().Add(startPause).Before(deadline) {
			return nil, fmt.Errorf("end offsets of %s: %w", s.topic, err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.clock.After(startPause):
		}
	}
}

// forEachOperator runs call for every operator, startParallel at a time.
// A 503 or no answer is retried with a backoff until the outage limit. Any
// other error answer stops the start: it is a configuration or contract
// problem that a retry does not fix.
func (s *Service) forEachOperator(ctx context.Context, action string, call func(context.Context, int) (opapi.Result, error)) error {
	var outage outageClock
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(startParallel)
	for i := range s.ids {
		g.Go(func() error {
			backoff := minBackoff
			for {
				res, err := call(gctx, i)
				if gctx.Err() != nil {
					return gctx.Err()
				}
				label, _, out := classify(res, err)
				s.m.calls.WithLabelValues(action, label).Inc()
				switch out {
				case outcomeSuccess:
					outage.reset()
					return nil
				case outcomeRejected, outcomeFailed:
					return fmt.Errorf("%s %s: %w", action, s.ids[i], err)
				}
				if failing := outage.failing(s.clock.Now()); failing >= s.cfg.OutageLimit {
					return fmt.Errorf("%s %s: %w: no answer for %s (limit %s): %w",
						action, s.ids[i], ErrOutage, failing.Round(time.Millisecond), s.cfg.OutageLimit, err)
				}
				s.m.retries.WithLabelValues(action).Inc()
				select {
				case <-gctx.Done():
					return gctx.Err()
				case <-s.clock.After(backoff):
				}
				backoff = min(backoff*2, maxBackoff)
			}
		})
	}
	return g.Wait()
}

// offlineAtStop puts every operator offline, startParallel at a time,
// within stopWait. A 503 or no answer is retried with a backoff until then.
// It logs how many operators it could not put offline. parent has already
// ended, so the calls use a new context.
func (s *Service) offlineAtStop(parent context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), s.stopWait)
	defer cancel()
	var left atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, startParallel)
	for i := range s.ids {
		wg.Go(func() {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				left.Add(1)
				return
			}
			defer func() { <-sem }()
			if !s.offlineOne(ctx, s.ids[i]) {
				left.Add(1)
			}
		})
	}
	wg.Wait()
	n := left.Load()
	if n > 0 {
		s.logger.Warn("some operators could not be put offline at stop; the router keeps them online until the next start",
			"operators", len(s.ids), "left", n, "wait", s.stopWait.String())
		return
	}
	s.logger.Info("every operator put offline at stop", "operators", len(s.ids))
}

// offlineOne puts one operator offline and reports whether the router
// confirmed it.
func (s *Service) offlineOne(ctx context.Context, id string) bool {
	backoff := minBackoff
	for {
		res, err := s.api.Offline(ctx, id)
		label, _, out := classify(res, err)
		s.m.calls.WithLabelValues(actionOffline, label).Inc()
		switch out {
		case outcomeSuccess:
			return true
		case outcomeRejected, outcomeFailed:
			s.logger.Warn("operator not put offline at stop", "operator_id", id, "error", err)
			return false
		}
		if ctx.Err() != nil {
			return false
		}
		s.m.retries.WithLabelValues(actionOffline).Inc()
		select {
		case <-ctx.Done():
			return false
		case <-s.clock.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// outageClock tracks since when calls have failed without an answer. It
// is safe for concurrent use.
type outageClock struct {
	since atomic.Pointer[time.Time]
}

func (o *outageClock) reset() { o.since.Store(nil) }

// failing records a failure at now and returns how long calls have failed.
func (o *outageClock) failing(now time.Time) time.Duration {
	o.since.CompareAndSwap(nil, &now)
	return now.Sub(*o.since.Load())
}

// newConsumer returns the consumer of the routing topic. It starts every
// partition at the larger of the committed offset and the end offset read
// before the operators went offline (see "Clean start").
func (s *Service) newConsumer(ends map[int32]int64) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(s.cfg.KafkaBrokers...),
		kgo.ConsumerGroup(s.cfg.GroupID),
		kgo.ConsumeTopics(s.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.AdjustFetchOffsetsFn(func(_ context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
			for p, o := range offsets[s.topic] {
				if end, ok := ends[p]; ok && o.EpochOffset().Offset < end {
					offsets[s.topic][p] = kgo.NewOffset().At(end).WithEpoch(-1)
				}
			}
			s.assigned.Store(true)
			return offsets, nil
		}),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			s.logger.Info("partitions assigned", "partitions", len(assigned[s.topic]))
		}),
	}
	cl, err := kgo.NewClient(append(opts, s.consumerOpts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return cl, nil
}

// consume reads the routing topic until ctx ends and gives every event to
// the engine. A run of fetch errors longer than the outage limit is an
// error, and so is a fatal fetch error.
func (s *Service) consume(ctx context.Context, cl *kgo.Client, eng *engine) error {
	var since time.Time
	for {
		fetches := cl.PollRecords(ctx, maxPollRecords)
		if ctx.Err() != nil {
			return nil
		}
		var fatal, last error
		fetches.EachError(func(topic string, partition int32, err error) {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			s.m.fetchErrors.Inc()
			s.logger.Warn("fetch error", "topic", topic, "partition", partition, "error", err)
			last = err
			if fatal == nil && isFatalFetchError(err) {
				fatal = fmt.Errorf("fetch %s partition %d: %w", topic, partition, err)
			}
		})
		if fatal != nil {
			return fatal
		}
		recs := fetches.Records()
		for _, rec := range recs {
			ev, err := event.UnmarshalRouting(rec.Value)
			if err != nil {
				s.m.eventsInvalid.Inc()
				s.logger.Warn("invalid routing record skipped", "partition", rec.Partition, "offset", rec.Offset, "error", err)
				continue
			}
			eng.handle(ev)
		}
		if n := len(recs); n > 0 {
			s.m.lag.Set(max(s.clock.Now().Sub(recs[n-1].Timestamp), 0).Seconds())
		}
		if last == nil || len(recs) > 0 {
			since = time.Time{}
			continue
		}
		now := s.clock.Now()
		if since.IsZero() {
			since = now
		}
		if now.Sub(since) >= s.cfg.OutageLimit {
			return fmt.Errorf("consume %s: fetch failing for %s: %w", s.topic, now.Sub(since).Round(time.Millisecond), last)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-s.clock.After(minBackoff):
		}
	}
}

// isFatalFetchError reports an error that the client does not retry.
func isFatalFetchError(err error) bool {
	if errors.Is(err, kgo.ErrClientClosed) {
		return true
	}
	for _, e := range []error{
		kerr.TopicAuthorizationFailed,
		kerr.GroupAuthorizationFailed,
		kerr.ClusterAuthorizationFailed,
		kerr.SaslAuthenticationFailed,
	} {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

// schedule hands every due task to the worker of its operator, then waits
// for the next task time or a newly planned task.
func (s *Service) schedule(ctx context.Context, eng *engine, queues []chan *task) error {
	for {
		for _, t := range eng.due(s.clock.Now()) {
			select {
			case queues[t.op.idx%len(queues)] <- t:
			case <-ctx.Done():
				return nil
			}
		}
		var timer <-chan time.Time
		if at, ok := eng.nextAt(); ok {
			timer = s.clock.After(max(at.Sub(s.clock.Now()), 0))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-eng.wake:
		case <-timer:
		}
	}
}

// work runs the calls of one worker, in order.
func (s *Service) work(ctx context.Context, eng *engine, q <-chan *task) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case t := <-q:
			if err := eng.execute(ctx, s.api, t); err != nil {
				return err
			}
		}
	}
}

// closeClient closes a Kafka client, bounded, so a stop never hangs on a
// broker that is gone. Close leaves the group, so a restart gets the
// partitions at once.
func closeClient(cl *kgo.Client, logger *slog.Logger) {
	done := make(chan struct{})
	go func() {
		cl.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(clientCloseTimeout):
		logger.Warn("kafka client did not close in time")
	}
}
