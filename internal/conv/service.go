package conv

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

const (
	// maxPollRecords bounds one input batch.
	maxPollRecords = 1000
	// drainBatch is the most outbox entries published in one call.
	drainBatch = 500
	// sweepLimit is the most deadlines one sweep script handles. The
	// sweeper runs the script again while it finds this many.
	sweepLimit = 500
	// batchTimeout bounds the work on one batch after a stop signal, so the
	// records already polled are applied, published and committed.
	batchTimeout = 3 * time.Minute
	// DefaultSweepInterval is how often the sweeper runs. A close can be
	// late by this much wall-clock time (decision 0007).
	DefaultSweepInterval = time.Second
)

// Options are the dependencies of a Service.
type Options struct {
	// Brokers are the Kafka seed brokers of the consumer.
	Brokers []string
	// GroupID is the consumer group.
	GroupID string
	// Store holds the state. Required.
	Store *Store
	// Publisher sends conversation events. Required.
	Publisher Publisher
	// SweepInterval is DefaultSweepInterval when 0.
	SweepInterval time.Duration
	// Logger may be nil.
	Logger *slog.Logger
	// Registerer gets the metrics. Required.
	Registerer prometheus.Registerer
	// ConsumerOpts are applied last to the consumer, for tests.
	ConsumerOpts []kgo.Opt
}

// ConsumerOptions returns the consumer settings of conv: one group for both
// input topics, read committed (normalize writes in Kafka transactions),
// manual commits, and rebalances only between batches. A new group starts at
// the oldest record, so a first start builds the state from everything the
// topics still hold.
func ConsumerOptions(brokers []string, groupID string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(bus.TopicEvents, bus.TopicControl),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	}
}

// Service consumes the input topics, applies them to the Store and
// publishes the outboxes.
//
// Partitions of the events topic that this instance owns are swept by this
// instance only. Ownership comes from the consumer group, so a rebalance
// moves the sweeping of a partition together with its records.
type Service struct {
	client    *kgo.Client
	store     *Store
	pub       Publisher
	logger    *slog.Logger
	m         *metrics
	interval  time.Duration
	ready     atomic.Bool
	drainLock sync.Mutex
	drainMu   map[int32]*sync.Mutex

	// ownMu guards owned. The sweeper holds the read lock while it works on
	// the owned partitions, so a revoke waits for a sweep in progress.
	ownMu sync.RWMutex
	owned map[int32]bool
}

// New builds the Service and its consumer. It does not connect until Run.
func New(o Options) (*Service, error) {
	if len(o.Brokers) == 0 {
		return nil, errors.New("new conv service: no brokers")
	}
	if o.GroupID == "" {
		return nil, errors.New("new conv service: group ID is empty")
	}
	if o.Store == nil || o.Publisher == nil || o.Registerer == nil {
		return nil, errors.New("new conv service: store, publisher and registerer are required")
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.SweepInterval <= 0 {
		o.SweepInterval = DefaultSweepInterval
	}
	m, err := newMetrics(o.Registerer)
	if err != nil {
		return nil, fmt.Errorf("new conv service: %w", err)
	}
	s := &Service{
		store:    o.Store,
		pub:      o.Publisher,
		logger:   o.Logger,
		m:        m,
		interval: o.SweepInterval,
		drainMu:  map[int32]*sync.Mutex{},
		owned:    map[int32]bool{},
	}
	opts := ConsumerOptions(o.Brokers, o.GroupID)
	opts = append(opts,
		kgo.OnPartitionsAssigned(s.onAssigned),
		kgo.OnPartitionsRevoked(s.onRevoked),
		kgo.OnPartitionsLost(s.onRevoked),
	)
	opts = append(opts, o.ConsumerOpts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("new conv service: create kafka client: %w", err)
	}
	s.client = cl
	return s, nil
}

// Ready reports whether the Service has joined the group and is consuming.
// It is safe for concurrent use.
func (s *Service) Ready() bool { return s.ready.Load() }

// Run consumes until ctx ends or a fatal error happens. Records already
// polled when ctx ends are still applied, published and committed. Run owns
// the consumer and closes it. It does not close the Publisher.
//
// Fatal errors: a Redis error, a publish error and a fatal fetch error. The
// offsets of the batch are then not committed. The process exits, a restart
// publishes the outboxes again and redelivered records change nothing.
func (s *Service) Run(ctx context.Context) error {
	sweepCtx, stopSweep := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() { s.sweepLoop(sweepCtx) })

	runErr := s.loop(ctx)

	stopSweep()
	wg.Wait()
	s.ready.Store(false)
	s.client.AllowRebalance()
	// Close leaves the group. Offsets are never committed here: every
	// successful batch committed its own.
	s.client.Close()
	if runErr != nil {
		return runErr
	}
	s.logger.Info("conv stopped")
	return nil
}

func (s *Service) loop(ctx context.Context) error {
	s.logger.Info("conv consuming", "topics", []string{bus.TopicEvents, bus.TopicControl})
	for {
		fetches := s.client.PollRecords(ctx, maxPollRecords)
		if err := s.checkFetchErrors(fetches); err != nil {
			return err
		}
		if !fetches.Empty() {
			bctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), batchTimeout)
			err := s.processBatch(bctx, fetches)
			cancel()
			if err != nil {
				return err
			}
		}
		if ctx.Err() != nil {
			return nil
		}
		s.client.AllowRebalance()
	}
}

// processBatch applies one poll, publishes the outboxes it wrote to and
// commits the offsets.
func (s *Service) processBatch(ctx context.Context, fetches kgo.Fetches) error {
	start := time.Now()
	var msgs []PartitionedMessage
	var controls []event.Control
	fetches.EachRecord(func(rec *kgo.Record) {
		switch rec.Topic {
		case bus.TopicEvents:
			m, err := event.UnmarshalMessage(rec.Value)
			if err != nil {
				// The error names fields only, never the text.
				s.m.messages.WithLabelValues(directionUnknown, resultInvalid).Inc()
				s.logger.Warn("invalid message event skipped", "partition", rec.Partition, "offset", rec.Offset, "error", err)
				return
			}
			msgs = append(msgs, PartitionedMessage{Partition: rec.Partition, Message: m})
		case bus.TopicControl:
			c, err := event.UnmarshalControl(rec.Value)
			if err != nil {
				s.m.control.WithLabelValues(kindUnknown, resultInvalid).Inc()
				s.logger.Warn("invalid control event skipped", "partition", rec.Partition, "offset", rec.Offset, "error", err)
				return
			}
			controls = append(controls, c)
		}
	})

	shards := map[int32]bool{}
	msgResults, err := s.store.ApplyMessages(ctx, msgs)
	if err != nil {
		s.m.redisErrors.Inc()
		return fmt.Errorf("apply message events: %w", err)
	}
	for i, r := range msgResults {
		s.m.messages.WithLabelValues(string(msgs[i].Message.Direction), r.Outcome).Inc()
		s.m.observe(r)
		for _, p := range r.Outboxes {
			shards[p] = true
		}
	}
	ctlResults, err := s.store.ApplyControls(ctx, controls)
	if err != nil {
		s.m.redisErrors.Inc()
		return fmt.Errorf("apply control events: %w", err)
	}
	for i, r := range ctlResults {
		s.m.control.WithLabelValues(string(controls[i].Kind()), r.Outcome).Inc()
		s.m.observe(r)
		for _, p := range r.Outboxes {
			shards[p] = true
		}
	}

	// Publish before the commit: an offset is committed only after the
	// events of its record are confirmed and removed from the outbox.
	for _, p := range slices.Sorted(maps.Keys(shards)) {
		if err := s.drain(ctx, p); err != nil {
			return err
		}
	}
	if err := s.client.CommitUncommittedOffsets(ctx); err != nil {
		// Not fatal: the next commit covers these offsets, and a
		// redelivered record changes nothing.
		s.logger.Warn("offset commit failed", "error", err)
	}
	s.ready.Store(true)
	s.m.process.Observe(time.Since(start).Seconds())
	return nil
}

// drain publishes the outbox of a partition from its head until it is
// empty. Within this process one drain per partition runs at a time.
func (s *Service) drain(ctx context.Context, p int32) error {
	mu := s.partitionLock(p)
	mu.Lock()
	defer mu.Unlock()
	for {
		entries, err := s.store.Outbox(ctx, p, drainBatch)
		if err != nil {
			s.m.redisErrors.Inc()
			return err
		}
		if len(entries) == 0 {
			return nil
		}
		ids := make([]string, 0, len(entries))
		recs := make([]*kgo.Record, 0, len(entries))
		types := make([]event.ConversationType, 0, len(entries))
		for _, e := range entries {
			ids = append(ids, e.ID)
			var value []byte
			if e.Err == nil {
				value, err = event.MarshalConversation(e.Event)
			}
			if e.Err != nil || err != nil {
				// A broken entry would block the outbox for ever. Drop it;
				// consumers see a gap in the sequence numbers.
				s.m.outboxInvalid.Inc()
				s.logger.Error("invalid outbox entry dropped", "partition", p, "id", e.ID, "error", errors.Join(e.Err, err))
				continue
			}
			recs = append(recs, &kgo.Record{Topic: bus.TopicConversations, Key: e.Event.Key(), Value: value})
			types = append(types, e.Event.Type)
		}
		if len(recs) > 0 {
			if err := s.pub.Publish(ctx, recs); err != nil {
				return err
			}
		}
		if err := s.store.RemoveFromOutbox(ctx, p, ids...); err != nil {
			s.m.redisErrors.Inc()
			return err
		}
		for _, t := range types {
			s.m.published.WithLabelValues(string(t)).Inc()
		}
		if len(entries) < drainBatch {
			return nil
		}
	}
}

func (s *Service) partitionLock(p int32) *sync.Mutex {
	s.drainLock.Lock()
	defer s.drainLock.Unlock()
	mu, ok := s.drainMu[p]
	if !ok {
		mu = &sync.Mutex{}
		s.drainMu[p] = mu
	}
	return mu
}

// onAssigned takes the new events partitions and publishes their outboxes
// before any record of them is processed, so events left by a crash go
// first.
func (s *Service) onAssigned(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
	parts := assigned[bus.TopicEvents]
	s.ownMu.Lock()
	for _, p := range parts {
		s.owned[p] = true
	}
	s.ownMu.Unlock()
	for _, p := range parts {
		if err := s.drain(ctx, p); err != nil {
			// The outbox stays and is published from its head after the
			// next batch of this partition or by the sweeper.
			s.logger.Error("publish outbox on assign failed", "partition", p, "error", err)
		}
	}
	s.ready.Store(true)
	s.logger.Info("partitions assigned", "events", parts, "control", assigned[bus.TopicControl])
}

// onRevoked stops sweeping the partitions. Offsets are not committed here:
// with rebalances blocked during a batch, every processed record is already
// committed, and after a fatal error nothing may be committed.
func (s *Service) onRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	parts := revoked[bus.TopicEvents]
	s.ownMu.Lock()
	for _, p := range parts {
		delete(s.owned, p)
		s.m.watermarkLag.DeleteLabelValues(strconv.FormatInt(int64(p), 10))
	}
	s.ownMu.Unlock()
	s.logger.Info("partitions revoked", "events", parts, "control", revoked[bus.TopicControl])
}

// sweepLoop runs the sweeper until ctx ends.
//
// A partition that is catching up: its watermark moves with the records of
// that partition only, as fast as they are processed. The sweeper closes its
// conversations in the order of their deadlines as the watermark passes
// them, so a catch-up of hours closes the same conversations at the same
// source times as live traffic would. Other partitions are not affected.
//
// A partition with no new records: its watermark does not move, so its open
// conversations stay open, even for hours of wall-clock time. This is left
// so on purpose. Decision 0007 measures every window against the watermark
// of the partition. It is decided to keep it so, with no wall-clock rule:
// the conversations of an idle partition stay open until the stream of that
// partition continues, and then close with their deadline as the source
// time. If a partition stays idle for longer than the key lifetimes (about
// 31 hours for the session record), its state expires and the sweeper only
// removes the deadline entry, with no close event.
// Note that the message script also closes an expired session when the next
// message of the same conversation arrives, so a late close never changes
// what a later message sees.
func (s *Service) sweepLoop(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		s.sweepOnce(ctx)
	}
}

// sweepOnce sweeps and publishes every owned partition, then refreshes the
// gauges from Redis. Errors are logged and counted: the next tick tries
// again.
func (s *Service) sweepOnce(ctx context.Context) {
	s.ownMu.RLock()
	parts := slices.Sorted(maps.Keys(s.owned))
	for _, p := range parts {
		if err := s.sweepPartition(ctx, p); err != nil && ctx.Err() == nil {
			s.logger.Warn("sweep failed", "partition", p, "error", err)
		}
	}
	s.ownMu.RUnlock()
	if ctx.Err() != nil {
		return
	}
	s.refreshGauges(ctx, parts)
}

func (s *Service) sweepPartition(ctx context.Context, p int32) error {
	for {
		r, err := s.store.Sweep(ctx, p, sweepLimit)
		if err != nil {
			s.m.redisErrors.Inc()
			return err
		}
		s.m.observe(r)
		s.m.textPurged.Add(float64(r.Purged))
		if r.Due < sweepLimit && r.PurgeDue < sweepLimit {
			break
		}
	}
	// Also publishes entries that a control consumer of another instance
	// appended and did not publish.
	return s.drain(ctx, p)
}

func (s *Service) refreshGauges(ctx context.Context, owned []int32) {
	st, err := s.store.Stats(ctx, bus.PartitionsEvents)
	if err != nil {
		s.m.redisErrors.Inc()
		s.logger.Warn("read stats failed", "error", err)
		return
	}
	s.m.companiesAdmitted.Set(float64(st.AdmittedCompanies))
	s.m.conversationsOpen.Set(float64(st.OpenConversations))
	s.m.outboxPending.Set(float64(st.OutboxPending))

	wms, err := s.store.Watermarks(ctx)
	if err != nil {
		s.m.redisErrors.Inc()
		s.logger.Warn("read watermarks failed", "error", err)
		return
	}
	now := time.Now()
	for _, p := range owned {
		if wm, ok := wms[p]; ok {
			s.m.watermarkLag.WithLabelValues(strconv.FormatInt(int64(p), 10)).Set(now.Sub(wm).Seconds())
		}
	}
}

// checkFetchErrors logs and counts fetch errors and returns the first fatal
// one: a closed client, or an authorization or authentication error. The
// client retries everything else itself.
func (s *Service) checkFetchErrors(fetches kgo.Fetches) error {
	var fatal error
	fetches.EachError(func(topic string, partition int32, err error) {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		s.logger.Error("fetch error", "topic", topic, "partition", partition, "error", err)
		if fatal == nil && isFatalFetchError(err) {
			fatal = fmt.Errorf("fetch %s partition %d: %w", topic, partition, err)
		}
	})
	return fatal
}

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
