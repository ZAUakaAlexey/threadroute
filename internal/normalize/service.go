// Package normalize reads source events from the raw topic and writes
// channel-agnostic events (decisions 0003 and 0006):
//
//   - Map turns one raw record into one output record or a drop.
//   - Deduper is the Redis layer of the deduplication.
//   - Service runs the consume, transform, produce loop in Kafka
//     transactions.
//   - Load reads the configuration.
package normalize

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
)

// Time limits of the service. They are not configuration, because they only
// shape start and stop.
const (
	// shutdownGrace is how long the batch in progress may still run after
	// the context of Run ends, so its transaction can commit. After that
	// its work is canceled and the transaction aborts. Together with
	// endTimeout it stays under the 30 second stop grace period of
	// deploy/compose.yaml.
	shutdownGrace = 10 * time.Second
	// endTimeout bounds the end of one transaction (flush, offset commit,
	// EndTxn). End must not get a canceled context: kgo documents that this
	// can leave the client in an invalid state.
	endTimeout = 15 * time.Second
	// rawTopicInterval is the pause between checks for the raw topic.
	rawTopicInterval = time.Second
)

// Service consumes bus.TopicRaw in a consumer group and writes message events
// to bus.TopicEvents and control events to bus.TopicControl. Each polled
// batch is one Kafka transaction: its output records and its consumer
// offsets commit together, or not at all.
//
// # The loop
//
// Run polls up to Config.MaxBatchRecords records, then for the batch:
//
//  1. begins a transaction,
//  2. maps every record with Map, in poll order,
//  3. checks every record that has an output with the Deduper, in one Redis
//     round trip for the whole batch,
//  4. produces the outputs of the records to process, each with the event ID
//     header, and waits until all are acknowledged,
//  5. ends the transaction: commit if every step worked, abort otherwise.
//     The kgo session commits the consumer offsets in the same
//     transaction.
//
// An empty poll starts no transaction.
//
// # Errors
//
// Any error in steps 2 to 4 aborts the transaction, and Run returns the
// error. The process exits, the container restart policy starts it again,
// and the batch is read again from the last committed offsets. A Redis
// error is handled the same way: the deduplication check is never skipped,
// and the raw topic keeps the records until Redis is back (decision 0003).
// An error from ending the transaction is also returned: kgo documents that
// no such error is retryable.
//
// # Rebalance in the middle of a transaction
//
// kgo.GroupTransactSession watches the group. If partitions are revoked or
// lost while a transaction is open, its End aborts instead of committing,
// even when commit was asked for, and rewinds the consumer to the last
// committed offsets. The batch then appears again: in this member, or in
// the member that got the partitions. Run sees "not committed, no error",
// counts an aborted transaction and goes on polling. Before a commit the
// session also forces a heartbeat, so a member that was already kicked out
// of the group cannot commit offsets for partitions it no longer owns.
//
// # Why the Redis value holds the raw position
//
// Redis is not part of the Kafka transaction. After an abort the dedup keys
// of the batch are already set. When the batch is read again, each key
// holds the position of the record itself, so the Deduper answers "same
// position" and the record is processed. Only a key with another position
// marks a duplicate (decision 0003).
//
// # What a consumer of the output topics must set
//
// Records of aborted transactions stay in the topics, marked as aborted. A
// consumer of bus.TopicEvents or bus.TopicControl must read with
// kgo.FetchIsolationLevel(kgo.ReadCommitted()) (isolation.level =
// read_committed). Otherwise it sees the records of aborted transactions,
// which are copies of records that a later transaction writes again.
//
// # Ordering and concurrency
//
// One goroutine handles one batch at a time, and the records of a batch in
// poll order, so the records of one raw partition are handled in offset
// order. Partitions are not handled concurrently. The raw topic is keyed by
// author, so the events of one author are in one raw partition, and their
// outputs are produced in source order. Control events are keyed by
// author, so they reach one control partition in that order. The idempotent
// producer keeps the order inside each output partition. Message events are
// keyed by conversation, which mixes two authors, so their order across
// authors is not kept (decision 0001). Consumers order them by source time.
//
// Concurrency across partitions was left out because one transaction per
// batch already covers all partitions of the member, and the expected rate
// (about 50 to 90 raw events per second) is far below what one goroutine
// handles. Throughput comes from batching instead: one Redis round trip and
// one commit per batch.
type Service struct {
	cfg     Config
	dedup   *Deduper
	logger  *slog.Logger
	metrics *metrics
	extra   []kgo.Opt
	now     func() time.Time

	// prepared is set once the topics are checked and ensured. joined is
	// set when the group assigns partitions or a transaction commits. The
	// group can assign partitions while prepare still runs, so Ready needs
	// both.
	prepared atomic.Bool
	joined   atomic.Bool

	// beforeEnd, when set, runs after the outputs are acknowledged and
	// before the transaction ends. An error aborts the transaction like any
	// other error. Tests use it to fail a batch in the middle.
	beforeEnd func(ctx context.Context, records []*kgo.Record) error
	// shutdownGrace and endTimeout are the constants above. Tests can
	// change them.
	shutdownGrace time.Duration
	endTimeout    time.Duration
}

// New checks the configuration and registers the metrics on reg. It does
// not connect to Kafka or Redis: Run does.
//
// rdb is the Redis client for the deduplication keys, see NewRedisClient.
// extra Kafka options are applied after the built-in ones, so tests can
// change them.
func New(cfg Config, rdb redis.Cmdable, logger *slog.Logger, reg prometheus.Registerer, extra ...kgo.Opt) (*Service, error) {
	if len(cfg.KafkaBrokers) == 0 {
		return nil, errors.New("new normalize service: no kafka brokers")
	}
	if cfg.GroupID == "" || cfg.TransactionalID == "" {
		return nil, errors.New("new normalize service: group ID and transactional ID must be set")
	}
	if cfg.MaxBatchRecords <= 0 {
		return nil, fmt.Errorf("new normalize service: max batch records %d is not positive", cfg.MaxBatchRecords)
	}
	if err := cfg.Producer.Validate(); err != nil {
		return nil, fmt.Errorf("new normalize service: %w", err)
	}
	if cfg.MaxBatchRecords > cfg.Producer.MaxBufferedRecords {
		return nil, fmt.Errorf("new normalize service: max batch records %d is larger than the producer buffer %d",
			cfg.MaxBatchRecords, cfg.Producer.MaxBufferedRecords)
	}
	if reg == nil {
		return nil, errors.New("new normalize service: prometheus registerer is nil")
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	dedup, err := NewDeduper(rdb, cfg.DedupKeyLifetime)
	if err != nil {
		return nil, fmt.Errorf("new normalize service: %w", err)
	}
	m, err := newMetrics(reg)
	if err != nil {
		return nil, fmt.Errorf("new normalize service: %w", err)
	}
	return &Service{
		cfg:           cfg,
		dedup:         dedup,
		logger:        logger,
		metrics:       m,
		extra:         extra,
		now:           time.Now,
		shutdownGrace: shutdownGrace,
		endTimeout:    endTimeout,
	}, nil
}

// Redis client limits. Short timeouts make "Redis down" a fast error, which
// stops the service, instead of a long wait inside a transaction.
const (
	redisDialTimeout = 2 * time.Second
	redisIOTimeout   = 2 * time.Second
	redisMaxRetries  = 1
)

// NewRedisClient returns a Redis client for cfg with short timeouts.
func NewRedisClient(cfg Config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		Password:     cfg.RedisPassword,
		DB:           cfg.RedisDB,
		DialTimeout:  redisDialTimeout,
		ReadTimeout:  redisIOTimeout,
		WriteTimeout: redisIOTimeout,
		MaxRetries:   redisMaxRetries,
	})
}

// Ready reports whether the service has ensured its topics, has joined its
// consumer group and got partitions (or has committed a transaction), and
// is still running. It is
// safe for concurrent use and is meant for the health endpoint.
//
// Joining the group is the signal, not the first commit, because with no
// new raw records no transaction ever starts, and the service is still
// healthy then.
func (s *Service) Ready() bool {
	return s.prepared.Load() && s.joined.Load()
}

// Run prepares the topics and runs the loop until ctx ends or a fatal error
// happens. It returns nil after a clean stop, also when ctx ends during the
// start.
//
// At start it waits up to Config.RawTopicWait for bus.TopicRaw, which ingest
// creates, and returns an error naming the topic if it does not appear.
// Then it ensures bus.TopicEvents and bus.TopicControl with bus.EnsureTopics.
//
// On stop the batch in progress gets shutdownGrace to commit, otherwise its
// transaction aborts. Then the client leaves the group, so another member
// gets the partitions at once, and closes. Run owns the Kafka client. It
// does not close the Redis client.
func (s *Service) Run(ctx context.Context) error {
	sess, err := kgo.NewGroupTransactSession(s.kafkaOpts()...)
	if err != nil {
		return fmt.Errorf("normalize: kafka client: %w", err)
	}
	defer func() {
		s.prepared.Store(false)
		s.joined.Store(false)
		// Close leaves the group. Every transaction is ended before this.
		sess.Close()
	}()

	if err := s.prepare(ctx, sess.Client()); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("normalize: %w", err)
	}
	s.prepared.Store(true)
	s.logger.Info("normalize running", "topic", bus.TopicRaw, "group", s.cfg.GroupID)
	if err := s.loop(ctx, sess); err != nil {
		return fmt.Errorf("normalize: %w", err)
	}
	s.logger.Info("normalize stopped")
	return nil
}

// kafkaOpts are the options of the transactional client.
//
// Producer settings follow decision 0001: acks from all in-sync replicas,
// idempotent writes (the kgo default, required by transactions), zstd,
// explicit buffer limits and a delivery timeout. The partitioner hashes the
// key with murmur2 like the Java client, as in the raw writer, so every
// service maps a key to the same partition.
func (s *Service) kafkaOpts() []kgo.Opt {
	reset := kgo.NewOffset().AtStart()
	if s.cfg.Start == StartEnd {
		reset = kgo.NewOffset().AtEnd()
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(s.cfg.KafkaBrokers...),
		kgo.ConsumerGroup(s.cfg.GroupID),
		kgo.ConsumeTopics(bus.TopicRaw),
		kgo.ConsumeResetOffset(reset),
		// The raw topic is not written in transactions today. Reading
		// committed only costs nothing and stays right if a writer ever
		// uses transactions.
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.TransactionalID(s.cfg.TransactionalID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.MaxBufferedRecords(s.cfg.Producer.MaxBufferedRecords),
		kgo.MaxBufferedBytes(s.cfg.Producer.MaxBufferedBytes),
		kgo.RecordDeliveryTimeout(s.cfg.Producer.DeliveryTimeout),
		kgo.ProducerLinger(0),
		kgo.RecordPartitioner(kgo.UniformBytesPartitioner(64<<10, true, true, nil)),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			s.logger.Info("partitions assigned", "partitions", assigned[bus.TopicRaw])
			s.joined.Store(true)
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
			s.logger.Info("partitions revoked", "partitions", revoked[bus.TopicRaw])
		}),
	}
	return append(opts, s.extra...)
}

func (s *Service) prepare(ctx context.Context, cl *kgo.Client) error {
	if err := waitForTopic(ctx, cl, bus.TopicRaw, s.cfg.RawTopicWait, s.logger); err != nil {
		return err
	}
	return bus.EnsureTopics(ctx, cl, s.logger, bus.EventsTopicSpec(), bus.ControlTopicSpec())
}

// waitForTopic checks with metadata requests until the topic exists, for up
// to wait. A broker that does not answer is retried the same way, so this is
// also the wait for Kafka at start.
func waitForTopic(ctx context.Context, cl *kgo.Client, topic string, wait time.Duration, logger *slog.Logger) error {
	wctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	var lastErr error
	for attempt := 1; ; attempt++ {
		exists, err := topicExists(wctx, cl, topic)
		if err == nil && exists {
			return nil
		}
		lastErr = err
		if attempt == 1 {
			logger.Warn("waiting for topic, it is created by ingest", "topic", topic, "wait", wait.String(), "error", err)
		}
		timer := time.NewTimer(rawTopicInterval)
		select {
		case <-wctx.Done():
			timer.Stop()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if lastErr != nil {
				return fmt.Errorf("topic %s not found within %s, start ingest first: %w", topic, wait, lastErr)
			}
			return fmt.Errorf("topic %s not found within %s, start ingest first", topic, wait)
		case <-timer.C:
		}
	}
}

// topicExists asks a broker directly, not the metadata cache of the client.
func topicExists(ctx context.Context, cl *kgo.Client, topic string) (bool, error) {
	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = false
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return false, fmt.Errorf("metadata: %w", err)
	}
	for _, t := range resp.Topics {
		if t.Topic == nil || *t.Topic != topic {
			continue
		}
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			if errors.Is(err, kerr.UnknownTopicOrPartition) {
				return false, nil
			}
			return false, fmt.Errorf("metadata: %w", err)
		}
		return len(t.Partitions) > 0, nil
	}
	return false, nil
}

func (s *Service) loop(ctx context.Context, sess *kgo.GroupTransactSession) error {
	for {
		fetches := sess.PollRecords(ctx, s.cfg.MaxBatchRecords)
		if ctx.Err() != nil {
			// Records of this poll are not handled. Their offsets are
			// not committed, so the next owner reads them.
			return nil
		}
		if err := s.checkFetchErrors(fetches); err != nil {
			return err
		}
		records := fetches.Records()
		if len(records) == 0 {
			continue
		}
		if err := s.runBatch(ctx, sess, records); err != nil {
			return err
		}
	}
}

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
	for _, e := range []error{
		kgo.ErrClientClosed,
		kerr.TopicAuthorizationFailed,
		kerr.GroupAuthorizationFailed,
		kerr.ClusterAuthorizationFailed,
		kerr.TransactionalIDAuthorizationFailed,
		kerr.SaslAuthenticationFailed,
	} {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

// runBatch handles one polled batch in one transaction. It returns an error
// only when the service must stop. A transaction aborted by a rebalance is
// not an error.
func (s *Service) runBatch(ctx context.Context, sess *kgo.GroupTransactSession, records []*kgo.Record) error {
	s.metrics.batchRecords.Observe(float64(len(records)))
	start := time.Now()
	if err := sess.Begin(); err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	// The work of the batch is not canceled by ctx at once: on stop it gets
	// shutdownGrace to finish, so a clean stop usually commits it.
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	stopGrace := context.AfterFunc(ctx, func() {
		timer := time.NewTimer(s.shutdownGrace)
		defer timer.Stop()
		select {
		case <-timer.C:
			cancelWork()
		case <-workCtx.Done():
		}
	})
	defer stopGrace()

	stats, last, workErr := s.process(workCtx, sess, records)
	if workErr == nil && s.beforeEnd != nil {
		workErr = s.beforeEnd(workCtx, records)
	}

	endCtx, cancelEnd := context.WithTimeout(context.WithoutCancel(ctx), s.endTimeout)
	defer cancelEnd()
	committed, endErr := sess.End(endCtx, kgo.TransactionEndTry(workErr == nil))
	s.metrics.txnSeconds.Observe(time.Since(start).Seconds())

	if !committed {
		s.metrics.transactions.WithLabelValues(resultAborted).Inc()
	}
	switch {
	case workErr != nil && endErr != nil:
		return fmt.Errorf("batch of %d records aborted: %w (and the abort failed: %w)", len(records), workErr, endErr)
	case workErr != nil:
		return fmt.Errorf("batch of %d records aborted: %w", len(records), workErr)
	case endErr != nil:
		return fmt.Errorf("end transaction of %d records: %w", len(records), endErr)
	case !committed:
		s.logger.Info("transaction aborted by a rebalance, the batch is read again", "records", len(records))
		return nil
	}

	s.metrics.transactions.WithLabelValues(resultCommitted).Inc()
	s.metrics.addCommitted(stats)
	if !last.IsZero() {
		s.metrics.lag.Set(s.now().Sub(last).Seconds())
	}
	s.joined.Store(true)
	s.logger.Debug("transaction committed",
		"records", stats.raw, "duplicates", stats.duplicates, "reprocessed", stats.reprocessed)
	return nil
}

// process maps, deduplicates and produces the records of one batch. It
// returns the counts of the batch and the source time of its last record
// that has one.
func (s *Service) process(ctx context.Context, sess *kgo.GroupTransactSession, records []*kgo.Record) (batchStats, time.Time, error) {
	stats := newBatchStats()
	var last time.Time

	results := make([]Result, len(records))
	items := make([]DedupItem, 0, len(records))
	index := make([]int, 0, len(records))
	for i, rec := range records {
		res := Map(rec)
		results[i] = res
		stats.raw++
		if !res.SourceTime.IsZero() {
			last = res.SourceTime
		}
		if res.DisplayTime {
			stats.timeFallback++
		}
		if res.Dropped() {
			stats.dropped[res.DropReason]++
			s.logDrop(rec, res.DropReason)
			continue
		}
		items = append(items, DedupItem{EventID: res.EventID, Position: Position{Partition: rec.Partition, Offset: rec.Offset}})
		index = append(index, i)
	}

	verdicts, err := s.dedup.Check(ctx, items)
	if err != nil {
		s.metrics.redisErrors.Inc()
		return stats, last, err
	}

	promise := kgo.AbortingFirstErrPromise(sess.Client())
	for j, v := range verdicts {
		res := results[index[j]]
		switch v {
		case VerdictDuplicate:
			stats.duplicates++
			continue
		case VerdictSamePosition:
			stats.reprocessed++
		}
		stats.emitted[res.Kind]++
		sess.Produce(ctx, res.OutputRecord(), promise.Promise())
	}
	if err := promise.Err(); err != nil {
		return stats, last, fmt.Errorf("produce: %w", err)
	}
	return stats, last, nil
}

// logDrop logs the drops that point at a problem upstream. Normal drops,
// such as posts that are not replies, are only counted. The log has the
// position and the reason, never the record value (decision 0004).
func (s *Service) logDrop(rec *kgo.Record, reason string) {
	switch reason {
	case DropDecodeFailed, DropMissingEventID, DropMissingTime, DropInvalidEvent, DropUnknownKind:
		s.logger.Warn("raw record dropped", "reason", reason, "partition", rec.Partition, "offset", rec.Offset)
	}
}
