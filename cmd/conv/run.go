package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
	"github.com/ZAUakaAlexey/threadroute/internal/conv"
	"github.com/ZAUakaAlexey/threadroute/internal/obs"
)

const (
	// connectTimeout bounds the wait for Kafka and for Redis at start.
	connectTimeout = 2 * time.Minute
	// pingTimeout bounds one connection attempt.
	pingTimeout = 5 * time.Second
	// retryPause is the pause between two connection attempts.
	retryPause = 2 * time.Second
	// publisherCloseTimeout bounds the flush of the producer at the end.
	publisherCloseTimeout = 10 * time.Second
)

// run wires conv and runs it until ctx ends or a fatal error happens. It
// returns nil after a clean stop, also when ctx ends during start. Any other
// error is fatal: the process exits with a non-zero code, and a restart
// publishes the outboxes again and continues from the committed offsets.
func run(ctx context.Context, cfg conv.Config, logger *slog.Logger, reg *prometheus.Registry) error {
	r := cfg.Rules
	logger.Info("conv starting",
		"kafka_brokers", cfg.KafkaBrokers,
		"group_id", cfg.GroupID,
		"redis_addr", cfg.RedisAddr,
		"redis_db", cfg.RedisDB,
		"metrics_addr", cfg.MetricsAddr,
		"admission_threshold", r.AdmissionThreshold,
		"admission_window", r.AdmissionWindow.String(),
		"admission_idle", r.AdmissionIdle.String(),
		"company_limit", r.CompanyLimit,
		"inactivity_timeout", r.InactivityTimeout.String(),
		"text_kept_after_close", r.TextKeptAfterClose.String(),
		"messages_kept", r.MessagesKept,
		"sweep_interval", cfg.SweepInterval.String(),
	)

	// Ready once the service has joined the group and runs.
	var current atomic.Pointer[conv.Service]
	ready := func() bool {
		svc := current.Load()
		return svc != nil && svc.Ready()
	}
	srv, err := obs.NewServer(ctx, cfg.MetricsAddr, reg, ready, logger)
	if err != nil {
		return err
	}
	srvCtx, stopSrv := context.WithCancel(context.WithoutCancel(ctx))
	waitSrv := srv.Start(srvCtx)
	defer func() {
		stopSrv()
		if serr := waitSrv(); serr != nil {
			logger.Warn("metrics server", "error", serr)
		}
	}()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPassword, DB: cfg.RedisDB})
	defer rdb.Close()
	if err := retry(ctx, logger, "redis", func(c context.Context) error { return rdb.Ping(c).Err() }); err != nil {
		return stopped(ctx, err)
	}

	admin, err := kgo.NewClient(kgo.SeedBrokers(cfg.KafkaBrokers...))
	if err != nil {
		return fmt.Errorf("kafka admin client: %w", err)
	}
	defer admin.Close()
	if err := retry(ctx, logger, "kafka", admin.Ping); err != nil {
		return stopped(ctx, err)
	}
	// conv owns the conversations topic. The input topics belong to
	// normalize, so conv only waits for them, for a bounded time.
	if err := bus.EnsureTopics(ctx, admin, logger, bus.ConversationsTopicSpec()); err != nil {
		return stopped(ctx, err)
	}
	if err := conv.WaitForTopics(ctx, admin, cfg.InputTopicWait, bus.TopicEvents, bus.TopicControl); err != nil {
		return stopped(ctx, err)
	}
	admin.Close()

	store, err := conv.NewStore(rdb, cfg.Rules, conv.DefaultKeyPrefix)
	if err != nil {
		return err
	}
	pub, err := conv.NewKafkaPublisher(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer func() {
		// Close flushes. Bound it, so a stop never hangs on a broker that
		// is gone. Unconfirmed events stay in the outbox.
		done := make(chan struct{})
		go func() { pub.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(publisherCloseTimeout):
			logger.Warn("producer did not close in time")
		}
	}()

	svc, err := conv.New(conv.Options{
		Brokers:       cfg.KafkaBrokers,
		GroupID:       cfg.GroupID,
		Store:         store,
		Publisher:     pub,
		SweepInterval: cfg.SweepInterval,
		Logger:        logger,
		Registerer:    reg,
	})
	if err != nil {
		return err
	}
	current.Store(svc)
	return svc.Run(ctx)
}

// stopped returns nil when ctx ended, because that is a clean stop during
// start, and err otherwise.
func stopped(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return nil
	}
	return err
}

// retry calls ping until it succeeds, ctx ends or connectTimeout passes.
func retry(ctx context.Context, logger *slog.Logger, name string, ping func(context.Context) error) error {
	deadline := time.Now().Add(connectTimeout)
	for attempt := 1; ; attempt++ {
		pctx, cancel := context.WithTimeout(ctx, pingTimeout)
		err := ping(pctx)
		cancel()
		if err == nil {
			logger.Info(name+" reachable", "attempt", attempt)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().Add(retryPause).After(deadline) {
			return fmt.Errorf("%s not reachable after %d attempts in %s: %w", name, attempt, connectTimeout, err)
		}
		logger.Warn(name+" not reachable, retrying", "attempt", attempt, "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryPause):
		}
	}
}
