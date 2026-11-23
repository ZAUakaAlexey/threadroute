package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
	"github.com/ZAUakaAlexey/threadroute/internal/config"
	"github.com/ZAUakaAlexey/threadroute/internal/obs"
	"github.com/ZAUakaAlexey/threadroute/internal/source"
	"github.com/ZAUakaAlexey/threadroute/internal/source/bluesky"
)

// connectorFactory builds the source connector. Tests pass a fake.
type connectorFactory func(cfg config.Config, logger *slog.Logger, reg prometheus.Registerer) (source.Connector, error)

func newBlueskyConnector(cfg config.Config, logger *slog.Logger, reg prometheus.Registerer) (source.Connector, error) {
	return bluesky.New(bluesky.Config{
		PrimaryHost:  cfg.SourcePrimaryHost,
		FailoverHost: cfg.SourceFailoverHost,
		Collection:   cfg.SourceCollection,
		Logger:       logger,
		Registerer:   reg,
	})
}

// timings are the time limits of start and stop.
type timings struct {
	// connectTimeout bounds the whole wait for Kafka at start.
	connectTimeout time.Duration
	// pingTimeout bounds one connection attempt.
	pingTimeout time.Duration
	// backoffMin and backoffMax bound the pause between attempts. The
	// pause doubles after each failed attempt.
	backoffMin, backoffMax time.Duration
	// batchGrace is how long the current batch may still run after a stop
	// signal, so its records and its cursor can be acknowledged.
	batchGrace time.Duration
	// closeTimeout bounds the flush of the producer buffer at the end.
	closeTimeout time.Duration
}

// defaultTimings keep a clean stop (batchGrace + closeTimeout) under the 30
// second stop grace period set in deploy/compose.yaml.
func defaultTimings() timings {
	return timings{
		connectTimeout: 2 * time.Minute,
		pingTimeout:    5 * time.Second,
		backoffMin:     500 * time.Millisecond,
		backoffMax:     15 * time.Second,
		batchGrace:     10 * time.Second,
		closeTimeout:   10 * time.Second,
	}
}

// run wires ingest and runs it until ctx ends or a fatal error happens.
//
// It returns nil after a clean stop, also when ctx ends during start. Any
// other error is fatal: the process exits with a non-zero code, the
// container restart policy starts it again, and it resumes from the stored
// cursor (decision 0001, "When Kafka is unavailable").
func run(ctx context.Context, cfg config.Config, logger *slog.Logger, reg *prometheus.Registry, newConnector connectorFactory, t timings) (err error) {
	logger.Info("ingest starting",
		"kafka_brokers", cfg.KafkaBrokers,
		"source_primary_host", cfg.SourcePrimaryHost,
		"source_failover_host", cfg.SourceFailoverHost,
		"source_collection", cfg.SourceCollection,
		"metrics_addr", cfg.MetricsAddr,
		"producer_max_buffered_records", cfg.ProducerMaxBufferedRecords,
		"producer_max_buffered_bytes", cfg.ProducerMaxBufferedBytes,
		"producer_delivery_timeout", cfg.ProducerDeliveryTimeout.String(),
		"raw_retention", cfg.RawLimits.Retention.String(),
		"raw_max_bytes", cfg.RawLimits.MaxBytes,
	)

	// Ready once the first batch of this run is written to Kafka.
	var ready atomic.Bool
	srv, err := obs.NewServer(ctx, cfg.MetricsAddr, reg, ready.Load, logger)
	if err != nil {
		return err
	}
	// The metrics server outlives ctx, so the stop can still be observed.
	// It stops when run returns.
	srvCtx, stopSrv := context.WithCancel(context.WithoutCancel(ctx))
	waitSrv := srv.Start(srvCtx)
	defer func() {
		stopSrv()
		if serr := waitSrv(); serr != nil {
			logger.Warn("metrics server", "error", serr)
		}
	}()
	logger.Info("metrics server listening", "addr", srv.Addr())

	rawSpec, err := bus.RawTopicSpec(cfg.RawLimits)
	if err != nil {
		return err
	}

	admin, err := kgo.NewClient(kgo.SeedBrokers(cfg.KafkaBrokers...))
	if err != nil {
		return fmt.Errorf("kafka admin client: %w", err)
	}
	defer admin.Close()

	if err := waitForKafka(ctx, admin, logger, t); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	if err := bus.EnsureTopics(ctx, admin, logger, rawSpec, bus.CursorTopicSpec()); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	admin.Close()

	from, found, err := bus.ReadCursor(ctx, cfg.KafkaBrokers, bus.SourceBluesky, logger)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	if found {
		logger.Info("resuming from the stored cursor",
			"witnessed", from.Witnessed, "seq", from.Seq, "host", from.Host,
			"behind_seconds", time.Since(from.Witnessed).Seconds())
	} else {
		logger.Info("no stored cursor, starting from live")
	}

	writer, err := bus.NewRawWriter(cfg.KafkaBrokers, bus.ProducerLimits{
		MaxBufferedRecords: cfg.ProducerMaxBufferedRecords,
		MaxBufferedBytes:   cfg.ProducerMaxBufferedBytes,
		DeliveryTimeout:    cfg.ProducerDeliveryTimeout,
	}, bus.SourceBluesky, logger, reg)
	if err != nil {
		return err
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), t.closeTimeout)
		defer cancel()
		if cerr := writer.Close(cctx); cerr != nil {
			logger.Warn("raw writer did not close cleanly", "error", cerr)
			if err == nil {
				err = cerr
			}
		}
	}()

	conn, err := newConnector(cfg, logger, reg)
	if err != nil {
		return err
	}

	// The sink gets its own context: when ctx ends, the batch in progress
	// still gets batchGrace to be acknowledged and to move the cursor. The
	// connector stops reading at once, because it watches ctx.
	writeCtx, cancelWrites := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWrites()
	stopGrace := context.AfterFunc(ctx, func() {
		time.AfterFunc(t.batchGrace, cancelWrites)
	})
	defer stopGrace()
	sink := func(_ context.Context, b source.Batch) error {
		if err := writer.WriteBatch(writeCtx, b); err != nil {
			return err
		}
		if !ready.Load() {
			ready.Store(true)
			logger.Info("first batch written, ingest is ready", "events", len(b.Events))
		}
		return nil
	}

	logger.Info("ingest running")
	runErr := conn.Run(ctx, from, sink)
	ready.Store(false)
	if runErr != nil {
		return fmt.Errorf("connector: %w", runErr)
	}
	return nil
}

// waitForKafka pings the brokers until one answers. It retries with a
// doubling pause and gives up after t.connectTimeout.
func waitForKafka(ctx context.Context, client *kgo.Client, logger *slog.Logger, t timings) error {
	deadline := time.Now().Add(t.connectTimeout)
	backoff := t.backoffMin
	for attempt := 1; ; attempt++ {
		pctx, cancel := context.WithTimeout(ctx, t.pingTimeout)
		err := client.Ping(pctx)
		cancel()
		if err == nil {
			logger.Info("kafka reachable", "attempt", attempt)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().Add(backoff).After(deadline) {
			logger.Error("kafka not reachable, giving up", "attempt", attempt, "error", err)
			return fmt.Errorf("kafka not reachable after %d attempts in %s: %w", attempt, t.connectTimeout, err)
		}
		logger.Warn("kafka not reachable, retrying", "attempt", attempt, "retry_in", backoff.String(), "error", err)

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff = min(2*backoff, t.backoffMax)
	}
}
