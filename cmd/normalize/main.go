// Command normalize reads the raw Kafka topic and writes channel-agnostic
// events to the events and events.control topics, in Kafka transactions,
// with deduplication in Redis (decisions 0003 and 0006). Configuration comes
// from THREADROUTE_* environment variables, see internal/normalize.Load.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZAUakaAlexey/threadroute/internal/normalize"
	"github.com/ZAUakaAlexey/threadroute/internal/obs"
)

func main() {
	os.Exit(runMain())
}

// runMain returns the exit code, so deferred calls run before os.Exit.
func runMain() int {
	cfg, err := normalize.Load(os.LookupEnv)
	if err != nil {
		obs.NewLogger(os.Stderr, slog.LevelInfo).Error("normalize cannot start", "error", err)
		return 2
	}
	logger := obs.NewLogger(os.Stderr, cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, logger, prometheus.NewRegistry()); err != nil {
		logger.Error("normalize stopped with an error", "error", err)
		return 1
	}
	return 0
}

// run wires the service and runs it until ctx ends or a fatal error
// happens. Any error is fatal: the process exits with a non-zero code, the
// container restart policy starts it again, and it continues from the last
// committed offsets.
func run(ctx context.Context, cfg normalize.Config, logger *slog.Logger, reg *prometheus.Registry) error {
	// The password is never logged.
	logger.Info("normalize starting",
		"kafka_brokers", cfg.KafkaBrokers,
		"group", cfg.GroupID,
		"transactional_id", cfg.TransactionalID,
		"start", cfg.Start,
		"max_batch_records", cfg.MaxBatchRecords,
		"raw_topic_wait", cfg.RawTopicWait.String(),
		"redis_addr", cfg.RedisAddr,
		"redis_db", cfg.RedisDB,
		"dedup_key_lifetime", cfg.DedupKeyLifetime.String(),
		"metrics_addr", cfg.MetricsAddr,
		"producer_max_buffered_records", cfg.Producer.MaxBufferedRecords,
		"producer_max_buffered_bytes", cfg.Producer.MaxBufferedBytes,
		"producer_delivery_timeout", cfg.Producer.DeliveryTimeout.String(),
	)

	rdb := normalize.NewRedisClient(cfg)
	defer func() {
		if err := rdb.Close(); err != nil {
			logger.Warn("redis client did not close cleanly", "error", err)
		}
	}()

	// Once at start: the deduplication needs Redis 7.0 or newer. An
	// unreachable Redis also fails here, which stops the service like a
	// Redis error in a batch would.
	if err := normalize.CheckRedisVersion(ctx, rdb); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	svc, err := normalize.New(cfg, rdb, logger, reg)
	if err != nil {
		return err
	}

	// Ready once the group is joined, see Service.Ready.
	srv, err := obs.NewServer(ctx, cfg.MetricsAddr, reg, svc.Ready, logger)
	if err != nil {
		return err
	}
	// The metrics server outlives ctx, so the stop can still be observed.
	// It stops when run returns.
	srvCtx, stopSrv := context.WithCancel(context.WithoutCancel(ctx))
	waitSrv := srv.Start(srvCtx)
	defer func() {
		stopSrv()
		if err := waitSrv(); err != nil {
			logger.Warn("metrics server", "error", err)
		}
	}()
	logger.Info("metrics server listening", "addr", srv.Addr())

	return svc.Run(ctx)
}
