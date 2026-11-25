package main

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZAUakaAlexey/threadroute/internal/obs"
	"github.com/ZAUakaAlexey/threadroute/internal/pg"
	"github.com/ZAUakaAlexey/threadroute/internal/router"
	"github.com/ZAUakaAlexey/threadroute/internal/router/routersvc"
)

// run wires the router and runs it until ctx ends or a fatal error
// happens. It returns nil after a clean stop, also when ctx ends during
// start. Any other error is fatal: the process exits with a non-zero code,
// the restart policy starts it again, and it continues from the committed
// offsets and the unsent outbox rows.
//
// Order: metrics server (not ready), PostgreSQL with the migrations, then
// routersvc.Service, which waits for Kafka and the topics, catches up and
// only then reports ready.
func run(ctx context.Context, cfg routersvc.Config, logger *slog.Logger, reg *prometheus.Registry) error {
	rc := cfg.Router
	// The DSN can hold a password and is never logged.
	logger.Info("router starting",
		"kafka_brokers", cfg.KafkaBrokers,
		"group_id", cfg.GroupID,
		"metrics_addr", cfg.MetricsAddr,
		"api_addr", cfg.APIAddr,
		"postgres_max_conns", cfg.Postgres.MaxConns,
		"speed_factor", rc.SpeedFactor,
		"capacity", rc.Capacity,
		"accept_timeout", rc.AcceptTimeout.String(),
		"assignment_target", rc.AssignmentTarget.String(),
		"first_response_target", rc.FirstResponseTarget.String(),
		// The team numbers are stand settings (decision 0014): the router
		// logs them when it has read the stored set.
		"assign_workers", cfg.AssignWorkers,
		"outage_limit", cfg.OutageLimit.String(),
		"operator_refresh", cfg.OperatorRefresh.String(),
	)

	// Ready once the catch-up has ended.
	var current atomic.Pointer[routersvc.Service]
	ready := func() bool {
		svc := current.Load()
		return svc != nil && svc.Ready()
	}
	srv, err := obs.NewServer(ctx, cfg.MetricsAddr, reg, ready, logger)
	if err != nil {
		return err
	}
	// The metrics server outlives ctx, so the stop can still be observed.
	srvCtx, stopSrv := context.WithCancel(context.WithoutCancel(ctx))
	waitSrv := srv.Start(srvCtx)
	defer func() {
		stopSrv()
		if serr := waitSrv(); serr != nil {
			logger.Warn("metrics server", "error", serr)
		}
	}()
	logger.Info("metrics server listening", "addr", srv.Addr())

	pool, err := pg.Open(ctx, cfg.Postgres, logger)
	if err != nil {
		return stopped(ctx, err)
	}
	defer pool.Close()
	if err := pg.Migrate(ctx, pool, logger); err != nil {
		return stopped(ctx, err)
	}

	svc, err := routersvc.New(ctx, routersvc.Options{
		Config:     cfg,
		Pool:       pool,
		Clock:      router.SystemClock{},
		Logger:     logger,
		Registerer: reg,
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
