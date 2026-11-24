package main

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZAUakaAlexey/threadroute/internal/obs"
	"github.com/ZAUakaAlexey/threadroute/internal/sim"
)

// run wires the simulator and runs it until ctx ends or a fatal error
// happens. It returns nil after a clean stop. Any other error is fatal: the
// process exits with a non-zero code and the restart policy starts it
// again, which is a clean start (see internal/sim).
//
// Order: metrics server (not ready), then sim.Service, which waits for the
// router and the routing topic, sets up the operators and reports ready
// once its consumer runs.
func run(ctx context.Context, cfg sim.Config, logger *slog.Logger, reg *prometheus.Registry) error {
	b := cfg.Behaviour
	logger.Info("sim starting",
		"kafka_brokers", cfg.KafkaBrokers,
		"router_url", cfg.RouterURL,
		"group_id", cfg.GroupID,
		"metrics_addr", cfg.MetricsAddr,
		"start_wait", cfg.StartWait.String(),
		"outage_limit", cfg.OutageLimit.String(),
		"operators", b.Operators,
		"languages", b.Languages.String(),
		"capacity", b.Capacity,
		"shift", []string{b.ShiftOnline.String(), b.ShiftOffline.String()},
		"accept_delay", []string{b.AcceptDelayMin.String(), b.AcceptDelayMax.String()},
		"missed_offer_share", b.MissedOfferShare,
		"handling_median", b.HandlingMedian.String(),
		"resolve_median", b.ResolveMedian.String(),
		"seed", b.Seed,
	)

	svc, err := sim.New(sim.Options{Config: cfg, Logger: logger, Registerer: reg})
	if err != nil {
		return err
	}
	srv, err := obs.NewServer(ctx, cfg.MetricsAddr, reg, svc.Ready, logger)
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
	return svc.Run(ctx)
}
