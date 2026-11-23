// Command record copies the raw topic into private, compressed segment files
// for load tests (decisions 0001 and 0004). Configuration comes from
// THREADROUTE_* environment variables, see internal/record.Load.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZAUakaAlexey/threadroute/internal/obs"
	"github.com/ZAUakaAlexey/threadroute/internal/record"
)

func main() {
	if err := run(); err != nil {
		obs.NewLogger(os.Stderr, slog.LevelInfo).Error("record failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := record.Load(os.LookupEnv)
	if err != nil {
		return err
	}
	logger := obs.NewLogger(os.Stderr, cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := prometheus.NewRegistry()
	// Ready while the recorder runs.
	var ready atomic.Bool
	srv, err := obs.NewServer(ctx, cfg.MetricsAddr, reg, ready.Load, logger)
	if err != nil {
		return err
	}
	// The metrics server outlives ctx, so the final commit can still be
	// observed. It stops when run returns.
	srvCtx, stopSrv := context.WithCancel(context.WithoutCancel(ctx))
	waitSrv := srv.Start(srvCtx)
	defer func() {
		stopSrv()
		if err := waitSrv(); err != nil {
			logger.Warn("metrics server", "error", err)
		}
	}()

	w, err := record.OpenWriter(cfg.Writer, logger, reg)
	if err != nil {
		return err
	}
	rec, err := record.NewRecorder(cfg.KafkaBrokers, cfg.GroupID, cfg.Start, w, logger, reg)
	if err != nil {
		if cerr := w.Close(); cerr != nil {
			logger.Error("close writer", "error", cerr)
		}
		return err
	}
	logger.Info("record starting",
		"brokers", cfg.KafkaBrokers,
		"group", cfg.GroupID,
		"start", cfg.Start,
		"dir", cfg.Writer.Dir,
		"segment_max_bytes", cfg.Writer.SegmentMaxBytes,
		"segment_max_age", cfg.Writer.SegmentMaxAge.String(),
		"retention", cfg.Writer.Retention.String(),
		"max_total_bytes", cfg.Writer.MaxTotalBytes,
		"metrics_addr", srv.Addr(),
	)
	ready.Store(true)
	err = rec.Run(ctx)
	ready.Store(false)
	if err != nil {
		return fmt.Errorf("recorder: %w", err)
	}
	return nil
}
