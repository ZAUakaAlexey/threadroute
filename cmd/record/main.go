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
	"syscall"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZAUakaAlexey/threadroute/internal/record"
)

func main() {
	if err := run(); err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("record failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := record.Load(os.LookupEnv)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A shared metrics endpoint is added later. Until then the registry is
	// only filled.
	reg := prometheus.NewRegistry()

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
	)
	if err := rec.Run(ctx); err != nil {
		return fmt.Errorf("recorder: %w", err)
	}
	return nil
}
