// Command ingest reads the Bluesky Jetstream and writes every event to the
// raw Kafka topic (decisions 0001 to 0003). Configuration comes from
// THREADROUTE_* environment variables, see internal/config.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZAUakaAlexey/threadroute/internal/config"
	"github.com/ZAUakaAlexey/threadroute/internal/obs"
)

func main() {
	os.Exit(runMain())
}

// runMain returns the exit code, so deferred calls run before os.Exit.
func runMain() int {
	cfg, err := config.Load(os.LookupEnv)
	if err != nil {
		obs.NewLogger(os.Stderr, slog.LevelInfo).Error("ingest cannot start", "error", err)
		return 2
	}
	logger := obs.NewLogger(os.Stderr, cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, logger, prometheus.NewRegistry(), newBlueskyConnector, defaultTimings()); err != nil {
		logger.Error("ingest stopped with an error", "error", err)
		return 1
	}
	logger.Info("ingest stopped")
	return 0
}
