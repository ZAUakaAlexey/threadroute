// Command conv builds conversations from the events and events.control
// topics, keeps their state in Redis and publishes conversation events to
// the conversations topic (decisions 0006 to 0008). Configuration comes from
// THREADROUTE_* environment variables, see internal/conv.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZAUakaAlexey/threadroute/internal/conv"
	"github.com/ZAUakaAlexey/threadroute/internal/obs"
)

func main() {
	os.Exit(runMain())
}

// runMain returns the exit code, so deferred calls run before os.Exit.
func runMain() int {
	cfg, err := conv.Load(os.LookupEnv)
	if err != nil {
		obs.NewLogger(os.Stderr, slog.LevelInfo).Error("conv cannot start", "error", err)
		return 2
	}
	logger := obs.NewLogger(os.Stderr, cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, logger, prometheus.NewRegistry()); err != nil {
		logger.Error("conv stopped with an error", "error", err)
		return 1
	}
	logger.Info("conv stopped")
	return 0
}
