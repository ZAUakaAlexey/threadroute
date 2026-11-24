// Command sim is the operator simulator (decision 0010). It plays a fixed
// set of simulated operators against the operator API of the router and
// reads the routing topic to learn about offers. Configuration comes from
// THREADROUTE_* environment variables, see internal/sim.Load.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZAUakaAlexey/threadroute/internal/obs"
	"github.com/ZAUakaAlexey/threadroute/internal/sim"
)

func main() {
	os.Exit(runMain())
}

// runMain returns the exit code, so deferred calls run before os.Exit.
func runMain() int {
	cfg, err := sim.Load(os.LookupEnv)
	if err != nil {
		obs.NewLogger(os.Stderr, slog.LevelInfo).Error("sim cannot start", "error", err)
		return 2
	}
	logger := obs.NewLogger(os.Stderr, cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, logger, prometheus.NewRegistry()); err != nil {
		logger.Error("sim stopped with an error", "error", err)
		return 1
	}
	logger.Info("sim stopped")
	return 0
}
