// Command gateway connects browsers to the pipeline over WebSocket and
// serves the public snapshot (decisions 0011, 0012 and 0013). It reads the
// conversations and routing topics, message text from Redis through conv,
// and calls the operator API of the router. Configuration comes from
// THREADROUTE_* environment variables, see internal/gateway.Load.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZAUakaAlexey/threadroute/internal/gateway"
	"github.com/ZAUakaAlexey/threadroute/internal/obs"
)

func main() {
	os.Exit(runMain())
}

// runMain returns the exit code, so deferred calls run before os.Exit.
func runMain() int {
	cfg, err := gateway.Load(os.LookupEnv)
	if err != nil {
		obs.NewLogger(os.Stderr, slog.LevelInfo).Error("gateway cannot start", "error", err)
		return 2
	}
	logger := obs.NewLogger(os.Stderr, cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, logger, prometheus.NewRegistry()); err != nil {
		logger.Error("gateway stopped with an error", "error", err)
		return 1
	}
	return 0
}
