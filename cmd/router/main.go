// Command router routes conversations to operators (decisions 0009 and
// 0010). It reads the conversations topic, keeps the routing state in
// PostgreSQL, serves the operator API and publishes routing events to the
// routing topic. Configuration comes from THREADROUTE_* environment
// variables, see internal/router/routersvc.Load.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZAUakaAlexey/threadroute/internal/obs"
	"github.com/ZAUakaAlexey/threadroute/internal/router/routersvc"
)

func main() {
	os.Exit(runMain())
}

// runMain returns the exit code, so deferred calls run before os.Exit.
func runMain() int {
	cfg, err := routersvc.Load(os.LookupEnv)
	if err != nil {
		obs.NewLogger(os.Stderr, slog.LevelInfo).Error("router cannot start", "error", err)
		return 2
	}
	logger := obs.NewLogger(os.Stderr, cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, logger, prometheus.NewRegistry()); err != nil {
		logger.Error("router stopped with an error", "error", err)
		return 1
	}
	return 0
}
