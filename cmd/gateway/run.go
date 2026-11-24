package main

import (
	"context"
	"log/slog"
	"net/url"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/ZAUakaAlexey/threadroute/internal/conv"
	"github.com/ZAUakaAlexey/threadroute/internal/gateway"
	"github.com/ZAUakaAlexey/threadroute/internal/obs"
	"github.com/ZAUakaAlexey/threadroute/internal/router/opclient"
)

// run wires the gateway and runs it until ctx ends. It returns nil after a
// clean stop. An outage of Kafka, Redis, the router or the site API does
// not stop it: the health endpoint says "not ready" and the browsers get
// the stale status.
//
// Order: metrics server (not ready), then the gateway service, which
// listens at once, reads the router settings, reads both topics from the
// beginning and reports ready when it has caught up.
//
// The local role is logged as a warning once here: every connection gets
// it. Config validation already refused it unless every allowed origin is
// a loopback origin.
func run(ctx context.Context, cfg gateway.Config, logger *slog.Logger, reg *prometheus.Registry) error {
	// The Redis password, the site API URL query and tokens are never
	// logged; the site API is named by its host only. The name of the site
	// cookie is logged, its value never.
	siteHost := ""
	if u, err := url.Parse(cfg.SiteAPIURL); err == nil {
		siteHost = u.Host
	}
	logger.Info("gateway starting",
		"kafka_brokers", cfg.KafkaBrokers,
		"redis_addr", cfg.RedisAddr,
		"redis_db", cfg.RedisDB,
		"metrics_addr", cfg.MetricsAddr,
		"http_addr", cfg.HTTPAddr,
		"path_prefix", cfg.PathPrefix,
		"router_url", cfg.RouterURL,
		"site_api_host", siteHost,
		"full_roles", len(cfg.FullRoles),
		"settings_roles", len(cfg.SettingsRoles),
		"site_cookie", cfg.SiteCookie,
		"local_role", cfg.LocalRole != "",
		"allowed_origins", cfg.AllowedOrigins,
		"trust_proxy", cfg.TrustProxy,
		"demo_operators", cfg.DemoOperators,
		"max_connections", cfg.MaxConnections,
		"max_connections_per_addr", cfg.MaxConnsPerAddr,
		"tick", cfg.Tick.String(),
		"offline_grace", cfg.OfflineGrace.String(),
		"forget_after", cfg.ForgetAfter.String(),
	)
	switch {
	case cfg.LocalRole != "":
		// Decision 0014, point L1: only for a stand on a developer machine.
		logger.Warn("local role is set: every connection gets this role, the site API is not asked",
			"role", cfg.LocalRole, "allowed_origins", cfg.AllowedOrigins)
	case cfg.SiteAPIURL == "":
		logger.Warn("no site API URL: every connection gets the demo level")
	}

	var current atomic.Pointer[gateway.Service]
	ready := func() bool {
		svc := current.Load()
		return svc != nil && svc.Ready()
	}
	srv, err := obs.NewServer(ctx, cfg.MetricsAddr, reg, ready, logger)
	if err != nil {
		return err
	}
	srvCtx, stopSrv := context.WithCancel(context.WithoutCancel(ctx))
	waitSrv := srv.Start(srvCtx)
	defer func() {
		stopSrv()
		if serr := waitSrv(); serr != nil {
			logger.Warn("metrics server", "error", serr)
		}
	}()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPassword, DB: cfg.RedisDB})
	defer rdb.Close()
	text, err := conv.NewTextReader(rdb, conv.DefaultKeyPrefix)
	if err != nil {
		return err
	}
	api, err := opclient.New(cfg.RouterURL, opclient.WithTimeout(cfg.RouterTimeout), opclient.WithLogger(logger))
	if err != nil {
		return err
	}
	roles := gateway.Roles{Full: cfg.FullRoles, Settings: cfg.SettingsRoles}
	auth := gateway.NewSiteChecker(cfg.SiteAPIURL, roles, cfg.SiteAPITimeout, cfg.SiteAPICache, nil, nil)

	svc, err := gateway.NewService(ctx, gateway.ServiceOptions{
		Config: cfg, API: api, Auth: auth, Text: text, Logger: logger, Registerer: reg,
	})
	if err != nil {
		return err
	}
	current.Store(svc)
	logger.Info("gateway listening", "http_addr", svc.Addr(), "metrics_addr", srv.Addr())
	return svc.Run(ctx)
}
