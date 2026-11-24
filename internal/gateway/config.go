package gateway

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/defaults"
	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
	"github.com/ZAUakaAlexey/threadroute/internal/router/opclient"
)

// Prefix is shared by every variable of the project.
const Prefix = "THREADROUTE_"

// Variable names. KAFKA_BROKERS, LOG_LEVEL and the REDIS_ variables are
// shared with the other services.
const (
	EnvKafkaBrokers  = Prefix + "KAFKA_BROKERS"
	EnvLogLevel      = Prefix + "LOG_LEVEL"
	EnvRedisAddr     = Prefix + "REDIS_ADDR"
	EnvRedisPassword = Prefix + "REDIS_PASSWORD"
	EnvRedisDB       = Prefix + "REDIS_DB"

	EnvMetricsAddr       = Prefix + "GATEWAY_METRICS_ADDR"
	EnvHTTPAddr          = Prefix + "GATEWAY_HTTP_ADDR"
	EnvPathPrefix        = Prefix + "GATEWAY_PATH_PREFIX"
	EnvRouterURL         = Prefix + "GATEWAY_ROUTER_URL"
	EnvRouterTimeout     = Prefix + "GATEWAY_ROUTER_TIMEOUT"
	EnvSiteAPIURL        = Prefix + "GATEWAY_SITE_API_URL"
	EnvSiteAPITimeout    = Prefix + "GATEWAY_SITE_API_TIMEOUT"
	EnvSiteAPICache      = Prefix + "GATEWAY_SITE_API_CACHE"
	EnvFullRoles         = Prefix + "GATEWAY_FULL_ROLES"
	EnvAllowedOrigins    = Prefix + "GATEWAY_ALLOWED_ORIGINS"
	EnvTrustProxy        = Prefix + "GATEWAY_TRUST_PROXY"
	EnvDemoOperators     = Prefix + "GATEWAY_DEMO_OPERATORS"
	EnvMaxConnections    = Prefix + "GATEWAY_MAX_CONNECTIONS"
	EnvMaxConnsPerAddr   = Prefix + "GATEWAY_MAX_CONNECTIONS_PER_ADDR"
	EnvTick              = Prefix + "GATEWAY_TICK"
	EnvOfflineGrace      = Prefix + "GATEWAY_OFFLINE_GRACE"
	EnvWriteTimeout      = Prefix + "GATEWAY_WRITE_TIMEOUT"
	EnvPingInterval      = Prefix + "GATEWAY_PING_INTERVAL"
	EnvLagLimit          = Prefix + "GATEWAY_LAG_LIMIT"
	EnvSourceTimeout     = Prefix + "GATEWAY_SOURCE_TIMEOUT"
	EnvForgetAfter       = Prefix + "GATEWAY_FORGET_AFTER"
	EnvSnapshotFrameRows = Prefix + "GATEWAY_SNAPSHOT_FRAME_ROWS"
)

// Defaults that are not fixed by the protocol.
const (
	DefaultKafkaBrokers = "localhost:9092"
	DefaultLogLevel     = slog.LevelInfo
	DefaultRedisAddr    = "localhost:6379"
	DefaultRedisDB      = 0
	// DefaultRouterURL is the operator API of the router. The router binds
	// it to localhost by default; deploy/compose.yaml sets the container
	// address.
	DefaultRouterURL = "http://127.0.0.1:9470"
	// DefaultRouterTimeout bounds one call of the operator API. The
	// console waits 10 seconds for a welcome, and a hello may need a call.
	DefaultRouterTimeout = 5 * time.Second
	// DefaultSiteAPITimeout bounds one call of the site API.
	DefaultSiteAPITimeout = 3 * time.Second
	// DefaultSiteAPICache is how long a successful answer of the site API
	// is kept.
	DefaultSiteAPICache = time.Minute
	// DefaultAllowedOrigins are the browser origins (host:port patterns)
	// allowed for local development: the console dev server and the
	// gateway on the same machine. A page from the same host as the
	// gateway is always allowed. Production sets the site's host.
	DefaultAllowedOrigins = "localhost:*,127.0.0.1:*"
	// DefaultMaxConnections is the most WebSocket connections at the same
	// time. Above it a new connection is closed with 1013.
	DefaultMaxConnections = 2000
	// DefaultMaxConnsPerAddr is the most WebSocket connections of one
	// client address. Above it a new connection is closed with 4429.
	DefaultMaxConnsPerAddr = 20
	// DefaultWriteTimeout closes a client whose socket does not take one
	// frame within this time.
	DefaultWriteTimeout = 10 * time.Second
	// DefaultPingInterval is the WebSocket ping interval (decision 0011).
	DefaultPingInterval = 20 * time.Second
	// DefaultLagLimit marks the data stale when the gateway has been
	// behind the end of the topics for longer.
	DefaultLagLimit = 10 * time.Second
	// DefaultSourceTimeout marks the data stale when the gateway could not
	// read the end of the topics for this long.
	DefaultSourceTimeout = 5 * time.Second
	// DefaultForgetAfter is how long a closed conversation, a released
	// team or an offline human operator stays in memory after its last
	// event, so repeated events are still recognised.
	DefaultForgetAfter = time.Hour
	// DefaultSnapshotFrameRows is the most rows of the large collection in
	// one state frame of a snapshot.
	DefaultSnapshotFrameRows = 500
)

// Config is the configuration of the gateway.
type Config struct {
	// KafkaBrokers are the seed brokers, as host:port.
	KafkaBrokers []string
	// RedisAddr is host:port. RedisPassword is never logged.
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	// MetricsAddr serves /metrics, /healthz and /debug/pprof/.
	MetricsAddr string
	// HTTPAddr serves the WebSocket and the public snapshot.
	HTTPAddr string
	// PathPrefix is put before the two paths of gwapi, for a web server
	// that forwards a prefix without removing it. Empty or "/x[/y]".
	PathPrefix string
	// RouterURL is the base URL of the operator API.
	RouterURL string
	// RouterTimeout bounds one call of the operator API.
	RouterTimeout time.Duration
	// SiteAPIURL is the "who am I" URL of the site API. Empty means that
	// no token is checked and nobody gets the full level.
	SiteAPIURL string
	// SiteAPITimeout bounds one call of the site API.
	SiteAPITimeout time.Duration
	// SiteAPICache is how long a successful answer is kept.
	SiteAPICache time.Duration
	// FullRoles are the role names of the site that get the full level.
	FullRoles []string
	// AllowedOrigins are host patterns (path.Match syntax, with an
	// optional "scheme://") of browser origins that may open the
	// WebSocket. The host of the gateway itself is always allowed.
	AllowedOrigins []string
	// TrustProxy takes the client address from the right-most entry of
	// X-Forwarded-For. Only behind a web server that sets it.
	TrustProxy bool
	// Limits.
	DemoOperators   int
	MaxConnections  int
	MaxConnsPerAddr int
	// Tick is the time between two state messages to one client.
	Tick time.Duration
	// OfflineGrace keeps a human operator online after their connection
	// closed.
	OfflineGrace time.Duration
	// WriteTimeout closes a client that does not take one frame in time.
	WriteTimeout time.Duration
	// PingInterval is the WebSocket ping interval.
	PingInterval time.Duration
	// LagLimit and SourceTimeout decide when the data is stale.
	LagLimit      time.Duration
	SourceTimeout time.Duration
	// ForgetAfter bounds the memory of the view, see View.
	ForgetAfter time.Duration
	// SnapshotFrameRows splits a large snapshot into frames.
	SnapshotFrameRows int
	// LogLevel is the minimum level for log/slog.
	LogLevel slog.Level
}

// DefaultConfig returns the defaults.
func DefaultConfig() Config {
	return Config{
		KafkaBrokers:      []string{DefaultKafkaBrokers},
		RedisAddr:         DefaultRedisAddr,
		RedisDB:           DefaultRedisDB,
		MetricsAddr:       gwapi.DefaultMetricsAddr,
		HTTPAddr:          gwapi.DefaultHTTPAddr,
		RouterURL:         DefaultRouterURL,
		RouterTimeout:     DefaultRouterTimeout,
		SiteAPITimeout:    DefaultSiteAPITimeout,
		SiteAPICache:      DefaultSiteAPICache,
		AllowedOrigins:    splitList(DefaultAllowedOrigins),
		DemoOperators:     defaults.GatewayDemoOperators,
		MaxConnections:    DefaultMaxConnections,
		MaxConnsPerAddr:   DefaultMaxConnsPerAddr,
		Tick:              defaults.GatewayTick,
		OfflineGrace:      defaults.GatewayOfflineGrace,
		WriteTimeout:      DefaultWriteTimeout,
		PingInterval:      DefaultPingInterval,
		LagLimit:          DefaultLagLimit,
		SourceTimeout:     DefaultSourceTimeout,
		ForgetAfter:       DefaultForgetAfter,
		SnapshotFrameRows: DefaultSnapshotFrameRows,
		LogLevel:          DefaultLogLevel,
	}
}

// Validate reports every invalid field.
func (c Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	if len(c.KafkaBrokers) == 0 {
		add("no kafka brokers")
	}
	if c.RedisAddr == "" {
		add("redis address is empty")
	}
	if c.RedisDB < 0 {
		add("redis db %d is negative", c.RedisDB)
	}
	if err := checkPathPrefix(c.PathPrefix); err != nil {
		add("path prefix: %v", err)
	}
	if _, err := opclient.New(c.RouterURL); err != nil {
		add("router URL: %v", err)
	}
	if c.SiteAPIURL != "" {
		if err := checkHTTPURL(c.SiteAPIURL); err != nil {
			add("site API URL: %v", err)
		}
	}
	for _, p := range c.AllowedOrigins {
		if _, err := path.Match(p, ""); err != nil {
			add("allowed origin %q: %v", p, err)
		}
	}
	for _, d := range []struct {
		name string
		v    time.Duration
	}{
		{"router timeout", c.RouterTimeout},
		{"site API timeout", c.SiteAPITimeout},
		{"site API cache", c.SiteAPICache},
		{"offline grace", c.OfflineGrace},
		{"write timeout", c.WriteTimeout},
		{"ping interval", c.PingInterval},
		{"lag limit", c.LagLimit},
		{"source timeout", c.SourceTimeout},
		{"forget after", c.ForgetAfter},
	} {
		if d.v <= 0 {
			add("%s %s is not positive", d.name, d.v)
		}
	}
	if c.Tick < gwapi.Tick || c.Tick > 10*time.Second {
		add("tick %s is not between %s and 10s", c.Tick, gwapi.Tick)
	}
	if c.DemoOperators < 0 {
		add("demo operators %d is negative", c.DemoOperators)
	}
	if c.MaxConnections < 1 || c.MaxConnsPerAddr < 1 {
		add("connection limits must be at least 1")
	}
	if c.SnapshotFrameRows < 1 {
		add("snapshot frame rows %d is less than 1", c.SnapshotFrameRows)
	}
	return errors.Join(errs...)
}

// LookupFunc returns the value of an environment variable and whether it
// is set. os.LookupEnv has this signature.
type LookupFunc func(key string) (string, bool)

// Load reads and validates the configuration. A variable that is unset or
// empty takes its default. The error names every invalid variable, never
// a password.
func Load(lookup LookupFunc) (Config, error) {
	d := DefaultConfig()
	r := reader{lookup: lookup}
	cfg := Config{
		KafkaBrokers:      r.brokers(EnvKafkaBrokers, DefaultKafkaBrokers),
		RedisAddr:         r.hostPort(EnvRedisAddr, d.RedisAddr),
		RedisPassword:     r.raw(EnvRedisPassword),
		RedisDB:           r.intBetween(EnvRedisDB, d.RedisDB, 0, 1<<20),
		MetricsAddr:       r.hostPort(EnvMetricsAddr, d.MetricsAddr),
		HTTPAddr:          r.hostPort(EnvHTTPAddr, d.HTTPAddr),
		PathPrefix:        r.pathPrefix(EnvPathPrefix),
		RouterURL:         r.routerURL(EnvRouterURL, d.RouterURL),
		RouterTimeout:     r.duration(EnvRouterTimeout, d.RouterTimeout),
		SiteAPIURL:        r.siteURL(EnvSiteAPIURL),
		SiteAPITimeout:    r.duration(EnvSiteAPITimeout, d.SiteAPITimeout),
		SiteAPICache:      r.duration(EnvSiteAPICache, d.SiteAPICache),
		FullRoles:         r.list(EnvFullRoles, ""),
		AllowedOrigins:    r.list(EnvAllowedOrigins, DefaultAllowedOrigins),
		TrustProxy:        r.boolean(EnvTrustProxy, false),
		DemoOperators:     r.intBetween(EnvDemoOperators, d.DemoOperators, 0, 10000),
		MaxConnections:    r.intBetween(EnvMaxConnections, d.MaxConnections, 1, 1_000_000),
		MaxConnsPerAddr:   r.intBetween(EnvMaxConnsPerAddr, d.MaxConnsPerAddr, 1, 1_000_000),
		Tick:              r.duration(EnvTick, d.Tick),
		OfflineGrace:      r.duration(EnvOfflineGrace, d.OfflineGrace),
		WriteTimeout:      r.duration(EnvWriteTimeout, d.WriteTimeout),
		PingInterval:      r.duration(EnvPingInterval, d.PingInterval),
		LagLimit:          r.duration(EnvLagLimit, d.LagLimit),
		SourceTimeout:     r.duration(EnvSourceTimeout, d.SourceTimeout),
		ForgetAfter:       r.duration(EnvForgetAfter, d.ForgetAfter),
		SnapshotFrameRows: r.intBetween(EnvSnapshotFrameRows, d.SnapshotFrameRows, 1, 100_000),
		LogLevel:          r.logLevel(EnvLogLevel, d.LogLevel),
	}
	if cfg.Tick < gwapi.Tick || cfg.Tick > 10*time.Second {
		r.fail(EnvTick, fmt.Errorf("%s is not between %s and 10s", cfg.Tick, gwapi.Tick))
	}
	for _, p := range cfg.AllowedOrigins {
		if _, err := path.Match(p, ""); err != nil {
			r.fail(EnvAllowedOrigins, fmt.Errorf("allowed origin %q: %w", p, err))
		}
	}
	if err := errors.Join(r.errs...); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}

// reader collects one error per invalid variable.
type reader struct {
	lookup LookupFunc
	errs   []error
}

func (r *reader) get(key string) (string, bool) {
	v, ok := r.lookup(key)
	v = strings.TrimSpace(v)
	if !ok || v == "" {
		return "", false
	}
	return v, true
}

// raw returns the value as it is, for a password.
func (r *reader) raw(key string) string {
	v, _ := r.lookup(key)
	return v
}

func (r *reader) fail(key string, err error) {
	r.errs = append(r.errs, fmt.Errorf("%s: %w", key, err))
}

func (r *reader) brokers(key, def string) []string {
	v, ok := r.get(key)
	if !ok {
		v = def
	}
	out := splitList(v)
	for _, b := range out {
		if err := checkHostPort(b); err != nil {
			r.fail(key, fmt.Errorf("broker %q: %w", b, err))
			return nil
		}
	}
	if len(out) == 0 {
		r.fail(key, errors.New("no brokers"))
	}
	return out
}

func (r *reader) list(key, def string) []string {
	v, ok := r.get(key)
	if !ok {
		v = def
	}
	return splitList(v)
}

func (r *reader) hostPort(key, def string) string {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	if err := checkHostPort(v); err != nil {
		r.fail(key, err)
		return ""
	}
	return v
}

func (r *reader) pathPrefix(key string) string {
	v, ok := r.get(key)
	if !ok {
		return ""
	}
	v = strings.TrimSuffix(v, "/")
	if err := checkPathPrefix(v); err != nil {
		r.fail(key, err)
		return ""
	}
	return v
}

func (r *reader) routerURL(key, def string) string {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	if _, err := opclient.New(v); err != nil {
		r.fail(key, err)
		return ""
	}
	return v
}

func (r *reader) siteURL(key string) string {
	v, ok := r.get(key)
	if !ok {
		return ""
	}
	if err := checkHTTPURL(v); err != nil {
		r.fail(key, err)
		return ""
	}
	return v
}

func (r *reader) intBetween(key string, def, least, most int) int {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.fail(key, fmt.Errorf("%q is not a whole number", v))
		return def
	}
	if n < least || n > most {
		r.fail(key, fmt.Errorf("%d is not between %d and %d", n, least, most))
		return def
	}
	return n
}

func (r *reader) duration(key string, def time.Duration) time.Duration {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.fail(key, fmt.Errorf("%q is not a duration such as 500ms, 20s or 15m", v))
		return def
	}
	if d <= 0 {
		r.fail(key, fmt.Errorf("%s is not positive", d))
		return def
	}
	return d
}

func (r *reader) boolean(key string, def bool) bool {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		r.fail(key, fmt.Errorf("%q is not true or false", v))
		return def
	}
	return b
}

func (r *reader) logLevel(key string, def slog.Level) slog.Level {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	var l slog.Level
	if err := l.UnmarshalText([]byte(v)); err != nil {
		r.fail(key, fmt.Errorf("%q is not a log level (debug, info, warn, error)", v))
		return def
	}
	return l
}

func splitList(v string) []string {
	var out []string
	for part := range strings.SplitSeq(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func checkHostPort(v string) error {
	_, port, err := net.SplitHostPort(v)
	if err != nil {
		return fmt.Errorf("%q is not host:port: %w", v, err)
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 0 || p > 65535 {
		return fmt.Errorf("%q has an invalid port", v)
	}
	return nil
}

func checkHTTPURL(v string) error {
	u, err := url.Parse(v)
	if err != nil {
		return fmt.Errorf("not a URL: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Fragment != "" {
		return fmt.Errorf("%q is not an http(s) URL with a host and without user info", v)
	}
	return nil
}

func checkPathPrefix(v string) error {
	if v == "" {
		return nil
	}
	if !strings.HasPrefix(v, "/") || strings.HasSuffix(v, "/") || strings.ContainsAny(v, "?#{} \t") || path.Clean(v) != v {
		return fmt.Errorf("%q is not a clean path such as /console", v)
	}
	return nil
}
