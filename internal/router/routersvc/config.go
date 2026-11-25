package routersvc

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/defaults"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/pg"
	"github.com/ZAUakaAlexey/threadroute/internal/router"
)

// Prefix is shared by every variable of the project.
const Prefix = "THREADROUTE_"

// Variable names. KAFKA_BROKERS and LOG_LEVEL have the same names as in the
// other services, and the POSTGRES_ variables are the ones of internal/pg.
const (
	EnvKafkaBrokers = Prefix + "KAFKA_BROKERS"
	EnvLogLevel     = Prefix + "LOG_LEVEL"

	EnvGroupID        = Prefix + "ROUTER_GROUP_ID"
	EnvMetricsAddr    = Prefix + "ROUTER_METRICS_ADDR"
	EnvAPIAddr        = Prefix + "ROUTER_API_ADDR"
	EnvInputTopicWait = Prefix + "ROUTER_INPUT_TOPIC_WAIT"

	EnvSpeedFactor         = Prefix + "ROUTER_SPEED_FACTOR"
	EnvOperatorCapacity    = Prefix + "ROUTER_OPERATOR_CAPACITY"
	EnvAcceptTimeout       = Prefix + "ROUTER_ACCEPT_TIMEOUT"
	EnvAssignmentTarget    = Prefix + "ROUTER_ASSIGNMENT_TARGET"
	EnvFirstResponseTarget = Prefix + "ROUTER_FIRST_RESPONSE_TARGET"

	EnvAssignWorkers   = Prefix + "ROUTER_ASSIGN_WORKERS"
	EnvAssignInterval  = Prefix + "ROUTER_ASSIGN_INTERVAL"
	EnvExpireInterval  = Prefix + "ROUTER_EXPIRE_INTERVAL"
	EnvSLAInterval     = Prefix + "ROUTER_SLA_INTERVAL"
	EnvTeamInterval    = Prefix + "ROUTER_TEAM_INTERVAL"
	EnvPublishInterval = Prefix + "ROUTER_PUBLISH_INTERVAL"
	EnvPublishBatch    = Prefix + "ROUTER_PUBLISH_BATCH"
	EnvOutboxKeep      = Prefix + "ROUTER_OUTBOX_KEEP"
	EnvPruneInterval   = Prefix + "ROUTER_PRUNE_INTERVAL"
	EnvCountsInterval  = Prefix + "ROUTER_COUNTS_INTERVAL"
	EnvOutageLimit     = Prefix + "ROUTER_OUTAGE_LIMIT"
	EnvOperatorRefresh = Prefix + "ROUTER_OPERATOR_REFRESH"
)

// RemovedVars are the variables of the team numbers that the router read
// before decision 0014 moved them into the stand settings. Load does not
// read them; WarnRemoved logs a warning for each one that is still set.
var RemovedVars = []string{
	Prefix + "ROUTER_TEAM_FORM_LOAD",
	Prefix + "ROUTER_TEAM_RELEASE_LOAD",
	Prefix + "ROUTER_TEAM_RELEASE_AFTER",
	Prefix + "ROUTER_TEAM_MIN_SIZE",
	Prefix + "ROUTER_TEAM_MAX_SIZE",
	Prefix + "ROUTER_TEAM_MAX_SHARE",
}

// WarnRemoved logs one warning for each variable of RemovedVars that is
// set to a value that is not empty. The value is not read.
func WarnRemoved(lookup LookupFunc, logger *slog.Logger) {
	for _, key := range RemovedVars {
		if v, ok := lookup(key); ok && strings.TrimSpace(v) != "" {
			logger.Warn("environment variable is no longer read: the value now lives in the stand settings; "+
				"change it in the console or with PUT /v1/settings of the operator API", "variable", key)
		}
	}
}

// Defaults that are not routing rules. The routing rules come from
// router.DefaultConfig, which takes them from internal/defaults and the team
// numbers from the stand settings defaults of internal/opapi.
const (
	DefaultKafkaBrokers = "localhost:9092"
	DefaultLogLevel     = slog.LevelInfo
	DefaultGroupID      = "threadroute-router"
	// DefaultMetricsAddr differs from the other services, so all can run
	// on one host.
	DefaultMetricsAddr = ":9468"
	// DefaultAPIAddr binds to localhost only: the operator API has no
	// authentication in stage 3 (decision 0010).
	DefaultAPIAddr = "127.0.0.1:9470"
	// DefaultInputTopicWait is how long the router waits at start for conv
	// to create the conversations topic.
	DefaultInputTopicWait = 2 * time.Minute

	// DefaultAssignWorkers is the number of assignment workers.
	DefaultAssignWorkers = 4
	// DefaultAssignInterval is how often an idle assignment worker looks
	// at the queues, in machine time. A worker also wakes up at once when
	// something that can make an offer possible happens in this process.
	DefaultAssignInterval = 250 * time.Millisecond
	// DefaultExpireInterval, DefaultSLAInterval and DefaultTeamInterval
	// are model time. The loops divide them by the speed factor, because
	// they drive model-time timers.
	DefaultExpireInterval = time.Second
	DefaultSLAInterval    = time.Second
	DefaultTeamInterval   = 10 * time.Second
	// DefaultPublishInterval is how often an idle publisher looks at the
	// outbox, in machine time.
	DefaultPublishInterval = 100 * time.Millisecond
	// DefaultPublishBatch is the most outbox rows sent in one round.
	DefaultPublishBatch = 500
	// DefaultOutboxKeep is how long a sent outbox row is kept, in machine
	// time.
	DefaultOutboxKeep = time.Hour
	// DefaultPruneInterval is how often sent rows are pruned.
	DefaultPruneInterval = time.Minute
	// DefaultCountsInterval is how often the state gauges are sampled.
	DefaultCountsInterval = 5 * time.Second
	// DefaultOutageLimit is how long a loop may fail without a break
	// before the process stops with an error.
	DefaultOutageLimit = 2 * time.Minute
	// DefaultOperatorRefresh is how long after its last operator event an
	// operator snapshot is published again, in machine time (decision
	// 0011).
	DefaultOperatorRefresh = defaults.OperatorRefresh
)

// Refresh of the operator snapshots.
const (
	// refreshBatch is the most operators one refresh transaction takes.
	refreshBatch = 100
	// maxRefreshCheck is the longest pause of the refresh loop. The loop
	// looks for due operators OperatorRefresh/60 apart, at most this long,
	// so a snapshot is late by at most a small part of the interval.
	maxRefreshCheck = time.Minute
)

// Limits of the numeric settings.
const (
	maxAssignWorkers = 64
	maxPublishBatch  = 10_000
)

// Config is the configuration of the router service.
type Config struct {
	// KafkaBrokers are the seed brokers, as host:port.
	KafkaBrokers []string
	// GroupID is the consumer group on the conversations topic.
	GroupID string
	// Postgres is the database connection. Its DSN must never be logged.
	Postgres pg.Config
	// Router holds the routing rules of decisions 0009 and 0010. Its team
	// numbers are the defaults of the code; the stored stand settings
	// replace them at start (decision 0014).
	Router router.Config
	// MetricsAddr is the listen address of the metrics, pprof and health
	// endpoint.
	MetricsAddr string
	// APIAddr is the listen address of the operator API.
	APIAddr string
	// InputTopicWait bounds the wait for the conversations topic at start.
	InputTopicWait time.Duration

	// AssignWorkers is the number of assignment workers.
	AssignWorkers int
	// AssignInterval and PublishInterval are machine time.
	AssignInterval time.Duration
	// ExpireInterval, SLAInterval and TeamInterval are model time.
	ExpireInterval time.Duration
	SLAInterval    time.Duration
	TeamInterval   time.Duration
	// PublishInterval is how often an idle publisher looks at the outbox.
	PublishInterval time.Duration
	// PublishBatch is the most outbox rows sent in one round.
	PublishBatch int
	// OutboxKeep is how long a sent row is kept before it is pruned.
	OutboxKeep time.Duration
	// PruneInterval is how often sent rows are pruned.
	PruneInterval time.Duration
	// CountsInterval is how often the state gauges are sampled.
	CountsInterval time.Duration
	// OutageLimit ends the process when a loop has failed for this long.
	OutageLimit time.Duration
	// OperatorRefresh is how long after its last operator event the
	// snapshot of an operator is published again, in machine time.
	OperatorRefresh time.Duration

	// LogLevel is the minimum level for log/slog.
	LogLevel slog.Level
}

// LookupFunc returns the value of an environment variable and whether it is
// set. os.LookupEnv has this signature.
type LookupFunc func(key string) (string, bool)

// Load reads and validates the configuration. A variable that is unset or
// empty takes its default. The error names every invalid variable and never
// contains the DSN.
func Load(lookup LookupFunc) (Config, error) {
	r := reader{lookup: lookup}
	d := router.DefaultConfig()
	pd := pg.DefaultConfig()
	cfg := Config{
		KafkaBrokers:   r.brokers(EnvKafkaBrokers, DefaultKafkaBrokers),
		GroupID:        r.groupID(EnvGroupID, DefaultGroupID),
		MetricsAddr:    r.hostPort(EnvMetricsAddr, DefaultMetricsAddr),
		APIAddr:        r.hostPort(EnvAPIAddr, DefaultAPIAddr),
		InputTopicWait: r.duration(EnvInputTopicWait, DefaultInputTopicWait),
		Postgres: pg.Config{
			DSN:         r.dsn(pg.EnvDSN, pd.DSN),
			MaxConns:    int32(r.intBetween(pg.EnvMaxConns, int(pd.MaxConns), 1, 1000)),
			ConnectWait: r.duration(pg.EnvConnectWait, pd.ConnectWait),
		},
		Router: router.Config{
			SpeedFactor:         r.speedFactor(EnvSpeedFactor, d.SpeedFactor),
			Capacity:            r.intBetween(EnvOperatorCapacity, d.Capacity, 1, opapi.MaxCapacity),
			AcceptTimeout:       r.duration(EnvAcceptTimeout, d.AcceptTimeout),
			AssignmentTarget:    r.duration(EnvAssignmentTarget, d.AssignmentTarget),
			FirstResponseTarget: r.duration(EnvFirstResponseTarget, d.FirstResponseTarget),
			TeamFormLoad:        d.TeamFormLoad,
			TeamReleaseLoad:     d.TeamReleaseLoad,
			TeamReleaseAfter:    d.TeamReleaseAfter,
			TeamMinSize:         d.TeamMinSize,
			TeamMaxSize:         d.TeamMaxSize,
			TeamMaxShare:        d.TeamMaxShare,
		},
		AssignWorkers:   r.intBetween(EnvAssignWorkers, DefaultAssignWorkers, 1, maxAssignWorkers),
		AssignInterval:  r.duration(EnvAssignInterval, DefaultAssignInterval),
		ExpireInterval:  r.duration(EnvExpireInterval, DefaultExpireInterval),
		SLAInterval:     r.duration(EnvSLAInterval, DefaultSLAInterval),
		TeamInterval:    r.duration(EnvTeamInterval, DefaultTeamInterval),
		PublishInterval: r.duration(EnvPublishInterval, DefaultPublishInterval),
		PublishBatch:    r.intBetween(EnvPublishBatch, DefaultPublishBatch, 1, maxPublishBatch),
		OutboxKeep:      r.duration(EnvOutboxKeep, DefaultOutboxKeep),
		PruneInterval:   r.duration(EnvPruneInterval, DefaultPruneInterval),
		CountsInterval:  r.duration(EnvCountsInterval, DefaultCountsInterval),
		OutageLimit:     r.duration(EnvOutageLimit, DefaultOutageLimit),
		OperatorRefresh: r.duration(EnvOperatorRefresh, DefaultOperatorRefresh),
		LogLevel:        r.logLevel(EnvLogLevel, DefaultLogLevel),
	}
	if err := errors.Join(r.errs...); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	// Anything the checks above missed.
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}

// Validate reports every invalid field. Load already checks each variable;
// this also covers a Config built in code, for example in tests.
func (c Config) Validate() error {
	var errs []error
	if len(c.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("no kafka brokers"))
	}
	if c.GroupID == "" {
		errs = append(errs, errors.New("group ID is empty"))
	}
	if err := c.Postgres.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("postgres: %w", err))
	}
	if err := c.Router.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("router: %w", err))
	}
	if c.AssignWorkers < 1 || c.AssignWorkers > maxAssignWorkers {
		errs = append(errs, fmt.Errorf("assign workers %d is not between 1 and %d", c.AssignWorkers, maxAssignWorkers))
	}
	if c.PublishBatch < 1 || c.PublishBatch > maxPublishBatch {
		errs = append(errs, fmt.Errorf("publish batch %d is not between 1 and %d", c.PublishBatch, maxPublishBatch))
	}
	for _, d := range []struct {
		name string
		v    time.Duration
	}{
		{"input topic wait", c.InputTopicWait},
		{"assign interval", c.AssignInterval},
		{"expire interval", c.ExpireInterval},
		{"sla interval", c.SLAInterval},
		{"team interval", c.TeamInterval},
		{"publish interval", c.PublishInterval},
		{"outbox keep", c.OutboxKeep},
		{"prune interval", c.PruneInterval},
		{"counts interval", c.CountsInterval},
		{"outage limit", c.OutageLimit},
		{"operator refresh", c.OperatorRefresh},
	} {
		if d.v <= 0 {
			errs = append(errs, fmt.Errorf("%s %s is not positive", d.name, d.v))
		}
	}
	return errors.Join(errs...)
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

func (r *reader) fail(key string, err error) {
	r.errs = append(r.errs, fmt.Errorf("%s: %w", key, err))
}

func (r *reader) brokers(key, def string) []string {
	v, ok := r.get(key)
	if !ok {
		v = def
	}
	var out []string
	for part := range strings.SplitSeq(v, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if err := checkHostPort(part); err != nil {
			r.fail(key, fmt.Errorf("broker %q: %w", part, err))
			return nil
		}
		out = append(out, part)
	}
	if len(out) == 0 {
		r.fail(key, errors.New("no brokers"))
	}
	return out
}

func (r *reader) groupID(key, def string) string {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	if strings.ContainsAny(v, " \t\r\n,") {
		r.fail(key, fmt.Errorf("%q must not contain spaces or commas", v))
		return ""
	}
	return v
}

// dsn returns the connection string. The value is never put into an
// error, because it can hold a password.
func (r *reader) dsn(key, def string) string {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	if err := (pg.Config{DSN: v, MaxConns: 1, ConnectWait: time.Second}).Validate(); err != nil {
		r.fail(key, errors.New("cannot be parsed as a connection string"))
		return ""
	}
	return v
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

func (r *reader) intBetween(key string, def, least, most int) int {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.fail(key, fmt.Errorf("%q is not a whole number", v))
		return 0
	}
	if n < least || n > most {
		r.fail(key, fmt.Errorf("%d is not between %d and %d", n, least, most))
		return 0
	}
	return n
}

// speedFactor reads the factor of decision 0009, "Clock": a positive number
// up to the router maximum.
func (r *reader) speedFactor(key string, def float64) float64 {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		r.fail(key, fmt.Errorf("%q is not a number", v))
		return 0
	}
	probe := router.DefaultConfig()
	probe.SpeedFactor = f
	if err := probe.Validate(); err != nil {
		r.fail(key, err)
		return 0
	}
	return f
}

// duration reads a positive duration in whole microseconds, the unit of
// the routing events and the database.
func (r *reader) duration(key string, def time.Duration) time.Duration {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.fail(key, fmt.Errorf("%q is not a duration such as 500ms, 20s or 15m", v))
		return 0
	}
	if d <= 0 {
		r.fail(key, fmt.Errorf("%s is not positive", d))
		return 0
	}
	if d%time.Microsecond != 0 {
		r.fail(key, fmt.Errorf("%s is not a whole number of microseconds", d))
		return 0
	}
	return d
}

func checkHostPort(v string) error {
	_, port, err := net.SplitHostPort(v)
	if err != nil {
		return fmt.Errorf("%q is not host:port: %w", v, err)
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return fmt.Errorf("%q has an invalid port", v)
	}
	return nil
}

// isLoopback reports whether a listen address binds to the loopback
// interface only.
func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
