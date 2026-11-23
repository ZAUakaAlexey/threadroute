package conv

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"
)

// Prefix is shared by every variable of the project.
const Prefix = "THREADROUTE_"

// Variable names. KAFKA_BROKERS and LOG_LEVEL have the same names as in the
// other services. The REDIS_ variables have no CONV_ part, so services that
// share one Redis can share them.
const (
	EnvKafkaBrokers       = Prefix + "KAFKA_BROKERS"
	EnvLogLevel           = Prefix + "LOG_LEVEL"
	EnvRedisAddr          = Prefix + "REDIS_ADDR"
	EnvRedisPassword      = Prefix + "REDIS_PASSWORD"
	EnvRedisDB            = Prefix + "REDIS_DB"
	EnvGroupID            = Prefix + "CONV_GROUP_ID"
	EnvMetricsAddr        = Prefix + "CONV_METRICS_ADDR"
	EnvSweepInterval      = Prefix + "CONV_SWEEP_INTERVAL"
	EnvInputTopicWait     = Prefix + "CONV_INPUT_TOPIC_WAIT"
	EnvAdmissionThreshold = Prefix + "CONV_ADMISSION_THRESHOLD"
	EnvAdmissionWindow    = Prefix + "CONV_ADMISSION_WINDOW"
	EnvAdmissionIdle      = Prefix + "CONV_ADMISSION_IDLE"
	EnvCompanyLimit       = Prefix + "CONV_COMPANY_LIMIT"
	EnvInactivityTimeout  = Prefix + "CONV_INACTIVITY_TIMEOUT"
	EnvTextKeptAfterClose = Prefix + "CONV_TEXT_KEPT_AFTER_CLOSE"
	EnvMessagesKept       = Prefix + "CONV_MESSAGES_KEPT"
	EnvSessionRecordKept  = Prefix + "CONV_SESSION_RECORD_KEPT"
	EnvEarlyDeletionKept  = Prefix + "CONV_EARLY_DELETION_KEPT"
	EnvAppliedLifetime    = Prefix + "CONV_APPLIED_LIFETIME"
	EnvKeyMargin          = Prefix + "CONV_KEY_MARGIN"
)

// Defaults that are not rules.
const (
	DefaultKafkaBrokers = "localhost:9092"
	DefaultLogLevel     = slog.LevelInfo
	DefaultRedisAddr    = "localhost:6379"
	DefaultRedisDB      = 0
	DefaultGroupID      = "threadroute-conv"
	// DefaultMetricsAddr differs from the other services, so all can run
	// on one host.
	DefaultMetricsAddr = ":9467"
	// DefaultInputTopicWait is how long conv waits at start for normalize
	// to create the input topics.
	DefaultInputTopicWait = 2 * time.Minute
)

// Config is the configuration of the conv service.
type Config struct {
	// KafkaBrokers are the seed brokers, as host:port.
	KafkaBrokers []string
	// GroupID is the consumer group of conv.
	GroupID string
	// RedisAddr is host:port. RedisPassword is empty when Redis has none.
	// It must never be logged.
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	// Rules are the lifecycle and admission numbers.
	Rules Rules
	// SweepInterval is how often the sweeper runs.
	SweepInterval time.Duration
	// InputTopicWait bounds the wait for the input topics at start.
	InputTopicWait time.Duration
	// MetricsAddr is the listen address of the metrics, pprof and health
	// endpoint.
	MetricsAddr string
	// LogLevel is the minimum level for log/slog.
	LogLevel slog.Level
}

// LookupFunc returns the value of an environment variable and whether it is
// set. os.LookupEnv has this signature.
type LookupFunc func(key string) (string, bool)

// Load reads and validates the configuration. A variable that is unset or
// empty takes its default. The error names every invalid variable.
func Load(lookup LookupFunc) (Config, error) {
	r := reader{lookup: lookup}
	d := DefaultRules()
	cfg := Config{
		KafkaBrokers:   r.brokers(EnvKafkaBrokers, DefaultKafkaBrokers),
		GroupID:        r.groupID(EnvGroupID, DefaultGroupID),
		RedisAddr:      r.hostPort(EnvRedisAddr, DefaultRedisAddr),
		RedisPassword:  r.raw(EnvRedisPassword),
		RedisDB:        r.intAtLeast(EnvRedisDB, DefaultRedisDB, 0),
		SweepInterval:  r.positiveDuration(EnvSweepInterval, DefaultSweepInterval),
		InputTopicWait: r.positiveDuration(EnvInputTopicWait, DefaultInputTopicWait),
		MetricsAddr:    r.hostPort(EnvMetricsAddr, DefaultMetricsAddr),
		LogLevel:       r.logLevel(EnvLogLevel, DefaultLogLevel),
		Rules: Rules{
			AdmissionThreshold: r.intAtLeast(EnvAdmissionThreshold, d.AdmissionThreshold, 1),
			AdmissionWindow:    r.positiveDuration(EnvAdmissionWindow, d.AdmissionWindow),
			AdmissionIdle:      r.positiveDuration(EnvAdmissionIdle, d.AdmissionIdle),
			CompanyLimit:       r.intAtLeast(EnvCompanyLimit, d.CompanyLimit, 0),
			InactivityTimeout:  r.positiveDuration(EnvInactivityTimeout, d.InactivityTimeout),
			TextKeptAfterClose: r.positiveDuration(EnvTextKeptAfterClose, d.TextKeptAfterClose),
			MessagesKept:       r.intAtLeast(EnvMessagesKept, d.MessagesKept, 1),
			SessionRecordKept:  r.positiveDuration(EnvSessionRecordKept, d.SessionRecordKept),
			EarlyDeletionKept:  r.positiveDuration(EnvEarlyDeletionKept, d.EarlyDeletionKept),
			AppliedLifetime:    r.positiveDuration(EnvAppliedLifetime, d.AppliedLifetime),
			KeyMargin:          r.positiveDuration(EnvKeyMargin, d.KeyMargin),
		},
	}
	// Decision 0008 relies on this: when an admission ends, every
	// conversation of the company has already closed.
	if rl := cfg.Rules; rl.AdmissionIdle > 0 && rl.InactivityTimeout > 0 && rl.AdmissionIdle <= rl.InactivityTimeout {
		r.fail(EnvAdmissionIdle, fmt.Errorf("%s is not longer than %s (%s)", rl.AdmissionIdle, EnvInactivityTimeout, rl.InactivityTimeout))
	}
	if err := errors.Join(r.errs...); err != nil {
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

// raw returns the value as it is, without trimming. Used for secrets.
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

func (r *reader) intAtLeast(key string, def, least int) int {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.fail(key, fmt.Errorf("%q is not a whole number", v))
		return 0
	}
	if n < least {
		r.fail(key, fmt.Errorf("%d is less than %d", n, least))
		return 0
	}
	return n
}

func (r *reader) positiveDuration(key string, def time.Duration) time.Duration {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.fail(key, fmt.Errorf("%q is not a duration such as 30m or 2h", v))
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
