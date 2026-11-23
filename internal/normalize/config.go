package normalize

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
	"github.com/ZAUakaAlexey/threadroute/internal/defaults"
)

// Prefix is shared by every variable of the project.
const Prefix = "THREADROUTE_"

// Variable names. KAFKA_BROKERS and LOG_LEVEL have the same names as in the
// ingest service. The REDIS_* names are not specific to normalize, because
// the services share one Redis.
const (
	EnvKafkaBrokers       = Prefix + "KAFKA_BROKERS"
	EnvLogLevel           = Prefix + "LOG_LEVEL"
	EnvRedisAddr          = Prefix + "REDIS_ADDR"
	EnvRedisPassword      = Prefix + "REDIS_PASSWORD"
	EnvRedisDB            = Prefix + "REDIS_DB"
	EnvGroupID            = Prefix + "NORMALIZE_GROUP_ID"
	EnvTransactionalID    = Prefix + "NORMALIZE_TRANSACTIONAL_ID"
	EnvStart              = Prefix + "NORMALIZE_START"
	EnvMaxBatchRecords    = Prefix + "NORMALIZE_MAX_BATCH_RECORDS"
	EnvDedupKeyLifetime   = Prefix + "NORMALIZE_DEDUP_KEY_LIFETIME"
	EnvRawTopicWait       = Prefix + "NORMALIZE_RAW_TOPIC_WAIT"
	EnvMetricsAddr        = Prefix + "NORMALIZE_METRICS_ADDR"
	EnvMaxBufferedRecords = Prefix + "NORMALIZE_PRODUCER_MAX_BUFFERED_RECORDS"
	EnvMaxBufferedBytes   = Prefix + "NORMALIZE_PRODUCER_MAX_BUFFERED_BYTES"
	EnvDeliveryTimeout    = Prefix + "NORMALIZE_PRODUCER_DELIVERY_TIMEOUT"
)

// Defaults.
const (
	DefaultKafkaBrokers = "localhost:9092"
	DefaultLogLevel     = slog.LevelInfo
	DefaultRedisAddr    = "localhost:6379"
	DefaultRedisDB      = 0
	DefaultGroupID      = "threadroute-normalize"
	// DefaultTransactionalID is fine for one replica only, see
	// Config.TransactionalID.
	DefaultTransactionalID = "threadroute-normalize"
	DefaultStart           = StartBeginning
	// DefaultMaxBatchRecords bounds the raw records of one transaction.
	DefaultMaxBatchRecords = 1000
	// DefaultRawTopicWait bounds the wait for the raw topic at start.
	DefaultRawTopicWait = 2 * time.Minute
	// DefaultMetricsAddr differs from ingest (:9464) and record (:9465), so
	// all three can run on one host.
	DefaultMetricsAddr        = ":9466"
	DefaultMaxBufferedRecords = 10_000
	DefaultMaxBufferedBytes   = 32 << 20
	DefaultDeliveryTimeout    = 2 * time.Minute
)

// StartPosition is where a new consumer group starts reading the raw topic.
// A group that has committed offsets always continues from them.
type StartPosition string

// Start positions.
const (
	// StartBeginning reads every record the raw topic still holds, so
	// nothing written before the first start of normalize is lost.
	StartBeginning StartPosition = "beginning"
	// StartEnd reads only records produced after the first start.
	StartEnd StartPosition = "end"
)

// Config is the configuration of the normalize service.
type Config struct {
	// KafkaBrokers are the seed brokers, as host:port.
	KafkaBrokers []string
	// GroupID is the consumer group on the raw topic.
	GroupID string
	// TransactionalID is the Kafka transactional ID. It must be unique for
	// each running replica: Kafka fences a producer when another one starts
	// with the same ID. The service runs as one replica, so the default is
	// enough. Running more than one replica needs a different ID per
	// replica, for example with the replica number appended.
	TransactionalID string
	// Start is where a new group starts reading.
	Start StartPosition
	// MaxBatchRecords is the most raw records one transaction handles. It
	// must not be larger than Producer.MaxBufferedRecords, because every
	// record of a batch can give one output record.
	MaxBatchRecords int
	// RawTopicWait bounds the wait for the raw topic at start.
	RawTopicWait time.Duration
	// Producer holds the producer buffer limits and delivery timeout.
	Producer bus.ProducerLimits
	// RedisAddr is the Redis address, as host:port.
	RedisAddr string
	// RedisPassword is the Redis password. It is empty by default and is
	// never logged.
	RedisPassword string
	// RedisDB is the Redis database number.
	RedisDB int
	// DedupKeyLifetime is the lifetime of a deduplication key (decision
	// 0003).
	DedupKeyLifetime time.Duration
	// LogLevel is the minimum level for log/slog.
	LogLevel slog.Level
	// MetricsAddr is the listen address of the metrics, pprof and health
	// endpoint.
	MetricsAddr string
}

// LookupFunc returns the value of an environment variable and whether it is
// set. os.LookupEnv has this signature.
type LookupFunc func(key string) (string, bool)

// Load reads and validates the configuration. A variable that is unset or
// empty takes its default. The error names every invalid variable.
func Load(lookup LookupFunc) (Config, error) {
	r := reader{lookup: lookup}
	cfg := Config{
		KafkaBrokers:    r.brokers(EnvKafkaBrokers, DefaultKafkaBrokers),
		GroupID:         r.name(EnvGroupID, DefaultGroupID),
		TransactionalID: r.name(EnvTransactionalID, DefaultTransactionalID),
		Start:           r.start(EnvStart, DefaultStart),
		MaxBatchRecords: int(r.positiveInt(EnvMaxBatchRecords, DefaultMaxBatchRecords)),
		RawTopicWait:    r.positiveDuration(EnvRawTopicWait, DefaultRawTopicWait),
		Producer: bus.ProducerLimits{
			MaxBufferedRecords: int(r.positiveInt(EnvMaxBufferedRecords, DefaultMaxBufferedRecords)),
			MaxBufferedBytes:   int(r.positiveInt(EnvMaxBufferedBytes, DefaultMaxBufferedBytes)),
			DeliveryTimeout:    r.positiveDuration(EnvDeliveryTimeout, DefaultDeliveryTimeout),
		},
		RedisAddr:        r.hostPort(EnvRedisAddr, DefaultRedisAddr),
		RedisPassword:    r.raw(EnvRedisPassword),
		RedisDB:          r.redisDB(EnvRedisDB, DefaultRedisDB),
		DedupKeyLifetime: r.positiveDuration(EnvDedupKeyLifetime, defaults.DedupKeyLifetime),
		LogLevel:         r.logLevel(EnvLogLevel, DefaultLogLevel),
		MetricsAddr:      r.hostPort(EnvMetricsAddr, DefaultMetricsAddr),
	}
	if d := cfg.Producer.DeliveryTimeout; d > 0 && d < time.Second {
		r.fail(EnvDeliveryTimeout, fmt.Errorf("%s is less than 1s", d))
	}
	if d := cfg.DedupKeyLifetime; d > 0 && d < time.Second {
		r.fail(EnvDedupKeyLifetime, fmt.Errorf("%s is less than 1s", d))
	}
	if b, m := cfg.MaxBatchRecords, cfg.Producer.MaxBufferedRecords; b > 0 && m > 0 && b > m {
		r.fail(EnvMaxBatchRecords, fmt.Errorf("%d is larger than %s (%d): one batch must fit in the producer buffer", b, EnvMaxBufferedRecords, m))
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

// raw returns the value without trimming, for a password.
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

// name reads a group or transactional ID.
func (r *reader) name(key, def string) string {
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

func (r *reader) start(key string, def StartPosition) StartPosition {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	switch p := StartPosition(strings.ToLower(v)); p {
	case StartBeginning, StartEnd:
		return p
	default:
		r.fail(key, fmt.Errorf("%q is not a start position (%s, %s)", v, StartBeginning, StartEnd))
		return ""
	}
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

func (r *reader) redisDB(key string, def int) int {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.fail(key, fmt.Errorf("%q is not a whole number", v))
		return 0
	}
	if n < 0 {
		r.fail(key, fmt.Errorf("%d is negative", n))
		return 0
	}
	return n
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

func (r *reader) positiveInt(key string, def int64) int64 {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		r.fail(key, fmt.Errorf("%q is not a whole number up to %d", v, int32(^uint32(0)>>1)))
		return 0
	}
	if n <= 0 {
		r.fail(key, fmt.Errorf("%d is not positive", n))
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
		r.fail(key, fmt.Errorf("%q is not a duration such as 30s or 10m", v))
		return 0
	}
	if d <= 0 {
		r.fail(key, fmt.Errorf("%s is not positive", d))
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
