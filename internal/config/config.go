// Package config reads the ingest service configuration from environment
// variables and validates it at start (decision 0005).
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
)

// Prefix is shared by every variable.
const Prefix = "THREADROUTE_"

// Variable names.
const (
	EnvKafkaBrokers               = Prefix + "KAFKA_BROKERS"
	EnvSourcePrimaryHost          = Prefix + "SOURCE_PRIMARY_HOST"
	EnvSourceFailoverHost         = Prefix + "SOURCE_FAILOVER_HOST"
	EnvSourceCollection           = Prefix + "SOURCE_COLLECTION"
	EnvMetricsAddr                = Prefix + "METRICS_ADDR"
	EnvLogLevel                   = Prefix + "LOG_LEVEL"
	EnvProducerMaxBufferedRecords = Prefix + "PRODUCER_MAX_BUFFERED_RECORDS"
	EnvProducerMaxBufferedBytes   = Prefix + "PRODUCER_MAX_BUFFERED_BYTES"
	EnvProducerDeliveryTimeout    = Prefix + "PRODUCER_DELIVERY_TIMEOUT"
	EnvRawRetention               = Prefix + "RAW_RETENTION"
	EnvRawMaxBytes                = Prefix + "RAW_MAX_BYTES"
)

// Defaults. The producer limits are small on purpose: the first server has
// 4 GB of RAM shared with Kafka and other services.
const (
	DefaultKafkaBrokers               = "localhost:9092"
	DefaultSourcePrimaryHost          = "jetstream.us-east.bsky.network"
	DefaultSourceFailoverHost         = "jetstream.us-west.bsky.network"
	DefaultSourceCollection           = "app.bsky.feed.post"
	DefaultMetricsAddr                = ":9464"
	DefaultLogLevel                   = slog.LevelInfo
	DefaultProducerMaxBufferedRecords = 10_000
	DefaultProducerMaxBufferedBytes   = 32 << 20 // 32 MiB
	DefaultProducerDeliveryTimeout    = 2 * time.Minute
)

// Config is the ingest service configuration.
type Config struct {
	// KafkaBrokers are the seed brokers, as host:port.
	KafkaBrokers []string
	// SourcePrimaryHost and SourceFailoverHost are the Jetstream hosts
	// (decision 0003).
	SourcePrimaryHost  string
	SourceFailoverHost string
	// SourceCollection is the only collection ingest subscribes to.
	SourceCollection string
	// MetricsAddr is the listen address of the metrics and pprof endpoint.
	MetricsAddr string
	// LogLevel is the minimum level for log/slog.
	LogLevel slog.Level
	// Producer limits that decision 0001 says are set explicitly.
	ProducerMaxBufferedRecords int
	ProducerMaxBufferedBytes   int
	ProducerDeliveryTimeout    time.Duration
	// RawLimits are the retention and the total size limit of the raw topic.
	RawLimits bus.RawLimits
}

// LookupFunc returns the value of an environment variable and whether it is
// set. os.LookupEnv has this signature.
type LookupFunc func(key string) (string, bool)

// Load reads and validates the configuration. A variable that is unset or
// empty takes its default. The error names every invalid variable.
func Load(lookup LookupFunc) (Config, error) {
	r := reader{lookup: lookup}
	defaults := bus.DefaultRawLimits()

	cfg := Config{
		KafkaBrokers:               r.brokers(EnvKafkaBrokers, DefaultKafkaBrokers),
		SourcePrimaryHost:          r.host(EnvSourcePrimaryHost, DefaultSourcePrimaryHost),
		SourceFailoverHost:         r.host(EnvSourceFailoverHost, DefaultSourceFailoverHost),
		SourceCollection:           r.collection(EnvSourceCollection, DefaultSourceCollection),
		MetricsAddr:                r.listenAddr(EnvMetricsAddr, DefaultMetricsAddr),
		LogLevel:                   r.logLevel(EnvLogLevel, DefaultLogLevel),
		ProducerMaxBufferedRecords: int(r.positiveInt(EnvProducerMaxBufferedRecords, DefaultProducerMaxBufferedRecords)),
		ProducerMaxBufferedBytes:   int(r.positiveInt(EnvProducerMaxBufferedBytes, DefaultProducerMaxBufferedBytes)),
		ProducerDeliveryTimeout:    r.positiveDuration(EnvProducerDeliveryTimeout, DefaultProducerDeliveryTimeout),
		RawLimits: bus.RawLimits{
			Retention: r.positiveDuration(EnvRawRetention, defaults.Retention),
			MaxBytes:  r.positiveInt(EnvRawMaxBytes, defaults.MaxBytes),
		},
	}

	if cfg.SourcePrimaryHost != "" && strings.EqualFold(cfg.SourcePrimaryHost, cfg.SourceFailoverHost) {
		r.fail(EnvSourceFailoverHost, errors.New("must differ from "+EnvSourcePrimaryHost))
	}
	// The same rules as bus.RawLimits.Validate, checked per variable so the
	// error names the right one.
	if d := cfg.RawLimits.Retention; d > 0 {
		if d < bus.RawSegmentInterval {
			r.fail(EnvRawRetention, fmt.Errorf("%s is shorter than the segment interval %s", d, bus.RawSegmentInterval))
		} else if d%time.Millisecond != 0 {
			r.fail(EnvRawRetention, fmt.Errorf("%s is not a whole number of milliseconds", d))
		}
	}
	if n := cfg.RawLimits.MaxBytes; n > 0 && n < bus.RawMinMaxBytes {
		r.fail(EnvRawMaxBytes, fmt.Errorf("%d is less than %d: each of the %d partitions needs room for two segments of at least %d bytes", n, bus.RawMinMaxBytes, bus.PartitionsRaw, bus.RawSegmentBytesMin))
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

func (r *reader) host(key, def string) string {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	if strings.ContainsAny(v, " \t\r\n,") {
		r.fail(key, fmt.Errorf("%q is not a single host", v))
		return ""
	}
	return v
}

func (r *reader) collection(key, def string) string {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	// An NSID has at least three dot-separated parts, for example
	// app.bsky.feed.post.
	parts := strings.Split(v, ".")
	if len(parts) < 3 || strings.ContainsAny(v, " \t*:,") {
		r.fail(key, fmt.Errorf("%q is not a collection NSID", v))
		return ""
	}
	for _, p := range parts {
		if p == "" {
			r.fail(key, fmt.Errorf("%q is not a collection NSID", v))
			return ""
		}
	}
	return v
}

func (r *reader) listenAddr(key, def string) string {
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

func (r *reader) positiveInt(key string, def int64) int64 {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		r.fail(key, fmt.Errorf("%q is not a whole number", v))
		return 0
	}
	if n <= 0 {
		r.fail(key, fmt.Errorf("%d is not positive", n))
		return 0
	}
	if int64(int(n)) != n {
		r.fail(key, fmt.Errorf("%d is too large", n))
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
		r.fail(key, fmt.Errorf("%q is not a duration such as 30s or 24h", v))
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
