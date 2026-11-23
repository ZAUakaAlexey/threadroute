package record

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Prefix is shared by every variable of the project.
const Prefix = "THREADROUTE_"

// Variable names. KAFKA_BROKERS and LOG_LEVEL have the same names as in the
// ingest service.
const (
	EnvKafkaBrokers         = Prefix + "KAFKA_BROKERS"
	EnvLogLevel             = Prefix + "LOG_LEVEL"
	EnvRecordGroupID        = Prefix + "RECORD_GROUP_ID"
	EnvRecordStart          = Prefix + "RECORD_START"
	EnvRecordDir            = Prefix + "RECORD_DIR"
	EnvRecordSegmentBytes   = Prefix + "RECORD_SEGMENT_MAX_BYTES"
	EnvRecordSegmentAge     = Prefix + "RECORD_SEGMENT_MAX_AGE"
	EnvRecordRetention      = Prefix + "RECORD_RETENTION"
	EnvRecordMaxTotalBytes  = Prefix + "RECORD_MAX_TOTAL_BYTES"
	envHome                 = "HOME"
	defaultRecordingsSubdir = "threadroute-recordings"
)

// Defaults that are not Writer defaults.
const (
	DefaultKafkaBrokers = "localhost:9092"
	DefaultLogLevel     = slog.LevelInfo
	DefaultGroupID      = "threadroute-record"
	DefaultStart        = StartEnd
)

// StartPosition is where a new consumer group starts reading. A group that
// has committed offsets always continues from them.
type StartPosition string

// Start positions.
const (
	// StartEnd reads only records produced after the first start.
	StartEnd StartPosition = "end"
	// StartBeginning reads every record the topic still holds.
	StartBeginning StartPosition = "beginning"
)

// Config is the configuration of the record tool.
type Config struct {
	// KafkaBrokers are the seed brokers, as host:port.
	KafkaBrokers []string
	// GroupID is the consumer group of the recorder.
	GroupID string
	// Start is where a new group starts reading.
	Start StartPosition
	// Writer holds the output directory, rotation and retention. Now and
	// CheckInterval are not read from the environment.
	Writer WriterOptions
	// LogLevel is the minimum level for log/slog.
	LogLevel slog.Level
}

// LookupFunc returns the value of an environment variable and whether it is
// set. os.LookupEnv has this signature.
type LookupFunc func(key string) (string, bool)

// Load reads and validates the configuration. A variable that is unset or
// empty takes its default. The default output directory is
// $HOME/threadroute-recordings, read through lookup. The error names every
// invalid variable.
//
// Load does not touch the file system. The git working tree check runs in
// OpenWriter.
func Load(lookup LookupFunc) (Config, error) {
	r := reader{lookup: lookup}
	cfg := Config{
		KafkaBrokers: r.brokers(EnvKafkaBrokers, DefaultKafkaBrokers),
		GroupID:      r.groupID(EnvRecordGroupID, DefaultGroupID),
		Start:        r.start(EnvRecordStart, DefaultStart),
		Writer: WriterOptions{
			Dir:             r.dir(EnvRecordDir),
			SegmentMaxBytes: r.positiveInt(EnvRecordSegmentBytes, DefaultSegmentMaxBytes),
			SegmentMaxAge:   r.positiveDuration(EnvRecordSegmentAge, DefaultSegmentMaxAge),
			Retention:       r.positiveDuration(EnvRecordRetention, DefaultRetention),
			MaxTotalBytes:   r.positiveInt(EnvRecordMaxTotalBytes, 0),
		},
		LogLevel: r.logLevel(EnvLogLevel, DefaultLogLevel),
	}
	// Decision 0004: a recording is deleted 14 days after it was made. A
	// shorter retention is allowed, a longer one is not.
	if d := cfg.Writer.Retention; d > DefaultRetention {
		r.fail(EnvRecordRetention, fmt.Errorf("%s is longer than %s, the limit from decision 0004", d, DefaultRetention))
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

func (r *reader) start(key string, def StartPosition) StartPosition {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	switch p := StartPosition(strings.ToLower(v)); p {
	case StartEnd, StartBeginning:
		return p
	default:
		r.fail(key, fmt.Errorf("%q is not a start position (%s, %s)", v, StartEnd, StartBeginning))
		return ""
	}
}

func (r *reader) dir(key string) string {
	v, ok := r.get(key)
	if !ok {
		home, ok := r.get(envHome)
		if !ok {
			r.fail(key, fmt.Errorf("not set and %s is not set, so the default $%s/%s cannot be used", envHome, envHome, defaultRecordingsSubdir))
			return ""
		}
		v = filepath.Join(home, defaultRecordingsSubdir)
	}
	if !filepath.IsAbs(v) {
		r.fail(key, fmt.Errorf("%q is not an absolute path", v))
		return ""
	}
	return filepath.Clean(v)
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
	return n
}

func (r *reader) positiveDuration(key string, def time.Duration) time.Duration {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.fail(key, fmt.Errorf("%q is not a duration such as 30m or 24h", v))
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
