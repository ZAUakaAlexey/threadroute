package sim

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
	"github.com/ZAUakaAlexey/threadroute/internal/router/opclient"
)

// Prefix is shared by every variable of the project.
const Prefix = "THREADROUTE_"

// Variable names. KAFKA_BROKERS and LOG_LEVEL have the same names as in the
// other services.
const (
	EnvKafkaBrokers = Prefix + "KAFKA_BROKERS"
	EnvLogLevel     = Prefix + "LOG_LEVEL"

	EnvRouterURL   = Prefix + "SIM_ROUTER_URL"
	EnvStartWait   = Prefix + "SIM_START_WAIT"
	EnvGroupID     = Prefix + "SIM_GROUP_ID"
	EnvMetricsAddr = Prefix + "SIM_METRICS_ADDR"
	EnvOutageLimit = Prefix + "SIM_OUTAGE_LIMIT"

	EnvOperators        = Prefix + "SIM_OPERATORS"
	EnvLanguages        = Prefix + "SIM_LANGUAGES"
	EnvCapacity         = Prefix + "SIM_CAPACITY"
	EnvShiftOnline      = Prefix + "SIM_SHIFT_ONLINE"
	EnvShiftOffline     = Prefix + "SIM_SHIFT_OFFLINE"
	EnvAcceptDelayMin   = Prefix + "SIM_ACCEPT_DELAY_MIN"
	EnvAcceptDelayMax   = Prefix + "SIM_ACCEPT_DELAY_MAX"
	EnvMissedOfferShare = Prefix + "SIM_MISSED_OFFER_SHARE"
	EnvHandlingMedian   = Prefix + "SIM_HANDLING_MEDIAN"
	EnvResolveMedian    = Prefix + "SIM_RESOLVE_MEDIAN"
	EnvSeed             = Prefix + "SIM_SEED"
)

// Defaults that are not behaviour rules. The behaviour defaults come from
// internal/defaults through DefaultBehaviour.
const (
	DefaultKafkaBrokers = "localhost:9092"
	DefaultLogLevel     = slog.LevelInfo
	// DefaultRouterURL is the default address of the operator API of the
	// router.
	DefaultRouterURL = "http://127.0.0.1:9470"
	// DefaultStartWait bounds the wait for the router settings and for the
	// routing topic at start.
	DefaultStartWait = 2 * time.Minute
	DefaultGroupID   = "threadroute-sim"
	// DefaultMetricsAddr differs from the other services, so all can run
	// on one host.
	DefaultMetricsAddr = ":9469"
	// DefaultOutageLimit is how long the router may stay unreachable
	// before the simulator stops with an error.
	DefaultOutageLimit = 2 * time.Minute
	// DefaultSeed is the seed of the random generator.
	DefaultSeed uint64 = 1
)

// Behaviour is how the simulated operators act (decision 0010). Every
// duration is model time: the simulator divides it by the speed factor of
// the router.
type Behaviour struct {
	// Operators is the number of operators, 1 to MaxOperators.
	Operators int
	// Languages is the language spread. Spread.langOrder gives the
	// language of each operator number.
	Languages Spread
	// Capacity is the capacity of each operator. 0 takes the router
	// default.
	Capacity int
	// ShiftOnline and ShiftOffline are the two parts of a shift.
	ShiftOnline  time.Duration
	ShiftOffline time.Duration
	// AcceptDelayMin and AcceptDelayMax bound the uniform delay before an
	// accept.
	AcceptDelayMin time.Duration
	AcceptDelayMax time.Duration
	// MissedOfferShare is the share of offers left to time out, 0 to 1.
	MissedOfferShare float64
	// HandlingMedian is the median time from the accept to the reply.
	HandlingMedian time.Duration
	// ResolveMedian is the median time from the reply to the resolve.
	ResolveMedian time.Duration
	// Seed is the seed of the random generator.
	Seed uint64
}

// DefaultBehaviour returns the starting values of decision 0010.
func DefaultBehaviour() Behaviour {
	// The constant parses, which a test checks. If it did not, Languages
	// would be nil and Validate would report it.
	spread, _ := ParseSpread(defaults.SimLanguages)
	return Behaviour{
		Operators:        defaults.SimOperators,
		Languages:        spread,
		ShiftOnline:      defaults.SimShiftOnline,
		ShiftOffline:     defaults.SimShiftOffline,
		AcceptDelayMin:   defaults.SimAcceptDelayMin,
		AcceptDelayMax:   defaults.SimAcceptDelayMax,
		MissedOfferShare: defaults.SimMissedOfferShare,
		HandlingMedian:   defaults.SimHandlingMedian,
		ResolveMedian:    defaults.SimResolveMedian,
		Seed:             DefaultSeed,
	}
}

// Validate reports every invalid field.
func (b Behaviour) Validate() error {
	var errs []error
	if b.Operators < 1 || b.Operators > MaxOperators {
		errs = append(errs, fmt.Errorf("operators %d is not between 1 and %d", b.Operators, MaxOperators))
	} else if err := b.Languages.Validate(b.Operators); err != nil {
		errs = append(errs, fmt.Errorf("languages: %w", err))
	}
	if b.Capacity < 0 || b.Capacity > opapi.MaxCapacity {
		errs = append(errs, fmt.Errorf("capacity %d is not between 0 and %d", b.Capacity, opapi.MaxCapacity))
	}
	for _, d := range []struct {
		name string
		v    time.Duration
	}{
		{"shift online", b.ShiftOnline},
		{"shift offline", b.ShiftOffline},
		{"accept delay max", b.AcceptDelayMax},
		{"handling median", b.HandlingMedian},
		{"resolve median", b.ResolveMedian},
	} {
		if d.v <= 0 {
			errs = append(errs, fmt.Errorf("%s %s is not positive", d.name, d.v))
		}
	}
	if b.AcceptDelayMin < 0 || b.AcceptDelayMin > b.AcceptDelayMax {
		errs = append(errs, fmt.Errorf("accept delay min %s is not between 0 and the max %s", b.AcceptDelayMin, b.AcceptDelayMax))
	}
	if math.IsNaN(b.MissedOfferShare) || b.MissedOfferShare < 0 || b.MissedOfferShare > 1 {
		errs = append(errs, fmt.Errorf("missed offer share %v is not between 0 and 1", b.MissedOfferShare))
	}
	return errors.Join(errs...)
}

// Config is the configuration of the sim service.
type Config struct {
	// KafkaBrokers are the seed brokers, as host:port.
	KafkaBrokers []string
	// RouterURL is the base URL of the operator API.
	RouterURL string
	// StartWait bounds the wait for the router settings and the routing
	// topic at start.
	StartWait time.Duration
	// GroupID is the consumer group on the routing topic.
	GroupID string
	// MetricsAddr is the listen address of the metrics, pprof and health
	// endpoint.
	MetricsAddr string
	// OutageLimit stops the simulator when the router has not answered for
	// this long.
	OutageLimit time.Duration
	// Behaviour is how the operators act.
	Behaviour Behaviour
	// LogLevel is the minimum level for log/slog.
	LogLevel slog.Level
}

// Validate reports every invalid field. Load already checks each variable;
// this also covers a Config built in code, for example in tests.
func (c Config) Validate() error {
	var errs []error
	if len(c.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("no kafka brokers"))
	}
	if _, err := opclient.New(c.RouterURL); err != nil {
		errs = append(errs, fmt.Errorf("router URL: %w", err))
	}
	if c.GroupID == "" {
		errs = append(errs, errors.New("group ID is empty"))
	}
	if c.StartWait <= 0 {
		errs = append(errs, fmt.Errorf("start wait %s is not positive", c.StartWait))
	}
	if c.OutageLimit <= 0 {
		errs = append(errs, fmt.Errorf("outage limit %s is not positive", c.OutageLimit))
	}
	if err := c.Behaviour.Validate(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// LookupFunc returns the value of an environment variable and whether it is
// set. os.LookupEnv has this signature.
type LookupFunc func(key string) (string, bool)

// Load reads and validates the configuration. A variable that is unset or
// empty takes its default. The error names every invalid variable.
func Load(lookup LookupFunc) (Config, error) {
	r := reader{lookup: lookup}
	d := DefaultBehaviour()
	cfg := Config{
		KafkaBrokers: r.brokers(EnvKafkaBrokers, DefaultKafkaBrokers),
		RouterURL:    r.url(EnvRouterURL, DefaultRouterURL),
		StartWait:    r.duration(EnvStartWait, DefaultStartWait, false),
		GroupID:      r.groupID(EnvGroupID, DefaultGroupID),
		MetricsAddr:  r.hostPort(EnvMetricsAddr, DefaultMetricsAddr),
		OutageLimit:  r.duration(EnvOutageLimit, DefaultOutageLimit, false),
		LogLevel:     r.logLevel(EnvLogLevel, DefaultLogLevel),
		Behaviour: Behaviour{
			Operators:        r.intBetween(EnvOperators, d.Operators, 1, MaxOperators),
			Languages:        r.spread(EnvLanguages, d.Languages),
			Capacity:         r.intBetween(EnvCapacity, d.Capacity, 0, opapi.MaxCapacity),
			ShiftOnline:      r.duration(EnvShiftOnline, d.ShiftOnline, false),
			ShiftOffline:     r.duration(EnvShiftOffline, d.ShiftOffline, false),
			AcceptDelayMin:   r.duration(EnvAcceptDelayMin, d.AcceptDelayMin, true),
			AcceptDelayMax:   r.duration(EnvAcceptDelayMax, d.AcceptDelayMax, false),
			MissedOfferShare: r.share(EnvMissedOfferShare, d.MissedOfferShare),
			HandlingMedian:   r.duration(EnvHandlingMedian, d.HandlingMedian, false),
			ResolveMedian:    r.duration(EnvResolveMedian, d.ResolveMedian, false),
			Seed:             r.seed(EnvSeed, d.Seed),
		},
	}
	b := cfg.Behaviour
	if b.AcceptDelayMax > 0 && b.AcceptDelayMin > b.AcceptDelayMax {
		r.fail(EnvAcceptDelayMin, fmt.Errorf("%s is above %s (%s)", b.AcceptDelayMin, EnvAcceptDelayMax, b.AcceptDelayMax))
	}
	if b.Operators > 0 && b.Languages != nil {
		if err := b.Languages.Validate(b.Operators); err != nil {
			r.fail(EnvLanguages, err)
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

func (r *reader) url(key, def string) string {
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

// duration reads a duration. zeroOK allows 0.
func (r *reader) duration(key string, def time.Duration, zeroOK bool) time.Duration {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.fail(key, fmt.Errorf("%q is not a duration such as 500ms, 20s or 15m", v))
		return 0
	}
	if d < 0 || (d == 0 && !zeroOK) {
		r.fail(key, fmt.Errorf("%s is not positive", d))
		return 0
	}
	return d
}

func (r *reader) share(key string, def float64) float64 {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || math.IsNaN(f) || f < 0 || f > 1 {
		r.fail(key, fmt.Errorf("%q is not a number between 0 and 1", v))
		return 0
	}
	return f
}

func (r *reader) seed(key string, def uint64) uint64 {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		r.fail(key, fmt.Errorf("%q is not a whole number from 0 to %d", v, uint64(math.MaxUint64)))
		return 0
	}
	return n
}

func (r *reader) spread(key string, def Spread) Spread {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	s, err := ParseSpread(v)
	if err != nil {
		r.fail(key, err)
		return nil
	}
	return s
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
