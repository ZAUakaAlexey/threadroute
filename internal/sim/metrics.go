package sim

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricsNamespace = "threadroute"
	metricsSubsystem = "sim"
)

// Actions of the API calls, the label of the call metrics.
const (
	actionPut     = "put_operator"
	actionOnline  = "online"
	actionOffline = "offline"
	actionAccept  = "accept"
	actionReply   = "reply"
	actionResolve = "resolve"
)

// Results of an API call that failed. A success has the result of the
// answer as its label: done or already_done.
const (
	// resultRejected: HTTP 409, the state moved on.
	resultRejected = "rejected"
	// resultUnavailable: HTTP 503 or no answer at all. Retried.
	resultUnavailable = "unavailable"
	// resultFailed: any other error answer (400, 404, 500). Not retried.
	resultFailed = "failed"
)

type metrics struct {
	operatorsOnline prometheus.Gauge
	offersSeen      prometheus.Counter
	accepts         prometheus.Counter
	offersMissed    prometheus.Counter
	replies         prometheus.Counter
	resolves        prometheus.Counter
	calls           *prometheus.CounterVec
	rejections      *prometheus.CounterVec
	failures        *prometheus.CounterVec
	retries         *prometheus.CounterVec
	planned         prometheus.Gauge
	dropped         prometheus.Counter
	events          *prometheus.CounterVec
	eventsSkipped   prometheus.Counter
	eventsInvalid   prometheus.Counter
	fetchErrors     prometheus.Counter
	lag             prometheus.Gauge

	// Stand settings (decision 0014).
	settingsValue   *prometheus.GaugeVec
	settingsVersion prometheus.Gauge
	settingsChanges prometheus.Counter
	operatorsActive prometheus.Gauge
	operatorsAside  prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) (*metrics, error) {
	opts := func(name, help string) prometheus.Opts {
		return prometheus.Opts{Namespace: metricsNamespace, Subsystem: metricsSubsystem, Name: name, Help: help}
	}
	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts(opts(name, help)))
	}
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts(opts(name, help)))
	}
	m := &metrics{
		operatorsOnline: gauge("operators_online",
			"Simulated operators that the router last confirmed as online."),
		offersSeen: counter("offers_seen_total",
			"Offers to simulated operators read from the routing topic."),
		accepts: counter("accepts_total",
			"Accepts that the router took (done or already done)."),
		offersMissed: counter("offers_missed_total",
			"Offers left unanswered on purpose, so the accept timeout runs out."),
		replies: counter("replies_total",
			"Reply markers that the router took (done or already done)."),
		resolves: counter("resolves_total",
			"Resolves that the router took (done or already done)."),
		calls: prometheus.NewCounterVec(prometheus.CounterOpts(opts("api_calls_total",
			"Operator API calls by action and result (done, already_done, rejected, unavailable, failed).")),
			[]string{"action", "result"}),
		rejections: prometheus.NewCounterVec(prometheus.CounterOpts(opts("rejections_total",
			"Calls rejected with HTTP 409 because the state moved on, by action and code. The planned work of that offer is dropped.")),
			[]string{"action", "code"}),
		failures: prometheus.NewCounterVec(prometheus.CounterOpts(opts("failures_total",
			"Calls that failed with an error that is not retried (400, 404, 500), by action and code.")),
			[]string{"action", "code"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts(opts("retries_total",
			"Calls planned again after a 503 or no answer, by action.")),
			[]string{"action"}),
		planned: gauge("planned_actions",
			"Planned actions waiting for their time: accepts, replies, resolves, retries and the next shift change of each operator."),
		dropped: counter("plans_cancelled_total",
			"Offers whose planned work was cancelled by a routing event (offer expired, ended, queued again, resolved)."),
		events: prometheus.NewCounterVec(prometheus.CounterOpts(opts("events_total",
			"Routing events read, by type.")), []string{"type"}),
		eventsSkipped: counter("events_skipped_total",
			"Routing events skipped because their sequence number was already seen for their key."),
		eventsInvalid: counter("events_invalid_total",
			"Routing records that could not be decoded."),
		fetchErrors: counter("fetch_errors_total",
			"Errors while fetching the routing topic."),
		lag: gauge("consumer_lag_seconds",
			"Machine time from the append of the last routing record read to the moment it was read."),
		settingsValue: prometheus.NewGaugeVec(prometheus.GaugeOpts(opts("settings_value",
			"Sim values in effect, by field name of the stand settings (decision 0014). Durations are microseconds of model time.")),
			[]string{"name"}),
		settingsVersion: gauge("settings_version",
			"Version of the stand settings whose sim part is applied, 0 when the simulator runs from its environment."),
		settingsChanges: counter("settings_changes_total",
			"Changes of the sim values applied while the simulator runs."),
		operatorsActive: gauge("operators_active",
			"Simulated operators in the shift cycle."),
		operatorsAside: gauge("operators_put_aside",
			"Simulated operators put aside by a smaller operator count: registered, offline and out of the shift cycle."),
	}
	for _, c := range []prometheus.Collector{
		m.operatorsOnline, m.offersSeen, m.accepts, m.offersMissed, m.replies, m.resolves,
		m.calls, m.rejections, m.failures, m.retries, m.planned, m.dropped,
		m.events, m.eventsSkipped, m.eventsInvalid, m.fetchErrors, m.lag,
		m.settingsValue, m.settingsVersion, m.settingsChanges, m.operatorsActive, m.operatorsAside,
	} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("register sim metrics: %w", err)
		}
	}
	return m, nil
}
