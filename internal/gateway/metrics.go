package gateway

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricsNamespace = "threadroute"
	metricsSubsystem = "gateway"
)

// Close reasons, the label of clients_closed_total. No label of any
// gateway metric holds a key, an operator ID, an address or a token.
const (
	reasonNormal       = "normal"
	reasonGoingAway    = "going_away"
	reasonTooBig       = "message_too_big"
	reasonBadMessage   = "bad_message"
	reasonHelloTimeout = "hello_timeout"
	reasonVersion      = "unsupported_version"
	reasonReplaced     = "replaced"
	reasonSlow         = "slow_client"
	reasonTooMany      = "too_many_connections"
	reasonNoSeat       = "no_operator_seat"
	reasonTryLater     = "try_again_later"
	reasonInternal     = "internal"
	reasonPing         = "ping_failed"
	reasonGone         = "client_gone"
)

type metrics struct {
	connections  *prometheus.GaugeVec
	sent         *prometheus.CounterVec
	sentBytes    *prometheus.CounterVec
	slowTicks    prometheus.Counter
	closed       *prometheus.CounterVec
	actions      *prometheus.CounterVec
	viewRows     *prometheus.GaugeVec
	events       *prometheus.CounterVec
	lag          prometheus.Gauge
	stale        *prometheus.GaugeVec
	siteCalls    *prometheus.CounterVec
	textReads    *prometheus.CounterVec
	offlinePuts  *prometheus.CounterVec
	tickDuration prometheus.Histogram
	fetchErrors  prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) (*metrics, error) {
	opts := func(name, help string) prometheus.Opts {
		return prometheus.Opts{Namespace: metricsNamespace, Subsystem: metricsSubsystem, Name: name, Help: help}
	}
	m := &metrics{
		connections: prometheus.NewGaugeVec(prometheus.GaugeOpts(opts("connections",
			"Open WebSocket connections after the hello, by view and level.")), []string{"view", "level"}),
		sent: prometheus.NewCounterVec(prometheus.CounterOpts(opts("messages_sent_total",
			"Server messages sent, by type.")), []string{"type"}),
		sentBytes: prometheus.NewCounterVec(prometheus.CounterOpts(opts("bytes_sent_total",
			"Bytes of server messages sent, by type.")), []string{"type"}),
		slowTicks: prometheus.NewCounter(prometheus.CounterOpts(opts("slow_ticks_total",
			"Ticks that found a client still writing the previous state."))),
		closed: prometheus.NewCounterVec(prometheus.CounterOpts(opts("clients_closed_total",
			"Closed connections by reason.")), []string{"reason"}),
		actions: prometheus.NewCounterVec(prometheus.CounterOpts(opts("requests_total",
			"Client requests (actions, watch, unwatch) by type and result code.")), []string{"type", "result"}),
		viewRows: prometheus.NewGaugeVec(prometheus.GaugeOpts(opts("view_rows",
			"Rows of the in-memory view by kind.")), []string{"kind"}),
		events: prometheus.NewCounterVec(prometheus.CounterOpts(opts("events_total",
			"Records read from the topics, by topic and result (applied, skipped, invalid).")), []string{"topic", "result"}),
		lag: prometheus.NewGauge(prometheus.GaugeOpts(opts("lag_seconds",
			"How long the gateway has been behind the end of the topics."))),
		stale: prometheus.NewGaugeVec(prometheus.GaugeOpts(opts("stale",
			"1 for the current stale reason of the data, fresh when it is current.")), []string{"reason"}),
		siteCalls: prometheus.NewCounterVec(prometheus.CounterOpts(opts("site_api_checks_total",
			"Token checks with the site API by outcome.")), []string{"outcome"}),
		textReads: prometheus.NewCounterVec(prometheus.CounterOpts(opts("text_reads_total",
			"Reads of message text from Redis by kind and result.")), []string{"kind", "result"}),
		offlinePuts: prometheus.NewCounterVec(prometheus.CounterOpts(opts("operators_put_offline_total",
			"Human operators put offline by the gateway, by reason and result.")), []string{"reason", "result"}),
		tickDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem, Name: "tick_seconds",
			Help:    "Time to compute the totals and signal every client on one tick.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 4, 8),
		}),
		fetchErrors: prometheus.NewCounter(prometheus.CounterOpts(opts("fetch_errors_total",
			"Fetch errors of the topic consumer."))),
	}
	for _, c := range []prometheus.Collector{
		m.connections, m.sent, m.sentBytes, m.slowTicks, m.closed, m.actions, m.viewRows, m.events,
		m.lag, m.stale, m.siteCalls, m.textReads, m.offlinePuts, m.tickDuration, m.fetchErrors,
	} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("register gateway metrics: %w", err)
		}
	}
	return m, nil
}
