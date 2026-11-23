package bluesky

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricsNamespace = "threadroute"
	metricsSubsystem = "source"
)

// Types of recoverable SDK errors. They are the values of the "type" label
// of errors_total.
const (
	errTypeReconnect       = "reconnect"
	errTypeConsumerTooSlow = "consumer_too_slow"
	errTypeDecode          = "decode"
	errTypeZstd            = "zstd"
	errTypeOther           = "other"
)

var errorTypes = []string{errTypeReconnect, errTypeConsumerTooSlow, errTypeDecode, errTypeZstd, errTypeOther}

// metrics holds the connector metrics.
type metrics struct {
	events        *prometheus.CounterVec
	skipped       *prometheus.CounterVec
	batches       prometheus.Counter
	payloadBytes  prometheus.Counter
	errors        *prometheus.CounterVec
	cursorClamped prometheus.Counter
	zeroWitnessed prometheus.Counter
	lastWitnessed prometheus.Gauge
	lag           prometheus.GaugeFunc

	// lastWitnessedUS is the witnessed time of the last delivered batch in
	// Unix microseconds, 0 when unset. The lag gauge reads it at scrape
	// time, so the lag keeps growing while the sink is blocked.
	lastWitnessedUS atomic.Int64
}

// newMetrics creates the metrics and registers them on reg.
func newMetrics(reg prometheus.Registerer, now func() time.Time) (*metrics, error) {
	m := &metrics{
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "events_total",
			Help: "Events delivered to the sink, by kind.",
		}, []string{"kind"}),
		skipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "skipped_total",
			Help: "Events skipped and not delivered, by reason.",
		}, []string{"reason"}),
		batches: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "batches_total",
			Help: "Batches delivered to the sink.",
		}),
		payloadBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "payload_bytes_total",
			Help: "Total size of the JSON payloads delivered to the sink.",
		}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "errors_total",
			Help: "Recoverable errors reported by the source SDK, by type.",
		}, []string{"type"}),
		cursorClamped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "cursor_clamped_total",
			Help: "Resumes where the stored cursor was older than the source keeps. Events may be lost.",
		}),
		zeroWitnessed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "zero_witnessed_total",
			Help: "Delivered events that had no witnessed time.",
		}),
		lastWitnessed: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "last_witnessed_timestamp_seconds",
			Help: "Witnessed time of the last delivered batch, in Unix seconds.",
		}),
	}
	m.lag = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "lag_seconds",
		Help: "Now minus the witnessed time of the last delivered batch. 0 before the first batch.",
	}, func() float64 {
		us := m.lastWitnessedUS.Load()
		if us == 0 {
			return 0
		}
		return now().Sub(time.UnixMicro(us)).Seconds()
	})

	for _, k := range subscribedKinds {
		m.events.WithLabelValues(string(k))
	}
	for _, r := range skipReasons {
		m.skipped.WithLabelValues(r)
	}
	for _, t := range errorTypes {
		m.errors.WithLabelValues(t)
	}

	collectors := []prometheus.Collector{
		m.events, m.skipped, m.batches, m.payloadBytes, m.errors,
		m.cursorClamped, m.zeroWitnessed, m.lastWitnessed, m.lag,
	}
	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("register metric: %w", err)
		}
	}
	return m, nil
}

// observeSkipped records the events of one mapped SDK batch that were
// skipped.
func (m *metrics) observeSkipped(res mapResult) {
	for r, n := range res.skipped {
		m.skipped.WithLabelValues(r).Add(float64(n))
	}
}

// observeDelivered records a batch the sink accepted.
func (m *metrics) observeDelivered(res mapResult) {
	for k, n := range res.kinds {
		m.events.WithLabelValues(string(k)).Add(float64(n))
	}
	m.zeroWitnessed.Add(float64(res.zeroWitnessed))
	m.batches.Inc()
	m.payloadBytes.Add(float64(res.payloadBytes))
	w := res.batch.Cursor.Witnessed
	if w.IsZero() {
		return
	}
	m.lastWitnessed.Set(float64(w.UnixMicro()) / 1e6)
	m.lastWitnessedUS.Store(w.UnixMicro())
}
