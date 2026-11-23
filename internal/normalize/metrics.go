package normalize

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricsNamespace = "threadroute"
	metricsSubsystem = "normalize"
)

// Values of the "result" label of transactions_total.
const (
	resultCommitted = "committed"
	resultAborted   = "aborted"
)

// metrics are the metrics of a Service.
//
// The record counters change only after a transaction committed, by the
// counts of that transaction. So for every scrape:
//
//	raw_records_total = sum(emitted_total) + sum(dropped_total) + duplicates_total
//
// reprocessed_total is a part of emitted_total: a record seen again at the
// same position is processed and emitted.
type metrics struct {
	rawRecords   prometheus.Counter
	emitted      *prometheus.CounterVec
	dropped      *prometheus.CounterVec
	duplicates   prometheus.Counter
	reprocessed  prometheus.Counter
	timeFallback prometheus.Counter
	transactions *prometheus.CounterVec
	txnSeconds   prometheus.Histogram
	batchRecords prometheus.Histogram
	redisErrors  prometheus.Counter
	lag          prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) (*metrics, error) {
	opts := func(name, help string) prometheus.Opts {
		return prometheus.Opts{Namespace: metricsNamespace, Subsystem: metricsSubsystem, Name: name, Help: help}
	}
	m := &metrics{
		rawRecords: prometheus.NewCounter(prometheus.CounterOpts(opts(
			"raw_records_total", "Raw records in committed transactions."))),
		emitted: prometheus.NewCounterVec(prometheus.CounterOpts(opts(
			"emitted_total", "Normalised events in committed transactions, by kind.")), []string{"kind"}),
		dropped: prometheus.NewCounterVec(prometheus.CounterOpts(opts(
			"dropped_total", "Raw records in committed transactions that gave no output, by reason.")), []string{"reason"}),
		duplicates: prometheus.NewCounter(prometheus.CounterOpts(opts(
			"duplicates_total", "Raw records in committed transactions skipped as duplicates of an event seen at another position."))),
		reprocessed: prometheus.NewCounter(prometheus.CounterOpts(opts(
			"reprocessed_total", "Raw records in committed transactions seen again at the same position, after a failure. They are also in emitted_total."))),
		timeFallback: prometheus.NewCounter(prometheus.CounterOpts(opts(
			"time_us_fallback_total", "Raw records in committed transactions whose source time came from time_us, because no witnessed time was present."))),
		transactions: prometheus.NewCounterVec(prometheus.CounterOpts(opts(
			"transactions_total", "Ended transactions, by result.")), []string{"result"}),
		txnSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "transaction_seconds",
			Help: "Time from the start of a transaction to its end, committed or aborted.",
			// 1 ms to about 16 s.
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
		}),
		batchRecords: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name:    "batch_records",
			Help:    "Raw records per transaction.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 14),
		}),
		redisErrors: prometheus.NewCounter(prometheus.CounterOpts(opts(
			"redis_errors_total", "Failed deduplication calls to Redis. Each one stops the service."))),
		lag: prometheus.NewGauge(prometheus.GaugeOpts(opts(
			"lag_seconds", "Now minus the source time of the last record of the last committed transaction."))),
	}
	for _, k := range emitKinds {
		m.emitted.WithLabelValues(string(k))
	}
	for _, r := range dropReasons {
		m.dropped.WithLabelValues(r)
	}
	m.transactions.WithLabelValues(resultCommitted)
	m.transactions.WithLabelValues(resultAborted)

	for _, c := range []prometheus.Collector{
		m.rawRecords, m.emitted, m.dropped, m.duplicates, m.reprocessed, m.timeFallback,
		m.transactions, m.txnSeconds, m.batchRecords, m.redisErrors, m.lag,
	} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("register metrics: %w", err)
		}
	}
	return m, nil
}

// batchStats are the counts of one batch. They are added to the metrics only
// when the transaction of the batch committed.
type batchStats struct {
	raw         int
	emitted     map[Kind]int
	dropped     map[string]int
	duplicates  int
	reprocessed int
	// timeFallback counts records whose source time came from time_us.
	// It is not part of the identity: such a record is also emitted,
	// dropped or a duplicate.
	timeFallback int
}

func newBatchStats() batchStats {
	return batchStats{emitted: map[Kind]int{}, dropped: map[string]int{}}
}

func (m *metrics) addCommitted(s batchStats) {
	m.rawRecords.Add(float64(s.raw))
	for k, n := range s.emitted {
		m.emitted.WithLabelValues(string(k)).Add(float64(n))
	}
	for r, n := range s.dropped {
		m.dropped.WithLabelValues(r).Add(float64(n))
	}
	m.duplicates.Add(float64(s.duplicates))
	m.reprocessed.Add(float64(s.reprocessed))
	m.timeFallback.Add(float64(s.timeFallback))
}
