package bus

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricsNamespace = "threadroute"
	metricsSubsystem = "bus"
)

// writerMetrics are the metrics of a RawWriter.
type writerMetrics struct {
	producedRecords *prometheus.CounterVec
	producedBytes   *prometheus.CounterVec
	produceErrors   *prometheus.CounterVec
	batchAck        prometheus.Histogram
	cursorWrites    prometheus.Counter
	blocked         prometheus.Counter

	reg        prometheus.Registerer
	collectors []prometheus.Collector
}

func (m *writerMetrics) unregister() {
	for _, c := range m.collectors {
		m.reg.Unregister(c)
	}
}

func newWriterMetrics(reg prometheus.Registerer, buffered func() float64) (*writerMetrics, error) {
	opts := func(name, help string) prometheus.Opts {
		return prometheus.Opts{Namespace: metricsNamespace, Subsystem: metricsSubsystem, Name: name, Help: help}
	}
	m := &writerMetrics{
		producedRecords: prometheus.NewCounterVec(prometheus.CounterOpts(opts(
			"produced_records_total", "Records acknowledged by Kafka.")), []string{"topic"}),
		producedBytes: prometheus.NewCounterVec(prometheus.CounterOpts(opts(
			"produced_bytes_total", "Value bytes of records acknowledged by Kafka.")), []string{"topic"}),
		produceErrors: prometheus.NewCounterVec(prometheus.CounterOpts(opts(
			"produce_errors_total", "Records that Kafka did not acknowledge.")), []string{"topic"}),
		batchAck: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "batch_ack_seconds",
			Help:      "Time from the start of a batch write to the acknowledgement of its last event record.",
			// 0.5 ms to about 8 s.
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 15),
		}),
		cursorWrites: prometheus.NewCounter(prometheus.CounterOpts(opts(
			"cursor_writes_total", "Cursor records acknowledged by Kafka."))),
		blocked: prometheus.NewCounter(prometheus.CounterOpts(opts(
			"blocked_seconds_total", "Time spent inside the producer call, which is almost all waiting for buffer space."))),
	}
	bufferedGauge := prometheus.NewGaugeFunc(prometheus.GaugeOpts(opts(
		"buffered_records", "Records in the producer buffer, including records waiting for space.")), buffered)

	var done []prometheus.Collector
	for _, c := range []prometheus.Collector{
		m.producedRecords, m.producedBytes, m.produceErrors, m.batchAck, m.cursorWrites, m.blocked, bufferedGauge,
	} {
		if err := reg.Register(c); err != nil {
			for _, d := range done {
				reg.Unregister(d)
			}
			return nil, fmt.Errorf("register metrics: %w", err)
		}
		done = append(done, c)
	}
	m.reg, m.collectors = reg, done
	return m, nil
}
