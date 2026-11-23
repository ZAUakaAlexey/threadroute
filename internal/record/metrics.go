package record

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricsNamespace = "threadroute"
	metricsSubsystem = "record"
)

// Label values.
const (
	stageRaw        = "raw"
	stageCompressed = "compressed"

	reasonAge  = "age"
	reasonSize = "size"

	opAppend    = "append"
	opFlush     = "flush"
	opRotate    = "rotate"
	opRetention = "retention"
	opFetch     = "fetch"
	opCommit    = "commit"
	opInvalid   = "invalid_entry"
	opHeader    = "invalid_header"
)

// writerMetrics are the metrics of a Writer.
type writerMetrics struct {
	entries         prometheus.Counter
	bytes           *prometheus.CounterVec
	segments        prometheus.Counter
	segmentsDeleted *prometheus.CounterVec
	errors          *prometheus.CounterVec
}

func newWriterMetrics(reg prometheus.Registerer) (*writerMetrics, error) {
	m := &writerMetrics{
		entries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "entries_total",
			Help: "Entries appended to segments.",
		}),
		bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "bytes_total",
			Help: "Bytes written to segments, before (raw) and after (compressed) zstd.",
		}, []string{"stage"}),
		segments: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "segments_total",
			Help: "Segments completed and renamed to their final name.",
		}),
		segmentsDeleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "segments_deleted_total",
			Help: "Complete segments deleted by retention, by reason (age, size).",
		}, []string{"reason"}),
	}
	errs, err := registerErrors(reg)
	if err != nil {
		return nil, err
	}
	m.errors = errs
	for _, c := range []prometheus.Collector{m.entries, m.bytes, m.segments, m.segmentsDeleted} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("register record metrics: %w", err)
		}
	}
	// Create the label values up front so the series exist at zero.
	m.bytes.WithLabelValues(stageRaw)
	m.bytes.WithLabelValues(stageCompressed)
	m.segmentsDeleted.WithLabelValues(reasonAge)
	m.segmentsDeleted.WithLabelValues(reasonSize)
	return m, nil
}

// registerErrors registers errors_total, or returns the collector that is
// already registered. The Writer and the Recorder both count errors, and in
// one process they share one registry.
func registerErrors(reg prometheus.Registerer) (*prometheus.CounterVec, error) {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "errors_total",
		Help: "Errors of the record tool, by operation.",
	}, []string{"op"})
	if err := reg.Register(c); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(*prometheus.CounterVec); ok {
				return existing, nil
			}
		}
		return nil, fmt.Errorf("register record errors metric: %w", err)
	}
	for _, op := range []string{opAppend, opFlush, opRotate, opRetention, opFetch, opCommit, opInvalid, opHeader} {
		c.WithLabelValues(op)
	}
	return c, nil
}
