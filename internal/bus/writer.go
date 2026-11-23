package bus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/source"
)

// ProducerLimits are the producer settings that decision 0001 says are set
// explicitly. The values come from the service configuration.
type ProducerLimits struct {
	// MaxBufferedRecords and MaxBufferedBytes bound the producer buffer.
	// When either is reached, a write blocks.
	MaxBufferedRecords int
	MaxBufferedBytes   int
	// DeliveryTimeout is how long a record may wait for delivery before it
	// is reported as failed (delivery.timeout.ms).
	DeliveryTimeout time.Duration
}

// Validate reports every invalid field.
func (l ProducerLimits) Validate() error {
	var errs []error
	if l.MaxBufferedRecords <= 0 {
		errs = append(errs, fmt.Errorf("max buffered records %d is not positive", l.MaxBufferedRecords))
	}
	if l.MaxBufferedBytes <= 0 {
		errs = append(errs, fmt.Errorf("max buffered bytes %d is not positive", l.MaxBufferedBytes))
	}
	if l.DeliveryTimeout < time.Second {
		// kgo rejects a record delivery timeout below one second.
		errs = append(errs, fmt.Errorf("delivery timeout %s is less than 1s", l.DeliveryTimeout))
	}
	return errors.Join(errs...)
}

// RawWriter writes source batches to TopicRaw and the batch cursor to
// TopicCursor. Its WriteBatch method is a source.Sink.
type RawWriter struct {
	client  *kgo.Client
	source  string
	logger  *slog.Logger
	metrics *writerMetrics
	now     func() time.Time

	rawRecords   prometheus.Counter
	rawBytes     prometheus.Counter
	rawErrors    prometheus.Counter
	cursorBytes  prometheus.Counter
	cursorErrors prometheus.Counter
	cursorCount  prometheus.Counter
}

// NewRawWriter builds a producer client for the given brokers with the
// settings of decision 0001 and registers its metrics on reg.
//
// sourceName is the key of the cursor record, for example SourceBluesky.
// opts are applied after the built-in options, so they can override them.
// Tests use them to point the client at a fake cluster.
//
// Producer settings and why:
//
//   - acks=all (kgo.AllISRAcks) and idempotent writes (the kgo default, not
//     turned off here). With idempotence the broker checks a sequence number
//     per partition, so a retried request is neither written twice nor
//     written out of order, even with up to 5 requests in flight per broker.
//     When kgo fails a record because of a timeout, a retry limit or an
//     ended context, it fails every buffered record of that partition with
//     it, so a later record is not written after an earlier one was dropped.
//     Together with the key partitioner below this keeps the order of
//     records with the same key.
//   - The partitioner hashes the key with murmur2, like the Java client, so
//     one key always maps to one partition. This is the kgo default, set
//     here so the choice is visible.
//   - zstd compression.
//   - Explicit buffer limits. Produce (not TryProduce) is used, so a full
//     buffer blocks the caller until there is space or the context ends.
//   - A delivery timeout (kgo.RecordDeliveryTimeout). kgo measures it from
//     the record timestamp, so the record timestamp is left unset and kgo
//     stamps the produce time. The witnessed time travels in the
//     HeaderEventWitnessed header instead. With idempotence kgo fails a
//     record only when that is safe for the sequence numbers, so a record
//     in doubt can keep WriteBatch waiting longer than the timeout. The
//     caller's context still ends the wait.
//   - No linger. WriteBatch waits for every record anyway, so lingering only
//     adds latency. Records still group into batches while a request is in
//     flight.
func NewRawWriter(
	brokers []string,
	limits ProducerLimits,
	sourceName string,
	logger *slog.Logger,
	reg prometheus.Registerer,
	opts ...kgo.Opt,
) (*RawWriter, error) {
	if err := limits.Validate(); err != nil {
		return nil, fmt.Errorf("new raw writer: %w", err)
	}
	if sourceName == "" {
		return nil, errors.New("new raw writer: source name is empty")
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if reg == nil {
		return nil, errors.New("new raw writer: prometheus registerer is nil")
	}

	all := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.MaxBufferedRecords(limits.MaxBufferedRecords),
		kgo.MaxBufferedBytes(limits.MaxBufferedBytes),
		kgo.RecordDeliveryTimeout(limits.DeliveryTimeout),
		kgo.ProducerLinger(0),
		kgo.RecordPartitioner(kgo.UniformBytesPartitioner(64<<10, true, true, nil)),
	}
	all = append(all, opts...)
	client, err := kgo.NewClient(all...)
	if err != nil {
		return nil, fmt.Errorf("new raw writer: %w", err)
	}

	m, err := newWriterMetrics(reg, func() float64 { return float64(client.BufferedProduceRecords()) })
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("new raw writer: %w", err)
	}
	return &RawWriter{
		client:       client,
		source:       sourceName,
		logger:       logger,
		metrics:      m,
		now:          time.Now,
		rawRecords:   m.producedRecords.WithLabelValues(TopicRaw),
		rawBytes:     m.producedBytes.WithLabelValues(TopicRaw),
		rawErrors:    m.produceErrors.WithLabelValues(TopicRaw),
		cursorBytes:  m.producedBytes.WithLabelValues(TopicCursor),
		cursorErrors: m.produceErrors.WithLabelValues(TopicCursor),
		cursorCount:  m.producedRecords.WithLabelValues(TopicCursor),
	}, nil
}

// WriteBatch writes one record per event to TopicRaw and waits until Kafka
// acknowledges all of them. Only then it writes the batch cursor to
// TopicCursor and waits for that acknowledgement. A batch with an unset
// cursor writes no cursor record.
//
// It blocks while the producer buffer is full. If any record fails, it
// returns an error and writes no cursor. If ctx ends first, it returns an
// error that wraps ctx.Err(). Records already handed to the client may still
// be written after that.
//
// WriteBatch has the source.Sink signature. Batches must come from one
// caller at a time, in source order.
func (w *RawWriter) WriteBatch(ctx context.Context, b source.Batch) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("write batch: %w", err)
	}

	if len(b.Events) > 0 {
		start := time.Now()
		records := make([]*kgo.Record, len(b.Events))
		for i, ev := range b.Events {
			records[i] = &kgo.Record{
				Topic:   TopicRaw,
				Key:     []byte(ev.Key),
				Value:   ev.Payload,
				Headers: eventHeaders(ev),
				// Timestamp stays unset: kgo stamps the produce time,
				// and the raw topic uses LogAppendTime anyway.
			}
		}
		err := w.produceAll(ctx, records, func(r *kgo.Record, err error) {
			if err != nil {
				w.rawErrors.Inc()
				return
			}
			w.rawRecords.Inc()
			w.rawBytes.Add(float64(len(r.Value)))
		})
		if err != nil {
			w.logger.WarnContext(ctx, "raw batch not written, cursor not moved",
				"topic", TopicRaw, "events", len(b.Events), "error", err)
			return fmt.Errorf("write batch of %d events to %s: %w", len(b.Events), TopicRaw, err)
		}
		w.metrics.batchAck.Observe(time.Since(start).Seconds())
	}

	if b.Cursor.IsZero() {
		return nil
	}
	if err := w.writeCursor(ctx, b.Cursor); err != nil {
		return fmt.Errorf("write batch: %w", err)
	}
	return nil
}

func eventHeaders(ev source.RawEvent) []kgo.RecordHeader {
	h := make([]kgo.RecordHeader, 2, 3)
	h[0] = kgo.RecordHeader{Key: HeaderEventID, Value: []byte(ev.ID)}
	h[1] = kgo.RecordHeader{Key: HeaderEventKind, Value: []byte(ev.Kind)}
	if !ev.Witnessed.IsZero() {
		h = append(h, kgo.RecordHeader{
			Key:   HeaderEventWitnessed,
			Value: strconv.AppendInt(nil, ev.Witnessed.UnixMicro(), 10),
		})
	}
	return h
}

func (w *RawWriter) writeCursor(ctx context.Context, c source.Cursor) error {
	value, err := MarshalCursor(CursorRecord{
		Host:      c.Host,
		Witnessed: c.Witnessed,
		Seq:       c.Seq,
		WrittenAt: w.now(),
	})
	if err != nil {
		return fmt.Errorf("write cursor: %w", err)
	}
	rec := &kgo.Record{Topic: TopicCursor, Key: CursorKey(w.source), Value: value}
	err = w.produceAll(ctx, []*kgo.Record{rec}, func(r *kgo.Record, err error) {
		if err != nil {
			w.cursorErrors.Inc()
			return
		}
		w.cursorCount.Inc()
		w.cursorBytes.Add(float64(len(r.Value)))
		w.metrics.cursorWrites.Inc()
	})
	if err != nil {
		return fmt.Errorf("write cursor to %s: %w", TopicCursor, err)
	}
	return nil
}

// produceAll hands the records to the client in order and waits until each
// one is acknowledged or failed, or until ctx ends. It stops handing over
// records once ctx ends or a record has failed. kgo also uses ctx to fail
// buffered records when it ends.
func (w *RawWriter) produceAll(ctx context.Context, records []*kgo.Record, onDone func(*kgo.Record, error)) error {
	acks := newAckWaiter()
	for _, r := range records {
		if ctx.Err() != nil || acks.failed() {
			break
		}
		acks.add()
		start := time.Now()
		// Produce blocks while the buffer is full. The promise runs on a
		// client goroutine and must not block.
		w.client.Produce(ctx, r, func(r *kgo.Record, err error) {
			onDone(r, err)
			acks.done(err)
		})
		w.metrics.blocked.Add(time.Since(start).Seconds())
	}
	acks.seal()

	select {
	case <-acks.all:
	case <-ctx.Done():
		return ctx.Err()
	}
	if failed, err := acks.result(); err != nil {
		return fmt.Errorf("%d of %d records failed: %w", failed, len(records), err)
	}
	if err := ctx.Err(); err != nil {
		// ctx ended before every record was handed over.
		return err
	}
	return nil
}

// Close flushes the records still in the buffer, closes the client and
// unregisters the metrics, so a new writer can use the same registerer. If
// ctx ends before the flush is done, the remaining records fail.
func (w *RawWriter) Close(ctx context.Context) error {
	err := w.client.Flush(ctx)
	w.client.Close()
	w.metrics.unregister()
	if err != nil {
		return fmt.Errorf("close raw writer: %w", err)
	}
	return nil
}

// ackWaiter counts records that are not yet acknowledged. The all channel
// is closed once seal was called and every added record is done. Its
// methods never block for long, so they are safe in a kgo promise.
type ackWaiter struct {
	mu       sync.Mutex
	pending  int
	sealed   bool
	failures int
	firstErr error
	all      chan struct{}
}

func newAckWaiter() *ackWaiter {
	return &ackWaiter{all: make(chan struct{})}
}

func (a *ackWaiter) add() {
	a.mu.Lock()
	a.pending++
	a.mu.Unlock()
}

func (a *ackWaiter) done(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pending--
	if err != nil {
		a.failures++
		if a.firstErr == nil {
			a.firstErr = err
		}
	}
	if a.sealed && a.pending == 0 {
		close(a.all)
	}
}

func (a *ackWaiter) seal() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sealed = true
	if a.pending == 0 {
		close(a.all)
	}
}

func (a *ackWaiter) failed() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.failures > 0
}

func (a *ackWaiter) result() (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.failures, a.firstErr
}
