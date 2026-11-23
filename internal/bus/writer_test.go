package bus

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/ZAUakaAlexey/threadroute/internal/source"
)

var testWitnessedBase = time.UnixMicro(1_700_000_000_000_000).UTC()

// testEvents returns n events spread over three synthetic authors. Event i
// has witnessed time base + i ms + 123 us, so millisecond truncation shows.
func testEvents(n int) []source.RawEvent {
	keys := []string{testAlice, testBob, testCarol}
	events := make([]source.RawEvent, n)
	for i := range events {
		events[i] = source.RawEvent{
			Key:       keys[i%len(keys)],
			ID:        fmt.Sprintf("c:%s:app.bsky.feed.post:synthetic%04d:rev1:create", keys[i%len(keys)], i),
			Kind:      source.KindCommit,
			Witnessed: testWitnessedBase.Add(time.Duration(i)*time.Millisecond + 123*time.Microsecond),
			Payload:   syntheticPayload(i),
		}
	}
	return events
}

func testCursor(n int) source.Cursor {
	return source.Cursor{
		Host:      "jetstream-east.example.test",
		Witnessed: testWitnessedBase.Add(time.Duration(n) * time.Millisecond),
		Seq:       uint64(1000 + n),
	}
}

func header(r *kgo.Record, key string) (string, bool) {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

func TestWriteBatchWritesEventsAndCursor(t *testing.T) {
	c := newTestClusterWithTopics(t)
	reg := prometheus.NewRegistry()
	w := newTestWriter(t, c, testProducerLimits(), reg)
	writtenAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	w.now = func() time.Time { return writtenAt }

	// The witnessed times are in 2023, far older than the delivery
	// timeout. They must not make the records time out.
	events := testEvents(30)
	events[0].Witnessed = time.Time{} // unset: no witnessed header
	batch := source.Batch{Events: events, Cursor: testCursor(30)}
	before := time.Now().Add(-time.Second)
	if err := w.WriteBatch(testContext(t), batch); err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}
	after := time.Now().Add(time.Second)

	records := readAll(t, c, TopicRaw)
	if len(records) != len(events) {
		t.Fatalf("got %d raw records, want %d", len(records), len(events))
	}
	byID := map[string]source.RawEvent{}
	for _, ev := range events {
		byID[ev.ID] = ev
	}
	keyPartition := map[string]int32{}
	lastN := map[string]int{}
	// Records come back in partition and offset order.
	for _, r := range records {
		id, ok := header(r, HeaderEventID)
		if !ok {
			t.Fatalf("record at %d/%d has no %s header", r.Partition, r.Offset, HeaderEventID)
		}
		ev, ok := byID[id]
		if !ok {
			t.Fatalf("unknown event id %q", id)
		}
		delete(byID, id)
		if string(r.Key) != ev.Key {
			t.Errorf("event %s: key = %q, want %q", id, r.Key, ev.Key)
		}
		if !bytes.Equal(r.Value, ev.Payload) {
			t.Errorf("event %s: value = %s, want %s", id, r.Value, ev.Payload)
		}
		if kind, _ := header(r, HeaderEventKind); kind != string(ev.Kind) {
			t.Errorf("event %s: kind header = %q, want %q", id, kind, ev.Kind)
		}
		witnessed, ok := header(r, HeaderEventWitnessed)
		switch {
		case ev.Witnessed.IsZero() && ok:
			t.Errorf("event %s: %s header = %q, want none for an unset time", id, HeaderEventWitnessed, witnessed)
		case !ev.Witnessed.IsZero() && witnessed != strconv.FormatInt(ev.Witnessed.UnixMicro(), 10):
			t.Errorf("event %s: %s header = %q, want %d", id, HeaderEventWitnessed, witnessed, ev.Witnessed.UnixMicro())
		}
		// The topic uses LogAppendTime, so the timestamp is the write time.
		if r.Timestamp.Before(before) || r.Timestamp.After(after) {
			t.Errorf("event %s: timestamp = %s, want the write time between %s and %s", id, r.Timestamp, before, after)
		}
		if r.Attrs.TimestampType() != 1 {
			t.Errorf("event %s: timestamp type = %d, want 1 (LogAppendTime)", id, r.Attrs.TimestampType())
		}

		// Same key, same partition, in order.
		if p, seen := keyPartition[ev.Key]; seen && p != r.Partition {
			t.Errorf("key %s is in partitions %d and %d", ev.Key, p, r.Partition)
		}
		keyPartition[ev.Key] = r.Partition
		var n int
		if _, err := fmt.Sscanf(strings.SplitAfter(string(r.Value), `"n":`)[1], "%d", &n); err != nil {
			t.Fatalf("parse payload %s: %v", r.Value, err)
		}
		if last, seen := lastN[ev.Key]; seen && n <= last {
			t.Errorf("key %s: event %d comes after event %d", ev.Key, n, last)
		}
		lastN[ev.Key] = n
	}
	if len(byID) != 0 {
		t.Errorf("%d events missing from the raw topic", len(byID))
	}

	cursors := readAll(t, c, TopicCursor)
	if len(cursors) != 1 {
		t.Fatalf("got %d cursor records, want 1", len(cursors))
	}
	if string(cursors[0].Key) != SourceBluesky {
		t.Errorf("cursor key = %q, want %q", cursors[0].Key, SourceBluesky)
	}
	got, err := UnmarshalCursor(cursors[0].Value)
	if err != nil {
		t.Fatalf("UnmarshalCursor() error = %v", err)
	}
	want := CursorRecord{Host: batch.Cursor.Host, Witnessed: batch.Cursor.Witnessed, Seq: batch.Cursor.Seq, WrittenAt: writtenAt}
	if got.Host != want.Host || got.Seq != want.Seq || !got.Witnessed.Equal(want.Witnessed) || !got.WrittenAt.Equal(want.WrittenAt) {
		t.Errorf("cursor = %+v, want %+v", got, want)
	}

	if n := testutil.ToFloat64(w.metrics.producedRecords.WithLabelValues(TopicRaw)); n != float64(len(events)) {
		t.Errorf("produced_records_total{topic=%s} = %v, want %d", TopicRaw, n, len(events))
	}
	if n := testutil.ToFloat64(w.metrics.cursorWrites); n != 1 {
		t.Errorf("cursor_writes_total = %v, want 1", n)
	}
	if n := testutil.CollectAndCount(w.metrics.batchAck); n != 1 {
		t.Errorf("batch_ack_seconds series = %d, want 1", n)
	}
	if err := testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP threadroute_bus_buffered_records Records in the producer buffer, including records waiting for space.
# TYPE threadroute_bus_buffered_records gauge
threadroute_bus_buffered_records 0
`), "threadroute_bus_buffered_records"); err != nil {
		t.Errorf("buffered_records: %v", err)
	}
}

// produceLog records the topics of every produce request in arrival order.
type produceLog struct {
	mu     sync.Mutex
	topics []string
}

func (l *produceLog) observe(c *kfake.Cluster) {
	c.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		pr := req.(*kmsg.ProduceRequest)
		l.mu.Lock()
		for _, tp := range pr.Topics {
			l.topics = append(l.topics, tp.Topic)
		}
		l.mu.Unlock()
		return nil, nil, false
	})
}

func (l *produceLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.topics...)
}

func TestWriteBatchCursorAfterEvents(t *testing.T) {
	tests := []struct {
		name       string
		batch      source.Batch
		wantRaw    int
		wantCursor int
	}{
		{name: "events and cursor", batch: source.Batch{Events: testEvents(10), Cursor: testCursor(10)}, wantRaw: 10, wantCursor: 1},
		{name: "unset cursor writes no cursor record", batch: source.Batch{Events: testEvents(10)}, wantRaw: 10},
		{name: "cursor only", batch: source.Batch{Cursor: testCursor(1)}, wantCursor: 1},
		{name: "empty batch", batch: source.Batch{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClusterWithTopics(t)
			log := &produceLog{}
			log.observe(c)
			w := newTestWriter(t, c, testProducerLimits(), nil)

			if err := w.WriteBatch(testContext(t), tt.batch); err != nil {
				t.Fatalf("WriteBatch() error = %v", err)
			}
			if got := endOffset(t, c, TopicRaw); got != int64(tt.wantRaw) {
				t.Errorf("raw records = %d, want %d", got, tt.wantRaw)
			}
			if got := endOffset(t, c, TopicCursor); got != int64(tt.wantCursor) {
				t.Errorf("cursor records = %d, want %d", got, tt.wantCursor)
			}
			// The cursor goes out in its own request, after every raw
			// request.
			topics := log.snapshot()
			for i, topic := range topics {
				if topic == TopicCursor && i != len(topics)-1 {
					t.Errorf("produce requests %v: cursor is not last", topics)
				}
			}
		})
	}
}

func TestWriteBatchFailureWritesNoCursor(t *testing.T) {
	c := newTestClusterWithTopics(t)
	reg := prometheus.NewRegistry()
	w := newTestWriter(t, c, testProducerLimits(), reg)

	// INVALID_RECORD is not retriable, so kgo fails the records at once.
	c.Fault(kfake.Fault{
		Keys:  []kmsg.Key{kmsg.Produce},
		Topic: TopicRaw,
		Err:   kerr.InvalidRecord,
		Count: -1,
	})

	err := w.WriteBatch(testContext(t), source.Batch{Events: testEvents(5), Cursor: testCursor(5)})
	if err == nil {
		t.Fatal("WriteBatch() error = nil, want error")
	}
	if !errors.Is(err, kerr.InvalidRecord) {
		t.Errorf("WriteBatch() error = %v, want it to wrap %v", err, kerr.InvalidRecord)
	}
	if got := endOffset(t, c, TopicCursor); got != 0 {
		t.Errorf("cursor records = %d, want 0", got)
	}
	if n := testutil.ToFloat64(w.metrics.produceErrors.WithLabelValues(TopicRaw)); n == 0 {
		t.Error("produce_errors_total is 0, want > 0")
	}
	if n := testutil.ToFloat64(w.metrics.cursorWrites); n != 0 {
		t.Errorf("cursor_writes_total = %v, want 0", n)
	}
}

func TestWriteBatchDeliveryTimeout(t *testing.T) {
	c := newTestClusterWithTopics(t)
	limits := testProducerLimits()
	limits.DeliveryTimeout = time.Second // the kgo minimum
	w := newTestWriter(t, c, limits, nil)

	// NOT_ENOUGH_REPLICAS is retriable: kgo retries until the delivery
	// timeout fails the records.
	c.Fault(kfake.Fault{
		Keys:  []kmsg.Key{kmsg.Produce},
		Topic: TopicRaw,
		Err:   kerr.NotEnoughReplicas,
		Count: -1,
	})
	start := time.Now()
	err := w.WriteBatch(testContext(t), source.Batch{Events: testEvents(3), Cursor: testCursor(3)})
	if !errors.Is(err, kgo.ErrRecordTimeout) {
		t.Fatalf("WriteBatch() error = %v, want kgo.ErrRecordTimeout", err)
	}
	if d := time.Since(start); d > 8*time.Second {
		t.Errorf("WriteBatch() took %s with a 1s delivery timeout", d)
	}
	if got := endOffset(t, c, TopicCursor); got != 0 {
		t.Errorf("cursor records = %d, want 0", got)
	}
}

// holdProduce holds every produce request until release is called.
func holdProduce(t *testing.T, c *kfake.Cluster) (release func()) {
	t.Helper()
	ch := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(ch) }) }
	c.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.SleepControl(func() { <-ch })
		return nil, nil, false
	})
	// Release before the cluster closes (cleanups run last in, first out).
	t.Cleanup(release)
	return release
}

// waitBlocked waits until the client holds more records than its buffer
// limit, which means one Produce call is blocked.
func waitBlocked(t *testing.T, w *RawWriter, limit int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for w.client.BufferedProduceRecords() <= int64(limit) {
		if time.Now().After(deadline) {
			t.Fatalf("write did not block: %d records buffered, limit %d", w.client.BufferedProduceRecords(), limit)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestWriteBatchBackpressure(t *testing.T) {
	const bufferLimit = 2
	tests := []struct {
		name   string
		cancel bool
	}{
		{name: "unblocks when acks arrive"},
		{name: "cancelled context returns its error", cancel: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClusterWithTopics(t)
			release := holdProduce(t, c)
			limits := testProducerLimits()
			limits.MaxBufferedRecords = bufferLimit
			w := newTestWriter(t, c, limits, nil)

			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- w.WriteBatch(ctx, source.Batch{Events: testEvents(6), Cursor: testCursor(6)})
			}()

			waitBlocked(t, w, bufferLimit)
			select {
			case err := <-done:
				t.Fatalf("WriteBatch() returned %v while acks are held", err)
			case <-time.After(50 * time.Millisecond):
			}

			if tt.cancel {
				cancel()
				err := <-done
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("WriteBatch() error = %v, want context.Canceled", err)
				}
				release()
				if got := endOffset(t, c, TopicCursor); got != 0 {
					t.Errorf("cursor records = %d, want 0", got)
				}
				return
			}

			release()
			if err := <-done; err != nil {
				t.Fatalf("WriteBatch() error = %v", err)
			}
			if got := endOffset(t, c, TopicRaw); got != 6 {
				t.Errorf("raw records = %d, want 6", got)
			}
			if got := endOffset(t, c, TopicCursor); got != 1 {
				t.Errorf("cursor records = %d, want 1", got)
			}
			if s := testutil.ToFloat64(w.metrics.blocked); s <= 0 {
				t.Errorf("blocked_seconds_total = %v, want > 0", s)
			}
		})
	}
}

func TestWriteBatchCancelledBeforeStart(t *testing.T) {
	c := newTestClusterWithTopics(t)
	w := newTestWriter(t, c, testProducerLimits(), nil)
	ctx, cancel := context.WithCancel(testContext(t))
	cancel()
	err := w.WriteBatch(ctx, source.Batch{Events: testEvents(2), Cursor: testCursor(2)})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("WriteBatch() error = %v, want context.Canceled", err)
	}
}

func TestNewRawWriterInvalid(t *testing.T) {
	c := newTestCluster(t)
	valid := testProducerLimits()
	tests := []struct {
		name   string
		limits ProducerLimits
		source string
		reg    prometheus.Registerer
	}{
		{name: "zero records", limits: ProducerLimits{MaxBufferedBytes: 1, DeliveryTimeout: time.Second}, source: SourceBluesky, reg: prometheus.NewRegistry()},
		{name: "zero bytes", limits: ProducerLimits{MaxBufferedRecords: 1, DeliveryTimeout: time.Second}, source: SourceBluesky, reg: prometheus.NewRegistry()},
		{name: "zero timeout", limits: ProducerLimits{MaxBufferedRecords: 1, MaxBufferedBytes: 1}, source: SourceBluesky, reg: prometheus.NewRegistry()},
		{name: "timeout below 1s", limits: ProducerLimits{MaxBufferedRecords: 1, MaxBufferedBytes: 1, DeliveryTimeout: 500 * time.Millisecond}, source: SourceBluesky, reg: prometheus.NewRegistry()},
		{name: "empty source", limits: valid, reg: prometheus.NewRegistry()},
		{name: "nil registerer", limits: valid, source: SourceBluesky},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, err := NewRawWriter(c.ListenAddrs(), tt.limits, tt.source, discardLogger(), tt.reg)
			if err == nil {
				_ = w.Close(testContext(t))
				t.Fatal("NewRawWriter() error = nil, want error")
			}
		})
	}

	t.Run("metrics registered twice", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		first := newTestWriter(t, c, valid, reg)
		if _, err := NewRawWriter(c.ListenAddrs(), valid, SourceBluesky, discardLogger(), reg); err == nil {
			t.Fatal("second NewRawWriter() on one registry error = nil, want error")
		}
		// After Close the registry is free again, as at a restart.
		if err := first.Close(testContext(t)); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		second, err := NewRawWriter(c.ListenAddrs(), valid, SourceBluesky, discardLogger(), reg)
		if err != nil {
			t.Fatalf("NewRawWriter() after Close error = %v", err)
		}
		_ = second.Close(testContext(t))
	})
}

func BenchmarkWriteBatch(b *testing.B) {
	c := newTestClusterWithTopics(b)
	limits := ProducerLimits{MaxBufferedRecords: 10_000, MaxBufferedBytes: 32 << 20, DeliveryTimeout: time.Minute}
	w := newTestWriter(b, c, limits, nil)
	ctx := context.Background()

	const perBatch = 64
	keys := []string{testAlice, testBob, testCarol}
	payload := bytes.Repeat([]byte("x"), 1000)
	payload = append([]byte(`{"text":"synthetic reply one","pad":"`), payload...)
	payload = append(payload, `"}`...)
	events := make([]source.RawEvent, perBatch)
	for i := range events {
		events[i] = source.RawEvent{
			Key:       keys[i%len(keys)],
			ID:        fmt.Sprintf("c:%s:app.bsky.feed.post:bench%04d:rev1:create", keys[i%len(keys)], i),
			Kind:      source.KindCommit,
			Witnessed: testWitnessedBase,
			Payload:   payload,
		}
	}

	b.ReportAllocs()
	b.SetBytes(int64(perBatch * len(payload)))
	batches := 0
	for b.Loop() {
		batches++
		if err := w.WriteBatch(ctx, source.Batch{Events: events, Cursor: testCursor(batches)}); err != nil {
			b.Fatalf("WriteBatch() error = %v", err)
		}
	}
	b.ReportMetric(float64(batches*perBatch)/b.Elapsed().Seconds(), "events/s")
}
