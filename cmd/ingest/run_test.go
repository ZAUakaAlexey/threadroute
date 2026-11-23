package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
	"github.com/ZAUakaAlexey/threadroute/internal/config"
	"github.com/ZAUakaAlexey/threadroute/internal/source"
)

// Synthetic, clearly fake test data (decision 0004).
var (
	testAuthors = []string{
		"did:plc:testalice0000000000000000",
		"did:plc:testbob000000000000000000",
		"did:plc:testcarol0000000000000000",
	}
	testBase = time.UnixMicro(1_700_000_000_000_000).UTC()
)

// testBatches returns n batches of size events each. Every batch has a
// cursor with a later witnessed time than the batch before.
func testBatches(n, size int) []source.Batch {
	batches := make([]source.Batch, n)
	i := 0
	for b := range batches {
		events := make([]source.RawEvent, size)
		for e := range events {
			key := testAuthors[i%len(testAuthors)]
			events[e] = source.RawEvent{
				Key:       key,
				ID:        fmt.Sprintf("c:%s:app.bsky.feed.post:synthetic%04d:rev1:create", key, i),
				Kind:      source.KindCommit,
				Witnessed: testBase.Add(time.Duration(i) * time.Millisecond),
				Payload:   fmt.Appendf(nil, `{"did":%q,"kind":"commit","n":%d}`, key, i),
			}
			i++
		}
		batches[b] = source.Batch{
			Events: events,
			Cursor: source.Cursor{
				Host:      "jetstream-east.example.test",
				Witnessed: testBase.Add(time.Duration(i-1) * time.Millisecond),
				Seq:       uint64(1000 + i),
			},
		}
	}
	return batches
}

// fakeConnector hands its batches to the sink, then waits for ctx.
type fakeConnector struct {
	batches []source.Batch
	// afterStop delivers the batches only after ctx ended, to check that
	// the batch in progress may finish during a stop.
	afterStop bool
	// fail is returned at once when set.
	fail error
	// release, when set, holds the batches back until it is closed.
	release chan struct{}

	mu   sync.Mutex
	from []source.Cursor
}

func (f *fakeConnector) Run(ctx context.Context, from source.Cursor, sink source.Sink) error {
	f.mu.Lock()
	f.from = append(f.from, from)
	f.mu.Unlock()
	if f.fail != nil {
		return f.fail
	}
	if f.afterStop {
		<-ctx.Done()
	}
	if f.release != nil {
		select {
		case <-f.release:
		case <-ctx.Done():
			return nil
		}
	}
	for _, b := range f.batches {
		if err := sink(ctx, b); err != nil {
			return fmt.Errorf("fake sink: %w", err)
		}
	}
	<-ctx.Done()
	return nil
}

func (f *fakeConnector) starts() []source.Cursor {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]source.Cursor(nil), f.from...)
}

func (f *fakeConnector) factory() connectorFactory {
	return func(config.Config, *slog.Logger, prometheus.Registerer) (source.Connector, error) {
		return f, nil
	}
}

func testTimings() timings {
	return timings{
		connectTimeout: 3 * time.Second,
		pingTimeout:    500 * time.Millisecond,
		backoffMin:     50 * time.Millisecond,
		backoffMax:     200 * time.Millisecond,
		batchGrace:     5 * time.Second,
		closeTimeout:   2 * time.Second,
	}
}

func testConfig(t *testing.T, brokers []string) config.Config {
	t.Helper()
	cfg, err := config.Load(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatalf("config.Load() error = %v", err)
	}
	cfg.KafkaBrokers = brokers
	cfg.MetricsAddr = freeAddr(t)
	cfg.ProducerDeliveryTimeout = 10 * time.Second
	cfg.RawLimits = bus.RawLimits{Retention: 2 * time.Hour, MaxBytes: 4 * bus.RawMinMaxBytes}
	return cfg
}

// freeAddr returns a local address that was free a moment ago.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func newCluster(t *testing.T) *kfake.Cluster {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("kfake.NewCluster() error = %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

// startRun runs run in a goroutine and returns a function that stops it and
// returns its error.
func startRun(t *testing.T, cfg config.Config, conn *fakeConnector) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	errc := make(chan error, 1)
	go func() {
		errc <- run(ctx, cfg, discard(), prometheus.NewRegistry(), conn.factory(), testTimings())
	}()
	var once sync.Once
	var err error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-errc:
			case <-time.After(15 * time.Second):
				err = errors.New("run did not return within 15s of the stop")
			}
		})
		return err
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func httpGet(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		return 0, ""
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}

// eventually calls cond until it returns true or 10 seconds pass.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func storedCursor(t *testing.T, c *kfake.Cluster) (source.Cursor, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cur, ok, err := bus.ReadCursor(ctx, c.ListenAddrs(), bus.SourceBluesky, discard())
	if err != nil {
		return source.Cursor{}, false
	}
	return cur, ok
}

// rawRecords reads every record of the raw topic.
func rawRecords(t *testing.T, c *kfake.Cluster) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumeTopics(bus.TopicRaw),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, bus.TopicRaw)
	if err != nil || ends.Error() != nil {
		t.Fatalf("ListEndOffsets() error = %v, %v", err, ends.Error())
	}
	want := 0
	ends.Each(func(o kadm.ListedOffset) { want += int(o.Offset) })
	var out []*kgo.Record
	for len(out) < want {
		f := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %d of %d raw records: %v", len(out), want, ctx.Err())
		}
		out = append(out, f.Records()...)
	}
	return out
}

func TestRunWritesBatchesAndResumes(t *testing.T) {
	c := newCluster(t)
	cfg := testConfig(t, c.ListenAddrs())
	batches := testBatches(3, 4)
	last := batches[len(batches)-1].Cursor

	conn := &fakeConnector{batches: batches}
	stop := startRun(t, cfg, conn)

	eventually(t, "the last cursor in kafka", func() bool {
		cur, ok := storedCursor(t, c)
		return ok && cur.Witnessed.Equal(last.Witnessed)
	})
	cur, _ := storedCursor(t, c)
	if cur.Seq != last.Seq || cur.Host != last.Host {
		t.Errorf("stored cursor = %+v, want %+v", cur, last)
	}

	records := rawRecords(t, c)
	if len(records) != 12 {
		t.Fatalf("got %d raw records, want 12", len(records))
	}
	for _, r := range records {
		if !strings.HasPrefix(string(r.Key), "did:plc:test") {
			t.Errorf("unexpected key %q", r.Key)
		}
		if len(r.Value) == 0 || len(r.Headers) < 2 {
			t.Errorf("record without value or headers at offset %d", r.Offset)
		}
	}

	base := "http://" + cfg.MetricsAddr
	if code, _ := httpGet(t, base+"/healthz"); code != http.StatusOK {
		t.Errorf("healthz status %d while running, want 200", code)
	}
	_, metrics := httpGet(t, base+"/metrics")
	for _, want := range []string{
		`threadroute_bus_produced_records_total{topic="raw.bluesky"} 12`,
		`threadroute_bus_cursor_writes_total 3`,
		"process_",
	} {
		if !strings.Contains(metrics, want) {
			t.Errorf("metrics do not contain %q", want)
		}
	}

	if err := stop(); err != nil {
		t.Fatalf("run() after stop = %v, want nil", err)
	}
	if code, _ := httpGet(t, base+"/healthz"); code != 0 {
		t.Errorf("metrics server still answers after run returned (status %d)", code)
	}
	if starts := conn.starts(); len(starts) != 1 || !starts[0].IsZero() {
		t.Errorf("first start from %+v, want one start from live", starts)
	}

	// A second run resumes from the stored cursor.
	again := &fakeConnector{}
	cfg.MetricsAddr = freeAddr(t)
	stop = startRun(t, cfg, again)
	eventually(t, "the second connector start", func() bool { return len(again.starts()) == 1 })
	if err := stop(); err != nil {
		t.Fatalf("second run() = %v, want nil", err)
	}
	if got := again.starts()[0]; !got.Witnessed.Equal(last.Witnessed) || got.Seq != last.Seq {
		t.Errorf("second start from %+v, want %+v", got, last)
	}
}

func TestRunFinishesBatchAfterStop(t *testing.T) {
	c := newCluster(t)
	cfg := testConfig(t, c.ListenAddrs())
	batches := testBatches(1, 5)
	conn := &fakeConnector{batches: batches, afterStop: true}
	stop := startRun(t, cfg, conn)

	eventually(t, "the connector start", func() bool { return len(conn.starts()) == 1 })
	if err := stop(); err != nil {
		t.Fatalf("run() = %v, want nil", err)
	}
	cur, ok := storedCursor(t, c)
	if !ok || !cur.Witnessed.Equal(batches[0].Cursor.Witnessed) {
		t.Errorf("cursor after stop = %+v, %v, want the cursor of the last batch", cur, ok)
	}
	if n := len(rawRecords(t, c)); n != 5 {
		t.Errorf("got %d raw records, want 5", n)
	}
}

func TestRunConnectorErrorIsFatal(t *testing.T) {
	c := newCluster(t)
	cfg := testConfig(t, c.ListenAddrs())
	boom := errors.New("synthetic fatal error")
	err := run(t.Context(), cfg, discard(), prometheus.NewRegistry(), (&fakeConnector{fail: boom}).factory(), testTimings())
	if !errors.Is(err, boom) {
		t.Fatalf("run() = %v, want it to wrap %v", err, boom)
	}
}

func TestRunKafkaUnreachable(t *testing.T) {
	// Port 1 on the loopback address refuses connections.
	cfg := testConfig(t, []string{"127.0.0.1:1"})
	tm := testTimings()
	tm.connectTimeout = 500 * time.Millisecond

	start := time.Now()
	err := run(t.Context(), cfg, discard(), prometheus.NewRegistry(), (&fakeConnector{}).factory(), tm)
	if err == nil || !strings.Contains(err.Error(), "kafka not reachable") {
		t.Fatalf("run() = %v, want a kafka not reachable error", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("run() gave up after %s, want about %s", took, tm.connectTimeout)
	}
}

func TestRunStopDuringKafkaWait(t *testing.T) {
	cfg := testConfig(t, []string{"127.0.0.1:1"})
	tm := testTimings()
	tm.connectTimeout = time.Minute
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if err := run(ctx, cfg, discard(), prometheus.NewRegistry(), (&fakeConnector{}).factory(), tm); err != nil {
		t.Fatalf("run() = %v, want nil after a stop during start", err)
	}
}

func TestRunReadyAfterFirstBatch(t *testing.T) {
	c := newCluster(t)
	cfg := testConfig(t, c.ListenAddrs())
	conn := &fakeConnector{batches: testBatches(1, 3), release: make(chan struct{})}
	stop := startRun(t, cfg, conn)
	health := "http://" + cfg.MetricsAddr + "/healthz"

	eventually(t, "the connector start", func() bool { return len(conn.starts()) == 1 })
	if code, _ := httpGet(t, health); code != http.StatusServiceUnavailable {
		t.Errorf("healthz status %d before the first batch, want 503", code)
	}

	close(conn.release)
	eventually(t, "ready after the first batch", func() bool {
		code, _ := httpGet(t, health)
		return code == http.StatusOK
	})
	if _, ok := storedCursor(t, c); !ok {
		t.Error("ready, but no cursor in kafka")
	}
	if err := stop(); err != nil {
		t.Fatalf("run() = %v, want nil", err)
	}
}
