package bluesky

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ZAUakaAlexey/threadroute/internal/source"
)

const (
	testPrimary  = "jetstream-primary.example.test"
	testFailover = "jetstream-failover.example.test"
)

// step is one item the fake stream yields: a batch or an error.
type step struct {
	events []jetstream.Event
	err    error
}

// fakeStream yields scripted steps, then waits for ctx when waitForCancel
// is set, or ends.
type fakeStream struct {
	steps         []step
	waitForCancel bool

	yielded atomic.Int32
	closed  atomic.Bool
}

func (f *fakeStream) Batches(ctx context.Context) iter.Seq2[[]jetstream.Event, error] {
	return func(yield func([]jetstream.Event, error) bool) {
		for _, s := range f.steps {
			f.yielded.Add(1)
			if !yield(s.events, s.err) {
				return
			}
		}
		if f.waitForCancel {
			<-ctx.Done()
		}
	}
}

func (f *fakeStream) Close() error {
	f.closed.Store(true)
	return nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testConfig(reg prometheus.Registerer) Config {
	return Config{
		PrimaryHost:  testPrimary,
		FailoverHost: testFailover,
		Collection:   testCollection,
		Logger:       testLogger(),
		Registerer:   reg,
	}
}

// newTestConnector returns a connector that reads from fs, with a fixed clock.
func newTestConnector(t *testing.T, fs *fakeStream, now time.Time) (*Connector, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	c, err := New(testConfig(reg))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.now = func() time.Time { return now }
	c.open = func(source.Cursor) (eventStream, error) { return fs, nil }
	return c, reg
}

func batchOf(events ...jetstream.Event) step { return step{events: events} }

func TestRunDeliversInOrder(t *testing.T) {
	fs := &fakeStream{
		steps: []step{
			batchOf(commitEvent(jetstream.OpCreate, 1, 1*time.Second)),
			batchOf(commitEvent(jetstream.OpUpdate, 2, 2*time.Second), accountEvent(false, 3, 3*time.Second)),
			batchOf(syncEvent(4, 4*time.Second)),
		},
		waitForCancel: true,
	}
	c, _ := newTestConnector(t, fs, testBase.Add(10*time.Second))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var seqs []uint64
	sink := func(_ context.Context, b source.Batch) error {
		seqs = append(seqs, b.Cursor.Seq)
		if len(seqs) == 3 {
			cancel()
		}
		return nil
	}
	if err := c.Run(ctx, source.Cursor{}, sink); err != nil {
		t.Fatalf("Run = %v, want nil after cancel", err)
	}
	want := []uint64{1, 3, 4}
	if fmt.Sprint(seqs) != fmt.Sprint(want) {
		t.Fatalf("cursor seqs = %v, want %v", seqs, want)
	}
	if !fs.closed.Load() {
		t.Fatalf("stream was not closed")
	}
}

func TestRunSinkError(t *testing.T) {
	sinkErr := errors.New("synthetic sink failure")
	fs := &fakeStream{steps: []step{
		batchOf(commitEvent(jetstream.OpCreate, 1, 0)),
		batchOf(commitEvent(jetstream.OpCreate, 2, time.Second)),
	}}
	c, _ := newTestConnector(t, fs, testBase)
	calls := 0
	err := c.Run(context.Background(), source.Cursor{}, func(context.Context, source.Batch) error {
		calls++
		return sinkErr
	})
	if !errors.Is(err, sinkErr) {
		t.Fatalf("Run = %v, want wrapped sink error", err)
	}
	if errors.Unwrap(err) == nil {
		t.Fatalf("sink error is not wrapped")
	}
	if calls != 1 || fs.yielded.Load() != 1 {
		t.Fatalf("sink calls = %d, yielded = %d, want 1 and 1", calls, fs.yielded.Load())
	}
	if got := testutil.ToFloat64(c.metrics.batches); got != 0 {
		t.Fatalf("batches_total = %v after a failed sink, want 0", got)
	}
}

func TestRunSinkContextErrorAfterCancelReturnsNil(t *testing.T) {
	fs := &fakeStream{steps: []step{batchOf(commitEvent(jetstream.OpCreate, 1, 0))}}
	c, _ := newTestConnector(t, fs, testBase)
	ctx, cancel := context.WithCancel(context.Background())
	err := c.Run(ctx, source.Cursor{}, func(ctx context.Context, _ source.Batch) error {
		cancel()
		return fmt.Errorf("produce: %w", ctx.Err())
	})
	if err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
}

func TestRunBlockedSinkBlocksReads(t *testing.T) {
	fs := &fakeStream{
		steps: []step{
			batchOf(commitEvent(jetstream.OpCreate, 1, 0)),
			batchOf(commitEvent(jetstream.OpCreate, 2, time.Second)),
		},
		waitForCancel: true,
	}
	c, _ := newTestConnector(t, fs, testBase)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	sink := func(context.Context, source.Batch) error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, source.Cursor{}, sink) }()

	<-entered
	time.Sleep(50 * time.Millisecond)
	if got := fs.yielded.Load(); got != 1 {
		t.Fatalf("stream yielded %d batches while the sink was blocked, want 1", got)
	}
	close(release)
	deadline := time.After(5 * time.Second)
	for calls.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("second batch was not delivered after release")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
}

func TestRunCancelReturnsNil(t *testing.T) {
	fs := &fakeStream{waitForCancel: true}
	c, _ := newTestConnector(t, fs, testBase)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.Run(ctx, source.Cursor{}, func(context.Context, source.Batch) error { return nil })
	}()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Run did not return after cancel")
	}
}

func TestRunStreamEndsWithoutError(t *testing.T) {
	fs := &fakeStream{}
	c, _ := newTestConnector(t, fs, testBase)
	err := c.Run(context.Background(), source.Cursor{}, func(context.Context, source.Batch) error { return nil })
	if !errors.Is(err, errStreamEnded) {
		t.Fatalf("Run = %v, want errStreamEnded", err)
	}
}

func TestRunStreamErrors(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantFatal   bool
		wantType    string // errors_total label, empty if not counted there
		wantClamped float64
	}{
		{
			name:     "reconnect is recoverable",
			err:      errors.New("jetstream: live tail reconnecting: dial: synthetic refused"),
			wantType: errTypeReconnect,
		},
		{
			name:     "consumer too slow is recoverable",
			err:      errors.New("jetstream: live tail reconnecting: jetstream: live stream error: ConsumerTooSlow: synthetic"),
			wantType: errTypeConsumerTooSlow,
		},
		{
			name:     "bad frame is recoverable",
			err:      errors.New("jetstream: decode live frame: synthetic"),
			wantType: errTypeDecode,
		},
		{
			name:        "clamped cursor is counted and recoverable",
			err:         fmt.Errorf("%w: synthetic floor", jetstream.ErrCursorClamped),
			wantClamped: 1,
		},
		{
			name:      "fatal error returns",
			err:       fmt.Errorf("%w: %w", jetstream.ErrFatal, errors.New("synthetic cursor too old")),
			wantFatal: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &fakeStream{steps: []step{
				batchOf(commitEvent(jetstream.OpCreate, 1, 0)),
				{err: tt.err},
				batchOf(commitEvent(jetstream.OpCreate, 2, time.Second)),
			}}
			c, _ := newTestConnector(t, fs, testBase)
			calls := 0
			err := c.Run(context.Background(), source.Cursor{}, func(context.Context, source.Batch) error {
				calls++
				return nil
			})
			if tt.wantFatal {
				if !errors.Is(err, jetstream.ErrFatal) {
					t.Fatalf("Run = %v, want wrapped ErrFatal", err)
				}
				if calls != 1 {
					t.Fatalf("sink calls = %d, want 1", calls)
				}
				return
			}
			if !errors.Is(err, errStreamEnded) {
				t.Fatalf("Run = %v, want the loop to continue to the end", err)
			}
			if calls != 2 {
				t.Fatalf("sink calls = %d, want 2", calls)
			}
			if tt.wantType != "" {
				if got := testutil.ToFloat64(c.metrics.errors.WithLabelValues(tt.wantType)); got != 1 {
					t.Fatalf("errors_total{type=%q} = %v, want 1", tt.wantType, got)
				}
			}
			if got := testutil.ToFloat64(c.metrics.cursorClamped); got != tt.wantClamped {
				t.Fatalf("cursor_clamped_total = %v, want %v", got, tt.wantClamped)
			}
		})
	}
}

func TestRunMetrics(t *testing.T) {
	zeroWit := syncEvent(5, 0)
	zeroWit.WitnessedAtUS = 0
	identity := jetstream.Event{DID: testDID, Seq: 6, WitnessedAtUS: witnessedUS(0), Kind: jetstream.KindIdentity, Identity: &jetstream.Identity{DID: testDID}}
	noDID := commitEvent(jetstream.OpCreate, 7, 0)
	noDID.DID = ""

	fs := &fakeStream{steps: []step{
		batchOf(commitEvent(jetstream.OpCreate, 1, 1*time.Second), commitEvent(jetstream.OpDelete, 2, 2*time.Second)),
		batchOf(accountEvent(false, 3, 3*time.Second), zeroWit, identity, noDID),
		// A batch where every event is skipped does not reach the sink.
		batchOf(identity),
	}}
	now := testBase.Add(10 * time.Second)
	c, reg := newTestConnector(t, fs, now)

	var payload int
	var delivered int
	err := c.Run(context.Background(), source.Cursor{}, func(_ context.Context, b source.Batch) error {
		delivered++
		for _, ev := range b.Events {
			payload += len(ev.Payload)
		}
		return nil
	})
	if !errors.Is(err, errStreamEnded) {
		t.Fatalf("Run = %v", err)
	}
	if delivered != 2 {
		t.Fatalf("sink calls = %d, want 2", delivered)
	}

	m := c.metrics
	checks := []struct {
		name string
		c    prometheus.Collector
		want float64
	}{
		{"events commit", m.events.WithLabelValues("commit"), 2},
		{"events account", m.events.WithLabelValues("account"), 1},
		{"events sync", m.events.WithLabelValues("sync"), 1},
		{"skipped unsubscribed", m.skipped.WithLabelValues(skipUnsubscribedKind), 2},
		{"skipped missing did", m.skipped.WithLabelValues(skipMissingDID), 1},
		{"batches", m.batches, 2},
		{"payload bytes", m.payloadBytes, float64(payload)},
		{"zero witnessed", m.zeroWitnessed, 1},
		{"last witnessed", m.lastWitnessed, float64(testBase.Add(3*time.Second).UnixMicro()) / 1e6},
		{"lag", m.lag, 7},
	}
	for _, ch := range checks {
		if got := testutil.ToFloat64(ch.c); got != ch.want {
			t.Errorf("%s = %v, want %v", ch.name, got, ch.want)
		}
	}
	// Every metric is registered under the threadroute_source_ prefix.
	n, err := testutil.GatherAndCount(reg,
		"threadroute_source_events_total", "threadroute_source_skipped_total",
		"threadroute_source_batches_total", "threadroute_source_payload_bytes_total",
		"threadroute_source_errors_total", "threadroute_source_cursor_clamped_total",
		"threadroute_source_zero_witnessed_total", "threadroute_source_last_witnessed_timestamp_seconds",
		"threadroute_source_lag_seconds")
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	// 3 kinds + 4 skip reasons + 5 error types + 6 single metrics.
	if n != 18 {
		t.Errorf("registered series = %d, want 18", n)
	}
	if payload == 0 {
		t.Fatalf("no payload bytes delivered")
	}
}

func TestLagBeforeFirstBatchIsZero(t *testing.T) {
	c, _ := newTestConnector(t, &fakeStream{}, testBase)
	if got := testutil.ToFloat64(c.metrics.lag); got != 0 {
		t.Fatalf("lag_seconds = %v, want 0", got)
	}
}

func TestRunNilSink(t *testing.T) {
	c, _ := newTestConnector(t, &fakeStream{}, testBase)
	if err := c.Run(context.Background(), source.Cursor{}, nil); err == nil {
		t.Fatalf("Run with nil sink = nil, want error")
	}
}

func TestRunOpenError(t *testing.T) {
	c, _ := newTestConnector(t, &fakeStream{}, testBase)
	openErr := errors.New("synthetic open failure")
	c.open = func(source.Cursor) (eventStream, error) { return nil, openErr }
	err := c.Run(context.Background(), source.Cursor{}, func(context.Context, source.Batch) error { return nil })
	if !errors.Is(err, openErr) {
		t.Fatalf("Run = %v, want wrapped open error", err)
	}
}

func TestRunPassesCursorToOpen(t *testing.T) {
	c, _ := newTestConnector(t, &fakeStream{}, testBase)
	from := source.Cursor{Witnessed: testBase, Seq: 42, Host: testPrimary}
	var got source.Cursor
	var mu sync.Mutex
	c.open = func(f source.Cursor) (eventStream, error) {
		mu.Lock()
		got = f
		mu.Unlock()
		return &fakeStream{}, nil
	}
	_ = c.Run(context.Background(), from, func(context.Context, source.Batch) error { return nil })
	mu.Lock()
	defer mu.Unlock()
	if got != from {
		t.Fatalf("open got %+v, want %+v", got, from)
	}
}

func TestCursorMicros(t *testing.T) {
	tests := []struct {
		name    string
		from    source.Cursor
		want    uint64
		wantErr bool
	}{
		{name: "unset means live", from: source.Cursor{}, want: 0},
		{name: "witnessed time", from: source.Cursor{Witnessed: testBase, Seq: 5}, want: uint64(testBase.UnixMicro())},
		{name: "too old for a time cursor", from: source.Cursor{Witnessed: time.Unix(1000, 0)}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cursorMicros(tt.from)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestClassifyError(t *testing.T) {
	tests := []struct {
		msg  string
		want string
	}{
		{"jetstream: live tail reconnecting: read: EOF", errTypeReconnect},
		{"jetstream: live tail reconnecting: jetstream: live stream error: ConsumerTooSlow", errTypeConsumerTooSlow},
		{"jetstream: zstd frame decode: bad", errTypeZstd},
		{"jetstream: decode live payload: bad", errTypeDecode},
		{"jetstream: live frame with invalid seq 0", errTypeDecode},
		{"something new", errTypeOther},
	}
	for _, tt := range tests {
		t.Run(tt.msg, func(t *testing.T) {
			if got := classifyError(errors.New(tt.msg)); got != tt.want {
				t.Fatalf("classifyError = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNewValidatesConfig(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Config)
	}{
		{"empty primary host", func(c *Config) { c.PrimaryHost = "" }},
		{"empty failover host", func(c *Config) { c.FailoverHost = "" }},
		{"same hosts", func(c *Config) { c.FailoverHost = c.PrimaryHost }},
		{"empty collection", func(c *Config) { c.Collection = "" }},
		{"invalid collection", func(c *Config) { c.Collection = "not a collection" }},
		{"invalid host scheme", func(c *Config) { c.PrimaryHost = "ftp://jetstream.example.test" }},
		{"nil logger", func(c *Config) { c.Logger = nil }},
		{"nil registerer", func(c *Config) { c.Registerer = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(prometheus.NewRegistry())
			tt.change(&cfg)
			if _, err := New(cfg); err == nil {
				t.Fatalf("New = nil error, want error")
			}
		})
	}
}

func TestNewRegistersMetricsOnce(t *testing.T) {
	reg := prometheus.NewRegistry()
	if _, err := New(testConfig(reg)); err != nil {
		t.Fatalf("first New: %v", err)
	}
	if _, err := New(testConfig(reg)); err == nil {
		t.Fatalf("second New on the same registry = nil error, want a register error")
	}
}
