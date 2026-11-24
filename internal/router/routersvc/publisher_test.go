package routersvc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/router"
)

// fakeStore is an outbox in memory.
type fakeStore struct {
	mu      sync.Mutex
	rows    []router.OutboxRecord
	sent    map[int64]bool
	markErr error
}

func newFakeStore(n int) *fakeStore {
	s := &fakeStore{sent: map[int64]bool{}}
	for i := range n {
		key := fmt.Sprintf("test-co|test-cu-%d", i%3)
		s.rows = append(s.rows, router.OutboxRecord{
			ID: int64(i + 1), Key: []byte(key), Value: fmt.Appendf(nil, `{"n":%d}`, i), Type: event.TypeQueued,
			Seq: uint64(i/3 + 1),
		})
	}
	return s
}

func (s *fakeStore) NextOutbox(_ context.Context, limit int) ([]router.OutboxRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []router.OutboxRecord
	for _, r := range s.rows {
		if !s.sent[r.ID] && len(out) < limit {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s *fakeStore) MarkSent(_ context.Context, ids []int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.markErr != nil {
		return 0, s.markErr
	}
	var n int64
	for _, id := range ids {
		if !s.sent[id] {
			n++
		}
		s.sent[id] = true
	}
	return n, nil
}

func (s *fakeStore) unsent() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rows) - len(s.sent)
}

// fakeProducer records produced values. failAt makes the record at that
// index of the next call fail, and every later record of the call too.
type fakeProducer struct {
	mu       sync.Mutex
	produced []string
	failAt   int
}

func (p *fakeProducer) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(kgo.ProduceResults, len(rs))
	for i, r := range rs {
		out[i].Record = r
		if p.failAt >= 0 && i >= p.failAt {
			out[i].Err = errors.New("broker gone")
			continue
		}
		p.produced = append(p.produced, string(r.Value))
	}
	p.failAt = -1
	return out
}

func (p *fakeProducer) values() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.produced)
}

// fakeLock is one lock shared by several publishers.
type fakeLock struct {
	mu     sync.Mutex
	holder string
}

type fakeLocker struct {
	lock    *fakeLock
	name    string
	lost    bool
	tryErr  error
	unlocks int
}

func (l *fakeLocker) TryLock(context.Context) (bool, error) {
	if l.tryErr != nil {
		return false, l.tryErr
	}
	l.lock.mu.Lock()
	defer l.lock.mu.Unlock()
	if l.lock.holder == "" {
		l.lock.holder = l.name
	}
	return l.lock.holder == l.name, nil
}

func (l *fakeLocker) Check(context.Context) error {
	if l.lost {
		l.lock.mu.Lock()
		if l.lock.holder == l.name {
			l.lock.holder = ""
		}
		l.lock.mu.Unlock()
		return errors.New("connection closed")
	}
	return nil
}

func (l *fakeLocker) Unlock(context.Context) {
	l.lock.mu.Lock()
	defer l.lock.mu.Unlock()
	l.unlocks++
	if l.lock.holder == l.name {
		l.lock.holder = ""
	}
}

func newTestPublisher(t *testing.T, store outboxStore, prod producer, lock locker, batch int) (*Publisher, *metrics) {
	t.Helper()
	m, err := newMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	r, _ := newTestRunner(newFakeTimer(), time.Minute)
	return &Publisher{
		store: store, producer: prod, lock: lock, topic: "routing-test", batch: batch,
		interval: time.Millisecond, runner: r, m: m, logger: slog.New(slog.DiscardHandler),
	}, m
}

func TestPublisherInOrderAndBatches(t *testing.T) {
	store := newFakeStore(7)
	prod := &fakeProducer{failAt: -1}
	p, m := newTestPublisher(t, store, prod, &fakeLocker{lock: &fakeLock{}, name: "a"}, 3)
	for i, wantMore := range []bool{true, true, false, false} {
		more, err := p.step(t.Context())
		if err != nil || more != wantMore {
			t.Fatalf("step %d = %v, %v; want more=%v", i, more, err, wantMore)
		}
	}
	want := make([]string, 7)
	for i := range want {
		want[i] = fmt.Sprintf(`{"n":%d}`, i)
	}
	if got := prod.values(); !slices.Equal(got, want) {
		t.Errorf("produced %v, want %v", got, want)
	}
	if store.unsent() != 0 || p.Sent() != 7 || !p.Holding() {
		t.Errorf("unsent %d, sent %d, holding %v", store.unsent(), p.Sent(), p.Holding())
	}
	if got := testutil.ToFloat64(m.published.WithLabelValues(string(event.TypeQueued))); got != 7 {
		t.Errorf("published queued = %v, want 7", got)
	}
	if got := testutil.ToFloat64(m.lockHeld); got != 1 {
		t.Errorf("lock held gauge = %v", got)
	}
}

// TestPublisherProduceError checks that only the rows before the first
// failed record are marked, and the next round starts from the first
// unsent row.
func TestPublisherProduceError(t *testing.T) {
	store := newFakeStore(5)
	prod := &fakeProducer{failAt: 2}
	p, m := newTestPublisher(t, store, prod, &fakeLocker{lock: &fakeLock{}, name: "a"}, 10)
	if _, err := p.step(t.Context()); err == nil {
		t.Fatal("step with a produce error = nil error")
	}
	if store.unsent() != 3 || p.Sent() != 2 {
		t.Fatalf("after the error: unsent %d, sent %d; want 3 and 2", store.unsent(), p.Sent())
	}
	if _, err := p.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := []string{`{"n":0}`, `{"n":1}`, `{"n":2}`, `{"n":3}`, `{"n":4}`}
	if got := prod.values(); !slices.Equal(got, want) {
		t.Errorf("produced %v, want %v", got, want)
	}
	if got := testutil.ToFloat64(m.publishErrors); got != 1 {
		t.Errorf("publish errors = %v", got)
	}
}

func TestPublisherMarkError(t *testing.T) {
	store := newFakeStore(2)
	store.markErr = errors.New("db down")
	prod := &fakeProducer{failAt: -1}
	p, _ := newTestPublisher(t, store, prod, &fakeLocker{lock: &fakeLock{}, name: "a"}, 10)
	if _, err := p.step(t.Context()); err == nil {
		t.Fatal("step with a mark error = nil error")
	}
	store.markErr = nil
	if _, err := p.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	// The rows were sent twice, as exact copies, in the same order.
	want := []string{`{"n":0}`, `{"n":1}`, `{"n":0}`, `{"n":1}`}
	if got := prod.values(); !slices.Equal(got, want) {
		t.Errorf("produced %v, want %v", got, want)
	}
}

func TestPublisherOnlyLockHolderPublishes(t *testing.T) {
	store := newFakeStore(4)
	shared := &fakeLock{}
	prodA, prodB := &fakeProducer{failAt: -1}, &fakeProducer{failAt: -1}
	lockA := &fakeLocker{lock: shared, name: "a"}
	lockB := &fakeLocker{lock: shared, name: "b"}
	a, _ := newTestPublisher(t, store, prodA, lockA, 2)
	b, _ := newTestPublisher(t, store, prodB, lockB, 2)

	if _, err := a.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := b.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if b.Holding() || len(prodB.values()) != 0 {
		t.Fatalf("b holds %v and produced %v while a holds the lock", b.Holding(), prodB.values())
	}
	// a loses its connection: the next round fails and lets the lock go.
	lockA.lost = true
	if _, err := a.step(t.Context()); err == nil || a.Holding() {
		t.Fatalf("a after the lost lock: holding %v, err %v", a.Holding(), err)
	}
	if _, err := b.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !b.Holding() || store.unsent() != 0 {
		t.Fatalf("b holding %v, unsent %d", b.Holding(), store.unsent())
	}
	if got := append(prodA.values(), prodB.values()...); len(got) != 4 {
		t.Errorf("produced %v, want 4 rows once", got)
	}
}

func TestPublisherRunUnlocks(t *testing.T) {
	lock := &fakeLocker{lock: &fakeLock{}, name: "a"}
	p, _ := newTestPublisher(t, newFakeStore(0), &fakeProducer{failAt: -1}, lock, 10)
	p.runner.timer = realTimer{}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for !p.Holding() {
		if time.Now().After(deadline) {
			t.Fatal("publisher never took the lock")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if p.Holding() || lock.unlocks != 1 || lock.lock.holder != "" {
		t.Errorf("after Run: holding %v, unlocks %d, holder %q", p.Holding(), lock.unlocks, lock.lock.holder)
	}
}

func TestPublisherLockError(t *testing.T) {
	lock := &fakeLocker{lock: &fakeLock{}, name: "a", tryErr: errors.New("no connection")}
	p, _ := newTestPublisher(t, newFakeStore(1), &fakeProducer{failAt: -1}, lock, 10)
	if _, err := p.step(t.Context()); err == nil || p.Holding() {
		t.Fatalf("step = %v, holding %v", err, p.Holding())
	}
}

func TestPublisherCountsSLATargets(t *testing.T) {
	at := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	ev := event.Routing{
		Type: event.TypeSLAMissed, CompanyID: "test-co", CustomerID: "test-cu", Session: 1, Seq: 2, At: at,
		Lang: "en", QueuedAt: at, SLATarget: event.SLAFirstResponse, Measured: 2 * time.Minute, Target: 2 * time.Minute,
	}
	b, err := event.MarshalRouting(ev)
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{sent: map[int64]bool{}, rows: []router.OutboxRecord{
		{ID: 1, Key: ev.Key(), Value: b, Type: event.TypeSLAMissed, Seq: 2},
	}}
	p, m := newTestPublisher(t, store, &fakeProducer{failAt: -1}, &fakeLocker{lock: &fakeLock{}, name: "a"}, 10)
	if _, err := p.step(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(m.slaMissed.WithLabelValues(string(event.SLAFirstResponse))); got != 1 {
		t.Errorf("sla_missed first_response = %v, want 1", got)
	}
}

// TestPublisherKafkaOutage runs a publisher against a broker address where
// nothing listens: every round fails after the delivery timeout, and Run
// returns an error once the outage limit has passed, so the process stops.
func TestPublisherKafkaOutage(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordDeliveryTimeout(time.Second),
		kgo.RecordRetries(2))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	store := newFakeStore(3)
	p, m := newTestPublisher(t, store, cl, &fakeLocker{lock: &fakeLock{}, name: "a"}, 10)
	p.runner.timer = realTimer{}
	p.runner.outageLimit = 2 * time.Second
	done := make(chan error, 1)
	go func() { done <- p.Run(t.Context()) }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "outage limit") {
			t.Fatalf("Run() = %v, want the outage error", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not stop on the outage")
	}
	if store.unsent() != 3 || testutil.ToFloat64(m.publishErrors) == 0 {
		t.Errorf("unsent %d, publish errors %v", store.unsent(), testutil.ToFloat64(m.publishErrors))
	}
}
