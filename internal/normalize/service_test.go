package normalize

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

const testGroup = "normalize-test"

// newTestCluster starts a one-broker fake cluster with the raw topic.
func newTestCluster(t testing.TB) *kfake.Cluster {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("kfake.NewCluster() error = %v", err)
	}
	t.Cleanup(c.Close)
	createRawTopic(t, c.ListenAddrs())
	return c
}

func createRawTopic(t testing.TB, brokers []string) {
	t.Helper()
	raw, err := bus.RawTopicSpec(bus.RawLimits{Retention: 2 * time.Hour, MaxBytes: 4 * bus.RawMinMaxBytes})
	if err != nil {
		t.Fatal(err)
	}
	cl := newClient(t, brokers)
	if err := bus.EnsureTopics(testContext(t), cl, nil, raw); err != nil {
		t.Fatalf("EnsureTopics(raw) error = %v", err)
	}
}

func newClient(t testing.TB, brokers []string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)...)
	if err != nil {
		t.Fatalf("kgo.NewClient() error = %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func testConfig(brokers []string, redisAddr string) Config {
	return Config{
		KafkaBrokers:    brokers,
		GroupID:         testGroup,
		TransactionalID: "normalize-test-txn",
		Start:           StartBeginning,
		MaxBatchRecords: 100,
		RawTopicWait:    10 * time.Second,
		Producer: bus.ProducerLimits{
			MaxBufferedRecords: 1000,
			MaxBufferedBytes:   8 << 20,
			DeliveryTimeout:    10 * time.Second,
		},
		RedisAddr:        redisAddr,
		DedupKeyLifetime: 10 * time.Minute,
	}
}

// produceRaw writes records to the raw topic, as ingest does.
func produceRaw(t testing.TB, brokers []string, recs ...*kgo.Record) {
	t.Helper()
	cl := newClient(t, brokers)
	if err := cl.ProduceSync(testContext(t), recs...).FirstErr(); err != nil {
		t.Fatalf("produce raw: %v", err)
	}
}

// running is a Service started in a goroutine.
type running struct {
	svc    *Service
	cancel context.CancelFunc
	done   chan error
}

func startService(t testing.TB, cfg Config, rdb redis.Cmdable, change func(*Service)) *running {
	t.Helper()
	svc, err := New(cfg, rdb, nil, prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if change != nil {
		change(svc)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{svc: svc, cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-r.done:
		case <-time.After(30 * time.Second):
			t.Error("service did not stop")
		}
	})
	return r
}

// stop cancels the service and returns the error of Run.
func (r *running) stop(t testing.TB) error {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		r.done <- err // for the cleanup
		return err
	case <-time.After(30 * time.Second):
		t.Fatal("service did not stop")
		return nil
	}
}

// wait returns the error of Run, which must end on its own.
func (r *running) wait(t testing.TB) error {
	t.Helper()
	select {
	case err := <-r.done:
		r.done <- err
		return err
	case <-time.After(30 * time.Second):
		t.Fatal("service did not end")
		return nil
	}
}

// waitCommitted waits until the committed transactions hold n raw records.
func (r *running) waitCommitted(t testing.TB, n int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for testutil.ToFloat64(r.svc.metrics.rawRecords) < float64(n) {
		select {
		case err := <-r.done:
			t.Fatalf("service ended early: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("committed %v raw records, want %d", testutil.ToFloat64(r.svc.metrics.rawRecords), n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// readOutput reads every record of a topic with the given isolation level.
// It reads until want records arrived and then for quiet more, so a test
// also sees records it did not expect. Records of aborted transactions and
// transaction markers have offsets but are not returned with read committed,
// so the count of the end offsets cannot be used.
func readOutput(t testing.TB, brokers []string, topic string, level kgo.IsolationLevel, want int, quiet time.Duration) []*kgo.Record {
	t.Helper()
	ctx := testContext(t)
	cl := newClient(t, brokers,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(level),
	)
	defer cl.Close()
	var out []*kgo.Record
	for len(out) < want {
		f := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %d of %d records of %s", len(out), want, topic)
		}
		out = append(out, f.Records()...)
	}
	qctx, cancel := context.WithTimeout(ctx, quiet)
	defer cancel()
	for {
		f := cl.PollFetches(qctx)
		out = append(out, f.Records()...)
		if qctx.Err() != nil {
			return out
		}
	}
}

func committedOutput(t testing.TB, brokers []string, topic string, want int) []*kgo.Record {
	t.Helper()
	return readOutput(t, brokers, topic, kgo.ReadCommitted(), want, 300*time.Millisecond)
}

func headerOf(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// groupOffsets returns the committed offsets of the test group on the raw
// topic, as partition -> next offset.
func groupOffsets(t testing.TB, brokers []string) map[int32]int64 {
	t.Helper()
	adm := kadm.NewClient(newClient(t, brokers))
	resp, err := adm.FetchOffsets(testContext(t), testGroup)
	if err != nil {
		t.Fatalf("FetchOffsets() error = %v", err)
	}
	out := map[int32]int64{}
	resp.Each(func(o kadm.OffsetResponse) {
		if o.Topic == bus.TopicRaw && o.Err == nil && o.At >= 0 {
			out[o.Partition] = o.At
		}
	})
	return out
}

func rawEndOffsets(t testing.TB, brokers []string) map[int32]int64 {
	t.Helper()
	adm := kadm.NewClient(newClient(t, brokers))
	ends, err := adm.ListEndOffsets(testContext(t), bus.TopicRaw)
	if err != nil || ends.Error() != nil {
		t.Fatalf("ListEndOffsets() error = %v, %v", err, ends.Error())
	}
	out := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			out[o.Partition] = o.Offset
		}
	})
	return out
}

func assertOffsetsAtEnd(t testing.TB, brokers []string) {
	t.Helper()
	got, want := groupOffsets(t, brokers), rawEndOffsets(t, brokers)
	if len(got) != len(want) {
		t.Errorf("committed offsets = %v, want %v", got, want)
		return
	}
	for p, end := range want {
		if got[p] != end {
			t.Errorf("committed offsets = %v, want %v", got, want)
			return
		}
	}
}

// assertIdentity checks raw = emitted + dropped + duplicates.
func assertIdentity(t testing.TB, m *metrics) {
	t.Helper()
	raw := testutil.ToFloat64(m.rawRecords)
	var emitted, dropped float64
	for _, k := range emitKinds {
		emitted += testutil.ToFloat64(m.emitted.WithLabelValues(string(k)))
	}
	for _, r := range dropReasons {
		dropped += testutil.ToFloat64(m.dropped.WithLabelValues(r))
	}
	dups := testutil.ToFloat64(m.duplicates)
	if raw != emitted+dropped+dups {
		t.Errorf("raw %v != emitted %v + dropped %v + duplicates %v", raw, emitted, dropped, dups)
	}
}

// scenario is a set of raw records with every kind of source event, and the
// outputs they give.
type scenario struct {
	records []*kgo.Record
	// events and control are the expected outputs, by event ID, with
	// their record keys.
	events  map[string]string
	control map[string]string
	dropped map[string]int
}

func newScenario(t testing.TB) scenario {
	t.Helper()
	root := postURI(testAlice, "root000")
	bobReply := postURI(testBob, "aaa111")
	s := scenario{events: map[string]string{}, control: map[string]string{}, dropped: map[string]int{}}
	add := func(key string, value []byte, id, kind string) {
		s.records = append(s.records, rawRecord(key, value, id, kind, testWitnessed))
	}

	id := commitID(testBob, "create", "aaa111")
	add(testBob, commitValue(t, testBob, "create", "app.bsky.feed.post", "aaa111", replyRecord(root, root, "en")), id, "commit")
	s.events[id] = string(event.ConversationKey(testAlice, testBob))

	id = commitID(testAlice, "create", "ccc333")
	add(testAlice, commitValue(t, testAlice, "create", "app.bsky.feed.post", "ccc333", replyRecord(root, bobReply)), id, "commit")
	s.events[id] = string(event.ConversationKey(testAlice, testBob))

	id = commitID(testCarol, "create", "ddd444")
	add(testCarol, commitValue(t, testCarol, "create", "app.bsky.feed.post", "ddd444", replyRecord(root, bobReply)), id, "commit")
	s.events[id] = string(event.ConversationKey(testAlice, testCarol))

	id = commitID(testAlice, "create", "eee555")
	add(testAlice, commitValue(t, testAlice, "create", "app.bsky.feed.post", "eee555", replyRecord(root, root)), id, "commit")
	s.dropped[DropSelfReply]++

	id = commitID(testAlice, "create", "root000")
	add(testAlice, commitValue(t, testAlice, "create", "app.bsky.feed.post", "root000", replyRecord("", "")), id, "commit")
	s.dropped[DropNotReply]++

	id = commitID(testBob, "delete", "aaa111")
	add(testBob, commitValue(t, testBob, "delete", "app.bsky.feed.post", "aaa111", nil), id, "commit")
	s.control[id] = testBob

	id = "a:" + testCarol + ":0:deleted:t"
	add(testCarol, accountValue(t, testCarol, false, "deleted"), id, "account")
	s.control[id] = testCarol

	add(testCarol, accountValue(t, testCarol, true, ""), "a:"+testCarol+":1::t", "account")
	s.dropped[DropAccountActive]++

	id = "s:" + testAlice + ":" + testRev + ":t"
	add(testAlice, syncValue(t, testAlice), id, "sync")
	s.control[id] = testAlice

	add(testBob, []byte(`{"did":"did:plc:testbob`), "c:broken", "commit")
	s.dropped[DropDecodeFailed]++

	add(testBob, commitValue(t, testBob, "create", "app.bsky.feed.post", "fff666", replyRecord(root, root)), "", "commit")
	s.dropped[DropMissingEventID]++
	return s
}

// checkOutputs reads both output topics with read committed and checks
// that every expected event is there exactly once, with its key and header.
func (s scenario) checkOutputs(t testing.TB, brokers []string) {
	t.Helper()
	check := func(topic string, want map[string]string, decode func([]byte) (string, error)) {
		got := committedOutput(t, brokers, topic, len(want))
		seen := map[string]int{}
		for _, r := range got {
			id := headerOf(r, bus.HeaderEventID)
			seen[id]++
			key, ok := want[id]
			if !ok {
				t.Errorf("%s: unexpected event %q", topic, id)
				continue
			}
			if string(r.Key) != key {
				t.Errorf("%s: event %q has key %q, want %q", topic, id, r.Key, key)
			}
			payloadID, err := decode(r.Value)
			if err != nil || payloadID != id {
				t.Errorf("%s: event %q payload ID %q, error %v", topic, id, payloadID, err)
			}
		}
		for id := range want {
			if seen[id] != 1 {
				t.Errorf("%s: event %q seen %d times, want 1", topic, id, seen[id])
			}
		}
	}
	check(bus.TopicEvents, s.events, func(b []byte) (string, error) {
		m, err := event.UnmarshalMessage(b)
		return m.EventID, err
	})
	check(bus.TopicControl, s.control, func(b []byte) (string, error) {
		c, err := event.UnmarshalControl(b)
		if err != nil {
			return "", err
		}
		switch v := c.(type) {
		case event.Deletion:
			return v.EventID, nil
		case event.AuthorRemoval:
			return v.EventID, nil
		}
		return "", fmt.Errorf("unexpected control type %T", c)
	})
}

func TestServiceEndToEnd(t *testing.T) {
	c := newTestCluster(t)
	brokers := c.ListenAddrs()
	_, rdb := newTestRedis(t)
	s := newScenario(t)
	produceRaw(t, brokers, s.records...)

	r := startService(t, testConfig(brokers, ""), rdb, nil)
	r.waitCommitted(t, len(s.records))
	if !r.svc.Ready() {
		t.Error("Ready() = false after a commit")
	}
	if err := r.stop(t); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if r.svc.Ready() {
		t.Error("Ready() = true after the stop")
	}

	s.checkOutputs(t, brokers)
	assertOffsetsAtEnd(t, brokers)

	m := r.svc.metrics
	assertIdentity(t, m)
	wantEmitted := map[Kind]float64{KindCustomerMessage: 2, KindCompanyMessage: 1, KindDeletion: 1, KindAuthorRemoval: 2}
	for k, want := range wantEmitted {
		if got := testutil.ToFloat64(m.emitted.WithLabelValues(string(k))); got != want {
			t.Errorf("emitted{%s} = %v, want %v", k, got, want)
		}
	}
	for reason, want := range s.dropped {
		if got := testutil.ToFloat64(m.dropped.WithLabelValues(reason)); got != float64(want) {
			t.Errorf("dropped{%s} = %v, want %v", reason, got, want)
		}
	}
	if got := testutil.ToFloat64(m.transactions.WithLabelValues(resultCommitted)); got < 1 {
		t.Errorf("committed transactions = %v, want at least 1", got)
	}
	if got := testutil.ToFloat64(m.transactions.WithLabelValues(resultAborted)); got != 0 {
		t.Errorf("aborted transactions = %v, want 0", got)
	}
	if got := testutil.ToFloat64(m.lag); got <= 0 {
		t.Errorf("lag_seconds = %v, want > 0", got)
	}
}

// TestServiceSameEventTwoOffsets writes one event ID at two raw offsets in
// one batch, and a third copy after the first transaction committed.
func TestServiceSameEventTwoOffsets(t *testing.T) {
	c := newTestCluster(t)
	brokers := c.ListenAddrs()
	_, rdb := newTestRedis(t)
	root := postURI(testAlice, "root000")
	id := commitID(testBob, "create", "aaa111")
	value := commitValue(t, testBob, "create", "app.bsky.feed.post", "aaa111", replyRecord(root, root))
	dup := func() *kgo.Record { return rawRecord(testBob, value, id, "commit", testWitnessed) }
	produceRaw(t, brokers, dup(), dup())

	r := startService(t, testConfig(brokers, ""), rdb, nil)
	r.waitCommitted(t, 2)
	produceRaw(t, brokers, dup())
	r.waitCommitted(t, 3)
	if err := r.stop(t); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	got := committedOutput(t, brokers, bus.TopicEvents, 1)
	if len(got) != 1 || headerOf(got[0], bus.HeaderEventID) != id {
		t.Fatalf("got %d records, want 1 with event %q", len(got), id)
	}
	if d := testutil.ToFloat64(r.svc.metrics.duplicates); d != 2 {
		t.Errorf("duplicates_total = %v, want 2", d)
	}
	assertIdentity(t, r.svc.metrics)
	assertOffsetsAtEnd(t, brokers)
}

var errInjected = errors.New("injected failure")

// TestServiceAbortAndRestart fails the first run after its outputs were
// produced, so the transaction aborts with the Redis keys already set. The
// next run must emit every event exactly once.
func TestServiceAbortAndRestart(t *testing.T) {
	c := newTestCluster(t)
	brokers := c.ListenAddrs()
	mr, rdb := newTestRedis(t)
	s := newScenario(t)
	produceRaw(t, brokers, s.records...)
	outputs := len(s.events) + len(s.control)

	var produced atomic.Int64
	r := startService(t, testConfig(brokers, ""), rdb, func(svc *Service) {
		svc.beforeEnd = func(context.Context, []*kgo.Record) error {
			produced.Store(1)
			return errInjected
		}
	})
	err := r.wait(t)
	if !errors.Is(err, errInjected) {
		t.Fatalf("Run() error = %v, want the injected error", err)
	}
	if produced.Load() != 1 {
		t.Fatal("the hook did not run")
	}

	// The aborted outputs are in the topics, visible to read uncommitted
	// only. The keys are set. No offset is committed. No record counter
	// moved.
	if got := readOutput(t, brokers, bus.TopicEvents, kgo.ReadUncommitted(), len(s.events), 100*time.Millisecond); len(got) != len(s.events) {
		t.Errorf("read uncommitted sees %d aborted records, want %d", len(got), len(s.events))
	}
	if got := committedOutput(t, brokers, bus.TopicEvents, 0); len(got) != 0 {
		t.Errorf("read committed sees %d records after the abort, want 0", len(got))
	}
	for id := range s.events {
		if !mr.Exists(DedupKey(id)) {
			t.Errorf("dedup key of %q is not set after the abort", id)
		}
	}
	if got := groupOffsets(t, brokers); len(got) != 0 {
		t.Errorf("offsets committed after the abort: %v", got)
	}
	m := r.svc.metrics
	if raw := testutil.ToFloat64(m.rawRecords); raw != 0 {
		t.Errorf("raw_records_total = %v after the abort, want 0", raw)
	}
	if a := testutil.ToFloat64(m.transactions.WithLabelValues(resultAborted)); a != 1 {
		t.Errorf("aborted transactions = %v, want 1", a)
	}
	if a := testutil.ToFloat64(m.aborts.WithLabelValues(abortError)); a != 1 {
		t.Errorf("aborted transactions{reason=error} = %v, want 1", a)
	}

	// Restart without the failure.
	r2 := startService(t, testConfig(brokers, ""), rdb, nil)
	r2.waitCommitted(t, len(s.records))
	if err := r2.stop(t); err != nil {
		t.Fatalf("second Run() error = %v", err)
	}
	s.checkOutputs(t, brokers)
	assertOffsetsAtEnd(t, brokers)
	m2 := r2.svc.metrics
	assertIdentity(t, m2)
	if got := testutil.ToFloat64(m2.reprocessed); got != float64(outputs) {
		t.Errorf("reprocessed_total = %v, want %d", got, outputs)
	}
	if got := testutil.ToFloat64(m2.duplicates); got != 0 {
		t.Errorf("duplicates_total = %v, want 0", got)
	}
}

// TestServiceProduceFailure makes the broker reject the produce requests of
// the first run. The transaction aborts, Run returns the error, and a
// restart emits everything once.
func TestServiceProduceFailure(t *testing.T) {
	c := newTestCluster(t)
	brokers := c.ListenAddrs()
	_, rdb := newTestRedis(t)
	s := newScenario(t)
	produceRaw(t, brokers, s.records...)

	cfg := testConfig(brokers, "")
	cfg.Producer.DeliveryTimeout = time.Second
	var failing atomic.Bool
	failing.Store(true)
	c.ControlKey(kmsg.Produce.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		pr := req.(*kmsg.ProduceRequest)
		// Only the transactional producer of the service is failed, not
		// the test producer of the raw topic.
		if !failing.Load() || pr.TransactionID == nil {
			return nil, nil, false
		}
		return produceError(pr, kerr.InvalidRecord), nil, true
	})

	r := startService(t, cfg, rdb, nil)
	err := r.wait(t)
	if err == nil || !strings.Contains(err.Error(), "produce") {
		t.Fatalf("Run() error = %v, want a produce error", err)
	}
	if got := committedOutput(t, brokers, bus.TopicEvents, 0); len(got) != 0 {
		t.Errorf("read committed sees %d records after the failure, want 0", len(got))
	}
	if got := groupOffsets(t, brokers); len(got) != 0 {
		t.Errorf("offsets committed after the failure: %v", got)
	}

	failing.Store(false)
	r2 := startService(t, cfg, rdb, nil)
	r2.waitCommitted(t, len(s.records))
	if err := r2.stop(t); err != nil {
		t.Fatalf("second Run() error = %v", err)
	}
	s.checkOutputs(t, brokers)
	assertOffsetsAtEnd(t, brokers)
	assertIdentity(t, r2.svc.metrics)
}

// TestServiceRedisDown starts with Redis down. The service must stop with
// an error and commit nothing. After Redis is back, a restart emits
// everything.
func TestServiceRedisDown(t *testing.T) {
	c := newTestCluster(t)
	brokers := c.ListenAddrs()
	mr, rdb := newTestRedis(t)
	s := newScenario(t)
	produceRaw(t, brokers, s.records...)
	mr.Close()

	r := startService(t, testConfig(brokers, ""), rdb, nil)
	err := r.wait(t)
	if err == nil || !strings.Contains(err.Error(), "dedup") {
		t.Fatalf("Run() error = %v, want a dedup error", err)
	}
	if got := testutil.ToFloat64(r.svc.metrics.redisErrors); got != 1 {
		t.Errorf("redis_errors_total = %v, want 1", got)
	}
	if got := testutil.ToFloat64(r.svc.metrics.rawRecords); got != 0 {
		t.Errorf("raw_records_total = %v, want 0", got)
	}
	if got := committedOutput(t, brokers, bus.TopicEvents, 0); len(got) != 0 {
		t.Errorf("read committed sees %d records, want 0", len(got))
	}
	if got := groupOffsets(t, brokers); len(got) != 0 {
		t.Errorf("offsets committed with Redis down: %v", got)
	}

	if err := mr.Restart(); err != nil {
		t.Fatalf("restart miniredis: %v", err)
	}
	r2 := startService(t, testConfig(brokers, ""), rdb, nil)
	r2.waitCommitted(t, len(s.records))
	if err := r2.stop(t); err != nil {
		t.Fatalf("second Run() error = %v", err)
	}
	s.checkOutputs(t, brokers)
	assertOffsetsAtEnd(t, brokers)
	assertIdentity(t, r2.svc.metrics)
}

// TestServiceRedisDownMidRun stops Redis while the service runs.
func TestServiceRedisDownMidRun(t *testing.T) {
	c := newTestCluster(t)
	brokers := c.ListenAddrs()
	mr, rdb := newTestRedis(t)
	s := newScenario(t)
	produceRaw(t, brokers, s.records...)

	r := startService(t, testConfig(brokers, ""), rdb, nil)
	r.waitCommitted(t, len(s.records))
	mr.Close()
	root := postURI(testAlice, "root000")
	id := commitID(testBob, "create", "zzz999")
	late := rawRecord(testBob, commitValue(t, testBob, "create", "app.bsky.feed.post", "zzz999", replyRecord(root, root)), id, "commit", testWitnessed)
	produceRaw(t, brokers, late)
	if err := r.wait(t); err == nil {
		t.Fatal("Run() error = nil with Redis down")
	}
	committedBefore := groupOffsets(t, brokers)

	if err := mr.Restart(); err != nil {
		t.Fatal(err)
	}
	r2 := startService(t, testConfig(brokers, ""), rdb, nil)
	r2.waitCommitted(t, 1)
	if err := r2.stop(t); err != nil {
		t.Fatal(err)
	}
	s.events[id] = string(event.ConversationKey(testAlice, testBob))
	s.checkOutputs(t, brokers)
	assertOffsetsAtEnd(t, brokers)
	if slices.Equal(mapValues(committedBefore), mapValues(groupOffsets(t, brokers))) {
		t.Error("offsets did not move after Redis came back")
	}
}

// produceError answers every partition of a produce request with err.
func produceError(req *kmsg.ProduceRequest, err *kerr.Error) *kmsg.ProduceResponse {
	resp := req.ResponseKind().(*kmsg.ProduceResponse)
	for _, rt := range req.Topics {
		st := kmsg.NewProduceResponseTopic()
		st.Topic, st.TopicID = rt.Topic, rt.TopicID
		for _, rp := range rt.Partitions {
			sp := kmsg.NewProduceResponseTopicPartition()
			sp.Partition = rp.Partition
			sp.ErrorCode = err.Code
			st.Partitions = append(st.Partitions, sp)
		}
		resp.Topics = append(resp.Topics, st)
	}
	return resp
}

func mapValues(m map[int32]int64) []int64 {
	var out []int64
	for p := range bus.PartitionsRaw {
		out = append(out, m[p])
	}
	return out
}

// TestServiceGracefulStop stops the service while a batch is in its
// transaction. The batch must still commit within the grace period.
func TestServiceGracefulStop(t *testing.T) {
	c := newTestCluster(t)
	brokers := c.ListenAddrs()
	_, rdb := newTestRedis(t)
	s := newScenario(t)
	produceRaw(t, brokers, s.records...)

	inBatch := make(chan struct{})
	release := make(chan struct{})
	var once atomic.Bool
	r := startService(t, testConfig(brokers, ""), rdb, func(svc *Service) {
		svc.beforeEnd = func(ctx context.Context, _ []*kgo.Record) error {
			if once.CompareAndSwap(false, true) {
				close(inBatch)
				<-release
			}
			return ctx.Err()
		}
	})
	<-inBatch
	r.cancel()
	close(release)
	if err := r.wait(t); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	got := testutil.ToFloat64(r.svc.metrics.rawRecords)
	if got == 0 {
		t.Fatal("the batch in progress did not commit on stop")
	}
	// Whatever was not in that batch is handled by a restart.
	r2 := startService(t, testConfig(brokers, ""), rdb, nil)
	if rest := len(s.records) - int(got); rest > 0 {
		r2.waitCommitted(t, rest)
	}
	if err := r2.stop(t); err != nil {
		t.Fatal(err)
	}
	s.checkOutputs(t, brokers)
	assertOffsetsAtEnd(t, brokers)
}

// TestServiceRebalanceAborts holds the first transaction of one member
// open while a second member joins the group. The rebalance revokes
// partitions in the middle of the transaction, so the session must abort it
// instead of committing, rewind, and go on. Every event must still be
// emitted exactly once, and the two members together must commit every raw
// record exactly once.
func TestServiceRebalanceAborts(t *testing.T) {
	c := newTestCluster(t)
	brokers := c.ListenAddrs()
	_, rdb := newTestRedis(t)
	s := newScenario(t)
	produceRaw(t, brokers, s.records...)

	inBatch := make(chan struct{})
	release := make(chan struct{})
	var first atomic.Bool
	r1 := startService(t, testConfig(brokers, ""), rdb, func(svc *Service) {
		svc.beforeEnd = func(context.Context, []*kgo.Record) error {
			if first.CompareAndSwap(false, true) {
				close(inBatch)
				<-release
			}
			return nil
		}
	})
	select {
	case <-inBatch:
	case <-time.After(30 * time.Second):
		t.Fatal("first member did not start a batch")
	}

	// A second member with its own transactional ID. The same ID would
	// fence the first member instead.
	cfg2 := testConfig(brokers, "")
	cfg2.TransactionalID = "normalize-test-txn-2"
	r2 := startService(t, cfg2, rdb, nil)
	deadline := time.Now().Add(30 * time.Second)
	for !r2.svc.Ready() {
		if time.Now().After(deadline) {
			t.Fatal("second member did not get partitions")
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(release)

	deadline = time.Now().Add(30 * time.Second)
	for {
		total := testutil.ToFloat64(r1.svc.metrics.rawRecords) + testutil.ToFloat64(r2.svc.metrics.rawRecords)
		if total >= float64(len(s.records)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("committed %v raw records, want %d", total, len(s.records))
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case err := <-r1.done:
		t.Fatalf("first member stopped after the rebalance abort: %v", err)
	default:
	}
	// At least the held transaction aborts. The cooperative rebalance has
	// two rounds, so a second batch can abort too.
	if got := testutil.ToFloat64(r1.svc.metrics.transactions.WithLabelValues(resultAborted)); got < 1 {
		t.Errorf("first member aborted transactions = %v, want at least 1", got)
	}
	if got := testutil.ToFloat64(r1.svc.metrics.aborts.WithLabelValues(abortRebalance)); got < 1 {
		t.Errorf("first member aborted transactions{reason=rebalance} = %v, want at least 1", got)
	}
	if err := r1.stop(t); err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
	if err := r2.stop(t); err != nil {
		t.Fatalf("second Run() error = %v", err)
	}

	total := testutil.ToFloat64(r1.svc.metrics.rawRecords) + testutil.ToFloat64(r2.svc.metrics.rawRecords)
	if total != float64(len(s.records)) {
		t.Errorf("both members committed %v raw records, want %d", total, len(s.records))
	}
	assertIdentity(t, r1.svc.metrics)
	assertIdentity(t, r2.svc.metrics)
	s.checkOutputs(t, brokers)
	assertOffsetsAtEnd(t, brokers)
	t.Logf("first member committed %v records, second %v",
		testutil.ToFloat64(r1.svc.metrics.rawRecords), testutil.ToFloat64(r2.svc.metrics.rawRecords))
}

// TestServiceOffsetCommitRefused makes the broker answer the first offset
// commit with CONCURRENT_TRANSACTIONS, as Kafka 4.x does for a transaction
// without output records that starts right after the previous one ended.
// End aborts with no error and no rebalance: Run counts it with reason
// "other", goes on, and the batch is committed once on the next try.
func TestServiceOffsetCommitRefused(t *testing.T) {
	c := newTestCluster(t)
	brokers := c.ListenAddrs()
	_, rdb := newTestRedis(t)
	s := newScenario(t)
	produceRaw(t, brokers, s.records...)

	var refused atomic.Int64
	c.ControlKey(int16(kmsg.TxnOffsetCommit), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.TxnOffsetCommitRequest)
		resp := req.ResponseKind().(*kmsg.TxnOffsetCommitResponse)
		resp.Version = req.Version
		for _, rt := range req.Topics {
			st := kmsg.NewTxnOffsetCommitResponseTopic()
			st.Topic, st.TopicID = rt.Topic, rt.TopicID
			for _, rp := range rt.Partitions {
				sp := kmsg.NewTxnOffsetCommitResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.ConcurrentTransactions.Code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		refused.Add(1)
		return resp, nil, true
	})

	r := startService(t, testConfig(brokers, ""), rdb, nil)
	r.waitCommitted(t, len(s.records))
	if err := r.stop(t); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if refused.Load() != 1 {
		t.Fatalf("refused %d offset commits, want 1", refused.Load())
	}

	m := r.svc.metrics
	for reason, want := range map[string]float64{abortOther: 1, abortRebalance: 0, abortError: 0} {
		if got := testutil.ToFloat64(m.aborts.WithLabelValues(reason)); got != want {
			t.Errorf("aborted transactions{reason=%s} = %v, want %v", reason, got, want)
		}
	}
	if got := testutil.ToFloat64(m.transactions.WithLabelValues(resultAborted)); got != 1 {
		t.Errorf("aborted transactions = %v, want 1", got)
	}
	// The batch was read again and committed once.
	assertIdentity(t, m)
	if got := testutil.ToFloat64(m.rawRecords); got != float64(len(s.records)) {
		t.Errorf("raw_records_total = %v, want %d", got, len(s.records))
	}
	s.checkOutputs(t, brokers)
	assertOffsetsAtEnd(t, brokers)
}

// TestServiceTimeFallback sends one record whose only time is time_us. It
// is emitted and counted in time_us_fallback_total.
func TestServiceTimeFallback(t *testing.T) {
	c := newTestCluster(t)
	brokers := c.ListenAddrs()
	_, rdb := newTestRedis(t)
	ev := baseEvent(testAlice, "sync")
	delete(ev, "witnessed_at_us")
	ev["sync"] = map[string]any{"did": testAlice, "rev": testRev}
	id := "s:" + testAlice + ":" + testRev + ":"
	produceRaw(t, brokers,
		rawRecord(testAlice, eventJSON(t, ev), id, "sync", time.Time{}),
		rawRecord(testAlice, syncValue(t, testAlice), "s:other", "sync", testWitnessed),
	)

	r := startService(t, testConfig(brokers, ""), rdb, nil)
	r.waitCommitted(t, 2)
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(r.svc.metrics.timeFallback); got != 1 {
		t.Errorf("time_us_fallback_total = %v, want 1", got)
	}
	if got := testutil.ToFloat64(r.svc.metrics.emitted.WithLabelValues(string(KindAuthorRemoval))); got != 2 {
		t.Errorf("emitted{author_removal} = %v, want 2", got)
	}
	assertIdentity(t, r.svc.metrics)
}

func TestServiceMissingRawTopic(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	_, rdb := newTestRedis(t)
	cfg := testConfig(c.ListenAddrs(), "")
	cfg.RawTopicWait = 1500 * time.Millisecond

	r := startService(t, cfg, rdb, nil)
	err = r.wait(t)
	if err == nil || !strings.Contains(err.Error(), bus.TopicRaw) {
		t.Fatalf("Run() error = %v, want an error that names %s", err, bus.TopicRaw)
	}
}

func TestServiceEnsuresOutputTopics(t *testing.T) {
	c := newTestCluster(t)
	brokers := c.ListenAddrs()
	_, rdb := newTestRedis(t)
	r := startService(t, testConfig(brokers, ""), rdb, nil)
	deadline := time.Now().Add(20 * time.Second)
	for !r.svc.Ready() {
		if time.Now().After(deadline) {
			t.Fatal("not ready after joining the group")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	adm := kadm.NewClient(newClient(t, brokers))
	details, err := adm.ListTopics(testContext(t), bus.TopicEvents, bus.TopicControl)
	if err != nil {
		t.Fatal(err)
	}
	for topic, want := range map[string]int{bus.TopicEvents: int(bus.PartitionsEvents), bus.TopicControl: int(bus.PartitionsControl)} {
		d, ok := details[topic]
		if !ok || d.Err != nil || len(d.Partitions) != want {
			t.Errorf("topic %s: %+v, want %d partitions", topic, d, want)
		}
	}
}

func TestNewInvalid(t *testing.T) {
	_, rdb := newTestRedis(t)
	valid := testConfig([]string{"localhost:9092"}, "")
	tests := []struct {
		name   string
		change func(*Config)
		rdb    redis.Cmdable
	}{
		{name: "no brokers", change: func(c *Config) { c.KafkaBrokers = nil }},
		{name: "no group", change: func(c *Config) { c.GroupID = "" }},
		{name: "no transactional id", change: func(c *Config) { c.TransactionalID = "" }},
		{name: "zero batch", change: func(c *Config) { c.MaxBatchRecords = 0 }},
		{name: "batch larger than buffer", change: func(c *Config) { c.MaxBatchRecords = c.Producer.MaxBufferedRecords + 1 }},
		{name: "bad producer limits", change: func(c *Config) { c.Producer.DeliveryTimeout = 0 }},
		{name: "zero dedup lifetime", change: func(c *Config) { c.DedupKeyLifetime = 0 }},
		{name: "no redis", change: func(*Config) {}, rdb: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			tt.change(&cfg)
			client := tt.rdb
			if tt.name != "no redis" {
				client = rdb
			}
			if _, err := New(cfg, client, nil, prometheus.NewRegistry()); err == nil {
				t.Error("New() error = nil")
			}
		})
	}
}

// BenchmarkServiceLoop measures the whole loop against the fake cluster and
// miniredis: map, one Redis pipeline and one transaction per batch of up to
// 500 records. The numbers show the overhead of the code, not the cost of a
// real broker or a real Redis.
func BenchmarkServiceLoop(b *testing.B) {
	c := newTestCluster(b)
	brokers := c.ListenAddrs()
	_, rdb := newTestRedis(b)
	root := postURI(testAlice, "root000")

	const perIteration = 500
	cfg := testConfig(brokers, "")
	cfg.MaxBatchRecords = perIteration
	r := startService(b, cfg, rdb, nil)
	producer := newClient(b, brokers)
	ctx := testContext(b)

	b.ResetTimer()
	start := time.Now()
	total := 0
	for i := 0; b.Loop(); i++ {
		recs := make([]*kgo.Record, perIteration)
		for j := range recs {
			rkey := fmt.Sprintf("b%07d%04d", i, j)
			id := commitID(testBob, "create", rkey)
			recs[j] = rawRecord(testBob, commitValue(b, testBob, "create", "app.bsky.feed.post", rkey, replyRecord(root, root)), id, "commit", testWitnessed)
		}
		if err := producer.ProduceSync(ctx, recs...).FirstErr(); err != nil {
			b.Fatal(err)
		}
		total += perIteration
		r.waitCommitted(b, total)
	}
	b.ReportMetric(float64(total)/time.Since(start).Seconds(), "records/s")
}
