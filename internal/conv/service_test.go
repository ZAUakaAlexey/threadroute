package conv

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

func newTestCluster(t *testing.T) *kfake.Cluster {
	t.Helper()
	c, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(bus.PartitionsEvents, bus.TopicEvents),
		kfake.SeedTopics(bus.PartitionsControl, bus.TopicControl),
		kfake.SeedTopics(bus.PartitionsConversations, bus.TopicConversations),
	)
	if err != nil {
		t.Fatalf("kfake.NewCluster() error = %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func newKafkaClient(t *testing.T, brokers []string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func messageRecord(t *testing.T, m event.Message) *kgo.Record {
	t.Helper()
	b, err := event.MarshalMessage(m)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{
		Topic:   bus.TopicEvents,
		Key:     event.ConversationKey(m.CompanyID, m.CustomerID),
		Value:   b,
		Headers: []kgo.RecordHeader{{Key: bus.HeaderEventID, Value: []byte(m.EventID)}},
	}
}

func produceMessages(t *testing.T, brokers []string, msgs ...event.Message) {
	t.Helper()
	cl := newKafkaClient(t, brokers)
	recs := make([]*kgo.Record, len(msgs))
	for i, m := range msgs {
		recs[i] = messageRecord(t, m)
	}
	if err := cl.ProduceSync(testCtx(t), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// failingPublisher fails every publish, like a producer that crashes
// before Kafka confirms. If inner is set, it first publishes for real, like
// a crash after Kafka confirmed and before the outbox entries are removed.
type failingPublisher struct {
	inner Publisher
	calls atomic.Int64
}

func (p *failingPublisher) Publish(ctx context.Context, recs []*kgo.Record) error {
	p.calls.Add(1)
	if p.inner != nil {
		if err := p.inner.Publish(ctx, recs); err != nil {
			return err
		}
	}
	return errors.New("simulated crash")
}

type runningService struct {
	svc    *Service
	cancel context.CancelFunc
	done   chan error
	reg    *prometheus.Registry
}

func startService(t *testing.T, brokers []string, store *Store, pub Publisher) *runningService {
	t.Helper()
	return startServiceGroup(t, brokers, "test-conv", store, pub)
}

func startServiceGroup(t *testing.T, brokers []string, group string, store *Store, pub Publisher) *runningService {
	t.Helper()
	reg := prometheus.NewRegistry()
	svc, err := New(Options{
		Brokers:       brokers,
		GroupID:       group,
		Store:         store,
		Publisher:     pub,
		SweepInterval: 20 * time.Millisecond,
		Logger:        discardLogger(),
		Registerer:    reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	rs := &runningService{svc: svc, cancel: cancel, done: done, reg: reg}
	t.Cleanup(func() { rs.stop() })
	return rs
}

func (r *runningService) stop() error {
	r.cancel()
	select {
	case err, ok := <-r.done:
		if ok {
			close(r.done)
		}
		return err
	case <-time.After(30 * time.Second):
		return errors.New("service did not stop")
	}
}

// waitDone waits until every input message is applied and every outbox is
// empty.
func waitDone(t *testing.T, env *testEnv, input []event.Message) {
	t.Helper()
	waitFor(t, "all input applied and published", func() bool {
		for _, m := range input {
			if n, _ := env.rdb.Exists(testCtx(t), DefaultKeyPrefix+"ap:"+m.EventID).Result(); n != 1 {
				return false
			}
		}
		st, err := env.store.Stats(testCtx(t), bus.PartitionsEvents)
		return err == nil && st.OutboxPending == 0
	})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// readConversations reads every record of the conversations topic.
func readConversations(t *testing.T, brokers []string, want int) []event.Conversation {
	t.Helper()
	cl := newKafkaClient(t, brokers,
		kgo.ConsumeTopics(bus.TopicConversations),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	var out []event.Conversation
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for len(out) < want || ctx.Err() == nil {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		fs.EachRecord(func(rec *kgo.Record) {
			ev, err := event.UnmarshalConversation(rec.Value)
			if err != nil {
				t.Fatalf("record at %d/%d: %v", rec.Partition, rec.Offset, err)
			}
			if string(rec.Key) != string(ev.Key()) {
				t.Fatalf("record key %q, want %q", rec.Key, ev.Key())
			}
			out = append(out, ev)
		})
	}
	return out
}

// dedupe is what a consumer of the conversations topic does (decision
// 0007): it skips an event whose sequence number it has already seen.
func dedupe(evs []event.Conversation) map[string][]event.Conversation {
	seen := map[string]uint64{}
	out := map[string][]event.Conversation{}
	for _, ev := range evs {
		k := string(ev.Key())
		if ev.Seq <= seen[k] {
			continue
		}
		seen[k] = ev.Seq
		out[k] = append(out[k], ev)
	}
	return out
}

// conversationInput is a short exchange in two conversations of
// testCompany, which the caller admits first. Records of different
// conversations sit in different partitions and can be processed in any
// order, so the input must give the same result in every order: the
// admission is not part of it, and no message of testCompany may come more
// than the idle period after another one, because it would end the
// admission before the messages of a partition processed later.
func conversationInput() []event.Message {
	return []event.Message{
		customerMsg(testCompany, testAlice, "test:a1", time.Minute),   // opened
		customerMsg(testCompany, testAlice, "test:a2", 2*time.Minute), // added
		companyMsg(testCompany, testAlice, "test:c1", 3*time.Minute),  // added, answered
		customerMsg(testCompany, testBob, "test:b1", 4*time.Minute),   // opened
		companyMsg(testCompany, testBob, "test:c2", 5*time.Minute),    // added, answered
		// Moves the watermark of its partition far ahead. Another company,
		// so it cannot end the admission of testCompany.
		customerMsg(testCompany2, testCarol, "test:z1", 3*time.Hour),
	}
}

func TestServiceOutboxAfterCrash(t *testing.T) {
	brokers := newTestCluster(t).ListenAddrs()
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	input := conversationInput()
	produceMessages(t, brokers, input...)

	// First run: the state changes, the publish fails, Run stops with an
	// error and commits nothing.
	failing := &failingPublisher{}
	first := startService(t, brokers, env.store, failing)
	select {
	case err := <-first.done:
		close(first.done)
		if err == nil {
			t.Fatal("Run() = nil, want the publish error")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("first run did not fail")
	}
	pending := len(env.outbox(t, 0))
	for p := range bus.PartitionsEvents {
		pending += len(env.outbox(t, p))
	}
	if pending == 0 {
		t.Fatal("outbox is empty after the failed publish")
	}

	// Restart with a working producer.
	pub, err := NewKafkaPublisher(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	second := startService(t, brokers, env.store, pub)
	waitDone(t, env, input)
	// Let the sweeper close what the watermarks allow, then stop.
	time.Sleep(200 * time.Millisecond)
	if err := second.stop(); err != nil {
		t.Fatalf("second Run() error = %v", err)
	}

	evs := readConversations(t, brokers, 1)
	assertAllPublished(t, env, dedupe(evs))
	// Every event exactly once, in sequence order per conversation.
	seen := map[string]uint64{}
	for _, ev := range evs {
		k := string(ev.Key())
		if ev.Seq != seen[k]+1 {
			t.Fatalf("%s: seq %d after %d", k, ev.Seq, seen[k])
		}
		seen[k] = ev.Seq
	}
	// Alice: opened, added, added, answered. Bob: opened, added, answered.
	// Each may also have a close, if the watermark of its partition passed
	// the deadline.
	for who, want := range map[string]uint64{testAlice: 4, testBob: 3} {
		if got := seen[convKey(testCompany, who)]; got < want {
			t.Errorf("%s: %d events, want at least %d", who, got, want)
		}
	}
	// The second run read the records again, because nothing was
	// committed, and found each one applied.
	if got := testutil.ToFloat64(second.svc.m.messages.WithLabelValues("customer", OutcomeDuplicate)); got < 1 {
		t.Errorf("redelivered customer messages = %v, want at least 1", got)
	}
}

func TestServiceRepublishSkippedBySequence(t *testing.T) {
	brokers := newTestCluster(t).ListenAddrs()
	env := newTestEnv(t, testRules())
	env.admit(t, testCompany, 0)
	input := conversationInput()
	produceMessages(t, brokers, input...)

	// Kafka confirms, then the process "crashes" before the entries leave
	// the outbox. The restart publishes them again.
	pub, err := NewKafkaPublisher(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	first := startService(t, brokers, env.store, &failingPublisher{inner: pub})
	select {
	case err := <-first.done:
		close(first.done)
		if err == nil {
			t.Fatal("Run() = nil, want the simulated crash")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("first run did not fail")
	}
	second := startService(t, brokers, env.store, pub)
	waitDone(t, env, input)
	if err := second.stop(); err != nil {
		t.Fatal(err)
	}

	evs := readConversations(t, brokers, 1)
	perConv := dedupe(evs)
	assertAllPublished(t, env, perConv)
	if len(evs) <= countAll(perConv) {
		t.Fatalf("expected repeats in the topic: %d records, %d unique", len(evs), countAll(perConv))
	}
	for k, list := range perConv {
		for i, ev := range list {
			if ev.Seq != uint64(i+1) {
				t.Errorf("%s: event %d has seq %d", k, i, ev.Seq)
			}
		}
	}
	if n := len(perConv[convKey(testCompany, testAlice)]); n < 4 {
		t.Errorf("alice: %d unique events, want at least 4", n)
	}
}

// assertAllPublished checks that the topic holds, for every conversation
// in Redis, each event from sequence number 1 to the last one the state
// assigned, in order. A lost event after a restart fails here, whatever the
// admission decided.
func assertAllPublished(t *testing.T, env *testEnv, perConv map[string][]event.Conversation) {
	t.Helper()
	ctx := testCtx(t)
	keys, err := env.rdb.Keys(ctx, DefaultKeyPrefix+"c:*").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) == 0 {
		t.Fatal("no conversation in Redis")
	}
	for _, k := range keys {
		q, err := env.rdb.HGet(ctx, k, "q").Uint64()
		if err != nil {
			t.Fatal(err)
		}
		key := strings.TrimPrefix(k, DefaultKeyPrefix+"c:")
		list := perConv[key]
		if uint64(len(list)) != q {
			t.Errorf("%s: %d events in the topic, the state assigned %d", key, len(list), q)
			continue
		}
		for i, ev := range list {
			if ev.Seq != uint64(i+1) {
				t.Errorf("%s: event %d has seq %d", key, i, ev.Seq)
			}
		}
	}
}

func countAll(m map[string][]event.Conversation) int {
	n := 0
	for _, l := range m {
		n += len(l)
	}
	return n
}

func TestServiceReadCommitted(t *testing.T) {
	brokers := newTestCluster(t).ListenAddrs()
	env := newTestEnv(t, testRules())
	ctx := testCtx(t)

	aborted := customerMsg(testCompany, testAlice, "test:aborted", 0)
	// Same conversation, so both records are in one partition and the
	// aborted one comes first: a read uncommitted consumer would see it.
	committed := customerMsg(testCompany, testAlice, "test:committed", time.Second)
	tx := newKafkaClient(t, brokers, kgo.TransactionalID("test-normalize"))
	for _, step := range []struct {
		m      event.Message
		commit kgo.TransactionEndTry
	}{{aborted, kgo.TryAbort}, {committed, kgo.TryCommit}} {
		if err := tx.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		if err := tx.ProduceSync(ctx, messageRecord(t, step.m)).FirstErr(); err != nil {
			t.Fatal(err)
		}
		if err := tx.EndTransaction(ctx, step.commit); err != nil {
			t.Fatal(err)
		}
	}

	pub, err := NewKafkaPublisher(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	svc := startService(t, brokers, env.store, pub)
	marker := func(id string) bool {
		n, _ := env.rdb.Exists(ctx, DefaultKeyPrefix+"ap:"+id).Result()
		return n == 1
	}
	waitFor(t, "the committed record", func() bool { return marker(committed.EventID) })
	if err := svc.stop(); err != nil {
		t.Fatal(err)
	}
	if marker(aborted.EventID) {
		t.Error("a record of an aborted transaction was applied")
	}
	if got := testutil.ToFloat64(svc.svc.m.messages.WithLabelValues("customer", OutcomeNotAdmitted)); got != 1 {
		t.Errorf("messages seen = %v, want 1", got)
	}
}

func TestWaitForTopics(t *testing.T) {
	brokers := newTestCluster(t).ListenAddrs()
	cl := newKafkaClient(t, brokers)
	if err := WaitForTopics(testCtx(t), cl, time.Second, bus.TopicEvents, bus.TopicControl); err != nil {
		t.Fatalf("existing topics: %v", err)
	}
	err := WaitForTopics(testCtx(t), cl, 1500*time.Millisecond, bus.TopicEvents, "test.missing")
	if err == nil || !strings.Contains(err.Error(), "test.missing") || !strings.Contains(err.Error(), "normalize") {
		t.Fatalf("missing topic: error = %v", err)
	}
}

// TestConversationInputIsOrderIndependent applies the input with the
// conversations in different orders, as partitions can be processed in any
// order, and expects the same state. The earlier input failed here: its last
// message ended the admission of testCompany when it came first.
func TestConversationInputIsOrderIndependent(t *testing.T) {
	groups := map[string][]event.Message{}
	var order []string
	for _, m := range conversationInput() {
		k := convKey(m.CompanyID, m.CustomerID)
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], m)
	}
	run := func(keys []string) string {
		env := newTestEnv(t, testRules())
		env.admit(t, testCompany, 0)
		for _, k := range keys {
			env.apply(t, groups[k]...)
		}
		return env.snapshot(t)
	}
	want := run(order)
	reversed := slices.Clone(order)
	slices.Reverse(reversed)
	if got := run(reversed); got != want {
		t.Errorf("state depends on the order of conversations:\nforward\n%s\nreversed\n%s", want, got)
	}
}
