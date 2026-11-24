//go:build integration

package routersvc

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
	"github.com/ZAUakaAlexey/threadroute/internal/bus/kafkatest"
	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/pg"
	"github.com/ZAUakaAlexey/threadroute/internal/router"
	"github.com/ZAUakaAlexey/threadroute/internal/router/opclient"
)

// startKafka starts one Apache Kafka node and returns its bootstrap
// address.
func startKafka(t *testing.T) string {
	t.Helper()
	return kafkatest.Start(t)
}

// harness is one Kafka and one PostgreSQL shared by the scenarios.
type harness struct {
	brokers []string
	dsn     string
	pool    *pgxpool.Pool
	n       atomic.Int64
}

// scenario has its own topics and group, and an emptied database.
type scenario struct {
	t            *testing.T
	h            *harness
	convTopic    string
	routingTopic string
	group        string
	core         *router.Service
	// rcfg is the router configuration of the services the scenario
	// starts. A test may change it before the start.
	rcfg router.Config
	// refresh is the operator refresh interval of the services, one hour
	// unless a test changes it before the start.
	refresh time.Duration
	// clock is the clock of the routing core and of every service the
	// scenario starts. It runs with the machine clock, and a test can move
	// it forward to pass a timer on purpose.
	clock *testClock
	cl    *kgo.Client
}

// testClock is the machine clock plus an offset that only grows. A test
// moves it forward to make a timer pass at a known point, so the test
// does not depend on how fast the machine is.
type testClock struct {
	offset atomic.Int64
}

// Now returns the machine time plus the offset.
func (c *testClock) Now() time.Time {
	return time.Now().Add(time.Duration(c.offset.Load()))
}

// advance moves the clock forward by d.
func (c *testClock) advance(d time.Duration) {
	c.offset.Add(int64(d))
}

// longAcceptTimeout is the accept timeout of the scenarios that do not
// test the expiry by the machine clock. At speed factor 20 it is 3 minutes
// of machine time, longer than every wait of these tests, so an offer
// never expires on its own while a test works with it. A scenario that
// needs an expiry moves the clock past it with passAcceptTimeout.
const longAcceptTimeout = time.Hour

// passAcceptTimeout moves the clock of the scenario past the accept
// timeout of every offer made before the call.
func (sc *scenario) passAcceptTimeout() {
	machine := time.Duration(float64(sc.rcfg.AcceptTimeout) / sc.rcfg.SpeedFactor)
	sc.clock.advance(machine + time.Second)
}

// testLogger writes warnings and errors, so a failing run shows why.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// testRouterConfig runs the timers 20 times faster than live: the accept
// timeout of 20 seconds takes 1 second. The SLA targets are long (72
// minutes of machine time, more than the test runs and more than any jump
// of the test clock), so no sla_missed event appears in the expected event
// lists.
func testRouterConfig() router.Config {
	c := router.DefaultConfig()
	c.SpeedFactor = 20
	c.AssignmentTarget = testSLATarget
	c.FirstResponseTarget = testSLATarget
	return c
}

// testSLATarget is both SLA targets of the test configuration.
const testSLATarget = 24 * time.Hour

func (h *harness) newScenario(t *testing.T) *scenario {
	t.Helper()
	n := h.n.Add(1)
	sc := &scenario{
		t: t, h: h,
		convTopic:    fmt.Sprintf("conversations-test-%d", n),
		routingTopic: fmt.Sprintf("routing-test-%d", n),
		group:        fmt.Sprintf("router-test-%d", n),
		rcfg:         testRouterConfig(),
		refresh:      time.Hour,
		clock:        &testClock{},
	}
	_, err := h.pool.Exec(t.Context(), `TRUNCATE outbox, replies, offer_skips, offers, conversations,
		companies, operator_languages, operators RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	core, err := router.New(h.pool, testRouterConfig(), sc.clock, nil)
	if err != nil {
		t.Fatal(err)
	}
	sc.core = core
	cl, err := kgo.NewClient(kgo.SeedBrokers(h.brokers...), kgo.DefaultProduceTopic(sc.convTopic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	sc.cl = cl
	spec := bus.ConversationsTopicSpec()
	spec.Name = sc.convTopic
	spec.Partitions = 4
	if err := bus.EnsureTopics(t.Context(), cl, nil, spec); err != nil {
		t.Fatal(err)
	}
	return sc
}

func (sc *scenario) config() Config {
	return Config{
		KafkaBrokers:    sc.h.brokers,
		GroupID:         sc.group,
		Postgres:        pg.Config{DSN: sc.h.dsn, MaxConns: 8, ConnectWait: 30 * time.Second},
		Router:          sc.rcfg,
		MetricsAddr:     "127.0.0.1:0",
		APIAddr:         "127.0.0.1:0",
		InputTopicWait:  30 * time.Second,
		AssignWorkers:   2,
		AssignInterval:  50 * time.Millisecond,
		ExpireInterval:  time.Second,
		SLAInterval:     time.Second,
		TeamInterval:    10 * time.Second,
		PublishInterval: 20 * time.Millisecond,
		PublishBatch:    100,
		OutboxKeep:      time.Hour,
		PruneInterval:   time.Second,
		CountsInterval:  200 * time.Millisecond,
		OutageLimit:     30 * time.Second,
		OperatorRefresh: sc.refresh,
		LogLevel:        slog.LevelWarn,
	}
}

// running is a Service that runs in the background.
type running struct {
	svc    *Service
	reg    *prometheus.Registry
	api    *opclient.Client
	cancel context.CancelFunc
	done   chan error
}

// newService builds a Service without starting it.
func (sc *scenario) newService() (*Service, *prometheus.Registry) {
	sc.t.Helper()
	reg := prometheus.NewRegistry()
	svc, err := New(sc.t.Context(), Options{
		Config: sc.config(), Pool: sc.h.pool, Logger: testLogger(), Registerer: reg, Clock: sc.clock,
		ConversationsTopic: sc.convTopic, RoutingTopic: sc.routingTopic,
	})
	if err != nil {
		sc.t.Fatal(err)
	}
	return svc, reg
}

// run starts svc in the background. The test stops it at its end, if it
// did not stop it before.
func (sc *scenario) run(svc *Service, reg *prometheus.Registry) *running {
	sc.t.Helper()
	ctx, cancel := context.WithCancel(context.WithoutCancel(sc.t.Context()))
	r := &running{svc: svc, reg: reg, cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- svc.Run(ctx) }()
	api, err := opclient.New("http://"+svc.APIAddr(), opclient.WithTimeout(10*time.Second))
	if err != nil {
		sc.t.Fatal(err)
	}
	r.api = api
	sc.t.Cleanup(func() { r.stop(sc.t) })
	return r
}

// start builds and runs a Service and waits until it is ready.
func (sc *scenario) start() *running {
	sc.t.Helper()
	r := sc.run(sc.newService())
	waitFor(sc.t, 60*time.Second, "service ready", r.svc.Ready)
	return r
}

// stop stops the service and checks that it stopped cleanly. It is safe
// to call twice.
func (r *running) stop(t *testing.T) {
	t.Helper()
	if r.cancel == nil {
		return
	}
	r.cancel()
	r.cancel = nil
	select {
	case err := <-r.done:
		if err != nil {
			t.Errorf("Run() = %v, want nil after a stop", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("service did not stop")
	}
}

// waitFor polls cond until it is true or the time is up.
func waitFor(t *testing.T, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", limit, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// produce writes conversation events to the conversations topic of the
// scenario, keyed like conv does.
func (sc *scenario) produce(evs ...event.Conversation) {
	sc.t.Helper()
	recs := make([]*kgo.Record, len(evs))
	for i, ev := range evs {
		b, err := event.MarshalConversation(ev)
		if err != nil {
			sc.t.Fatal(err)
		}
		recs[i] = &kgo.Record{Key: ev.Key(), Value: b}
	}
	if err := sc.cl.ProduceSync(sc.t.Context(), recs...).FirstErr(); err != nil {
		sc.t.Fatalf("produce: %v", err)
	}
}

// produceRaw writes one record as it is.
func (sc *scenario) produceRaw(key, value []byte) {
	sc.t.Helper()
	if err := sc.cl.ProduceSync(sc.t.Context(), &kgo.Record{Key: key, Value: value}).FirstErr(); err != nil {
		sc.t.Fatalf("produce: %v", err)
	}
}

// operator creates an operator through the core and puts it online.
func (sc *scenario) operator(id string, capacity int, langs ...string) {
	sc.t.Helper()
	ctx := sc.t.Context()
	if _, err := sc.core.PutOperator(ctx, id, opapi.OperatorRequest{Languages: langs, Capacity: capacity}); err != nil {
		sc.t.Fatal(err)
	}
	if _, err := sc.core.Online(ctx, id); err != nil {
		sc.t.Fatal(err)
	}
}

func (sc *scenario) count(query string, args ...any) int64 {
	sc.t.Helper()
	var n int64
	if err := sc.h.pool.QueryRow(sc.t.Context(), query, args...).Scan(&n); err != nil {
		sc.t.Fatal(err)
	}
	return n
}

func (sc *scenario) unsent() int64 {
	return sc.count(`SELECT count(*) FROM outbox WHERE sent_at IS NULL`)
}

// Synthetic, clearly fake test data (decision 0004).
func company(i int) string  { return fmt.Sprintf("test-co-%04d", i) }
func customer(i int) string { return fmt.Sprintf("test-cu-%06d", i) }
func opID(i int) string     { return fmt.Sprintf("sim-op-%04d", i) }

// convStream produces the events of one conversation the way conv numbers
// them: the sequence number grows over every session.
type convStream struct {
	company, customer, lang string
	session, seq            uint64
	src                     time.Time
}

func newStream(company, customer string) *convStream {
	return &convStream{company: company, customer: customer, lang: "en",
		src: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
}

func (s *convStream) next(typ event.ConversationType) event.Conversation {
	s.seq++
	s.src = s.src.Add(time.Second)
	return event.Conversation{
		Type: typ, Channel: event.ChannelBluesky, CompanyID: s.company, CustomerID: s.customer,
		Session: s.session, Seq: s.seq, SourceTime: s.src, OpenedAt: s.src, Lang: s.lang,
		MessageID: fmt.Sprintf("test-msg-%s-%d", s.customer, s.seq), Direction: event.FromCustomer,
	}
}

func (s *convStream) opened() event.Conversation {
	s.session++
	return s.next(event.TypeOpened)
}

func (s *convStream) message() event.Conversation { return s.next(event.TypeMessageAdded) }

func (s *convStream) closed() event.Conversation {
	ev := s.next(event.TypeClosed)
	ev.MessageID, ev.Direction, ev.CloseReason = "", "", event.CloseInactivity
	return ev
}

// removed is a "message removed" event of the current session (decision
// 0011).
func (s *convStream) removed() event.Conversation {
	ev := s.next(event.TypeMessageRemoved)
	ev.RemoveReason = event.RemoveDeleted
	return ev
}

func (s *convStream) key() string { return string(event.ConversationKey(s.company, s.customer)) }

// routingReader reads the routing topic of a scenario from its start.
type routingReader struct {
	t    *testing.T
	cl   *kgo.Client
	recs []*kgo.Record
}

func (sc *scenario) reader() *routingReader {
	sc.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(sc.h.brokers...), kgo.ConsumeTopics(sc.routingTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		sc.t.Fatal(err)
	}
	sc.t.Cleanup(cl.Close)
	return &routingReader{t: sc.t, cl: cl}
}

// waitFor polls the topic until cond holds for the records read so far.
func (r *routingReader) waitFor(limit time.Duration, what string, cond func([]*kgo.Record) bool) []*kgo.Record {
	r.t.Helper()
	deadline := time.Now().Add(limit)
	for {
		if cond(r.recs) {
			return r.recs
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("timed out after %s waiting for %s; read %d routing records", limit, what, len(r.recs))
		}
		ctx, cancel := context.WithTimeout(r.t.Context(), 200*time.Millisecond)
		fetches := r.cl.PollFetches(ctx)
		cancel()
		r.recs = append(r.recs, fetches.Records()...)
	}
}

// events decodes the records of one key, in topic order, without repeats.
func (r *routingReader) events(key string) []event.Routing {
	r.t.Helper()
	return checkOrder(r.t, r.recs)[key]
}

// waitEvent waits until the key has an event that matches.
func (r *routingReader) waitEvent(key string, what string, match func(event.Routing) bool) event.Routing {
	r.t.Helper()
	var found event.Routing
	r.waitFor(30*time.Second, what, func(recs []*kgo.Record) bool {
		for _, ev := range checkOrder(r.t, recs)[key] {
			if match(ev) {
				found = ev
				return true
			}
		}
		return false
	})
	return found
}

// checkOrder checks the per-key order of routing records in topic order:
// the first record of each sequence number comes in order 1, 2, 3 with no
// gap, and a repeat is an exact copy of the first record with that number.
// It returns the events of each key without the repeats.
func checkOrder(t *testing.T, recs []*kgo.Record) map[string][]event.Routing {
	t.Helper()
	type state struct {
		values map[uint64][]byte
		max    uint64
	}
	// The topic holds three key families (conversation, team, operator),
	// and a reader keeps the sequence numbers per family and key.
	type famKey struct{ family, key string }
	keys := map[famKey]*state{}
	out := map[string][]event.Routing{}
	for _, rec := range recs {
		ev, err := event.UnmarshalRouting(rec.Value)
		if err != nil {
			t.Fatalf("routing record %s: %v", rec.Value, err)
		}
		key := string(rec.Key)
		if key != string(ev.Key()) {
			t.Fatalf("record key %q, event key %q", key, ev.Key())
		}
		fk := famKey{family: family(ev.Type), key: key}
		st := keys[fk]
		if st == nil {
			st = &state{values: map[uint64][]byte{}}
			keys[fk] = st
		}
		if prev, ok := st.values[ev.Seq]; ok {
			if !bytes.Equal(prev, rec.Value) {
				t.Fatalf("key %s seq %d repeated with a different payload:\n%s\n%s", key, ev.Seq, prev, rec.Value)
			}
			continue
		}
		if ev.Seq != st.max+1 {
			t.Fatalf("key %s: seq %d after %d", key, ev.Seq, st.max)
		}
		st.values[ev.Seq] = rec.Value
		st.max = ev.Seq
		out[key] = append(out[key], ev)
	}
	return out
}

// family names the key family of a routing type.
func family(typ event.RoutingType) string {
	switch {
	case typ.IsTeam():
		return "team"
	case typ.IsOperator():
		return "operator"
	}
	return "conversation"
}

// types lists event types with the queue reason, for short messages.
func types(evs []event.Routing) string {
	parts := make([]string, len(evs))
	for i, ev := range evs {
		parts[i] = string(ev.Type)
		if ev.QueueReason != "" {
			parts[i] += "(" + string(ev.QueueReason) + ")"
		}
	}
	return strings.Join(parts, ",")
}

// outboxRow is one row of the outbox.
type outboxRow struct {
	family  string
	key     string
	seq     uint64
	payload []byte
}

func (sc *scenario) outbox() []outboxRow {
	sc.t.Helper()
	rows, err := sc.h.pool.Query(sc.t.Context(), `SELECT family, record_key, seq, payload FROM outbox ORDER BY id`)
	if err != nil {
		sc.t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (outboxRow, error) {
		var o outboxRow
		var key []byte
		var seq int64
		err := r.Scan(&o.family, &key, &seq, &o.payload)
		o.key, o.seq = string(key), uint64(seq)
		return o, err
	})
	if err != nil {
		sc.t.Fatal(err)
	}
	return out
}

// matchOutbox checks that the topic holds every outbox row once (without
// repeats), with the same payload, and nothing else. The outbox is read
// again while it waits, so rows that loops keep writing (the refresh of
// operator snapshots) do not make it fail: it waits until the topic has
// caught up with the outbox as it is at that moment.
func (sc *scenario) matchOutbox(r *routingReader) {
	sc.t.Helper()
	var rows []outboxRow
	recs := r.waitFor(60*time.Second, "every outbox row in the topic", func(recs []*kgo.Record) bool {
		rows = sc.outbox()
		n := 0
		for _, evs := range checkOrder(sc.t, recs) {
			n += len(evs)
		}
		return n == len(rows)
	})
	first := map[string]map[uint64][]byte{}
	for _, rec := range recs {
		ev, _ := event.UnmarshalRouting(rec.Value)
		k := family(ev.Type) + "/" + string(rec.Key)
		if first[k] == nil {
			first[k] = map[uint64][]byte{}
		}
		if _, ok := first[k][ev.Seq]; !ok {
			first[k][ev.Seq] = rec.Value
		}
	}
	for _, row := range rows {
		if !bytes.Equal(first[row.family+"/"+row.key][row.seq], row.payload) {
			sc.t.Fatalf("key %s seq %d: topic %s, outbox %s", row.key, row.seq, first[row.family+"/"+row.key][row.seq], row.payload)
		}
	}
}
