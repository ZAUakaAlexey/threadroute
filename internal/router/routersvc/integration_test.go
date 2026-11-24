//go:build integration

package routersvc

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/obs"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/pg"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgtest"
	"github.com/ZAUakaAlexey/threadroute/internal/router"
	"github.com/ZAUakaAlexey/threadroute/internal/router/opclient"
)

// TestIntegration runs the router service against real Kafka and real
// PostgreSQL. The scenarios share one broker and one database; each has
// its own topics, its own group and an emptied database.
func TestIntegration(t *testing.T) {
	h := &harness{brokers: []string{startKafka(t)}}
	h.dsn = pgtest.Start(t)
	h.pool = pgtest.Open(t, h.dsn)
	if err := pg.Migrate(t.Context(), h.pool, nil); err != nil {
		t.Fatal(err)
	}

	t.Run("CatchUp", func(t *testing.T) { testCatchUp(t, h) })
	t.Run("EmptyBacklog", func(t *testing.T) { testEmptyBacklog(t, h) })
	t.Run("LiveFlow", func(t *testing.T) { testLiveFlow(t, h) })
	t.Run("LateAndRepeatedCalls", func(t *testing.T) { testLateCalls(t, h) })
	t.Run("Restart", func(t *testing.T) { testRestart(t, h) })
	t.Run("TwoPublishers", func(t *testing.T) { testTwoPublishers(t, h) })
}

// testCatchUp: the topic holds a conversation that opened and closed and
// one that is still open. The first never reaches an operator and gets no
// routing event; the second is queued with the reason catch_up. The service
// is ready only after that.
func testCatchUp(t *testing.T, h *harness) {
	sc := h.newScenario(t)
	sc.operator(opID(1), 2, "en")
	done := newStream(company(1), customer(1))
	open := newStream(company(1), customer(2))
	// Two records that do not decode, and one that decodes but that
	// PostgreSQL rejects (a NUL byte in a text column). Each is counted and
	// skipped, and the records after them are applied.
	nul := newStream(company(1), "test-cu-\x00-nul")
	sc.produceRaw([]byte("bad-key"), []byte("not json"))
	sc.produceRaw([]byte("bad-key"), []byte(`{"type":"opened"}`))
	sc.produce(nul.opened())
	sc.produce(done.opened(), open.opened(), done.message(), open.message(), done.closed())

	svc, reg := sc.newService()
	health := startHealth(t, reg, svc.Ready)
	if svc.Ready() || healthCode(t, health) != http.StatusServiceUnavailable {
		t.Fatal("ready before Run")
	}
	r := sc.run(svc, reg)
	waitFor(t, 60*time.Second, "ready", svc.Ready)
	if code := healthCode(t, health); code != http.StatusOK {
		t.Errorf("healthz after the catch-up = %d", code)
	}
	// Ready means the catch-up has ended: the open conversation is in the
	// outbox with catch_up already, and the closed one never got a row.
	if n := sc.count(`SELECT count(*) FROM outbox WHERE record_key = $1`, []byte(done.key())); n != 0 {
		t.Errorf("the closed conversation has %d outbox rows", n)
	}
	var reason string
	err := h.pool.QueryRow(t.Context(),
		`SELECT payload->>'queue_reason' FROM (SELECT convert_from(payload, 'UTF8')::jsonb AS payload, id
		 FROM outbox WHERE record_key = $1) AS x ORDER BY id LIMIT 1`, []byte(open.key())).Scan(&reason)
	if err != nil || reason != string(event.QueueCatchUp) {
		t.Fatalf("first outbox row of the open conversation: reason %q, %v", reason, err)
	}
	if got := testutil.ToFloat64(r.svc.m.catchUpQueued); got != 1 {
		t.Errorf("catchup_queued = %v, want 1", got)
	}
	if got := testutil.ToFloat64(r.svc.m.catchUpActive); got != 0 {
		t.Errorf("catchup_active = %v, want 0", got)
	}
	if got := testutil.ToFloat64(r.svc.m.applied.WithLabelValues(string(event.TypeOpened), resultPending)); got != 2 {
		t.Errorf("opened events applied as pending = %v, want 2", got)
	}
	if got := testutil.ToFloat64(r.svc.m.applied.WithLabelValues(typeUnknown, resultInvalid)); got != 2 {
		t.Errorf("invalid records = %v, want 2", got)
	}
	if got := testutil.ToFloat64(r.svc.m.applied.WithLabelValues(string(event.TypeOpened), resultRejected)); got != 1 {
		t.Errorf("rejected records = %v, want 1", got)
	}

	// After the catch-up the open conversation gets an offer.
	rd := sc.reader()
	rd.waitEvent(open.key(), "offer after the catch-up", func(ev event.Routing) bool { return ev.Type == event.TypeOffered })
	if got := types(rd.events(open.key())); got != "queued(catch_up),offered" {
		t.Errorf("open conversation events %s", got)
	}
	if evs := rd.events(done.key()); len(evs) != 0 {
		t.Errorf("closed conversation events %s, want none", types(evs))
	}
	// A conversation that opens now is queued with the reason new.
	live := newStream(company(1), customer(3))
	sc.produce(live.opened())
	ev := rd.waitEvent(live.key(), "queued", func(ev event.Routing) bool { return ev.Type == event.TypeQueued })
	if ev.QueueReason != event.QueueNew {
		t.Errorf("live conversation queued with %q", ev.QueueReason)
	}
}

// testEmptyBacklog: the topic is empty, so the catch-up phase is empty,
// but EndCatchUp still runs: a conversation left pending by an earlier run
// is queued with the reason catch_up.
func testEmptyBacklog(t *testing.T, h *harness) {
	sc := h.newScenario(t)
	s := newStream(company(1), customer(1))
	if _, err := sc.core.ApplyConversation(t.Context(), s.opened(), true); err != nil {
		t.Fatal(err)
	}
	sc.start()
	rd := sc.reader()
	ev := rd.waitEvent(s.key(), "queued", isType(event.TypeQueued))
	if ev.QueueReason != event.QueueCatchUp || ev.Seq != 1 {
		t.Errorf("queued %+v, want catch_up with seq 1", ev)
	}
}

// testLiveFlow runs one conversation through offer, accept, reply and
// resolve over the real HTTP API, and one whose offer times out and goes to
// the next operator.
func testLiveFlow(t *testing.T, h *harness) {
	sc := h.newScenario(t)
	// The offer lives 4 seconds of machine time, so a slow test machine
	// still accepts in time.
	sc.rcfg.AcceptTimeout = 80 * time.Second
	r := sc.start()
	ctx := t.Context()
	api := r.api

	s, err := api.Settings(ctx)
	if err != nil || s.SpeedFactor != 20 || s.AcceptTimeout() != 80*time.Second || s.CapacityDefault != 4 {
		t.Fatalf("settings %+v, %v", s, err)
	}
	for _, op := range []string{opID(1), opID(2)} {
		resp, err := api.PutOperator(ctx, op, opapi.OperatorRequest{Languages: []string{"en"}, Capacity: 1})
		if err != nil || resp.Result != opapi.ResultDone || resp.Operator.Presence != opapi.PresenceOffline {
			t.Fatalf("put %s: %+v, %v", op, resp, err)
		}
		wantResult(t, "online "+op, opapi.ResultDone)(api.Online(ctx, op))
	}
	resp, err := api.PutOperator(ctx, opID(1), opapi.OperatorRequest{Languages: []string{"en"}, Capacity: 1})
	if err != nil || resp.Result != opapi.ResultAlreadyDone {
		t.Fatalf("put again: %+v, %v", resp, err)
	}

	rd := sc.reader()
	c1 := newStream(company(1), customer(1))
	sc.produce(c1.opened())
	off := rd.waitEvent(c1.key(), "offer of c1", isType(event.TypeOffered))
	wantResult(t, "accept", opapi.ResultDone)(api.Accept(ctx, off.OperatorID, off.OfferID))
	wantResult(t, "reply", opapi.ResultDone)(api.Reply(ctx, off.OperatorID, off.OfferID, "reply-1"))
	wantResult(t, "resolve", opapi.ResultDone)(api.Resolve(ctx, off.OperatorID, off.OfferID))
	rd.waitEvent(c1.key(), "resolved c1", isType(event.TypeResolved))
	if got := types(rd.events(c1.key())); got != "queued(new),offered,accepted,replied,resolved" {
		t.Errorf("c1 events %s", got)
	}

	// c2: the first offer is not answered and times out after 4 seconds
	// of machine time; the conversation goes to the other operator.
	c2 := newStream(company(1), customer(2))
	sc.produce(c2.opened())
	first := rd.waitEvent(c2.key(), "first offer of c2", isType(event.TypeOffered))
	second := rd.waitEvent(c2.key(), "second offer of c2", func(ev event.Routing) bool {
		return ev.Type == event.TypeOffered && ev.OfferID != first.OfferID
	})
	if second.OperatorID == first.OperatorID {
		t.Errorf("second offer went to the same operator %s", first.OperatorID)
	}
	if !second.QueuedAt.Equal(first.QueuedAt) {
		t.Errorf("waiting time not kept: queued at %s, then %s", first.QueuedAt, second.QueuedAt)
	}
	wantResult(t, "accept second", opapi.ResultDone)(api.Accept(ctx, second.OperatorID, second.OfferID))
	sc.produce(c2.closed())
	ended := rd.waitEvent(c2.key(), "ended c2", isType(event.TypeEnded))
	if ended.OfferID != second.OfferID || ended.EndReason != event.CloseInactivity {
		t.Errorf("ended %+v", ended)
	}
	evs := rd.events(c2.key())
	if got := types(evs); got != "queued(new),offered,offer_expired,queued(offer_expired),offered,accepted,ended" {
		t.Errorf("c2 events %s", got)
	}
	if evs[2].OfferEndReason != event.OfferTimeout {
		t.Errorf("offer end reason %q", evs[2].OfferEndReason)
	}
	// Sequence numbers from 1, in order, without a gap: checkOrder
	// checked it while reading; here the numbers themselves.
	for _, key := range []string{c1.key(), c2.key()} {
		for i, ev := range rd.events(key) {
			if ev.Seq != uint64(i+1) {
				t.Errorf("key %s event %d has seq %d", key, i, ev.Seq)
			}
		}
	}
	waitFor(t, 10*time.Second, "outbox sent", func() bool { return sc.unsent() == 0 })
	sc.matchOutbox(rd)
	if got := testutil.ToFloat64(r.svc.m.expired); got != 1 {
		t.Errorf("offers_expired_total = %v, want 1", got)
	}
}

// testLateCalls: late and repeated operator calls give already_done or the
// right 409 code and change nothing.
func testLateCalls(t *testing.T, h *harness) {
	sc := h.newScenario(t)
	sc.rcfg.AcceptTimeout = 60 * time.Second
	r := sc.start()
	ctx := t.Context()
	api := r.api
	for _, op := range []string{opID(1), opID(2)} {
		if _, err := api.PutOperator(ctx, op, opapi.OperatorRequest{Languages: []string{"en"}, Capacity: 2}); err != nil {
			t.Fatal(err)
		}
		wantResult(t, "online", opapi.ResultDone)(api.Online(ctx, op))
	}
	rd := sc.reader()

	// a: accepted, replied and resolved.
	a := newStream(company(1), customer(1))
	sc.produce(a.opened())
	oa := rd.waitEvent(a.key(), "offer a", isType(event.TypeOffered))
	wantResult(t, "accept a", opapi.ResultDone)(api.Accept(ctx, oa.OperatorID, oa.OfferID))
	wantResult(t, "reply a", opapi.ResultDone)(api.Reply(ctx, oa.OperatorID, oa.OfferID, "r-1"))
	wantResult(t, "resolve a", opapi.ResultDone)(api.Resolve(ctx, oa.OperatorID, oa.OfferID))

	// b: the offer times out and is not answered by anyone afterwards.
	// Both operators are taken offline after the expiry, so no new offer
	// is made.
	b := newStream(company(1), customer(2))
	sc.produce(b.opened())
	ob := rd.waitEvent(b.key(), "offer b", isType(event.TypeOffered))
	rd.waitEvent(b.key(), "expiry b", isType(event.TypeOfferExpired))

	// c: accepted, then closed by conv.
	c := newStream(company(1), customer(3))
	sc.produce(c.opened())
	oc := rd.waitEvent(c.key(), "offer c", func(ev event.Routing) bool { return ev.Type == event.TypeOffered })
	wantResult(t, "accept c", opapi.ResultDone)(api.Accept(ctx, oc.OperatorID, oc.OfferID))
	sc.produce(c.closed())
	rd.waitEvent(c.key(), "ended c", isType(event.TypeEnded))

	for _, op := range []string{opID(1), opID(2)} {
		wantResult(t, "offline", opapi.ResultDone)(api.Offline(ctx, op))
	}
	waitFor(t, 10*time.Second, "outbox sent", func() bool { return sc.unsent() == 0 })
	before := sc.snapshot()

	other := opID(1)
	if oa.OperatorID == other {
		other = opID(2)
	}
	calls := []struct {
		name string
		call func() (opapi.Result, error)
		want opapi.Result
		code opapi.ErrorCode
	}{
		{"accept a again", func() (opapi.Result, error) { return api.Accept(ctx, oa.OperatorID, oa.OfferID) }, opapi.ResultAlreadyDone, ""},
		{"reply a same ID", func() (opapi.Result, error) { return api.Reply(ctx, oa.OperatorID, oa.OfferID, "r-1") }, opapi.ResultAlreadyDone, ""},
		{"resolve a again", func() (opapi.Result, error) { return api.Resolve(ctx, oa.OperatorID, oa.OfferID) }, opapi.ResultAlreadyDone, ""},
		{"refuse a after accept", func() (opapi.Result, error) { return api.Refuse(ctx, oa.OperatorID, oa.OfferID) }, "", opapi.CodeOfferAccepted},
		{"reply a after resolve", func() (opapi.Result, error) { return api.Reply(ctx, oa.OperatorID, oa.OfferID, "r-2") }, "", opapi.CodeAssignmentEnded},
		{"accept b late", func() (opapi.Result, error) { return api.Accept(ctx, ob.OperatorID, ob.OfferID) }, "", opapi.CodeOfferExpired},
		{"refuse b late", func() (opapi.Result, error) { return api.Refuse(ctx, ob.OperatorID, ob.OfferID) }, "", opapi.CodeOfferExpired},
		{"resolve b never accepted", func() (opapi.Result, error) { return api.Resolve(ctx, ob.OperatorID, ob.OfferID) }, "", opapi.CodeOfferExpired},
		{"resolve c after close", func() (opapi.Result, error) { return api.Resolve(ctx, oc.OperatorID, oc.OfferID) }, "", opapi.CodeAssignmentEnded},
		{"accept c after close", func() (opapi.Result, error) { return api.Accept(ctx, oc.OperatorID, oc.OfferID) }, opapi.ResultAlreadyDone, ""},
		{"offer of another operator", func() (opapi.Result, error) { return api.Accept(ctx, other, oa.OfferID) }, "", opapi.CodeOfferNotFound},
		{"unknown offer", func() (opapi.Result, error) { return api.Accept(ctx, oa.OperatorID, "offer-unknown") }, "", opapi.CodeOfferNotFound},
		{"unknown operator", func() (opapi.Result, error) { return api.Online(ctx, "sim-op-unknown") }, "", opapi.CodeOperatorNotFound},
		{"offline again", func() (opapi.Result, error) { return api.Offline(ctx, opID(1)) }, opapi.ResultAlreadyDone, ""},
	}
	for range 2 {
		for _, c := range calls {
			got, err := c.call()
			if c.code != "" {
				e, ok := errors.AsType[*opapi.Error](err)
				if !ok || e.Code != c.code || e.Status != c.code.Status() {
					t.Errorf("%s: %q, %v; want code %s", c.name, got, err, c.code)
				}
				continue
			}
			if err != nil || got != c.want {
				t.Errorf("%s: %q, %v; want %q", c.name, got, err, c.want)
			}
		}
	}
	if after := sc.snapshot(); after != before {
		t.Errorf("state changed:\nbefore %s\nafter  %s", before, after)
	}
}

// snapshot is the routing state as one string: outbox rows, conversations,
// offers and replies.
func (sc *scenario) snapshot() string {
	sc.t.Helper()
	var s string
	err := sc.h.pool.QueryRow(sc.t.Context(), `SELECT
		(SELECT count(*) FROM outbox)::text || '|' ||
		(SELECT string_agg(customer_id || ':' || state || ':' || route_seq, ',' ORDER BY customer_id) FROM conversations) || '|' ||
		(SELECT string_agg(id || ':' || state, ',' ORDER BY num) FROM offers) || '|' ||
		(SELECT count(*) FROM replies)::text`).Scan(&s)
	if err != nil {
		sc.t.Fatal(err)
	}
	return s
}

// testRestart stops the service in the middle and starts it again on the
// same database and group. Nothing is lost, the per-key order holds, and
// a repeat in the topic is an exact copy.
func testRestart(t *testing.T, h *harness) {
	sc := h.newScenario(t)
	for i := 1; i <= 3; i++ {
		sc.operator(opID(i), 2, "en")
	}
	var streams []*convStream
	var evs []event.Conversation
	for i := range 30 {
		s := newStream(company(i%3), customer(i))
		streams = append(streams, s)
		evs = append(evs, s.opened(), s.message())
		if i%3 == 0 {
			evs = append(evs, s.closed())
		}
	}
	sc.produce(evs...)

	first := sc.start()
	// Offers expire every second and are made again, so the outbox grows.
	waitFor(t, 60*time.Second, "routing events published", func() bool {
		return sc.count(`SELECT count(*) FROM outbox WHERE sent_at IS NOT NULL`) >= 60
	})
	first.stop(t)
	// A crash between produce and MarkSent: the newest sent rows count as
	// unsent again, so the second run sends them a second time.
	reset := sc.count(`WITH r AS (UPDATE outbox SET sent_at = NULL WHERE id IN
		(SELECT id FROM outbox WHERE sent_at IS NOT NULL ORDER BY id DESC LIMIT 20) RETURNING 1)
		SELECT count(*) FROM r`)
	if reset != 20 {
		t.Fatalf("reset %d rows, want 20", reset)
	}

	// While the router is down, conv goes on.
	evs = evs[:0]
	for i, s := range streams {
		if i%3 == 1 {
			evs = append(evs, s.closed())
		}
	}
	for i := 30; i < 40; i++ {
		s := newStream(company(i%3), customer(i))
		streams = append(streams, s)
		evs = append(evs, s.opened())
	}
	sc.produce(evs...)

	second := sc.start()
	// Nothing lost: every conversation has its last event applied.
	waitFor(t, 30*time.Second, "every event applied", func() bool {
		for _, s := range streams {
			if sc.count(`SELECT count(*) FROM conversations WHERE company_id = $1 AND customer_id = $2 AND conv_seq = $3`,
				s.company, s.customer, int64(s.seq)) != 1 {
				return false
			}
		}
		return true
	})
	if n := sc.count(`SELECT count(*) FROM conversations WHERE state = 'closed'`); n != 20 {
		t.Errorf("closed conversations = %d, want 20", n)
	}
	// Stop the offer churn, then let the outbox drain.
	for i := 1; i <= 3; i++ {
		wantResult(t, "offline", opapi.ResultDone)(second.api.Offline(t.Context(), opID(i)))
	}
	waitFor(t, 30*time.Second, "outbox sent", func() bool { return sc.unsent() == 0 })
	rd := sc.reader()
	sc.matchOutbox(rd)
	if distinct := len(sc.outbox()); len(rd.recs) <= distinct {
		t.Errorf("topic has %d records for %d rows, want repeats from the reset rows", len(rd.recs), distinct)
	}
	// A conversation closed while the router was down ended its offer.
	for i, s := range streams[:30] {
		if i%3 != 1 {
			continue
		}
		evs := rd.events(s.key())
		if len(evs) == 0 || evs[len(evs)-1].Type != event.TypeEnded {
			t.Errorf("conversation %d closed during the stop: events %s", i, types(evs))
		}
	}
}

// testTwoPublishers runs two publishers on one database: only the holder
// of the lock publishes, and the per-key order in the topic holds. When
// the holder stops, the other one takes over.
func testTwoPublishers(t *testing.T, h *harness) {
	sc := h.newScenario(t)
	spec := sc.config()
	// The routing topic is created here, because no service runs.
	rspec := bus.RoutingTopicSpec()
	rspec.Name = sc.routingTopic
	if err := bus.EnsureTopics(t.Context(), sc.cl, nil, rspec); err != nil {
		t.Fatal(err)
	}

	var streams []*convStream
	apply := func(n int) {
		t.Helper()
		for range n {
			s := newStream(company(len(streams)%4), customer(len(streams)))
			streams = append(streams, s)
			for _, ev := range []event.Conversation{s.opened(), s.message(), s.closed()} {
				if _, err := sc.core.ApplyConversation(t.Context(), ev, false); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	apply(40)

	newPub := func(name string) (*Publisher, func() error) {
		pool := pgtest.Open(t, h.dsn)
		prod, err := kgo.NewClient(kgo.SeedBrokers(h.brokers...), kgo.RecordDeliveryTimeout(deliveryTimeout))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(prod.Close)
		core, err := router.New(pool, testRouterConfig(), router.SystemClock{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		m, err := newMetrics(prometheus.NewRegistry())
		if err != nil {
			t.Fatal(err)
		}
		p := &Publisher{
			store: core, producer: prod, lock: newPGLocker(pool, PublisherLockKey), topic: sc.routingTopic,
			batch: 7, interval: 10 * time.Millisecond, m: m, logger: testLogger().With("publisher", name),
			runner: &runner{timer: realTimer{}, outageLimit: spec.OutageLimit, logger: testLogger()},
		}
		ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
		done := make(chan error, 1)
		go func() { done <- p.Run(ctx) }()
		stop := sync.OnceValue(func() error {
			cancel()
			return <-done
		})
		t.Cleanup(func() { _ = stop() })
		return p, stop
	}
	p1, stop1 := newPub("one")
	p2, stop2 := newPub("two")

	// While both run and new rows arrive, at most one holds the lock.
	var wg sync.WaitGroup
	wg.Go(func() { apply(40) })
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p1.Holding() && p2.Holding() {
			t.Fatal("both publishers hold the lock")
		}
		time.Sleep(2 * time.Millisecond)
	}
	wg.Wait()
	waitFor(t, 30*time.Second, "outbox sent", func() bool { return sc.unsent() == 0 })
	holder, standby, stopHolder := p1, p2, stop1
	if p2.Holding() {
		holder, standby, stopHolder = p2, p1, stop2
	}
	if !holder.Holding() || standby.Sent() != 0 || holder.Sent() == 0 {
		t.Fatalf("holder sent %d, standby sent %d (holding %v %v)", holder.Sent(), standby.Sent(), p1.Holding(), p2.Holding())
	}

	// The holder stops; the standby takes the lock and sends new rows.
	if err := stopHolder(); err != nil {
		t.Fatal(err)
	}
	apply(20)
	waitFor(t, 30*time.Second, "standby takes over", func() bool { return standby.Holding() && sc.unsent() == 0 })
	if standby.Sent() == 0 {
		t.Error("standby sent nothing after the holder stopped")
	}
	sc.matchOutbox(sc.reader())
}

func isType(typ event.RoutingType) func(event.Routing) bool {
	return func(ev event.Routing) bool { return ev.Type == typ }
}

// wantResult returns a check for the two results of an API call.
func wantResult(t *testing.T, what string, want opapi.Result) func(opapi.Result, error) {
	t.Helper()
	return func(got opapi.Result, err error) {
		t.Helper()
		if err != nil || got != want {
			t.Fatalf("%s = %q, %v (code %q); want %q", what, got, err, opclient.Code(err), want)
		}
	}
}

// startHealth serves /healthz of obs with the ready function and returns
// its base URL.
func startHealth(t *testing.T, reg *prometheus.Registry, ready func() bool) string {
	t.Helper()
	srv, err := obs.NewServer(t.Context(), "127.0.0.1:0", reg, ready, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	wait := srv.Start(ctx)
	t.Cleanup(func() { cancel(); _ = wait() })
	return "http://" + srv.Addr()
}

func healthCode(t *testing.T, base string) int {
	t.Helper()
	resp, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}
