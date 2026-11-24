//go:build integration

package sim

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
	"github.com/ZAUakaAlexey/threadroute/internal/bus/kafkatest"
	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/pg"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgtest"
	"github.com/ZAUakaAlexey/threadroute/internal/router"
	"github.com/ZAUakaAlexey/threadroute/internal/router/routersvc"
)

// Timing of the integration test. The router runs 25 times faster than
// live: the accept timeout of 20 seconds takes 0.8 seconds of machine
// time. The simulator reads the factor from the router.
const (
	itSpeed    = 25
	itCapacity = 2
)

// itBehaviour has short shifts, so operators go offline while they hold
// conversations, and short handling times, so the test ends soon.
func itBehaviour() Behaviour {
	b := DefaultBehaviour()
	b.Operators = 6
	b.Languages = Spread{{Lang: "en", Count: 4}, {Lang: "ja", Count: 2}}
	b.Capacity = itCapacity
	b.ShiftOnline = 60 * time.Second
	b.ShiftOffline = 15 * time.Second
	b.HandlingMedian = 10 * time.Second
	b.ResolveMedian = 20 * time.Second
	b.Seed = 42
	return b
}

func itLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// itEnv is one Kafka, one PostgreSQL and a running router.
type itEnv struct {
	t       *testing.T
	brokers []string
	pool    *pgxpool.Pool
	apiURL  string
	cl      *kgo.Client
}

func startRouter(t *testing.T) *itEnv {
	t.Helper()
	brokers := []string{kafkatest.Start(t)}
	dsn := pgtest.Start(t)
	pool := pgtest.Open(t, dsn)
	if err := pg.Migrate(t.Context(), pool, nil); err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(bus.TopicConversations))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	spec := bus.ConversationsTopicSpec()
	spec.Partitions = 4
	if err := bus.EnsureTopics(t.Context(), cl, nil, spec); err != nil {
		t.Fatal(err)
	}

	rc := router.DefaultConfig()
	rc.SpeedFactor = itSpeed
	rc.AssignmentTarget = time.Hour
	rc.FirstResponseTarget = time.Hour
	cfg := routersvc.Config{
		KafkaBrokers:    brokers,
		GroupID:         "router-it",
		Postgres:        pg.Config{DSN: dsn, MaxConns: 8, ConnectWait: 30 * time.Second},
		Router:          rc,
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
		CountsInterval:  time.Second,
		OutageLimit:     30 * time.Second,
		LogLevel:        slog.LevelWarn,
	}
	svc, err := routersvc.New(t.Context(), routersvc.Options{
		Config: cfg, Pool: pool, Logger: itLogger(), Registerer: prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("router Run() = %v", err)
			}
		case <-time.After(60 * time.Second):
			t.Error("router did not stop")
		}
	})
	itWait(t, 60*time.Second, "router ready", svc.Ready)
	return &itEnv{t: t, brokers: brokers, pool: pool, apiURL: "http://" + svc.APIAddr(), cl: cl}
}

// runningSim is a simulator that runs in the background.
type runningSim struct {
	svc    *Service
	cancel context.CancelFunc
	done   chan error
}

func (env *itEnv) startSim() *runningSim {
	env.t.Helper()
	cfg := Config{
		KafkaBrokers: env.brokers,
		RouterURL:    env.apiURL,
		StartWait:    30 * time.Second,
		GroupID:      "sim-it",
		MetricsAddr:  "127.0.0.1:0",
		OutageLimit:  30 * time.Second,
		Behaviour:    itBehaviour(),
		LogLevel:     slog.LevelWarn,
	}
	svc, err := New(Options{Config: cfg, Logger: itLogger(), Registerer: prometheus.NewRegistry()})
	if err != nil {
		env.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(env.t.Context()))
	r := &runningSim{svc: svc, cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- svc.Run(ctx) }()
	env.t.Cleanup(func() { r.stop(env.t) })
	itWait(env.t, 60*time.Second, "sim ready", svc.Ready)
	return r
}

// stop stops the simulator. It is safe to call twice.
func (r *runningSim) stop(t *testing.T) {
	t.Helper()
	if r.cancel == nil {
		return
	}
	r.cancel()
	r.cancel = nil
	select {
	case err := <-r.done:
		if err != nil {
			t.Errorf("sim Run() = %v, want nil after a stop", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("sim did not stop")
	}
}

func itWait(t *testing.T, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", limit, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (env *itEnv) count(query string, args ...any) int64 {
	env.t.Helper()
	var n int64
	if err := env.pool.QueryRow(env.t.Context(), query, args...).Scan(&n); err != nil {
		env.t.Fatal(err)
	}
	return n
}

// conv produces the events of one synthetic conversation the way conv
// numbers them.
type conv struct {
	company, customer, lang string
	seq                     uint64
	src                     time.Time
}

func (c *conv) key() string { return string(event.ConversationKey(c.company, c.customer)) }

func (c *conv) next(typ event.ConversationType) event.Conversation {
	c.seq++
	c.src = c.src.Add(time.Second)
	ev := event.Conversation{
		Type: typ, Channel: event.ChannelBluesky, CompanyID: c.company, CustomerID: c.customer,
		Session: 1, Seq: c.seq, SourceTime: c.src, OpenedAt: c.src, Lang: c.lang,
		MessageID: fmt.Sprintf("test-msg-%s-%d", c.customer, c.seq), Direction: event.FromCustomer,
	}
	if typ == event.TypeClosed {
		ev.MessageID, ev.Direction, ev.CloseReason = "", "", event.CloseInactivity
	}
	return ev
}

func (env *itEnv) produce(evs ...event.Conversation) {
	env.t.Helper()
	recs := make([]*kgo.Record, len(evs))
	for i, ev := range evs {
		b, err := event.MarshalConversation(ev)
		if err != nil {
			env.t.Fatal(err)
		}
		recs[i] = &kgo.Record{Key: ev.Key(), Value: b}
	}
	if err := env.cl.ProduceSync(env.t.Context(), recs...).FirstErr(); err != nil {
		env.t.Fatalf("produce: %v", err)
	}
}

// feed opens n conversations, one every gap. Every closeEvery-th one is
// closed again shortly after, which ends its offer or assignment. It
// returns the conversations that stay open.
func (env *itEnv) feed(from, n, closeEvery int, gap time.Duration) (open []*conv) {
	env.t.Helper()
	langs := []string{"en", "en", "en", "ja", event.LangUndetermined}
	var closing []*conv
	for i := from; i < from+n; i++ {
		c := &conv{
			// Ten companies, so no company reaches the team threshold.
			company:  fmt.Sprintf("test-co-%04d", i%10),
			customer: fmt.Sprintf("test-cu-%06d", i),
			lang:     langs[i%len(langs)],
			src:      time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		}
		env.produce(c.next(event.TypeOpened), c.next(event.TypeMessageAdded))
		if closeEvery > 0 && i%closeEvery == 0 {
			closing = append(closing, c)
		} else {
			open = append(open, c)
		}
		time.Sleep(gap)
	}
	for _, c := range closing {
		env.produce(c.next(event.TypeClosed))
	}
	return open
}

// checkLoad checks in the database that no operator holds more than its
// capacity. The unique index offers_one_live_idx already allows one live
// offer per conversation.
func (env *itEnv) checkLoad() {
	env.t.Helper()
	over := env.count(`SELECT count(*) FROM (SELECT operator_id FROM offers
		WHERE state IN ('offered', 'accepted') GROUP BY operator_id HAVING count(*) > $1) AS x`, itCapacity)
	if over != 0 {
		env.t.Errorf("%d operators hold more than their capacity", over)
	}
}

// routingLog reads the whole routing topic.
func (env *itEnv) routingLog(want int64) map[string][]event.Routing {
	env.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(env.brokers...), kgo.ConsumeTopics(bus.TopicRouting),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		env.t.Fatal(err)
	}
	defer cl.Close()
	byKey := map[string][]event.Routing{}
	seen := map[string]uint64{}
	var n int64
	deadline := time.Now().Add(30 * time.Second)
	for n < want {
		if time.Now().After(deadline) {
			env.t.Fatalf("read %d routing events, want %d", n, want)
		}
		ctx, cancel := context.WithTimeout(env.t.Context(), time.Second)
		fetches := cl.PollFetches(ctx)
		cancel()
		for _, rec := range fetches.Records() {
			ev, err := event.UnmarshalRouting(rec.Value)
			if err != nil {
				env.t.Fatalf("routing record: %v", err)
			}
			k := string(rec.Key)
			if ev.Seq <= seen[k] {
				continue // a repeat from the publisher
			}
			if ev.Seq != seen[k]+1 {
				env.t.Fatalf("key %s: seq %d after %d", k, ev.Seq, seen[k])
			}
			seen[k] = ev.Seq
			byKey[k] = append(byKey[k], ev)
			n++
		}
	}
	return byKey
}

// TestIntegration runs the real router service with the simulator against
// real Kafka and PostgreSQL, at a high speed factor. Conversations are fed
// into the conversations topic; the simulated operators serve them, go
// offline in shift changes, and the simulator is restarted in the middle.
func TestIntegration(t *testing.T) {
	env := startRouter(t)
	sim1 := env.startSim()

	// Check the load all the time while the test runs.
	stopLoad := make(chan struct{})
	var loadWG sync.WaitGroup
	loadWG.Go(func() {
		for {
			select {
			case <-stopLoad:
				return
			case <-time.After(100 * time.Millisecond):
				env.checkLoad()
			}
		}
	})
	defer func() {
		close(stopLoad)
		loadWG.Wait()
	}()

	open := env.feed(0, 40, 10, 50*time.Millisecond)
	// Work is under way: let the first simulator hold assignments, then
	// stop it in the middle.
	itWait(t, 30*time.Second, "resolves before the restart", func() bool {
		return env.count(`SELECT count(*) FROM offers WHERE state = 'resolved'`) >= 5
	})
	itWait(t, 30*time.Second, "an assignment held at the restart", func() bool {
		return env.count(`SELECT count(*) FROM offers WHERE state = 'accepted'`) > 0
	})
	sim1.stop(t)
	held := env.count(`SELECT count(*) FROM offers WHERE state IN ('offered', 'accepted')`)
	t.Logf("simulator stopped with %d live offers and assignments", held)
	restartAt := time.Now()

	env.startSim()
	// Every operator went offline in the start of the second simulator.
	// A return to the queue after this point comes from a shift change.
	shiftsFrom := time.Now()
	open = append(open, env.feed(40, 40, 10, 50*time.Millisecond)...)

	// Everything that stays open is served to the end, and nothing stays
	// with an operator.
	itWait(t, 60*time.Second, "every open conversation resolved and no live offer", func() bool {
		return env.count(`SELECT count(*) FROM conversations WHERE state NOT IN ('resolved', 'closed')`) == 0 &&
			env.count(`SELECT count(*) FROM offers WHERE state IN ('offered', 'accepted')`) == 0 &&
			env.count(`SELECT count(*) FROM conversations`) == 80
	})
	itWait(t, 30*time.Second, "outbox sent", func() bool {
		return env.count(`SELECT count(*) FROM outbox WHERE sent_at IS NULL`) == 0
	})
	byKey := env.routingLog(env.count(`SELECT count(*) FROM outbox WHERE family = 'conversation'`))

	// Every open conversation was offered, accepted, replied and resolved
	// by a simulated operator.
	for _, c := range open {
		evs := byKey[c.key()]
		var accepted, replied, resolved bool
		for _, ev := range evs {
			simOp := strings.HasPrefix(ev.OperatorID, "sim-op-")
			switch {
			case ev.Type == event.TypeAccepted && simOp:
				accepted = true
			case ev.Type == event.TypeReplied && simOp:
				replied = true
			case ev.Type == event.TypeResolved && simOp:
				resolved = true
			}
		}
		if !accepted || !replied || !resolved {
			t.Errorf("%s: accepted %v, replied %v, resolved %v: %s", c.customer, accepted, replied, resolved, itTypes(evs))
		}
	}

	// Nothing is held twice at the same time, and a conversation that an
	// operator held when it went offline was served again.
	servedAgain, byShift, afterRestart := 0, 0, 0
	for key, evs := range byKey {
		live := ""
		returned, shift := false, false
		for _, ev := range evs {
			switch ev.Type {
			case event.TypeOffered:
				if live != "" {
					t.Errorf("%s: offer %s while %s is live: %s", key, ev.OfferID, live, itTypes(evs))
				}
				live = ev.OfferID
			case event.TypeAccepted:
				if live != ev.OfferID {
					t.Errorf("%s: accepted %s while %q is live", key, ev.OfferID, live)
				}
				if returned {
					servedAgain++
					if shift {
						byShift++
					}
					returned, shift = false, false
				}
				if ev.At.After(restartAt) {
					afterRestart++
				}
			case event.TypeOfferExpired, event.TypeResolved, event.TypeEnded:
				live = ""
			case event.TypeQueued:
				if ev.QueueReason == event.QueueOperatorOffline && live != "" {
					returned, shift = true, ev.At.After(shiftsFrom)
				}
				live = ""
			}
		}
	}
	if servedAgain == 0 {
		t.Error("no conversation was returned by an operator going offline and served again")
	}
	if byShift == 0 {
		t.Error("no conversation was returned by a shift change and served again")
	}
	if afterRestart == 0 {
		t.Error("no accept after the restart")
	}
	t.Logf("%d conversations served again after an operator went offline (%d by a shift change), %d accepts after the restart",
		servedAgain, byShift, afterRestart)
}

func itTypes(evs []event.Routing) string {
	parts := make([]string, len(evs))
	for i, ev := range evs {
		parts[i] = string(ev.Type)
		if ev.QueueReason != "" {
			parts[i] += "(" + string(ev.QueueReason) + ")"
		}
		if ev.OperatorID != "" {
			parts[i] += "@" + ev.OperatorID
		}
	}
	return strings.Join(parts, ",")
}
