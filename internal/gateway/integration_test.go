//go:build integration

package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
	"github.com/ZAUakaAlexey/threadroute/internal/bus/kafkatest"
	"github.com/ZAUakaAlexey/threadroute/internal/conv"
	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/pg"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgtest"
	"github.com/ZAUakaAlexey/threadroute/internal/router"
	"github.com/ZAUakaAlexey/threadroute/internal/router/opclient"
	"github.com/ZAUakaAlexey/threadroute/internal/router/routersvc"
)

// redisImage is the official Redis image, as in the conv tests.
const redisImage = "redis:7-alpine"

// itGrace is the offline grace time of the integration tests.
const itGrace = 2 * time.Second

func startRedis(t *testing.T) string {
	t.Helper()
	ctr, err := testcontainers.Run(t.Context(), redisImage,
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithWaitStrategy(wait.ForLog("Ready to accept connections").WithStartupTimeout(time.Minute)),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start %s: %v", redisImage, err)
	}
	addr, err := ctr.PortEndpoint(t.Context(), "6379/tcp", "")
	if err != nil {
		t.Fatal(err)
	}
	return addr
}

func itLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// stack is Kafka, Redis and PostgreSQL with the real conv and router
// services, a fake site API and a producer of synthetic input.
type stack struct {
	t         *testing.T
	brokers   []string
	rdb       *redis.Client
	routerURL string
	siteURL   string
	prod      *kgo.Client
	seq       atomic.Int64
}

func startStack(t *testing.T) *stack {
	t.Helper()
	var wg sync.WaitGroup
	var kafkaAddr, redisAddr, dsn string
	wg.Go(func() { kafkaAddr = kafkatest.Start(t) })
	wg.Go(func() { redisAddr = startRedis(t) })
	wg.Go(func() { dsn = pgtest.Start(t) })
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	s := &stack{t: t, brokers: []string{kafkaAddr}}
	ctx := context.WithoutCancel(t.Context())

	prod, err := kgo.NewClient(kgo.SeedBrokers(s.brokers...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	s.prod = prod
	if err := bus.EnsureTopics(ctx, prod, nil, bus.EventsTopicSpec(), bus.ControlTopicSpec(), bus.ConversationsTopicSpec()); err != nil {
		t.Fatal(err)
	}

	// conv, with every company admitted by its first customer.
	s.rdb = redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = s.rdb.Close() })
	rules := conv.DefaultRules()
	rules.AdmissionThreshold = 1
	store, err := conv.NewStore(s.rdb, rules, conv.DefaultKeyPrefix)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := conv.NewKafkaPublisher(s.brokers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)
	convSvc, err := conv.New(conv.Options{
		Brokers: s.brokers, GroupID: "it-conv", Store: store, Publisher: pub,
		SweepInterval: 200 * time.Millisecond, Logger: itLogger(), Registerer: prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatal(err)
	}
	runInBackground(t, "conv", convSvc.Run)

	// The router, with a long accept timeout and long targets.
	pool, err := pg.Open(ctx, pg.Config{DSN: dsn, MaxConns: 8, ConnectWait: 30 * time.Second}, itLogger())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pg.Migrate(ctx, pool, itLogger()); err != nil {
		t.Fatal(err)
	}
	rc := router.DefaultConfig()
	rc.AcceptTimeout = 5 * time.Minute
	rc.AssignmentTarget, rc.FirstResponseTarget = time.Hour, time.Hour
	rsvc, err := routersvc.New(ctx, routersvc.Options{
		Config: routersvc.Config{
			KafkaBrokers: s.brokers, GroupID: "it-router",
			Postgres: pg.Config{DSN: dsn, MaxConns: 8, ConnectWait: 30 * time.Second},
			Router:   rc, MetricsAddr: "127.0.0.1:0", APIAddr: "127.0.0.1:0",
			InputTopicWait: 30 * time.Second, AssignWorkers: 2, AssignInterval: 50 * time.Millisecond,
			ExpireInterval: time.Second, SLAInterval: time.Second, TeamInterval: 10 * time.Second,
			PublishInterval: 20 * time.Millisecond, PublishBatch: 100, OutboxKeep: time.Hour,
			PruneInterval: time.Minute, CountsInterval: time.Second, OutageLimit: time.Minute,
			OperatorRefresh: time.Hour, LogLevel: slog.LevelWarn,
		},
		Pool: pool, Logger: itLogger(), Registerer: prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatal(err)
	}
	runInBackground(t, "router", rsvc.Run)
	waitUntil(t, 90*time.Second, "router ready", rsvc.Ready)
	s.routerURL = "http://" + rsvc.APIAddr()

	// The site API: "full-token" has a role of the list, any other token
	// is a signed-in user without it.
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("Authorization") {
		case "Bearer full-token":
			_, _ = w.Write([]byte(`{"user":{"id":7},"role":{"name":"reviewer"}}`))
		default:
			w.WriteHeader(http.StatusUnauthorized)
		}
	}))
	t.Cleanup(site.Close)
	s.siteURL = site.URL
	return s
}

func runInBackground(t *testing.T, name string, run func(context.Context) error) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("%s: %v", name, err)
			}
		case <-time.After(60 * time.Second):
			t.Errorf("%s did not stop", name)
		}
	})
}

func waitUntil(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// message produces a synthetic customer message.
func (s *stack) message(company, cust, msgID, text string) {
	s.t.Helper()
	n := s.seq.Add(1)
	m := event.Message{
		EventID: fmt.Sprintf("it-event-%d", n), Channel: event.ChannelBluesky,
		SourceTime: time.Now().UTC(), MessageID: msgID, CompanyID: company, CustomerID: cust,
		Direction: event.FromCustomer, ParentID: "it-parent-" + msgID, Text: text,
		Langs: []string{"en"}, Lang: "en",
	}
	b, err := event.MarshalMessage(m)
	if err != nil {
		s.t.Fatal(err)
	}
	rec := &kgo.Record{Topic: bus.TopicEvents, Key: event.ConversationKey(company, cust), Value: b,
		Headers: []kgo.RecordHeader{{Key: bus.HeaderEventID, Value: []byte(m.EventID)}}}
	if err := s.prod.ProduceSync(s.t.Context(), rec).FirstErr(); err != nil {
		s.t.Fatal(err)
	}
}

// deletion produces a synthetic deletion of a message by its author.
func (s *stack) deletion(author, msgID string) {
	s.t.Helper()
	n := s.seq.Add(1)
	d := event.Deletion{EventID: fmt.Sprintf("it-event-%d", n), Channel: event.ChannelBluesky,
		SourceTime: time.Now().UTC(), MessageID: msgID, AuthorID: author}
	b, err := event.MarshalControl(d)
	if err != nil {
		s.t.Fatal(err)
	}
	rec := &kgo.Record{Topic: bus.TopicControl, Key: event.AuthorKey(author), Value: b,
		Headers: []kgo.RecordHeader{{Key: bus.HeaderEventID, Value: []byte(d.EventID)}}}
	if err := s.prod.ProduceSync(s.t.Context(), rec).FirstErr(); err != nil {
		s.t.Fatal(err)
	}
}

// running is a gateway service in the background.
type running struct {
	svc    *Service
	cancel context.CancelFunc
	done   chan error
}

func (s *stack) startGateway() *running {
	s.t.Helper()
	cfg := DefaultConfig()
	cfg.KafkaBrokers = s.brokers
	cfg.HTTPAddr = "127.0.0.1:0"
	cfg.RouterURL = s.routerURL
	cfg.SiteAPIURL = s.siteURL
	cfg.FullRoles = []string{"reviewer"}
	cfg.SettingsRoles = []string{"reviewer"}
	cfg.OfflineGrace = itGrace
	cfg.LogLevel = slog.LevelWarn
	api, err := opclient.New(cfg.RouterURL, opclient.WithTimeout(5*time.Second))
	if err != nil {
		s.t.Fatal(err)
	}
	text, err := conv.NewTextReader(s.rdb, conv.DefaultKeyPrefix)
	if err != nil {
		s.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(s.t.Context()))
	svc, err := NewService(ctx, ServiceOptions{
		Config: cfg, API: api, Text: text, Logger: itLogger(), Registerer: prometheus.NewRegistry(),
		Auth: NewSiteChecker(cfg.SiteAPIURL, Roles{Full: cfg.FullRoles, Settings: cfg.SettingsRoles}, time.Second, time.Minute, nil, nil),
	})
	if err != nil {
		s.t.Fatal(err)
	}
	r := &running{svc: svc, cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- svc.Run(ctx) }()
	s.t.Cleanup(r.stop)
	waitUntil(s.t, 60*time.Second, "gateway ready", svc.Ready)
	waitUntil(s.t, 10*time.Second, "router settings", func() bool { return svc.g.settings.Load() != nil })
	return r
}

func (r *running) stop() {
	if r.cancel == nil {
		return
	}
	r.cancel()
	r.cancel = nil
	<-r.done
}

// wsClient is a browser.
type wsClient struct {
	t      *testing.T
	c      *websocket.Conn
	aud    gwapi.Audience
	frames chan []byte
	closed chan error
	w      gwapi.Welcome
}

func (s *stack) dial(r *running, h gwapi.Hello) *wsClient {
	s.t.Helper()
	ctx := s.t.Context()
	c, _, err := websocket.Dial(ctx, "ws://"+r.svc.Addr()+gwapi.WebSocketPath, nil)
	if err != nil {
		s.t.Fatal(err)
	}
	c.SetReadLimit(16 << 20)
	cl := &wsClient{t: s.t, c: c, frames: make(chan []byte, 1000), closed: make(chan error, 1)}
	go func() {
		for {
			_, b, err := c.Read(context.WithoutCancel(ctx))
			if err != nil {
				cl.closed <- err
				close(cl.frames)
				return
			}
			cl.frames <- b
		}
	}()
	s.t.Cleanup(func() { _ = c.CloseNow() })
	h.Version = gwapi.ProtocolVersion
	cl.send(h)
	b := cl.next(10 * time.Second)
	if err := json.Unmarshal(b, &cl.w); err != nil || cl.w.Version != 1 {
		s.t.Fatalf("welcome %s: %v", b, err)
	}
	cl.aud = audOf(cl.w)
	return cl
}

func (cl *wsClient) send(m gwapi.ClientMessage) {
	cl.t.Helper()
	b, err := gwapi.EncodeClient(m)
	if err != nil {
		cl.t.Fatal(err)
	}
	if err := cl.c.Write(cl.t.Context(), websocket.MessageText, b); err != nil {
		cl.t.Fatal(err)
	}
}

func (cl *wsClient) next(d time.Duration) []byte {
	cl.t.Helper()
	select {
	case b, ok := <-cl.frames:
		if !ok {
			cl.t.Fatal("connection closed")
		}
		return b
	case <-time.After(d):
		cl.t.Fatal("no frame in time")
		return nil
	}
}

// until reads frames, each checked against the protocol, until fn
// returns true for one.
func (cl *wsClient) until(what string, fn func(m gwapi.ServerMessage) bool) gwapi.ServerMessage {
	cl.t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			cl.t.Fatalf("timeout: %s", what)
		}
		b := cl.next(left)
		m, err := gwapi.DecodeServer(b, cl.aud)
		if err != nil {
			cl.t.Fatalf("invalid frame %s: %v", b, err)
		}
		if cl.aud.Level == gwapi.LevelDemo && strings.Contains(string(b), "synthetic") {
			cl.t.Fatalf("demo frame with text: %s", b)
		}
		if fn(m) {
			return m
		}
	}
}

func (cl *wsClient) action(id string, a gwapi.ActionName, offer string) gwapi.Result {
	cl.t.Helper()
	cl.send(gwapi.Action{RequestID: id, Action: a, OfferID: offer})
	return cl.result(id)
}

func (cl *wsClient) result(id string) gwapi.Result {
	cl.t.Helper()
	m := cl.until("result "+id, func(m gwapi.ServerMessage) bool {
		r, ok := m.(gwapi.Result)
		return ok && r.RequestID == id
	})
	return m.(gwapi.Result)
}

// myConv waits for a my_conversations row in the given state.
func (cl *wsClient) myConv(state gwapi.ConversationState) gwapi.OperatorConversation {
	cl.t.Helper()
	var row gwapi.OperatorConversation
	cl.until("my conversation "+string(state), func(m gwapi.ServerMessage) bool {
		st, ok := m.(gwapi.State)
		if !ok || st.MyConversations == nil {
			return false
		}
		for _, r := range st.MyConversations.Set {
			if r.State == state {
				row = r
				return true
			}
		}
		return false
	})
	return row
}

func wantDone(t *testing.T, what string, r gwapi.Result) {
	t.Helper()
	if r.Outcome != gwapi.OutcomeDone {
		t.Fatalf("%s: %+v", what, r)
	}
}

// viewJSON is the full-level supervisor snapshot of a gateway's view.
func viewJSON(t *testing.T, g *Gateway, now time.Time) string {
	t.Helper()
	b := &builder{aud: gwapi.Audience{Level: gwapi.LevelFull, View: gwapi.ViewSupervisor}, ps: g.ps, timing: g.timing(), frameRows: 1 << 20}
	var out []byte
	g.view.Read(func(rd Reader) {
		frames := b.snapshot(rd, rd.Totals(now))
		var seq uint64
		stamp(frames, &seq, now)
		out, _ = json.Marshal(frames)
	})
	return string(out)
}

const (
	itCompany = "did:plc:itsynthcompany000000000001"
	itCust1   = "did:plc:itsynthcustomer00000000001"
	itCust2   = "did:plc:itsynthcustomer00000000002"
	itCust3   = "did:plc:itsynthcustomer00000000003"
)

func TestIntegration(t *testing.T) {
	s := startStack(t)
	gw := s.startGateway()

	t.Run("DemoOperatorLifecycle", func(t *testing.T) {
		op := s.dial(gw, gwapi.Hello{View: gwapi.ViewOperator, Languages: []string{"en", "und"}})
		if op.w.Level != gwapi.LevelDemo || !strings.HasPrefix(op.w.OperatorID, "web-") {
			t.Fatalf("welcome %+v", op.w)
		}
		op.until("first state", func(m gwapi.ServerMessage) bool { st, ok := m.(gwapi.State); return ok && st.Snapshot })
		wantDone(t, "online", op.action("on", gwapi.ActionOnline, ""))
		s.message(itCompany, itCust1, "it-msg-1", "synthetic question one")
		offer := op.myConv(gwapi.StateOffered)
		if !gwapi.ValidPseudonym(offer.ID) || offer.CustomerMessages != 1 {
			t.Fatalf("offer %+v", offer)
		}
		wantDone(t, "accept", op.action("acc", gwapi.ActionAccept, offer.OfferID))
		op.myConv(gwapi.StateAssigned)
		op.send(gwapi.Watch{RequestID: "w", ConversationID: offer.ID})
		wantDone(t, "watch", op.result("w"))
		m := op.until("messages", func(m gwapi.ServerMessage) bool { _, ok := m.(gwapi.Messages); return ok }).(gwapi.Messages)
		if len(m.Messages) != 1 || m.Messages[0].Text != nil || m.Messages[0].Length != len("synthetic question one") {
			t.Fatalf("demo messages %+v", m)
		}
		wantDone(t, "reply", op.action("rep-1", gwapi.ActionReply, offer.OfferID))
		if r := op.action("rep-1", gwapi.ActionReply, offer.OfferID); r.Outcome != gwapi.OutcomeAlreadyDone {
			t.Fatalf("repeated reply %+v", r)
		}
		wantDone(t, "resolve", op.action("res", gwapi.ActionResolve, offer.OfferID))
		op.until("assignment removed", func(m gwapi.ServerMessage) bool {
			st, ok := m.(gwapi.State)
			return ok && st.MyConversations != nil && len(st.MyConversations.Remove) == 1
		})
		wantDone(t, "offline", op.action("off", gwapi.ActionOffline, ""))
	})

	t.Run("FullLevelTextRemovalAndGrace", func(t *testing.T) {
		sup := s.dial(gw, gwapi.Hello{View: gwapi.ViewSupervisor, Token: "full-token"})
		op := s.dial(gw, gwapi.Hello{View: gwapi.ViewOperator, Languages: []string{"en"}, Token: "full-token"})
		if op.w.Level != gwapi.LevelFull {
			t.Fatalf("level %s", op.w.Level)
		}
		wantDone(t, "online", op.action("on", gwapi.ActionOnline, ""))
		s.message(itCompany, itCust2, "it-msg-2", "synthetic question two")
		offer := op.myConv(gwapi.StateOffered)
		if offer.ID != string(event.ConversationKey(itCompany, itCust2)) {
			t.Fatalf("full level ID %q", offer.ID)
		}
		wantDone(t, "accept", op.action("acc", gwapi.ActionAccept, offer.OfferID))
		op.myConv(gwapi.StateAssigned)
		op.send(gwapi.Watch{RequestID: "w", ConversationID: offer.ID})
		wantDone(t, "watch", op.result("w"))
		m := op.until("messages", func(m gwapi.ServerMessage) bool { _, ok := m.(gwapi.Messages); return ok }).(gwapi.Messages)
		if len(m.Messages) != 1 || m.Messages[0].Text == nil || *m.Messages[0].Text != "synthetic question two" {
			t.Fatalf("full messages %+v", m)
		}
		s.deletion(itCust2, "it-msg-2")
		rm := op.until("message removed", func(m gwapi.ServerMessage) bool { _, ok := m.(gwapi.MessageRemoved); return ok }).(gwapi.MessageRemoved)
		if rm.MessageID != "it-msg-2" || rm.Reason != event.RemoveDeleted {
			t.Fatalf("removed %+v", rm)
		}

		// Closing the socket: offline after the grace time, and the
		// conversation goes back to the queue.
		start := time.Now()
		_ = op.c.Close(websocket.StatusNormalClosure, "")
		sup.until("conversation back in the queue", func(m gwapi.ServerMessage) bool {
			st, ok := m.(gwapi.State)
			if !ok || st.Conversations == nil {
				return false
			}
			for _, r := range st.Conversations.Set {
				if r.ID == offer.ID && r.State == gwapi.StateWaiting {
					return true
				}
			}
			return false
		})
		if took := time.Since(start); took < itGrace {
			t.Fatalf("back in the queue after %s, before the grace time", took)
		}
	})

	t.Run("ResumeWithinGrace", func(t *testing.T) {
		op := s.dial(gw, gwapi.Hello{View: gwapi.ViewOperator, Languages: []string{"en"}})
		wantDone(t, "online", op.action("on", gwapi.ActionOnline, ""))
		// The waiting conversation of the test before comes here.
		offer := op.myConv(gwapi.StateOffered)
		wantDone(t, "accept", op.action("acc", gwapi.ActionAccept, offer.OfferID))
		op.myConv(gwapi.StateAssigned)
		_ = op.c.Close(websocket.StatusNormalClosure, "")
		time.Sleep(itGrace / 2)
		again := s.dial(gw, gwapi.Hello{View: gwapi.ViewOperator, Languages: []string{"en"}, ResumeToken: op.w.ResumeToken})
		if again.w.OperatorID != op.w.OperatorID {
			t.Fatalf("resume gave %s, want %s", again.w.OperatorID, op.w.OperatorID)
		}
		st := again.until("snapshot", func(m gwapi.ServerMessage) bool { st, ok := m.(gwapi.State); return ok && st.Snapshot }).(gwapi.State)
		if st.MyConversations == nil || len(st.MyConversations.Set) != 1 || st.MyConversations.Set[0].State != gwapi.StateAssigned {
			t.Fatalf("snapshot after resume %+v", st)
		}
		time.Sleep(2 * itGrace)
		if r := again.action("res", gwapi.ActionResolve, offer.OfferID); r.Outcome != gwapi.OutcomeDone {
			t.Fatalf("resolve after the grace time %+v: the operator lost the assignment", r)
		}
		wantDone(t, "offline", again.action("off", gwapi.ActionOffline, ""))
	})

	t.Run("StandSettingsWithTheRealRouter", func(t *testing.T) {
		sup := s.dial(gw, gwapi.Hello{View: gwapi.ViewSupervisor, Token: "full-token"})
		other := s.dial(gw, gwapi.Hello{View: gwapi.ViewSupervisor})
		ws := sup.w.Settings
		if ws == nil || !ws.MayChange || other.w.Settings == nil || other.w.Settings.MayChange {
			t.Fatalf("welcome settings %+v / %+v", ws, other.w.Settings)
		}
		// No simulator runs here: version 0, no sim part, nothing logged.
		if ws.Version != 0 || ws.Current.Sim != nil || len(ws.Changes) != 0 {
			t.Fatalf("first settings %+v", ws)
		}
		next := cloneSet(ws.Current)
		next.Teams.MaxShare = 0.3
		sup.send(gwapi.ChangeSettings{RequestID: "set-1", BaseVersion: 0, Settings: next})
		var pushed *gwapi.Settings
		res := sup.until("result set-1", func(m gwapi.ServerMessage) bool {
			if u, ok := m.(gwapi.SettingsUpdate); ok {
				pushed = &u.Settings
			}
			r, ok := m.(gwapi.Result)
			return ok && r.RequestID == "set-1"
		}).(gwapi.Result)
		if res.Outcome != gwapi.OutcomeDone || pushed == nil || pushed.Version != 1 ||
			len(pushed.Changes) != 1 || pushed.Changes[0].By != "user:7" || pushed.Current.Teams.MaxShare != 0.3 {
			t.Fatalf("result %+v after settings %+v", res, pushed)
		}
		u := other.until("settings push", func(m gwapi.ServerMessage) bool { _, ok := m.(gwapi.SettingsUpdate); return ok }).(gwapi.SettingsUpdate)
		if u.Settings.Version != 1 || u.Settings.Changes[0].By != "" {
			t.Fatalf("push %+v", u.Settings)
		}
		// The same set again changes nothing; an old base is a conflict.
		sup.send(gwapi.ChangeSettings{RequestID: "set-2", BaseVersion: 1, Settings: next})
		if r := sup.result("set-2"); r.Outcome != gwapi.OutcomeAlreadyDone {
			t.Fatalf("repeat %+v", r)
		}
		sup.send(gwapi.ChangeSettings{RequestID: "set-3", BaseVersion: 0, Settings: next})
		if r := sup.result("set-3"); r.Code != gwapi.CodeSettingsConflict {
			t.Fatalf("old base %+v", r)
		}
		// One change per 10 seconds for the whole stand.
		again := cloneSet(next)
		again.Teams.MaxShare = 0.4
		sup.send(gwapi.ChangeSettings{RequestID: "set-4", BaseVersion: 1, Settings: again})
		if r := sup.result("set-4"); r.Code != gwapi.CodeSettingsTooSoon || r.RetryAfterMS < 1 || r.RetryAfterMS > 10000 {
			t.Fatalf("too soon %+v", r)
		}
	})

	t.Run("RestartRebuildsTheView", func(t *testing.T) {
		s.message(itCompany, itCust3, "it-msg-3", "synthetic question three")
		waitUntil(t, 30*time.Second, "third conversation in the view", func() bool {
			ok := false
			gw.svc.g.view.Read(func(rd Reader) { _, ok = rd.Conversation(string(event.ConversationKey(itCompany, itCust3))) })
			return ok
		})
		// Quiet: no session holds an operator, so the stop writes nothing.
		waitUntil(t, 30*time.Second, "no human operator online", func() bool {
			var online []string
			gw.svc.g.view.Read(func(rd Reader) { online = rd.OnlineHumans() })
			return len(online) == 0
		})
		time.Sleep(2 * time.Second)
		now := time.Now()
		before := viewJSON(t, gw.svc.g, now)
		gw.stop()
		gw2 := s.startGateway()
		after := viewJSON(t, gw2.svc.g, now)
		if before != after {
			t.Fatalf("view after the restart differs:\n%s\nbefore:\n%s", after, before)
		}
		if !strings.Contains(before, itCust3) {
			t.Fatalf("the view has not the test conversations: %s", before)
		}

		// A human operator left online by a gateway that is gone ends
		// up offline when the next gateway has caught up.
		gw2.stop()
		api, err := opclient.New(s.routerURL)
		if err != nil {
			t.Fatal(err)
		}
		const orphan = "web-itorphan00000001"
		if _, err := api.PutOperator(t.Context(), orphan, opapi.OperatorRequest{Languages: []string{"en"}, Capacity: 2}); err != nil {
			t.Fatal(err)
		}
		if _, err := api.Online(t.Context(), orphan); err != nil {
			t.Fatal(err)
		}
		gw3 := s.startGateway()
		waitUntil(t, 30*time.Second, "orphan offline", func() bool {
			off := false
			gw3.svc.g.view.Read(func(rd Reader) {
				row, ok := rd.Operator(orphan)
				off = ok && row.Presence == gwapi.PresenceOffline
			})
			return off
		})
	})
}

// TestIntegrationSettingsAndTrend runs the gateway service on a real Kafka
// with a fake router that offers stand settings (no router package): the
// trend strip starts only after the view caught up, and a change of the
// settings over a real WebSocket reaches the other connection at once.
func TestIntegrationSettingsAndTrend(t *testing.T) {
	brokers := []string{kafkatest.Start(t)}
	ctx := context.WithoutCancel(t.Context())
	prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	if err := bus.EnsureTopics(ctx, prod, nil, bus.ConversationsTopicSpec(), bus.RoutingTopicSpec()); err != nil {
		t.Fatal(err)
	}
	// History before the start: 40 operators online, one team of two online
	// members.
	const online = 40
	var recs []*kgo.Record
	for i := range online {
		id := fmt.Sprintf("sim-op-%04d", i)
		ev := opEv(event.TypeOperatorOnline, id, 1, time.Now(), event.PresenceOnline)
		if i < 2 {
			ev.DedicatedCompanyID = itCompany
		}
		b, err := event.MarshalRouting(ev)
		if err != nil {
			t.Fatal(err)
		}
		recs = append(recs, &kgo.Record{Topic: bus.TopicRouting, Key: event.OperatorKey(id), Value: b})
	}
	team, err := event.MarshalRouting(teamEv(event.TypeTeamFormed, itCompany, 1, time.Now(), "sim-op-0000", "sim-op-0001"))
	if err != nil {
		t.Fatal(err)
	}
	recs = append(recs, &kgo.Record{Topic: bus.TopicRouting, Key: event.CompanyKey(itCompany), Value: team})
	if err := prod.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}

	api := newFakeAPI()
	router := newFakeRouter(t, routerStand, api.settings, testStand())
	api.router = router
	cfg := DefaultConfig()
	cfg.KafkaBrokers = brokers
	cfg.HTTPAddr = "127.0.0.1:0"
	cfg.LogLevel = slog.LevelWarn
	svc, err := NewService(ctx, ServiceOptions{
		Config: cfg, API: api, Auth: fakeAuth{}, Text: &fakeText{}, Logger: itLogger(), Registerer: prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatal(err)
	}
	svc.trendEvery = 100 * time.Millisecond
	runInBackground(t, "gateway", svc.Run)
	waitUntil(t, 60*time.Second, "gateway ready", svc.Ready)
	waitUntil(t, 10*time.Second, "trend points", func() bool { return len(svc.g.trendHistory()) >= 3 })
	for _, p := range svc.g.trendHistory() {
		if p.OperatorsOnline != online || p.Teams != 1 || p.OperatorsInTeams != 2 {
			t.Fatalf("a point before the view caught up: %+v", p)
		}
	}
	waitUntil(t, 10*time.Second, "stand settings", func() bool {
		s := svc.g.settings.Load()
		return s != nil && s.Stand != nil
	})

	url := "ws://" + svc.Addr() + gwapi.WebSocketPath
	type conn struct {
		c   *websocket.Conn
		aud gwapi.Audience
	}
	open := func(h gwapi.Hello) conn {
		t.Helper()
		c, _, err := websocket.Dial(t.Context(), url, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.CloseNow() })
		c.SetReadLimit(16 << 20)
		h.Version = gwapi.ProtocolVersion
		b, _ := gwapi.EncodeClient(h)
		if err := c.Write(t.Context(), websocket.MessageText, b); err != nil {
			t.Fatal(err)
		}
		_, b, err = c.Read(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		var w gwapi.Welcome
		if err := json.Unmarshal(b, &w); err != nil || w.Settings == nil || w.Trend == nil || len(w.Trend.Points) == 0 {
			t.Fatalf("welcome %s: %v", b, err)
		}
		return conn{c: c, aud: audOf(w)}
	}
	// until reads frames, each checked, until fn returns true.
	until := func(x conn, what string, fn func(m gwapi.ServerMessage) bool) []gwapi.ServerMessage {
		t.Helper()
		rctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		var seen []gwapi.ServerMessage
		for {
			_, b, err := x.c.Read(rctx)
			if err != nil {
				t.Fatalf("%s: %v", what, err)
			}
			m, err := gwapi.DecodeServer(b, x.aud)
			if err != nil {
				t.Fatalf("frame %s: %v", b, err)
			}
			if fn(m) {
				return seen
			}
			seen = append(seen, m)
		}
	}
	asker := open(gwapi.Hello{View: gwapi.ViewSupervisor, Token: settingsToken})
	other := open(gwapi.Hello{View: gwapi.ViewSupervisor})
	until(other, "a trend point", func(m gwapi.ServerMessage) bool { _, ok := m.(gwapi.TrendUpdate); return ok })

	req := change("it-settings", testStand(), func(s *opapi.StandSettings) { s.Teams.MaxShare = 0.3 })
	b, _ := gwapi.EncodeClient(req)
	if err := asker.c.Write(t.Context(), websocket.MessageText, b); err != nil {
		t.Fatal(err)
	}
	var res gwapi.Result
	before := until(asker, "result", func(m gwapi.ServerMessage) bool {
		r, ok := m.(gwapi.Result)
		res = r
		return ok && r.RequestID == "it-settings"
	})
	if res.Outcome != gwapi.OutcomeDone {
		t.Fatalf("result %+v", res)
	}
	if ups := settingsUpdates(before); len(ups) != 1 || ups[0].Settings.Version != 4 || ups[0].Settings.Changes[0].By != "user:9" {
		t.Fatalf("settings before the result: %+v", ups)
	}
	var got gwapi.SettingsUpdate
	until(other, "settings push", func(m gwapi.ServerMessage) bool {
		u, ok := m.(gwapi.SettingsUpdate)
		got = u
		return ok
	})
	if got.Settings.Version != 4 || got.Settings.MayChange || got.Settings.Changes[0].By != "" {
		t.Fatalf("push %+v", got.Settings)
	}
	if v := router.version(); v != 4 {
		t.Fatalf("router version %d", v)
	}
}
