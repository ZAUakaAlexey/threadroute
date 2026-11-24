package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
)

func TestPublicSnapshot(t *testing.T) {
	e := newEnv(t)
	for _, s := range history() {
		e.apply(s...)
	}
	q := rt(event.TypeQueued, testCompany, testCustomer, 1, 1, t0, "", "")
	e.apply(ci(convEv(event.TypeOpened, testCompany, testCustomer, 1, 1, t0), t0), ri(q, t0))
	pub := e.g.NewPublicSnapshots()
	e.clock.Advance(61 * time.Second)
	srv := httptest.NewServer(e.g.Handler(t.Context(), pub))
	defer srv.Close()
	resp, err := http.Get(srv.URL + gwapi.PublicSnapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || resp.Header.Get("Cache-Control") != "public, max-age=1" {
		t.Fatalf("status %d headers %v", resp.StatusCode, resp.Header)
	}
	snap, err := gwapi.DecodePublic(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Queues) == 0 || snap.Queues[0].Lang != "de" {
		t.Fatalf("queues %+v", snap.Queues)
	}
	var en gwapi.PublicQueue
	for _, pq := range snap.Queues {
		if pq.Lang == "en" {
			en = pq
		}
	}
	if en.Waiting != 1 || en.OldestWaitUS != 61_000_000 {
		t.Fatalf("en queue %+v", en)
	}
	// Totals only: no ID of the test data anywhere.
	for _, s := range []string{"did:plc", "sim-op", "web-", "synth"} {
		if strings.Contains(string(body), s) {
			t.Errorf("public snapshot holds %q", s)
		}
	}
	// The field set is exactly the protocol's.
	var generic map[string]any
	if err := json.Unmarshal(body, &generic); err != nil {
		t.Fatal(err)
	}
	keys := slices.Sorted(func(yield func(string) bool) {
		for k := range generic {
			if !yield(k) {
				return
			}
		}
	})
	if want := []string{"generated_at_us", "queues", "speed_factor", "stale", "totals", "version"}; !reflect.DeepEqual(keys, want) {
		t.Errorf("fields %v", keys)
	}
}

func listed(topic string, offsets ...int64) kadm.ListedOffsets {
	out := kadm.ListedOffsets{topic: {}}
	for p, o := range offsets {
		out[topic][int32(p)] = kadm.ListedOffset{Topic: topic, Partition: int32(p), Offset: o}
	}
	return out
}

func merge(a, b kadm.ListedOffsets) kadm.ListedOffsets {
	for k, v := range b {
		a[k] = v
	}
	return a
}

func TestSourceStatus(t *testing.T) {
	e := newEnv(t)
	src := e.g.NewSource(SourceOptions{ConversationsTopic: "c", RoutingTopic: "r"})
	starts := func() kadm.ListedOffsets { return merge(listed("c", 0, 5), listed("r", 0)) }
	ends := func(c0, c1, r0 int64) kadm.ListedOffsets { return merge(listed("c", c0, c1), listed("r", r0)) }
	now := t0
	step := func(d time.Duration, st, en kadm.ListedOffsets, err error) gwapi.Status {
		now = now.Add(d)
		src.apply(now, st, en, err)
		return e.g.Status()
	}
	// Partition c/1 is empty after its records expired (start 5 = end 5).
	if st := step(0, starts(), ends(3, 5, 2), nil); st.Reason != gwapi.StaleCatchingUp || src.Ready() {
		t.Fatalf("start %+v", st)
	}
	src.mu.Lock()
	src.pos[topicPartition{"c", 0}] = 3
	src.pos[topicPartition{"r", 0}] = 2
	src.mu.Unlock()
	if st := step(time.Second, starts(), ends(3, 5, 2), nil); st.Stale || !src.Ready() {
		t.Fatalf("caught up %+v", st)
	}
	// New records: behind, but not for long.
	if st := step(time.Second, starts(), ends(9, 5, 2), nil); st.Stale {
		t.Fatalf("just behind %+v", st)
	}
	if st := step(11*time.Second, starts(), ends(12, 5, 2), nil); st.Reason != gwapi.StaleLagging || st.LagUS != 11_000_000 {
		t.Fatalf("lagging %+v", st)
	}
	src.mu.Lock()
	src.pos[topicPartition{"c", 0}] = 12
	src.mu.Unlock()
	if st := step(time.Second, starts(), ends(12, 5, 2), nil); st.Stale {
		t.Fatalf("after catching up %+v", st)
	}
	// Kafka does not answer: stale after the source timeout.
	if st := step(2*time.Second, nil, nil, errors.New("down")); st.Stale {
		t.Fatalf("short outage %+v", st)
	}
	if st := step(4*time.Second, nil, nil, errors.New("down")); st.Reason != gwapi.StaleSourceUnavailable || st.LagUS != 6_000_000 || src.Ready() {
		t.Fatalf("outage %+v", st)
	}
	if st := step(time.Second, starts(), ends(12, 5, 2), nil); st.Stale || !src.Ready() {
		t.Fatalf("back %+v", st)
	}
}

func TestSnapshotInSeveralFrames(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.SnapshotFrameRows = 4 })
	for i := range 10 {
		e.apply(ci(convEv(event.TypeOpened, testCompany, customer(100+i), 1, 1, t0), t0))
	}
	c, _ := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor}, "10.0.0.1")
	aud := gwapi.Audience{Level: gwapi.LevelDemo, View: gwapi.ViewSupervisor}
	rows := 0
	for i := range 3 {
		st := decodeState(t, c.nextOf(t, gwapi.TypeState), aud)
		if st.Seq != uint64(i+1) || st.Snapshot != (i == 0) || (i == 0) != (st.Totals != nil) {
			t.Fatalf("frame %d: %+v", i, st)
		}
		rows += len(st.Conversations.Set)
	}
	if rows != 10 {
		t.Fatalf("%d rows", rows)
	}
}

func TestLoadConfig(t *testing.T) {
	cfg, err := Load(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MetricsAddr != ":9471" || cfg.HTTPAddr != ":9472" || cfg.Tick != 250*time.Millisecond ||
		cfg.OfflineGrace != 15*time.Second || cfg.DemoOperators != 5 || cfg.SiteAPIURL != "" ||
		len(cfg.AllowedOrigins) != 2 || cfg.TrustProxy {
		t.Fatalf("defaults %+v", cfg)
	}
	envs := map[string]string{
		EnvSiteAPIURL: "https://site.invalid/api/v1/users/me", EnvFullRoles: "admin, reviewer",
		EnvAllowedOrigins: "console.invalid", EnvTrustProxy: "true", EnvPathPrefix: "/console/",
		EnvDemoOperators: "3",
	}
	cfg, err = Load(func(k string) (string, bool) { v, ok := envs[k]; return v, ok })
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cfg.FullRoles, []string{"admin", "reviewer"}) || cfg.PathPrefix != "/console" || !cfg.TrustProxy || cfg.DemoOperators != 3 {
		t.Fatalf("set %+v", cfg)
	}
	bad := map[string]string{
		EnvTick: "100ms", EnvSiteAPIURL: "ftp://x", EnvRouterURL: "nope", EnvTrustProxy: "maybe",
		EnvAllowedOrigins: "[", EnvPathPrefix: "x", EnvRedisPassword: "secret-value",
	}
	_, err = Load(func(k string) (string, bool) { v, ok := bad[k]; return v, ok })
	if err == nil {
		t.Fatal("no error")
	}
	for _, k := range []string{EnvTick, EnvSiteAPIURL, EnvRouterURL, EnvTrustProxy, EnvPathPrefix, "allowed origin"} {
		if !strings.Contains(err.Error(), k) {
			t.Errorf("error does not name %s: %v", k, err)
		}
	}
	if strings.Contains(err.Error(), "secret-value") {
		t.Error("the error holds the password")
	}
}

func TestClientAddr(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "192.0.2.1:5555"
	r.Header.Add("X-Forwarded-For", "203.0.113.9, 198.51.100.7")
	if got := clientAddr(r, false); got != "192.0.2.1" {
		t.Errorf("without proxy %q", got)
	}
	if got := clientAddr(r, true); got != "198.51.100.7" {
		t.Errorf("behind a proxy %q", got)
	}
}

// TestRealWebSocket runs the handler with the real library behind an
// HTTP server with short timeouts: the connection outlives them, a large
// message gets 1009 with the protocol's reason, and a foreign origin is
// refused.
func TestRealWebSocket(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.AllowedOrigins = []string{"localhost:*"} })
	srv := httptest.NewUnstartedServer(e.g.Handler(e.ctx, e.g.NewPublicSnapshots()))
	srv.Config.ReadTimeout = 300 * time.Millisecond
	srv.Config.WriteTimeout = 300 * time.Millisecond
	srv.Start()
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + gwapi.WebSocketPath
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	_, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": {"https://elsewhere.invalid"}}})
	if err == nil || resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("foreign origin: %v %v", err, resp)
	}

	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": {"http://localhost:5173"}}})
	if err != nil {
		t.Fatal(err)
	}
	c.SetReadLimit(1 << 20)
	hello, _ := gwapi.EncodeClient(gwapi.Hello{Version: 1, View: gwapi.ViewSupervisor})
	if err := c.Write(ctx, websocket.MessageText, hello); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, _, err := c.Read(ctx); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(600 * time.Millisecond)
	e.apply(ci(convEv(event.TypeOpened, testCompany, testCustomer, 1, 1, t0), t0))
	e.g.Tick(e.clock.Now())
	if _, b, err := c.Read(ctx); err != nil || !strings.Contains(string(b), `"state"`) {
		t.Fatalf("after the server timeouts: %s %v", b, err)
	}
	big := strings.Repeat("x", gwapi.MaxClientMessageBytes+10)
	_ = c.Write(ctx, websocket.MessageText, []byte(big))
	for {
		_, _, err = c.Read(ctx)
		if err != nil {
			break
		}
	}
	var ce websocket.CloseError
	if !errors.As(err, &ce) || ce.Code != websocket.StatusMessageTooBig || ce.Reason != "message_too_big" {
		t.Fatalf("close %v", err)
	}
}

// realDial starts the handler of e behind an HTTP server and opens a
// WebSocket. hello is sent when it is not nil.
func realDial(t *testing.T, e *env, hello gwapi.ClientMessage) (*websocket.Conn, func() (gwapi.Welcome, error)) {
	t.Helper()
	srv := httptest.NewServer(e.g.Handler(e.ctx, e.g.NewPublicSnapshots()))
	t.Cleanup(srv.Close)
	return dialURL(t, "ws"+strings.TrimPrefix(srv.URL, "http")+gwapi.WebSocketPath, hello)
}

func dialURL(t *testing.T, url string, hello gwapi.ClientMessage) (*websocket.Conn, func() (gwapi.Welcome, error)) {
	t.Helper()
	c, _, err := websocket.Dial(t.Context(), url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.CloseNow() })
	c.SetReadLimit(1 << 20)
	if hello != nil {
		b, _ := gwapi.EncodeClient(hello)
		if err := c.Write(t.Context(), websocket.MessageText, b); err != nil {
			t.Fatal(err)
		}
	}
	return c, func() (gwapi.Welcome, error) {
		var w gwapi.Welcome
		_, b, err := c.Read(t.Context())
		if err != nil {
			return w, err
		}
		return w, json.Unmarshal(b, &w)
	}
}

// readUntilClose reads until the server closes and returns the close
// status and reason.
func readUntilClose(t *testing.T, c *websocket.Conn) (websocket.StatusCode, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for {
		_, _, err := c.Read(ctx)
		if err == nil {
			continue
		}
		var ce websocket.CloseError
		if errors.As(err, &ce) {
			return ce.Code, ce.Reason
		}
		t.Fatalf("no close frame: %v", err)
	}
}

// TestRealCloseCodes checks with the real library that the close frames
// reach the browser with the protocol's code and reason, also for the
// closes that come from another goroutine.
func TestRealCloseCodes(t *testing.T) {
	t.Run("replaced", func(t *testing.T) {
		e := newEnv(t)
		old, welcome := realDial(t, e, operatorHello())
		w, err := welcome()
		if err != nil {
			t.Fatal(err)
		}
		h := operatorHello()
		h.ResumeToken = w.ResumeToken
		_, again := realDial(t, e, h)
		if w2, err := again(); err != nil || w2.OperatorID != w.OperatorID {
			t.Fatalf("resume %+v %v", w2, err)
		}
		if code, reason := readUntilClose(t, old); code != websocket.StatusCode(gwapi.CloseReplaced) || reason != "replaced" {
			t.Fatalf("close %d %q", code, reason)
		}
	})
	t.Run("going away on stop", func(t *testing.T) {
		e := newEnv(t)
		c, welcome := realDial(t, e, gwapi.Hello{Version: 1, View: gwapi.ViewSupervisor})
		if _, err := welcome(); err != nil {
			t.Fatal(err)
		}
		e.stop()
		if code, reason := readUntilClose(t, c); code != websocket.StatusGoingAway || reason != "going_away" {
			t.Fatalf("close %d %q", code, reason)
		}
	})
	t.Run("bad message", func(t *testing.T) {
		e := newEnv(t)
		c, _ := realDial(t, e, gwapi.Watch{RequestID: "a", ConversationID: "x"})
		if code, reason := readUntilClose(t, c); code != websocket.StatusCode(gwapi.CloseBadMessage) || reason != "bad_message" {
			t.Fatalf("close %d %q", code, reason)
		}
	})
	t.Run("hello timeout", func(t *testing.T) {
		if testing.Short() {
			t.Skip("waits 5 seconds")
		}
		e := newEnv(t)
		c, _ := realDial(t, e, nil)
		if code, reason := readUntilClose(t, c); code != websocket.StatusCode(gwapi.CloseHelloTimeout) || reason != "hello_timeout" {
			t.Fatalf("close %d %q", code, reason)
		}
	})
}

// TestServiceWithoutKafka runs the whole service against a broker
// address where nothing listens: it keeps running, reports the source as
// unavailable and is not ready, and stops cleanly.
func TestServiceWithoutKafka(t *testing.T) {
	cfg := DefaultConfig()
	cfg.KafkaBrokers = []string{"127.0.0.1:1"}
	cfg.HTTPAddr = "127.0.0.1:0"
	cfg.SourceTimeout = time.Second
	ctx, cancel := context.WithCancel(t.Context())
	svc, err := NewService(ctx, ServiceOptions{
		Config: cfg, API: newFakeAPI(), Auth: fakeAuth{}, Text: &fakeText{}, Registerer: prometheusRegistry(),
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for svc.Gateway().Status().Reason != gwapi.StaleSourceUnavailable {
		if time.Now().After(deadline) {
			t.Fatalf("status %+v", svc.Gateway().Status())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if svc.Ready() {
		t.Fatal("ready without Kafka")
	}
	// The public snapshot says stale.
	resp, err := http.Get("http://" + svc.Addr() + gwapi.PublicSnapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), `"stale":true`) {
		t.Fatalf("snapshot %s", body)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("did not stop")
	}
}
