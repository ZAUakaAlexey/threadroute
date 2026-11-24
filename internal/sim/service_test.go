package sim

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

func testSettings() opapi.Settings {
	return opapi.Settings{SpeedFactor: 100, AcceptTimeoutUS: (20 * time.Second).Microseconds(), CapacityDefault: 4}
}

// testConfig is a small simulator with every operator online at start.
func testConfig(brokers []string) Config {
	b := testBehaviour()
	b.Operators = 3
	b.Languages = Spread{{Lang: "en", Count: 2}, {Lang: "ja", Count: 1}}
	return Config{
		KafkaBrokers: brokers,
		RouterURL:    "http://127.0.0.1:1",
		StartWait:    5 * time.Second,
		GroupID:      "sim-unit",
		MetricsAddr:  "127.0.0.1:0",
		OutageLimit:  500 * time.Millisecond,
		Behaviour:    b,
		LogLevel:     slog.LevelWarn,
	}
}

func newCluster(t *testing.T) *kfake.Cluster {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(4, bus.TopicRouting))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func produceRouting(t *testing.T, cl *kgo.Client, evs ...event.Routing) {
	t.Helper()
	for _, ev := range evs {
		b, err := event.MarshalRouting(ev)
		if err != nil {
			t.Fatal(err)
		}
		if err := cl.ProduceSync(t.Context(), &kgo.Record{Topic: bus.TopicRouting, Key: ev.Key(), Value: b}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

func offeredEvent(customer, op, offer string, seq uint64) event.Routing {
	now := time.Now()
	return event.Routing{
		Type: event.TypeOffered, CompanyID: "test-co-0001", CustomerID: customer, Session: 1, Seq: seq,
		At: now, Lang: "en", QueuedAt: now, OperatorID: op, OfferID: offer, AcceptBy: now.Add(time.Second),
	}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestServiceStart runs the whole service against a fake Kafka and a fake
// API: the start-up order, the consumer position at the end of the topic,
// and the outage exit.
func TestServiceStart(t *testing.T) {
	c := newCluster(t)
	prod, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	// An offer from before the start. The simulator must not see it.
	produceRouting(t, prod, offeredEvent(customerID(1), op1, offerID(1), 1))

	api := &fakeAPI{clock: SystemClock{}, settings: testSettings()}
	svc, err := New(Options{Config: testConfig(c.ListenAddrs()), API: api, Registerer: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	waitUntil(t, "ready", svc.Ready)

	// Order: every put, then every offline, then the online calls of the
	// shifts.
	waitUntil(t, "online calls", func() bool { return len(api.callsOf(actionOnline)) == 3 })
	var actions []string
	for _, cl := range api.callsOf("") {
		if len(actions) == 0 || actions[len(actions)-1] != cl.action {
			actions = append(actions, cl.action)
		}
	}
	if !slices.Equal(actions, []string{actionPut, actionOffline, actionOnline}) {
		t.Errorf("call order %v", actions)
	}

	produceRouting(t, prod, offeredEvent(customerID(2), op1, offerID(2), 1))
	waitUntil(t, "accept", func() bool { return len(api.callsOf(actionAccept)) == 1 })
	if got := api.callsOf(actionAccept)[0].offer; got != offerID(2) {
		t.Errorf("accepted %s, want %s; the offer from before the start was read", got, offerID(2))
	}

	// The router goes away: the next call retries until the outage limit,
	// then Run stops with ErrOutage.
	api.mu.Lock()
	api.answer = func(call) (opapi.Result, error) { return "", errNoAnswer }
	api.mu.Unlock()
	produceRouting(t, prod, offeredEvent(customerID(3), op1, offerID(3), 1))
	select {
	case err := <-done:
		if !errors.Is(err, ErrOutage) {
			t.Fatalf("Run() = %v, want ErrOutage", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after the outage limit")
	}
	if svc.Ready() {
		t.Error("ready after Run returned")
	}
}

// TestServiceStartErrors: no start without the router settings, and an
// outage while the operators are created ends the start.
func TestServiceStartErrors(t *testing.T) {
	c := newCluster(t)
	tests := []struct {
		name string
		api  *fakeAPI
		want error
	}{
		{"no settings", &fakeAPI{clock: SystemClock{}, settingsErr: errNoAnswer}, errNoAnswer},
		{"put without answer", &fakeAPI{clock: SystemClock{}, settings: testSettings(),
			answer: func(call) (opapi.Result, error) { return "", errNoAnswer }}, ErrOutage},
		{"put rejected", &fakeAPI{clock: SystemClock{}, settings: testSettings(),
			answer: func(call) (opapi.Result, error) { return "", apiError(opapi.CodeInvalidRequest) }}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(c.ListenAddrs())
			cfg.StartWait = 100 * time.Millisecond
			svc, err := New(Options{Config: cfg, API: tt.api, Registerer: prometheus.NewRegistry()})
			if err != nil {
				t.Fatal(err)
			}
			err = svc.Run(t.Context())
			if err == nil || (tt.want != nil && !errors.Is(err, tt.want)) {
				t.Fatalf("Run() = %v, want %v", err, tt.want)
			}
			if n := len(tt.api.callsOf(actionOnline)); n != 0 {
				t.Errorf("%d online calls after a failed start", n)
			}
		})
	}
}

// TestServiceStop: a stop during the run returns nil.
func TestServiceStop(t *testing.T) {
	c := newCluster(t)
	api := &fakeAPI{clock: SystemClock{}, settings: testSettings()}
	svc, err := New(Options{Config: testConfig(c.ListenAddrs()), API: api, Registerer: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	waitUntil(t, "ready", svc.Ready)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() = %v after a stop", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not stop")
	}
}

// TestServiceStopPutsOperatorsOffline: a clean stop puts every operator
// offline after its last online call, and gives up after the stop wait when
// the router does not answer, with the count in the log.
func TestServiceStopPutsOperatorsOffline(t *testing.T) {
	c := newCluster(t)
	tests := []struct {
		name string
		// down makes every call after the stop go unanswered.
		down     bool
		wantLeft string
	}{
		{name: "router answers"},
		{name: "router gone", down: true, wantLeft: `"left":3`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := &fakeAPI{clock: SystemClock{}, settings: testSettings()}
			var logs lockedBuffer
			logger := slog.New(slog.NewJSONHandler(&logs, nil))
			svc, err := New(Options{Config: testConfig(c.ListenAddrs()), API: api, Logger: logger, Registerer: prometheus.NewRegistry()})
			if err != nil {
				t.Fatal(err)
			}
			svc.stopWait = 300 * time.Millisecond
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- svc.Run(ctx) }()
			waitUntil(t, "online calls", func() bool { return len(api.callsOf(actionOnline)) == 3 })

			before := len(api.callsOf(""))
			if tt.down {
				api.mu.Lock()
				api.answer = func(call) (opapi.Result, error) { return "", errNoAnswer }
				api.mu.Unlock()
			}
			stopped := time.Now()
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("Run() = %v after a stop", err)
				}
			case <-time.After(15 * time.Second):
				t.Fatal("Run did not stop")
			}
			if took := time.Since(stopped); took > 5*time.Second {
				t.Errorf("stop took %s", took)
			}

			after := api.callsOf("")[before:]
			offline := map[string]bool{}
			for _, cl := range after {
				if cl.action != actionOffline {
					t.Errorf("call %s after the stop, want only offline calls", cl)
					continue
				}
				offline[cl.op] = true
			}
			for _, id := range svc.ids {
				if !offline[id] {
					t.Errorf("no offline call for %s after the stop", id)
				}
			}
			out := logs.String()
			if tt.wantLeft != "" {
				if !strings.Contains(out, tt.wantLeft) {
					t.Errorf("log has no %s: %s", tt.wantLeft, out)
				}
			} else if !strings.Contains(out, "every operator put offline at stop") {
				t.Errorf("log has no confirmation of the offline calls: %s", out)
			}
		})
	}
}

// TestServiceOutageSkipsOfflineAtStop: after an outage exit nothing is
// called at stop; the restart puts the operators offline.
func TestServiceOutageSkipsOfflineAtStop(t *testing.T) {
	c := newCluster(t)
	prod, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	api := &fakeAPI{clock: SystemClock{}, settings: testSettings()}
	svc, err := New(Options{Config: testConfig(c.ListenAddrs()), API: api, Registerer: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- svc.Run(t.Context()) }()
	waitUntil(t, "online calls", func() bool { return len(api.callsOf(actionOnline)) == 3 })
	api.mu.Lock()
	api.answer = func(call) (opapi.Result, error) { return "", errNoAnswer }
	api.mu.Unlock()
	produceRouting(t, prod, offeredEvent(customerID(1), op1, offerID(1), 1))
	select {
	case err := <-done:
		if !errors.Is(err, ErrOutage) {
			t.Fatalf("Run() = %v, want ErrOutage", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after the outage limit")
	}
	// The start also asks for sim-op-0004, a leftover of an earlier run,
	// and gets CodeOperatorNotFound.
	var offline []string
	for _, cl := range api.callsOf(actionOffline) {
		offline = append(offline, cl.op)
	}
	if want := []string{op1, operatorID(2), operatorID(3), operatorID(4)}; !slices.Equal(sortedCopy(offline), want) {
		t.Errorf("offline calls %v, want only %v of the start", offline, want)
	}
}

func sortedCopy(s []string) []string {
	out := slices.Clone(s)
	slices.Sort(out)
	return out
}

// lockedBuffer is a bytes.Buffer that is safe for concurrent writes.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// TestServiceRestartSkipsOldEvents: a restart in the same group starts at
// the end of the topic, not at the offset the earlier run committed. An
// offer made while the simulator was down is not answered: the start puts
// the operator offline, which ends that offer in the router.
func TestServiceRestartSkipsOldEvents(t *testing.T) {
	c := newCluster(t)
	prod, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	start := func() (*fakeAPI, context.CancelFunc, chan error) {
		api := &fakeAPI{clock: SystemClock{}, settings: testSettings()}
		svc, err := New(Options{Config: testConfig(c.ListenAddrs()), API: api, Registerer: prometheus.NewRegistry()})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- svc.Run(ctx) }()
		waitUntil(t, "ready", svc.Ready)
		return api, cancel, done
	}
	stop := func(cancel context.CancelFunc, done chan error) {
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run() = %v", err)
		}
	}

	api1, cancel1, done1 := start()
	produceRouting(t, prod, offeredEvent(customerID(1), op1, offerID(1), 1))
	waitUntil(t, "first accept", func() bool { return len(api1.callsOf(actionAccept)) == 1 })
	stop(cancel1, done1)

	// Commit the current end for the group, as an earlier run that read
	// everything would have.
	adm := kadm.NewClient(prod)
	ends, err := adm.ListEndOffsets(t.Context(), bus.TopicRouting)
	if err != nil || ends.Error() != nil {
		t.Fatalf("end offsets: %v, %v", err, ends.Error())
	}
	resp, err := adm.CommitOffsets(t.Context(), "sim-unit", ends.Offsets())
	if err != nil || resp.Error() != nil {
		t.Fatalf("commit: %v, %v", err, resp.Error())
	}

	produceRouting(t, prod, offeredEvent(customerID(2), op1, offerID(2), 1))
	api2, cancel2, done2 := start()
	defer stop(cancel2, done2)
	produceRouting(t, prod, offeredEvent(customerID(3), op1, offerID(3), 1))
	waitUntil(t, "accept after the restart", func() bool { return len(api2.callsOf(actionAccept)) >= 1 })
	time.Sleep(200 * time.Millisecond)
	for _, cl := range api2.callsOf(actionAccept) {
		if cl.offer != offerID(3) {
			t.Errorf("accepted %s after the restart, want only %s", cl.offer, offerID(3))
		}
	}
}
