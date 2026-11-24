package gateway

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
)

func countCalls(calls []string, prefix string) int {
	n := 0
	for _, c := range calls {
		if strings.HasPrefix(c, prefix) {
			n++
		}
	}
	return n
}

func TestResumeWithinGrace(t *testing.T) {
	e := newEnv(t)
	c1, w1 := e.connect(operatorHello(), "10.0.0.1")
	if !strings.HasPrefix(w1.ResumeToken, "rt_") || len(w1.ResumeToken) < 40 {
		t.Fatalf("resume token %q", w1.ResumeToken)
	}
	c1.Close(gwapi.CloseNormal)
	waitFor(t, "grace started", func() bool { return e.g.hub.count() == 0 })
	e.clock.Advance(10 * time.Second)

	h := operatorHello()
	h.ResumeToken = w1.ResumeToken
	_, w2 := e.connect(h, "10.0.0.1")
	if w2.OperatorID != w1.OperatorID || w2.ResumeToken == w1.ResumeToken {
		t.Fatalf("resume gave %s %s, want the same operator and a new token", w2.OperatorID, w2.ResumeToken)
	}
	e.clock.Advance(time.Minute)
	if n := countCalls(e.api.Calls(), "offline"); n != 0 {
		t.Fatalf("%d offline calls after a resume", n)
	}
	// The old token no longer works.
	h.ResumeToken = w1.ResumeToken
	_, w3 := e.connect(h, "10.0.0.1")
	if w3.OperatorID == w1.OperatorID {
		t.Fatal("an old token took the operator")
	}
}

func TestOfflineAfterGrace(t *testing.T) {
	e := newEnv(t)
	c, w := e.connect(operatorHello(), "10.0.0.1")
	c.send(t, gwapi.Action{RequestID: "on", Action: gwapi.ActionOnline})
	decodeResult(t, c.nextOf(t, gwapi.TypeResult))
	c.Close(gwapi.CloseNormal)
	waitFor(t, "client gone", func() bool { return e.g.hub.count() == 0 })
	e.clock.Advance(14 * time.Second)
	if countCalls(e.api.Calls(), "offline") != 0 {
		t.Fatal("offline before the grace time")
	}
	e.clock.Advance(2 * time.Second)
	waitFor(t, "offline call", func() bool { return slices.Contains(e.api.Calls(), "offline "+w.OperatorID) })
	// The token ended with the operator.
	h := operatorHello()
	h.ResumeToken = w.ResumeToken
	_, w2 := e.connect(h, "10.0.0.1")
	if w2.OperatorID == w.OperatorID {
		t.Fatal("resume after the grace time")
	}
}

func TestResumeTokenIsBoundToIdentity(t *testing.T) {
	e := newEnv(t)
	h := operatorHello()
	h.Token = "full-token"
	c, w := e.connect(h, "10.0.0.1")
	c.Close(gwapi.CloseNormal)
	waitFor(t, "client gone", func() bool { return e.g.hub.count() == 0 })

	// An anonymous visitor with the token of a signed-in user.
	h2 := operatorHello()
	h2.ResumeToken = w.ResumeToken
	_, w2 := e.connect(h2, "10.0.0.2")
	if w2.OperatorID == w.OperatorID {
		t.Fatal("another identity took the operator")
	}
	// A guessed token.
	h3 := operatorHello()
	h3.ResumeToken = "rt_" + strings.Repeat("A", 43)
	_, w3 := e.connect(h3, "10.0.0.3")
	if w3.OperatorID == w.OperatorID {
		t.Fatal("a guessed token took the operator")
	}
	// The owner gets it back.
	h.ResumeToken = w.ResumeToken
	_, w4 := e.connect(h, "10.0.0.1")
	if w4.OperatorID != w.OperatorID {
		t.Fatal("the owner did not get the operator back")
	}
}

func TestResumeReplacesOpenConnection(t *testing.T) {
	e := newEnv(t)
	c1, w1 := e.connect(operatorHello(), "10.0.0.1")
	h := operatorHello()
	h.ResumeToken = w1.ResumeToken
	c2, w2 := e.connect(h, "10.0.0.1")
	waitClosed(t, c1, gwapi.CloseReplaced)
	if w2.OperatorID != w1.OperatorID {
		t.Fatal("not resumed")
	}
	e.clock.Advance(time.Minute)
	if countCalls(e.api.Calls(), "offline") != 0 {
		t.Fatal("the closed old connection put the operator offline")
	}
	c2.send(t, gwapi.Action{RequestID: "on", Action: gwapi.ActionOnline})
	if r := decodeResult(t, c2.nextOf(t, gwapi.TypeResult)); r.Outcome != gwapi.OutcomeDone {
		t.Fatalf("%+v", r)
	}
}

func TestDemoSeats(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.DemoOperators = 2 })
	e.connect(operatorHello(), "10.0.0.1")
	c2, _ := e.connect(operatorHello(), "10.0.0.2")
	// Full level does not take a demo seat.
	h := operatorHello()
	h.Token = "full-token"
	e.connect(h, "10.0.0.3")

	c := newFakeConn()
	e.wg.Go(func() { e.g.Serve(e.ctx, c, "10.0.0.4") })
	c.send(t, operatorHello())
	waitClosed(t, c, gwapi.CloseNoOperatorSeat)

	// A seat stays taken in the grace time, and is free after it.
	c2.Close(gwapi.CloseNormal)
	waitFor(t, "client gone", func() bool { return e.g.hub.count() == 2 })
	c = newFakeConn()
	e.wg.Go(func() { e.g.Serve(e.ctx, c, "10.0.0.4") })
	c.send(t, operatorHello())
	waitClosed(t, c, gwapi.CloseNoOperatorSeat)
	e.clock.Advance(16 * time.Second)
	waitFor(t, "offline", func() bool { return countCalls(e.api.Calls(), "offline") == 1 })
	e.connect(operatorHello(), "10.0.0.4")
}

func TestOrphanSweep(t *testing.T) {
	e := newEnv(t)
	_, w := e.connect(operatorHello(), "10.0.0.1")
	e.apply(
		ri(opEv(event.TypeOperatorOnline, "web-orphan0001", 2, t0, event.PresenceOnline), t0),
		ri(opEv(event.TypeOperatorOnline, w.OperatorID, 2, t0, event.PresenceOnline), t0),
		ri(opEv(event.TypeOperatorOnline, "sim-op-0001", 2, t0, event.PresenceOnline), t0),
	)
	var online []string
	e.g.view.Read(func(rd Reader) { online = rd.OnlineHumans() })
	e.g.ops.SweepOrphans(t.Context(), online)
	calls := e.api.Calls()
	if !slices.Contains(calls, "offline web-orphan0001") || countCalls(calls, "offline") != 1 {
		t.Fatalf("calls %v", calls)
	}
}

func TestCloseCodes(t *testing.T) {
	big := make([]byte, gwapi.MaxClientMessageBytes+1)
	tests := []struct {
		name  string
		first []byte
		then  []byte
		addr  string
		want  gwapi.CloseCode
	}{
		{"bad json", []byte("{"), nil, "", gwapi.CloseBadMessage},
		{"first is not hello", []byte(`{"type":"watch","request_id":"a","conversation_id":"x"}`), nil, "", gwapi.CloseBadMessage},
		{"unknown type", []byte(`{"type":"nope"}`), nil, "", gwapi.CloseBadMessage},
		{"old version", []byte(`{"type":"hello","version":0,"view":"operator"}`), nil, "", gwapi.CloseUnsupportedVersion},
		{"too big", big, nil, "", gwapi.CloseMessageTooBig},
		{"second hello", []byte(`{"type":"hello","version":1,"view":"supervisor"}`), []byte(`{"type":"hello","version":1,"view":"supervisor"}`), "", gwapi.CloseBadMessage},
		{"unknown type later", []byte(`{"type":"hello","version":1,"view":"supervisor"}`), []byte(`{"type":"x"}`), "", gwapi.CloseBadMessage},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			c := newFakeConn()
			e.wg.Go(func() { e.g.Serve(e.ctx, c, "10.0.0.1") })
			c.in <- tt.first
			if tt.then != nil {
				c.nextOf(t, gwapi.TypeState)
				c.in <- tt.then
			}
			waitClosed(t, c, tt.want)
		})
	}
}

func TestConnectionLimits(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.MaxConnsPerAddr = 2; c.MaxConnections = 3 })
	e.connect(gwapi.Hello{View: gwapi.ViewSupervisor}, "10.0.0.1")
	e.connect(gwapi.Hello{View: gwapi.ViewSupervisor}, "10.0.0.1")
	c := newFakeConn()
	e.wg.Go(func() { e.g.Serve(e.ctx, c, "10.0.0.1") })
	waitClosed(t, c, gwapi.CloseTooManyConnections)
	e.connect(gwapi.Hello{View: gwapi.ViewSupervisor}, "10.0.0.2")
	c = newFakeConn()
	e.wg.Go(func() { e.g.Serve(e.ctx, c, "10.0.0.3") })
	waitClosed(t, c, gwapi.CloseTryAgainLater)
}

func TestHelloTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("waits 5 seconds")
	}
	t.Parallel()
	e := newEnv(t)
	c := newFakeConn()
	e.wg.Go(func() { e.g.Serve(e.ctx, c, "10.0.0.1") })
	select {
	case <-c.closed:
	case <-time.After(gwapi.HelloTimeout + 2*time.Second):
		t.Fatal("not closed")
	}
	if c.closeCode() != gwapi.CloseHelloTimeout {
		t.Fatalf("code %d", c.closeCode())
	}
}

func TestGoingAwayOnStop(t *testing.T) {
	e := newEnv(t)
	c, _ := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor}, "10.0.0.1")
	e.g.hub.closeAll(gwapi.CloseGoingAway, reasonGoingAway)
	waitClosed(t, c, gwapi.CloseGoingAway)
}

// TestDayChangeSendsSnapshot: the pseudonym key changes at midnight UTC
// and demo clients get a new snapshot, which ends their watches.
func TestDayChangeSendsSnapshot(t *testing.T) {
	e := newEnv(t)
	demo, _ := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor}, "10.0.0.1")
	full, _ := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor, Token: "full-token"}, "10.0.0.2")
	e.apply(ci(convEv(event.TypeOpened, testCompany, testCustomer, 1, 1, t0), t0))
	e.g.Tick(e.clock.Now())
	aud := gwapi.Audience{Level: gwapi.LevelDemo, View: gwapi.ViewSupervisor}
	var before string
	for _, f := range demo.drain(100 * time.Millisecond) {
		if frameType(t, f) == gwapi.TypeState {
			if st := decodeState(t, f, aud); st.Conversations != nil {
				before = st.Conversations.Set[0].ID
			}
		}
	}
	demo.send(t, gwapi.Watch{RequestID: "w", ConversationID: before})
	decodeResult(t, demo.nextOf(t, gwapi.TypeResult))
	full.drain(50 * time.Millisecond)

	e.clock.Advance(24 * time.Hour)
	e.g.Tick(e.clock.Now())
	st := decodeState(t, demo.nextOf(t, gwapi.TypeState), aud)
	if !st.Snapshot || st.Conversations == nil || st.Conversations.Set[0].ID == before {
		t.Fatalf("after midnight %+v", st)
	}
	for _, f := range full.drain(100 * time.Millisecond) {
		if frameType(t, f) == gwapi.TypeState && strings.Contains(string(f), `"snapshot":true`) {
			t.Fatal("the full level got a snapshot")
		}
	}
	e.g.hub.each(func(c *client) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.aud.Level == gwapi.LevelDemo && len(c.watches) != 0 {
			t.Error("the snapshot did not end the watch")
		}
	})
}

// TestHumanOperatorPriority checks that every PutOperator call for a human
// operator sends capacity 2 and priority 1: when the operator is created,
// when the person changes the languages, and when a resume brings new
// languages. A PUT replaces the priority, so a call without it would set
// it back to 0 (decision 0009, "An operator priority").
func TestHumanOperatorPriority(t *testing.T) {
	e := newEnv(t)
	c1, w1 := e.connect(operatorHello("en", "und"), "10.0.0.1")
	c1.send(t, gwapi.Action{RequestID: "r1", Action: gwapi.ActionSetLanguages, Languages: []string{"de", "und"}})
	if r := decodeResult(t, c1.nextOf(t, gwapi.TypeResult)); r.Outcome != gwapi.OutcomeDone {
		t.Fatalf("set_languages %+v", r)
	}
	c1.Close(gwapi.CloseNormal)
	waitFor(t, "grace started", func() bool { return e.g.hub.count() == 0 })
	h := operatorHello("fr", "und")
	h.ResumeToken = w1.ResumeToken
	if _, w2 := e.connect(h, "10.0.0.1"); w2.OperatorID != w1.OperatorID {
		t.Fatalf("resume gave %s, want %s", w2.OperatorID, w1.OperatorID)
	}

	want := []string{
		"put " + w1.OperatorID + " [en und] 2 p1",
		"put " + w1.OperatorID + " [de und] 2 p1",
		"put " + w1.OperatorID + " [fr und] 2 p1",
	}
	var puts []string
	for _, c := range e.api.Calls() {
		if strings.HasPrefix(c, "put ") {
			puts = append(puts, c)
		}
	}
	if !slices.Equal(puts, want) {
		t.Errorf("put calls %v, want %v", puts, want)
	}
	if gwapi.HumanPriority != 1 {
		t.Errorf("gwapi.HumanPriority = %d, want 1", gwapi.HumanPriority)
	}
}
