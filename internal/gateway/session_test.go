package gateway

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/conv"
	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// drain returns the frames that arrive within d.
func (f *fakeConn) drain(d time.Duration) [][]byte {
	var out [][]byte
	timeout := time.After(d)
	for {
		select {
		case b := <-f.frames:
			out = append(out, b)
		case <-timeout:
			return out
		}
	}
}

func decodeResult(t *testing.T, b []byte) gwapi.Result {
	t.Helper()
	var r gwapi.Result
	if frameType(t, b) != gwapi.TypeResult {
		t.Fatalf("frame %s is not a result", b)
	}
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	return r
}

func waitClosed(t *testing.T, c *fakeConn, want gwapi.CloseCode) {
	t.Helper()
	select {
	case <-c.closed:
	case <-time.After(3 * time.Second):
		t.Fatalf("not closed, want %d", want)
	}
	if got := c.closeCode(); got != want {
		t.Fatalf("close code %d (%s), want %d (%s)", got, got.Name(), want, want.Name())
	}
}

func TestOperatorSessionFlow(t *testing.T) {
	e := newEnv(t)
	c, w := e.connect(operatorHello("und", "en"), "10.0.0.1")
	aud := gwapi.Audience{Level: gwapi.LevelDemo, View: gwapi.ViewOperator}
	if _, err := gwapi.EncodeServer(w, aud); err != nil || !strings.HasPrefix(w.OperatorID, "web-") || w.TickMS != 250 {
		t.Fatalf("welcome %+v: %v", w, err)
	}
	if got := e.api.Calls(); len(got) != 1 || got[0] != "put "+w.OperatorID+" [en und] 2" {
		t.Fatalf("calls %v", got)
	}
	if frameType(t, c.next(t)) != gwapi.TypeStatus {
		t.Fatal("no status after welcome")
	}
	snap := decodeState(t, c.next(t), aud)
	if snap.Seq != 1 || !snap.Snapshot || snap.Me == nil || snap.Me.ID != w.OperatorID || snap.Me.Presence != gwapi.PresenceOffline {
		t.Fatalf("snapshot %+v", snap)
	}

	// An offer to this operator arrives.
	key := convKey(testCompany, testCustomer)
	e.apply(
		ci(convEv(event.TypeOpened, testCompany, testCustomer, 1, 1, t0), t0),
		ri(opEv(event.TypeOperatorOnline, w.OperatorID, 2, t0, event.PresenceOnline), t0),
		ri(rt(event.TypeOffered, testCompany, testCustomer, 1, 1, t0, w.OperatorID, "of-1"), t0),
	)
	e.g.Tick(e.clock.Now())
	up := decodeState(t, c.nextOf(t, gwapi.TypeState), aud)
	if up.Seq != 2 || up.Snapshot || up.Me == nil || up.Me.Load != 1 || up.Me.Offered != 1 ||
		up.MyConversations == nil || len(up.MyConversations.Set) != 1 {
		t.Fatalf("update %+v", up)
	}
	row := up.MyConversations.Set[0]
	if row.State != gwapi.StateOffered || row.OfferID != "of-1" || row.ID == key || !gwapi.ValidPseudonym(row.ID) {
		t.Fatalf("row %+v", row)
	}

	c.send(t, gwapi.Action{RequestID: "r1", Action: gwapi.ActionAccept, OfferID: "of-1"})
	if r := decodeResult(t, c.nextOf(t, gwapi.TypeResult)); r.Outcome != gwapi.OutcomeDone || r.RequestID != "r1" {
		t.Fatalf("accept %+v", r)
	}
	c.send(t, gwapi.Action{RequestID: "r2", Action: gwapi.ActionAccept, OfferID: "of-9"})
	if r := decodeResult(t, c.nextOf(t, gwapi.TypeResult)); r.Code != gwapi.CodeUnknownOffer {
		t.Fatalf("unknown offer %+v", r)
	}
	// Router answers map to the protocol.
	e.api.mu.Lock()
	e.api.err = &opapi.Error{Status: 409, ErrorResponse: opapi.ErrorResponse{Code: opapi.CodeOfferExpired, Message: "offer of " + testCustomer}}
	e.api.mu.Unlock()
	c.send(t, gwapi.Action{RequestID: "r3", Action: gwapi.ActionAccept, OfferID: "of-1"})
	r := decodeResult(t, c.nextOf(t, gwapi.TypeResult))
	if r.Code != gwapi.CodeOfferExpired || r.Message != "accept: the offer is expired" {
		t.Fatalf("rejected %+v", r)
	}
	e.api.mu.Lock()
	e.api.err = errors.New("connection refused")
	e.api.mu.Unlock()
	c.send(t, gwapi.Action{RequestID: "r4", Action: gwapi.ActionOnline})
	if r := decodeResult(t, c.nextOf(t, gwapi.TypeResult)); r.Code != gwapi.CodeUnavailable {
		t.Fatalf("no answer %+v", r)
	}
	e.api.mu.Lock()
	e.api.err, e.api.result = nil, opapi.ResultAlreadyDone
	e.api.mu.Unlock()
	c.send(t, gwapi.Action{RequestID: "r5", Action: gwapi.ActionOnline})
	if r := decodeResult(t, c.nextOf(t, gwapi.TypeResult)); r.Outcome != gwapi.OutcomeAlreadyDone {
		t.Fatalf("already done %+v", r)
	}
	// While the data is stale, offer actions are not checked.
	e.g.SetStatus(gwapi.Status{Stale: true, Reason: gwapi.StaleLagging, LagUS: 11_000_000})
	c.send(t, gwapi.Action{RequestID: "r6", Action: gwapi.ActionRefuse, OfferID: "of-1"})
	if r := decodeResult(t, c.nextOf(t, gwapi.TypeResult)); r.Code != gwapi.CodeStaleConnection || r.Message != "refuse: "+staleDataMessage {
		t.Fatalf("stale %+v", r)
	}
	// The reply ID is the request ID.
	e.g.SetStatus(gwapi.Status{})
	c.send(t, gwapi.Action{RequestID: "reply-42", Action: gwapi.ActionReply, OfferID: "of-1"})
	decodeResult(t, c.nextOf(t, gwapi.TypeResult))
	calls := e.api.Calls()
	if last := calls[len(calls)-1]; last != "reply "+w.OperatorID+" of-1 reply-42" {
		t.Fatalf("last call %q", last)
	}
}

func TestSupervisorCannotAct(t *testing.T) {
	e := newEnv(t)
	c, _ := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor}, "10.0.0.1")
	c.send(t, gwapi.Action{RequestID: "r1", Action: gwapi.ActionOnline})
	if r := decodeResult(t, c.nextOf(t, gwapi.TypeResult)); r.Code != gwapi.CodeNotAllowed {
		t.Fatalf("%+v", r)
	}
	if len(e.api.Calls()) != 0 {
		t.Fatal("a supervisor created an operator or called the router")
	}
}

// TestSlowClientGetsOneMergedState: while the socket blocks, ticks and
// changes pile up as keys, not as messages. After the block the client
// gets one state with the newest values.
func TestSlowClientGetsOneMergedState(t *testing.T) {
	e := newEnv(t)
	c, _ := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor, Token: "full-token"}, "10.0.0.1")
	aud := gwapi.Audience{Level: gwapi.LevelFull, View: gwapi.ViewSupervisor}
	c.nextOf(t, gwapi.TypeState)

	gate := make(chan struct{})
	c.mu.Lock()
	c.gate = gate
	c.mu.Unlock()
	e.apply(ci(convEv(event.TypeOpened, testCompany, testCustomer, 1, 1, t0), t0))
	e.g.Tick(e.clock.Now())
	// The writer is now blocked in the first update. More changes and
	// ticks come.
	for i := 2; i <= 30; i++ {
		e.apply(ci(convEv(event.TypeMessageAdded, testCompany, testCustomer, 1, uint64(i), t0), t0))
		e.g.Tick(e.clock.Now())
	}
	if n := c.drain(50 * time.Millisecond); len(n) != 0 {
		t.Fatalf("%d frames got through the gate", len(n))
	}
	close(gate)
	frames := c.drain(300 * time.Millisecond)
	var states []gwapi.State
	for _, f := range frames {
		if frameType(t, f) == gwapi.TypeState {
			states = append(states, decodeState(t, f, aud))
		}
	}
	if len(states) != 2 {
		t.Fatalf("%d states after the block, want 2 (the blocked one and one merged)", len(states))
	}
	last := states[1].Conversations.Set[0]
	if last.CustomerMessages != 30 {
		t.Fatalf("merged state has %d messages, want the newest 30", last.CustomerMessages)
	}
	e.g.Tick(e.clock.Now())
	if more := c.drain(50 * time.Millisecond); len(more) != 0 {
		t.Fatalf("an idle tick sent %d frames", len(more))
	}
}

// TestDemoClientsNeverSeeIDsOrText walks every frame sent to demo
// clients of both views through a whole life cycle with watches and a
// removal, and fails on any author ID, key, message ID or text of the
// test data.
func TestDemoClientsNeverSeeIDsOrText(t *testing.T) {
	e := newEnv(t)
	op, w := e.connect(operatorHello(), "10.0.0.1")
	sup, _ := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor, Token: "signed-in-not-full"}, "10.0.0.2")
	full, _ := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor, Token: "full-token"}, "10.0.0.3")
	for _, s := range history() {
		e.apply(s...)
	}
	mid := "synth-msg-1-1"
	e.text.put(testCompany, testCustomer, conv.StoredMessage{ID: mid, Direction: event.FromCustomer, SourceTime: t0, Text: testText})
	e.apply(
		ci(convEv(event.TypeOpened, testCompany, testCustomer, 1, 1, t0), t0),
		ri(opEv(event.TypeOperatorOnline, w.OperatorID, 1, t0, event.PresenceOnline), t0),
		ri(rt(event.TypeAccepted, testCompany, testCustomer, 1, 1, t0, w.OperatorID, "of-x"), t0),
		ri(teamEv(event.TypeTeamFormed, testCompany, 1, t0, w.OperatorID), t0),
	)
	e.g.Tick(e.clock.Now())

	var opID, supID string
	opAud := gwapi.Audience{Level: gwapi.LevelDemo, View: gwapi.ViewOperator}
	for _, f := range op.drain(200 * time.Millisecond) {
		if frameType(t, f) == gwapi.TypeState {
			if st := decodeState(t, f, opAud); st.MyConversations != nil && len(st.MyConversations.Set) > 0 {
				opID = st.MyConversations.Set[0].ID
			}
		}
	}
	key := convKey(testCompany, testCustomer)
	e.g.view.Read(func(rd Reader) { _, _ = rd.Conversation(key) })
	supID = e.g.ps.Of(key)
	if opID != supID {
		t.Fatalf("operator sees %q, supervisor pseudonym %q", opID, supID)
	}
	for _, c := range []*fakeConn{op, sup} {
		c.send(t, gwapi.Watch{RequestID: "w1", ConversationID: supID})
	}
	full.send(t, gwapi.Watch{RequestID: "w1", ConversationID: key})
	time.Sleep(100 * time.Millisecond)
	e.apply(ci(convEv(event.TypeMessageAdded, testCompany, testCustomer, 1, 2, t0), t0))
	e.text.put(testCompany, testCustomer, conv.StoredMessage{ID: "synth-msg-1-2", Direction: event.FromCustomer, SourceTime: t0.Add(time.Second), Text: testText + " two"})
	e.g.Tick(e.clock.Now())
	rm := convEv(event.TypeMessageRemoved, testCompany, testCustomer, 1, 3, t0)
	rm.MessageID = mid
	e.apply(ci(rm, t0))
	e.g.Tick(e.clock.Now())

	forbidden := []string{testCompany, testCustomer, customer(1), customer(2), testCompany2, mid, "synth-msg-1-2", "synthetic", "did:plc"}
	sawText := map[gwapi.ServerType]int{}
	for _, c := range []*fakeConn{op, sup} {
		for _, f := range c.drain(300 * time.Millisecond) {
			sawText[frameType(t, f)]++
			for _, s := range forbidden {
				if strings.Contains(string(f), s) {
					t.Errorf("demo frame holds %q: %s", s, f)
				}
			}
		}
		if code := c.closeCode(); code != 0 {
			t.Errorf("closed with %d", code)
		}
	}
	if sawText[gwapi.TypeMessages] < 3 || sawText[gwapi.TypeMessageRemoved] != 2 {
		t.Errorf("frame types %v: watches did not work", sawText)
	}
	// The full level sees the text and the removal.
	var gotText, gotRemoved bool
	for _, f := range full.drain(300 * time.Millisecond) {
		gotText = gotText || strings.Contains(string(f), testText)
		gotRemoved = gotRemoved || (frameType(t, f) == gwapi.TypeMessageRemoved && strings.Contains(string(f), mid))
	}
	if !gotText || !gotRemoved {
		t.Errorf("full level: text %v, removed %v", gotText, gotRemoved)
	}
}

func TestWatchRules(t *testing.T) {
	e := newEnv(t)
	c, _ := e.connect(gwapi.Hello{View: gwapi.ViewSupervisor, Token: "full-token"}, "10.0.0.1")
	var keys []string
	for i := range 6 {
		e.apply(ci(convEv(event.TypeOpened, testCompany, customer(i), 1, 1, t0), t0))
		keys = append(keys, convKey(testCompany, customer(i)))
	}
	results := func(n int) []gwapi.Result {
		var out []gwapi.Result
		for len(out) < n {
			out = append(out, decodeResult(t, c.nextOf(t, gwapi.TypeResult)))
		}
		return out
	}
	for i := range 4 {
		c.send(t, gwapi.Watch{RequestID: "w" + string(rune('a'+i)), ConversationID: keys[i]})
	}
	for _, r := range results(4) {
		if r.Outcome != gwapi.OutcomeDone {
			t.Fatalf("watch %+v", r)
		}
	}
	c.send(t, gwapi.Watch{RequestID: "w5", ConversationID: keys[4]})
	c.send(t, gwapi.Watch{RequestID: "w6", ConversationID: keys[0]})
	c.send(t, gwapi.Watch{RequestID: "w7", ConversationID: "did:plc:synthnobody|did:plc:synthnobody2"})
	c.send(t, gwapi.Unwatch{RequestID: "u1", ConversationID: keys[5]})
	c.send(t, gwapi.Unwatch{RequestID: "u2", ConversationID: keys[1]})
	got := map[string]gwapi.Result{}
	for _, r := range results(5) {
		got[r.RequestID] = r
	}
	if got["w5"].Code != gwapi.CodeTooManyWatched || got["w6"].Outcome != gwapi.OutcomeAlreadyDone ||
		got["w7"].Code != gwapi.CodeUnknownConversation || got["u1"].Outcome != gwapi.OutcomeAlreadyDone ||
		got["u2"].Outcome != gwapi.OutcomeDone {
		t.Fatalf("results %+v", got)
	}
	// The rate limit: 10 at once, then 5 per second. The fake clock does
	// not move, so only the bucket counts.
	for i := range 3 {
		c.send(t, gwapi.Unwatch{RequestID: "x" + string(rune('a'+i)), ConversationID: keys[5]})
	}
	limited := 0
	for _, r := range results(3) {
		if r.Code == gwapi.CodeRateLimited {
			limited++
		}
	}
	// 9 requests so far: the 10th passes, the next two do not.
	if limited != 2 {
		t.Fatalf("%d of 3 rate limited, want 2", limited)
	}
	e.clock.Advance(time.Second)
	c.send(t, gwapi.Unwatch{RequestID: "y", ConversationID: keys[5]})
	if r := results(1)[0]; r.Outcome != gwapi.OutcomeAlreadyDone {
		t.Fatalf("after a second %+v", r)
	}
}

// TestOperatorWatchEndsWithAssignment: an operator watches only what it
// holds, and the server ends the watch when the assignment ends.
func TestOperatorWatchEndsWithAssignment(t *testing.T) {
	e := newEnv(t)
	c, w := e.connect(gwapi.Hello{View: gwapi.ViewOperator, Languages: []string{"en"}, Token: "full-token"}, "10.0.0.1")
	key := convKey(testCompany, testCustomer)
	e.text.put(testCompany, testCustomer, conv.StoredMessage{ID: testMsgID, Direction: event.FromCustomer, SourceTime: t0, Text: testText})
	e.apply(
		ci(convEv(event.TypeOpened, testCompany, testCustomer, 1, 1, t0), t0),
		ri(rt(event.TypeOffered, testCompany, testCustomer, 1, 1, t0, w.OperatorID, "of-1"), t0),
	)
	c.send(t, gwapi.Watch{RequestID: "w1", ConversationID: key})
	if r := decodeResult(t, c.nextOf(t, gwapi.TypeResult)); r.Code != gwapi.CodeUnknownConversation {
		t.Fatalf("watch of an offer %+v", r)
	}
	e.apply(ri(rt(event.TypeAccepted, testCompany, testCustomer, 1, 2, t0, w.OperatorID, "of-1"), t0))
	c.send(t, gwapi.Watch{RequestID: "w2", ConversationID: key})
	if r := decodeResult(t, c.nextOf(t, gwapi.TypeResult)); r.Outcome != gwapi.OutcomeDone {
		t.Fatalf("watch %+v", r)
	}
	if b := c.nextOf(t, gwapi.TypeMessages); !strings.Contains(string(b), testText) {
		t.Fatalf("messages %s", b)
	}
	e.apply(ri(rt(event.TypeResolved, testCompany, testCustomer, 1, 3, t0, w.OperatorID, "of-1"), t0))
	e.g.Tick(e.clock.Now())
	waitFor(t, "watch ended", func() bool {
		n := -1
		e.g.hub.each(func(cl *client) {
			cl.mu.Lock()
			n = len(cl.watches)
			cl.mu.Unlock()
		})
		return n == 0
	})
	e.apply(ci(convEv(event.TypeMessageRemoved, testCompany, testCustomer, 1, 2, t0), t0))
	for _, f := range c.drain(100 * time.Millisecond) {
		if frameType(t, f) == gwapi.TypeMessageRemoved {
			t.Fatal("message_removed after the watch ended")
		}
	}
	c.send(t, gwapi.Unwatch{RequestID: "u1", ConversationID: key})
	if r := decodeResult(t, c.nextOf(t, gwapi.TypeResult)); r.Outcome != gwapi.OutcomeAlreadyDone {
		t.Fatalf("unwatch after the end %+v", r)
	}
}
