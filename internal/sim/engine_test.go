package sim

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

const op1 = "sim-op-0001"

// TestAcceptDelayBounds: every accept comes 1 to 8 seconds of model time
// after its offer, and the delays cover the range.
func TestAcceptDelayBounds(t *testing.T) {
	h := newHarness(t, testBehaviour(), 1, 1)
	const n = 2000
	offeredAt := map[string]time.Duration{}
	for i := range n {
		h.send(event.TypeOffered, customerID(i), op1, offerID(i))
		offeredAt[offerID(i)] = h.model()
		h.until(h.model() + 10*time.Millisecond)
	}
	h.until(h.model() + 10*time.Second)
	accepts := h.api.callsOf(actionAccept)
	if len(accepts) != n {
		t.Fatalf("%d accepts, want %d", len(accepts), n)
	}
	lo, hi := time.Hour, time.Duration(0)
	for _, c := range accepts {
		d := c.model - offeredAt[c.offer]
		lo, hi = min(lo, d), max(hi, d)
		if d < time.Second || d > 8*time.Second {
			t.Errorf("accept of %s after %s, want 1s to 8s", c.offer, d)
		}
	}
	if lo > 1100*time.Millisecond || hi < 7900*time.Millisecond {
		t.Errorf("delays from %s to %s do not cover 1s to 8s", lo, hi)
	}
	if got := testutil.ToFloat64(h.m.accepts); got != n {
		t.Errorf("accepts metric %v, want %d", got, n)
	}
}

// TestMissedOfferShare: with the default 3 percent and a fixed seed, about
// 3 percent of the offers get no accept, and the same seed misses the same
// offers.
func TestMissedOfferShare(t *testing.T) {
	missed := func(seed uint64) []string {
		b := testBehaviour()
		b.MissedOfferShare = 0.03
		b.Seed = seed
		h := newHarness(t, b, 1, 1)
		const n = 10000
		for i := range n {
			h.send(event.TypeOffered, customerID(i), op1, offerID(i))
		}
		h.until(10 * time.Second)
		accepted := map[string]bool{}
		for _, c := range h.api.callsOf(actionAccept) {
			accepted[c.offer] = true
		}
		var out []string
		for i := range n {
			if !accepted[offerID(i)] {
				out = append(out, offerID(i))
			}
		}
		if got := testutil.ToFloat64(h.m.offersMissed); got != float64(len(out)) {
			t.Errorf("missed metric %v, missed %d", got, len(out))
		}
		if got := testutil.ToFloat64(h.m.offersSeen); got != n {
			t.Errorf("offers seen %v, want %d", got, n)
		}
		share := float64(len(out)) / n
		if share < 0.025 || share > 0.035 {
			t.Errorf("seed %d: missed share %.4f, want about 0.03", seed, share)
		}
		return out
	}
	a, b := missed(7), missed(7)
	if !slices.Equal(a, b) {
		t.Error("the same seed missed different offers")
	}
	if c := missed(8); slices.Equal(a, c) {
		t.Error("another seed missed the same offers")
	}
}

// TestReplyThenResolve: after an accept the reply comes after the handling
// time, then the resolve after the resolve time, in this order, also when
// the reply needs retries.
func TestReplyThenResolve(t *testing.T) {
	for _, replyFails := range []int{0, 3} {
		h := newHarness(t, testBehaviour(), 1, 1)
		failures := 0
		h.api.answer = func(c call) (opapi.Result, error) {
			if c.action == actionReply && failures < replyFails {
				failures++
				return "", apiError(opapi.CodeUnavailable)
			}
			return "", nil
		}
		h.send(event.TypeOffered, customerID(1), op1, offerID(1))
		h.until(10 * time.Second)
		h.send(event.TypeAccepted, customerID(1), op1, offerID(1))
		acceptedAt := h.model()
		h.until(4 * time.Hour)
		got := h.api.callsOf("")
		var actions []string
		for _, c := range got {
			actions = append(actions, c.action)
		}
		want := []string{actionAccept}
		for range replyFails + 1 {
			want = append(want, actionReply)
		}
		want = append(want, actionResolve)
		if !slices.Equal(actions, want) {
			t.Fatalf("reply failures %d: calls %v, want %v", replyFails, actions, want)
		}
		reply, resolve := got[len(got)-2], got[len(got)-1]
		if reply.model <= acceptedAt || resolve.model <= reply.model {
			t.Errorf("accepted at %s, reply at %s, resolve at %s", acceptedAt, reply.model, resolve.model)
		}
		if h.e.pending() != 0 || len(h.e.offers) != 0 {
			t.Errorf("after the resolve: %d planned, %d offers", h.e.pending(), len(h.e.offers))
		}
		if got := testutil.ToFloat64(h.m.retries.WithLabelValues(actionReply)); got != float64(replyFails) {
			t.Errorf("reply retries %v, want %d", got, replyFails)
		}
	}
}

// TestCancel: each cancelling event drops what was planned for the offer,
// both while the accept is planned and while the reply or resolve is.
func TestCancel(t *testing.T) {
	tests := []struct {
		name string
		send func(h *harness)
	}{
		{"offer_expired", func(h *harness) { h.send(event.TypeOfferExpired, customerID(1), op1, offerID(1)) }},
		{"ended", func(h *harness) { h.send(event.TypeEnded, customerID(1), op1, offerID(1)) }},
		{"ended without offer ID", func(h *harness) { h.send(event.TypeEnded, customerID(1), "", "") }},
		{"queued again", func(h *harness) { h.send(event.TypeQueued, customerID(1), "", "") }},
		{"resolved", func(h *harness) { h.send(event.TypeResolved, customerID(1), op1, offerID(1)) }},
		{"another offer", func(h *harness) { h.send(event.TypeOffered, customerID(1), "other-op", offerID(2)) }},
	}
	stages := []struct {
		name   string
		before func(h *harness)
		// want are the calls made before the cancelling event.
		want []string
	}{
		{"accept planned", func(h *harness) {
			h.send(event.TypeOffered, customerID(1), op1, offerID(1))
		}, nil},
		{"reply planned", func(h *harness) {
			h.send(event.TypeOffered, customerID(1), op1, offerID(1))
			h.until(10 * time.Second)
			h.send(event.TypeAccepted, customerID(1), op1, offerID(1))
		}, []string{actionAccept}},
		{"resolve planned", func(h *harness) {
			h.send(event.TypeOffered, customerID(1), op1, offerID(1))
			h.until(10 * time.Second)
			h.send(event.TypeAccepted, customerID(1), op1, offerID(1))
			// Until the reply is done; the resolve is then planned.
			for len(h.api.callsOf(actionReply)) == 0 {
				h.until(h.model() + time.Second)
			}
		}, []string{actionAccept, actionReply}},
	}
	for _, st := range stages {
		for _, tt := range tests {
			t.Run(st.name+"/"+tt.name, func(t *testing.T) {
				h := newHarness(t, testBehaviour(), 1, 1)
				st.before(h)
				if h.e.pending() != 1 {
					t.Fatalf("%d planned before the event, want 1", h.e.pending())
				}
				tt.send(h)
				if h.e.pending() != 0 || len(h.e.offers) != 0 || len(h.e.byKey) != 0 {
					t.Fatalf("after the event: %d planned, %d offers, %d keys", h.e.pending(), len(h.e.offers), len(h.e.byKey))
				}
				h.until(10 * time.Hour)
				var got []string
				for _, c := range h.api.callsOf("") {
					got = append(got, c.action)
				}
				if !slices.Equal(got, st.want) {
					t.Errorf("calls %v, want %v", got, st.want)
				}
				if got := testutil.ToFloat64(h.m.dropped); got != 1 {
					t.Errorf("cancelled %v, want 1", got)
				}
			})
		}
	}
}

// TestCancelWhileRunning: a call that is running when its offer is
// cancelled is not retried and plans nothing after it.
func TestCancelWhileRunning(t *testing.T) {
	h := newHarness(t, testBehaviour(), 1, 1)
	h.send(event.TypeOffered, customerID(1), op1, offerID(1))
	h.until(10 * time.Second)
	h.send(event.TypeAccepted, customerID(1), op1, offerID(1))
	h.api.answer = func(c call) (opapi.Result, error) {
		// The router puts the conversation back while the reply runs.
		h.send(event.TypeQueued, customerID(1), "", "")
		return "", errNoAnswer
	}
	h.until(time.Hour)
	if n := len(h.api.callsOf(actionReply)); n != 1 {
		t.Errorf("%d reply calls, want 1", n)
	}
	if h.e.pending() != 0 {
		t.Errorf("%d planned, want 0", h.e.pending())
	}
}

// TestDuplicateEvents: an event whose sequence number was already seen for
// its key is skipped, also an older one.
func TestDuplicateEvents(t *testing.T) {
	h := newHarness(t, testBehaviour(), 1, 1)
	ev := h.send(event.TypeOffered, customerID(1), op1, offerID(1))
	h.e.handle(ev)
	older := ev
	older.Seq = 0
	h.e.handle(older)
	// A seen key with a new sequence number is applied.
	h.send(event.TypeQueued, customerID(2), "", "")
	if got := testutil.ToFloat64(h.m.eventsSkipped); got != 2 {
		t.Errorf("skipped %v, want 2", got)
	}
	if got := testutil.ToFloat64(h.m.offersSeen); got != 1 {
		t.Errorf("offers seen %v, want 1", got)
	}
	h.until(time.Minute)
	if n := len(h.api.callsOf(actionAccept)); n != 1 {
		t.Errorf("%d accepts, want 1", n)
	}
	// The same offered event again after its end does not start new work.
	h.send(event.TypeOfferExpired, customerID(1), op1, offerID(1))
	h.e.handle(ev)
	if h.e.pending() != 0 {
		t.Errorf("%d planned after a repeated event, want 0", h.e.pending())
	}
}

// TestOtherOperators: events about operators that are not simulated plan
// nothing.
func TestOtherOperators(t *testing.T) {
	h := newHarness(t, testBehaviour(), 1, 1)
	h.send(event.TypeOffered, customerID(1), "console-op-1", offerID(1))
	h.send(event.TypeAccepted, customerID(2), "console-op-1", offerID(2))
	h.e.handle(event.Routing{Type: event.TypeTeamFormed, CompanyID: "test-co-0001", Seq: 1, Members: []string{op1}})
	if h.e.pending() != 0 {
		t.Errorf("%d planned, want 0", h.e.pending())
	}
}

// TestRejected: a 409 drops what was planned for the offer and is counted.
func TestRejected(t *testing.T) {
	tests := []struct {
		action string
		code   opapi.ErrorCode
		want   []string
	}{
		{actionAccept, opapi.CodeOfferExpired, []string{actionAccept}},
		{actionReply, opapi.CodeAssignmentEnded, []string{actionAccept, actionReply}},
		{actionResolve, opapi.CodeOfferEnded, []string{actionAccept, actionReply, actionResolve}},
	}
	for _, tt := range tests {
		t.Run(tt.action, func(t *testing.T) {
			h := newHarness(t, testBehaviour(), 1, 1)
			h.api.answer = func(c call) (opapi.Result, error) {
				if c.action == tt.action {
					return "", apiError(tt.code)
				}
				return "", nil
			}
			h.send(event.TypeOffered, customerID(1), op1, offerID(1))
			h.until(10 * time.Second)
			// The router sends "accepted" also when the accept call is
			// rejected in this test, so the later stages can be reached.
			h.send(event.TypeAccepted, customerID(1), op1, offerID(1))
			h.until(10 * time.Hour)
			var got []string
			for _, c := range h.api.callsOf("") {
				got = append(got, c.action)
			}
			if tt.action == actionAccept {
				// The reply was planned by the accepted event, which came
				// after the rejection.
				tt.want = append(tt.want, actionReply, actionResolve)
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("calls %v, want %v", got, tt.want)
			}
			if got := testutil.ToFloat64(h.m.rejections.WithLabelValues(tt.action, string(tt.code))); got != 1 {
				t.Errorf("rejections %v, want 1", got)
			}
			if h.e.pending() != 0 || len(h.e.offers) != 0 {
				t.Errorf("%d planned, %d offers, want none", h.e.pending(), len(h.e.offers))
			}
		})
	}
}

// TestRejectedAcceptDropsOffer: a rejected accept drops the offer at once.
func TestRejectedAcceptDropsOffer(t *testing.T) {
	h := newHarness(t, testBehaviour(), 1, 1)
	h.api.answer = func(call) (opapi.Result, error) { return "", apiError(opapi.CodeOfferExpired) }
	h.send(event.TypeOffered, customerID(1), op1, offerID(1))
	h.until(10 * time.Second)
	if len(h.e.offers) != 0 || h.e.pending() != 0 {
		t.Errorf("%d offers, %d planned after a rejected accept", len(h.e.offers), h.e.pending())
	}
}

// TestFailedNotRetried: a 404 or 500 is counted and not retried.
func TestFailedNotRetried(t *testing.T) {
	for _, code := range []opapi.ErrorCode{opapi.CodeOfferNotFound, opapi.CodeInternal, opapi.CodeInvalidRequest} {
		h := newHarness(t, testBehaviour(), 1, 1)
		h.api.answer = func(call) (opapi.Result, error) { return "", apiError(code) }
		h.send(event.TypeOffered, customerID(1), op1, offerID(1))
		h.until(time.Hour)
		if n := len(h.api.callsOf(actionAccept)); n != 1 {
			t.Errorf("%s: %d accept calls, want 1", code, n)
		}
		if got := testutil.ToFloat64(h.m.failures.WithLabelValues(actionAccept, string(code))); got != 1 {
			t.Errorf("%s: failures %v, want 1", code, got)
		}
	}
}

// TestRetryAndOutage: 503 and network errors are retried with a growing
// backoff up to maxBackoff. An answer ends the outage. When no answer
// comes for the outage limit, execute returns ErrOutage.
func TestRetryAndOutage(t *testing.T) {
	h := newHarness(t, testBehaviour(), 1, 1)
	fail := 4
	h.api.answer = func(c call) (opapi.Result, error) {
		if fail > 0 {
			fail--
			if fail%2 == 0 {
				return "", errNoAnswer
			}
			return "", apiError(opapi.CodeUnavailable)
		}
		return "", nil
	}
	h.send(event.TypeOffered, customerID(1), op1, offerID(1))
	h.until(time.Minute)
	accepts := h.api.callsOf(actionAccept)
	if len(accepts) != 5 {
		t.Fatalf("%d accept calls, want 5", len(accepts))
	}
	for i, want := range []time.Duration{100, 200, 400, 800} {
		if gap := accepts[i+1].model - accepts[i].model; gap != want*time.Millisecond {
			t.Errorf("gap %d is %s, want %dms", i, gap, want)
		}
	}
	if h.fatal != nil || !h.e.outageSince.IsZero() {
		t.Fatalf("outage after a success: %v, since %v", h.fatal, h.e.outageSince)
	}
	if got := testutil.ToFloat64(h.m.retries.WithLabelValues(actionAccept)); got != 4 {
		t.Errorf("retries %v, want 4", got)
	}

	// Now the router stays away. The outage limit of the harness is one
	// minute.
	h.api.answer = func(call) (opapi.Result, error) { return "", errNoAnswer }
	h.send(event.TypeOffered, customerID(2), op1, offerID(2))
	start := h.model()
	h.until(start + 10*time.Minute)
	if !errors.Is(h.fatal, ErrOutage) {
		t.Fatalf("fatal %v, want ErrOutage", h.fatal)
	}
	calls := h.api.callsOf(actionAccept)[5:]
	last := calls[len(calls)-1].model - calls[0].model
	if last < time.Minute || last > time.Minute+maxBackoff {
		t.Errorf("stopped %s after the first failure, want about 1m", last)
	}
	for i := 1; i < len(calls); i++ {
		if gap := calls[i].model - calls[i-1].model; gap > maxBackoff {
			t.Errorf("gap %s above the maximum backoff", gap)
		}
	}
}

// TestShifts: each operator goes online for ShiftOnline and offline for
// ShiftOffline, in turn, with offsets that differ between operators.
func TestShifts(t *testing.T) {
	b := testBehaviour()
	b.ShiftOnline, b.ShiftOffline = 4*time.Hour, 30*time.Minute
	const ops = 100
	h := newHarness(t, b, 1, ops)
	h.e.start()
	h.until(0)
	// Online at the start: the share of the cycle in the online part.
	online := len(h.api.callsOf(actionOnline))
	if online < 80 || online > 97 {
		t.Errorf("%d of %d operators online at the start, want about 89", online, ops)
	}
	if got := testutil.ToFloat64(h.m.operatorsOnline); got != float64(online) {
		t.Errorf("online gauge %v, want %d", got, online)
	}
	h.until(20 * time.Hour)
	byOp := map[string][]call{}
	for _, c := range h.api.callsOf("") {
		byOp[c.op] = append(byOp[c.op], c)
	}
	firstChange := map[time.Duration]bool{}
	for id, calls := range byOp {
		for i, c := range calls {
			if i > 0 && c.action == calls[i-1].action {
				t.Fatalf("%s: %s twice in a row", id, c.action)
			}
			if i == 0 {
				continue
			}
			if i == 1 {
				// The first part is partial: its length is the offset.
				firstChange[c.model] = true
				if calls[0].model == 0 {
					continue
				}
			}
			gap := c.model - calls[i-1].model
			want := b.ShiftOnline
			if c.action == actionOnline {
				want = b.ShiftOffline
			}
			if gap != want {
				t.Errorf("%s: %s after %s, want %s", id, c.action, gap, want)
			}
		}
		if len(calls) < 8 {
			t.Errorf("%s: %d presence changes in 20 hours", id, len(calls))
		}
	}
	if len(byOp) != ops {
		t.Errorf("%d operators had shift changes, want %d", len(byOp), ops)
	}
	if len(firstChange) < 10 {
		t.Errorf("only %d different first change times, the offsets do not spread", len(firstChange))
	}
	// Every operator has its next shift change planned.
	if h.e.pending() != ops {
		t.Errorf("%d planned, want %d", h.e.pending(), ops)
	}
}

// TestShiftRetryReplaced: a presence call that is still retried when the
// next shift change comes is dropped.
func TestShiftRetryReplaced(t *testing.T) {
	b := testBehaviour()
	b.ShiftOnline, b.ShiftOffline = time.Second, time.Second
	h := newHarness(t, b, 1, 1)
	h.e.outageLimit = time.Hour
	down := true
	h.api.answer = func(call) (opapi.Result, error) {
		if down {
			return "", errNoAnswer
		}
		return "", nil
	}
	h.e.start()
	h.until(10 * time.Second)
	down = false
	h.until(14*time.Second + 500*time.Millisecond)
	calls := h.api.callsOf("")
	// The last calls follow the shift exactly again, one per second.
	tail := calls[len(calls)-4:]
	for i := 1; i < len(tail); i++ {
		if tail[i].action == tail[i-1].action {
			t.Errorf("calls %v: the same presence twice in a row after the outage", tail)
		}
	}
	if h.e.pending() != 1 {
		t.Errorf("%d planned, want only the next shift change", h.e.pending())
	}
}

// scenario feeds a fixed event script and returns the calls in model time.
func scenario(t *testing.T, speed float64, seed uint64) []call {
	t.Helper()
	b := testBehaviour()
	b.ShiftOnline, b.ShiftOffline = 30*time.Minute, 5*time.Minute
	b.MissedOfferShare = 0.2
	b.Seed = seed
	h := newHarness(t, b, speed, 3)
	h.e.start()
	at := func(m time.Duration) { h.until(m) }
	for i := range 40 {
		op := operatorID(i%3 + 1)
		at(time.Duration(i) * 7 * time.Second)
		h.send(event.TypeOffered, customerID(i), op, offerID(i))
		at(time.Duration(i)*7*time.Second + 9*time.Second)
		if accepted := slices.ContainsFunc(h.api.callsOf(actionAccept), func(c call) bool { return c.offer == offerID(i) }); accepted {
			h.send(event.TypeAccepted, customerID(i), op, offerID(i))
		} else {
			h.send(event.TypeOfferExpired, customerID(i), op, offerID(i))
		}
	}
	at(3 * time.Hour)
	return h.api.callsOf("")
}

// TestSpeedFactor: the same scenario at factor 1 and 50 gives the same
// calls at the same model times.
func TestSpeedFactor(t *testing.T) {
	a := scenario(t, 1, 3)
	b := scenario(t, 50, 3)
	if !slices.Equal(a, b) {
		t.Fatalf("factor 1 and 50 differ:\n%v\n%v", a, b)
	}
	if len(a) < 60 {
		t.Errorf("only %d calls in the scenario", len(a))
	}
}

// TestRepeatable: the same seed and the same events give the same calls;
// another seed gives other calls.
func TestRepeatable(t *testing.T) {
	a, b, c := scenario(t, 1, 11), scenario(t, 1, 11), scenario(t, 1, 12)
	if !slices.Equal(a, b) {
		t.Error("the same seed gave different calls")
	}
	if slices.Equal(a, c) {
		t.Error("another seed gave the same calls")
	}
}

// TestDistributions: the handling and resolve times have the configured
// median, a long tail, and the cap.
func TestDistributions(t *testing.T) {
	rng := newRand(1)
	const n = 20000
	median := 45 * time.Second
	draws := make([]time.Duration, n)
	for i := range draws {
		draws[i] = logNormal(rng, median)
	}
	slices.Sort(draws)
	got := draws[n/2]
	if got < 42*time.Second || got > 48*time.Second {
		t.Errorf("median %s, want about 45s", got)
	}
	p95 := draws[n*95/100]
	if p95 < 200*time.Second || p95 > 270*time.Second {
		t.Errorf("95th percentile %s, want about 233s", p95)
	}
	if top := draws[n-1]; top > tailCap*median {
		t.Errorf("largest draw %s above the cap %s", top, tailCap*median)
	}
	for range 1000 {
		d := uniform(rng, time.Second, 8*time.Second)
		if d < time.Second || d > 8*time.Second {
			t.Fatalf("uniform draw %s out of range", d)
		}
	}
}

// TestPruneSeen: keys without events for seenKeep are pruned, except keys
// of conversations that an operator holds.
func TestPruneSeen(t *testing.T) {
	h := newHarness(t, testBehaviour(), 1, 1)
	h.send(event.TypeOffered, customerID(0), op1, offerID(0))
	h.send(event.TypeQueued, customerID(1), "", "")
	h.clock.Advance(seenKeep + time.Minute)
	for i := range pruneEvery {
		h.send(event.TypeQueued, customerID(i+2), "", "")
	}
	if _, ok := h.e.seen[string(event.ConversationKey("test-co-0001", customerID(1)))]; ok {
		t.Error("an idle key was not pruned")
	}
	if _, ok := h.e.seen[string(event.ConversationKey("test-co-0001", customerID(0)))]; !ok {
		t.Error("the key of a held conversation was pruned")
	}
}

// TestOperatorEvents: operator events plan nothing, are not counted as
// skipped, and do not touch the sequence numbers seen for conversations,
// even when an operator event comes with the same sequence number as a
// conversation event (decision 0011, three key families in one topic).
func TestOperatorEvents(t *testing.T) {
	h := newHarness(t, testBehaviour(), 1, 2)
	opEvent := func(typ event.RoutingType, op string, seq uint64, presence event.Presence) event.Routing {
		return event.Routing{
			Type: typ, OperatorID: op, Seq: seq, At: h.clock.Now(),
			Languages: []string{"en", "und"}, Capacity: 4, Presence: presence,
		}
	}
	// Echoes of the simulator's own calls, a team change, and an operator
	// the simulator does not play.
	for i, ev := range []event.Routing{
		opEvent(event.TypeOperatorUpdated, op1, 1, event.PresenceOffline),
		opEvent(event.TypeOperatorOnline, op1, 2, event.PresenceOnline),
		opEvent(event.TypeOperatorUpdated, operatorID(2), 1, event.PresenceOffline),
		opEvent(event.TypeOperatorUpdated, "console-op-1", 1, event.PresenceOffline),
	} {
		if err := ev.Validate(); err != nil {
			t.Fatalf("operator event %d is not valid: %v", i, err)
		}
		h.e.handle(ev)
	}
	team := opEvent(event.TypeOperatorUpdated, op1, 3, event.PresenceOnline)
	team.DedicatedCompanyID = "test-co-0001"
	h.e.handle(team)
	// The same operator event again, as after a restart of the publisher.
	h.e.handle(opEvent(event.TypeOperatorOnline, op1, 2, event.PresenceOnline))
	if h.e.pending() != 0 {
		t.Errorf("%d planned after operator events, want 0", h.e.pending())
	}
	if got := testutil.ToFloat64(h.m.eventsSkipped); got != 0 {
		t.Errorf("skipped %v, want 0", got)
	}
	if got := testutil.ToFloat64(h.m.events.WithLabelValues(string(event.TypeOperatorUpdated))); got != 4 {
		t.Errorf("operator_updated events counted %v, want 4", got)
	}

	// Conversation events of the same operator still work, and their own
	// repeats are still skipped.
	ev := h.send(event.TypeOffered, customerID(1), op1, offerID(1))
	h.e.handle(ev)
	h.e.handle(opEvent(event.TypeOperatorOffline, op1, 1, event.PresenceOffline))
	if got := testutil.ToFloat64(h.m.eventsSkipped); got != 1 {
		t.Errorf("skipped %v, want 1 (the repeated offer)", got)
	}
	h.until(time.Minute)
	if n := len(h.api.callsOf(actionAccept)); n != 1 {
		t.Errorf("%d accepts, want 1", n)
	}
	// A queued event with the previous pair (stage 4) cancels the offer as
	// before.
	q := event.Routing{
		Type: event.TypeQueued, CompanyID: "test-co-0001", CustomerID: customerID(1), Session: 1,
		At: h.clock.Now(), Lang: "en", QueuedAt: h.clock.Now(), QueueReason: event.QueueOfferRefused,
		PreviousOperatorID: op1, PreviousOfferID: offerID(1), Seq: 2,
	}
	if err := q.Validate(); err != nil {
		t.Fatal(err)
	}
	h.e.handle(q)
	if h.e.pending() != 0 {
		t.Errorf("%d planned after the requeue, want 0", h.e.pending())
	}
}

// TestRecordsBeforeStage4: the routing topic keeps 24 hours, so records
// written before stage 4 are still read after an upgrade. A "queued"
// record that follows an offer has no previous pair then; it decodes and
// cancels the held offer as before.
func TestRecordsBeforeStage4(t *testing.T) {
	h := newHarness(t, testBehaviour(), 1, 1)
	h.send(event.TypeOffered, customerID(1), op1, offerID(1))
	at := h.clock.Now().UnixMicro()
	old := fmt.Sprintf(`{"type":"queued","company_id":"test-co-0001","customer_id":%q,"session":1,"seq":2,`+
		`"at_us":%d,"lang":"en","team_queue":false,"priority":0,"queued_at_us":%d,"queue_reason":"offer_refused"}`,
		customerID(1), at, at)
	ev, err := event.UnmarshalRouting([]byte(old))
	if err != nil {
		t.Fatalf("old queued record: %v", err)
	}
	if ev.PreviousOperatorID != "" || ev.PreviousOfferID != "" {
		t.Fatalf("old record has a previous pair: %+v", ev)
	}
	h.e.handle(ev)
	if h.e.pending() != 0 {
		t.Errorf("%d planned after the old queued record, want 0", h.e.pending())
	}
	if got := testutil.ToFloat64(h.m.dropped); got != 1 {
		t.Errorf("plans cancelled %v, want 1", got)
	}
}
