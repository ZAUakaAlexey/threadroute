package sim

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// changeHarness starts an engine with n operators of a spread and capacity
// 4, every operator online.
func changeHarness(t *testing.T, spread Spread, n int) *harness {
	t.Helper()
	b := testBehaviour()
	b.Languages = spread
	b.Operators = n
	b.Capacity = 4
	h := newHarness(t, b, 1, n)
	h.e.start()
	h.until(0)
	if got := len(h.api.callsOf(actionOnline)); got != n {
		t.Fatalf("%d operators online at start, want %d", got, n)
	}
	return h
}

// simWith returns the sim values in effect with another count and
// capacity.
func (h *harness) simWith(operators, capacity int) opapi.SimSettings {
	v := h.e.inEffect()
	v.Operators, v.Capacity = operators, capacity
	return v
}

// apply applies a sim part and runs what is due now.
func (h *harness) apply(s opapi.SimSettings) changeResult {
	r := h.e.applySim(s)
	h.until(h.model())
	return r
}

// activeByLang returns the active operators by language, in number order.
func (h *harness) activeByLang() map[string][]string {
	h.e.mu.Lock()
	defer h.e.mu.Unlock()
	out := map[string][]string{}
	for _, op := range h.e.opList {
		if op.active {
			out[op.lang] = append(out[op.lang], op.id)
		}
	}
	return out
}

// presence returns the last presence call of each operator, as the router
// sees it.
func (h *harness) presence() map[string]string {
	out := map[string]string{}
	for _, c := range h.api.callsOf("") {
		if c.action == actionOnline || c.action == actionOffline {
			out[c.op] = c.action
		}
	}
	return out
}

// checkState checks that the active operators are the numbers 1 to n with
// the languages of their numbers, that the router has them online, and
// every other operator offline.
func (h *harness) checkState(n int) {
	h.t.Helper()
	targets := h.e.b.Languages.layout(n)
	order := h.e.b.Languages.langOrder(len(h.e.opList))
	byLang := h.activeByLang()
	for _, l := range targets {
		if got := len(byLang[l.Lang]); got != l.Count {
			h.t.Errorf("count %d: %d active %s operators, want %d", n, got, l.Lang, l.Count)
		}
	}
	pres := h.presence()
	active := 0
	h.e.mu.Lock()
	for _, op := range h.e.opList {
		want := actionOffline
		if op.active {
			want = actionOnline
			active++
		}
		if op.active != (op.idx < n) {
			h.t.Errorf("count %d: %s active %v", n, op.id, op.active)
		}
		if op.lang != order[op.idx] || strings.Join(op.langs, ",") != order[op.idx]+",und" {
			h.t.Errorf("count %d: %s has %v, want %s", n, op.id, op.langs, order[op.idx])
		}
		if pres[op.id] != want {
			h.t.Errorf("count %d: %s (active %v) last %q, want %q", n, op.id, op.active, pres[op.id], want)
		}
		if op.starting {
			h.t.Errorf("count %d: %s still waits to start", n, op.id)
		}
	}
	h.e.mu.Unlock()
	if active != n {
		h.t.Errorf("%d active, want %d", active, n)
	}
	if got := testutil.ToFloat64(h.m.operatorsActive); got != float64(n) {
		h.t.Errorf("active gauge %v, want %d", got, n)
	}
	if got := testutil.ToFloat64(h.m.operatorsAside); got != float64(len(h.e.opList)-n) {
		h.t.Errorf("put aside gauge %v, want %d", got, len(h.e.opList)-n)
	}
}

var defaultSpread = Spread{
	{Lang: "en", Count: 176}, {Lang: "ja", Count: 6}, {Lang: "de", Count: 4}, {Lang: "fr", Count: 4},
	{Lang: "es", Count: 4}, {Lang: "pt", Count: 3}, {Lang: "ko", Count: 3},
}

// TestChangeTargets: for a row of counts, the active operators are the
// numbers 1 to the count with the languages of their numbers, the router
// has exactly them online, and no more operators are created than the
// largest count needs.
func TestChangeTargets(t *testing.T) {
	h := changeHarness(t, defaultSpread, 200)
	h.checkState(200)
	if got := defaultSpread.layout(200); !slices.Equal(got, defaultSpread) {
		t.Errorf("200 operators are %s, want the spread %s", got, defaultSpread)
	}
	peak := 200
	for _, n := range []int{100, 10, 37, 200, 500, 199, 11, 333} {
		h.apply(h.simWith(n, 4))
		h.until(h.model() + time.Minute)
		h.checkState(n)
		peak = max(peak, n)
	}
	if len(h.e.opList) != peak {
		t.Errorf("%d operators registered, want %d (the largest count)", len(h.e.opList), peak)
	}
	// Numbers have no gap.
	for i, op := range h.e.opList {
		if op.id != operatorID(i+1) {
			t.Fatalf("operator %d is %s", i+1, op.id)
		}
	}
}

// TestChangeKeepsLanguages: a change of the count never changes the
// language of an operator. One more operator creates only the next number,
// with the language of that number, and one less puts aside only the
// highest number.
func TestChangeKeepsLanguages(t *testing.T) {
	spread := Spread{{Lang: "de", Count: 1}, {Lang: "ja", Count: 3}, {Lang: "en", Count: 3}}
	order := spread.langOrder(12)
	h := changeHarness(t, spread, 10)
	h.checkState(10)
	before := len(h.api.callsOf(""))
	r := h.apply(h.simWith(11, 4))
	if r.putAside != 0 || r.created != 1 || r.back != 0 {
		t.Errorf("result %+v, want 1 created", r)
	}
	var got []string
	for _, c := range h.api.callsOf("")[before:] {
		got = append(got, c.action+" "+c.op+" "+c.langs)
	}
	wantCalls := []string{"put_operator sim-op-0011 " + order[10] + ",und", "online sim-op-0011 "}
	if !slices.Equal(got, wantCalls) {
		t.Errorf("calls %q, want %q", got, wantCalls)
	}
	h.checkState(11)

	before = len(h.api.callsOf(""))
	r = h.apply(h.simWith(9, 4))
	if r.putAside != 2 || r.created != 0 || r.back != 0 {
		t.Errorf("result %+v, want 2 put aside", r)
	}
	got = nil
	for _, c := range h.api.callsOf("")[before:] {
		got = append(got, c.action+" "+c.op)
	}
	if want := []string{"offline sim-op-0010", "offline sim-op-0011"}; !slices.Equal(got, want) {
		t.Errorf("calls %q, want %q", got, want)
	}
	h.checkState(9)

	r = h.apply(h.simWith(12, 4))
	if r.putAside != 0 || r.created != 1 || r.back != 2 {
		t.Errorf("result %+v, want 2 back and 1 created", r)
	}
	h.checkState(12)
	// Every put of an operator carried the language of its number.
	for _, c := range h.api.callsOf(actionPut) {
		var num int
		if _, err := fmt.Sscanf(c.op, "sim-op-%04d", &num); err != nil {
			t.Fatal(err)
		}
		if c.langs != order[num-1]+",und" {
			t.Errorf("%s put with %s, want %s", c.op, c.langs, order[num-1])
		}
	}
}

// TestChangeFewer: the operators put aside go offline through the normal
// call, their planned accepts, replies and resolves never fire, and later
// offers to them are ignored.
func TestChangeFewer(t *testing.T) {
	h := changeHarness(t, Spread{{Lang: "en", Count: 1}}, 12)
	// sim-op-0012 has a resolve planned, sim-op-0011 a reply, sim-op-0010
	// an accept.
	h.send(event.TypeOffered, customerID(12), operatorID(12), offerID(12))
	h.until(10 * time.Second)
	h.send(event.TypeAccepted, customerID(12), operatorID(12), offerID(12))
	for len(h.api.callsOf(actionReply)) == 0 {
		h.until(h.model() + time.Second)
	}
	h.send(event.TypeOffered, customerID(11), operatorID(11), offerID(11))
	h.send(event.TypeAccepted, customerID(11), operatorID(11), offerID(11))
	h.send(event.TypeOffered, customerID(10), operatorID(10), offerID(10))
	if h.e.pending() != 12+3 {
		t.Fatalf("%d planned, want 12 shift changes and 3 steps", h.e.pending())
	}
	before := len(h.api.callsOf(""))

	r := h.apply(h.simWith(9, 4))
	if r.putAside != 3 {
		t.Fatalf("put aside %d, want 3", r.putAside)
	}
	if len(h.e.offers) != 0 || len(h.e.byKey) != 0 {
		t.Errorf("%d offers and %d keys kept for operators put aside", len(h.e.offers), len(h.e.byKey))
	}
	// Offers read after the change were made before the offline calls.
	h.send(event.TypeOffered, customerID(13), operatorID(12), offerID(13))
	h.send(event.TypeAccepted, customerID(10), operatorID(10), offerID(10))
	h.until(10 * time.Hour)
	// The shifts of the active operators go on; every other call is
	// listed.
	var after []string
	for _, c := range h.api.callsOf("")[before:] {
		shift := c.action == actionOnline || c.action == actionOffline
		if !shift || c.op >= operatorID(10) {
			after = append(after, c.action+" "+c.op)
		}
	}
	want := []string{"offline sim-op-0010", "offline sim-op-0011", "offline sim-op-0012"}
	if !slices.Equal(after, want) {
		t.Errorf("calls after the change %v, want only %v", after, want)
	}
	if h.e.pending() != 9 {
		t.Errorf("%d planned, want the 9 shift changes of the active operators", h.e.pending())
	}
	h.checkState(9)
}

// TestChangeOfflineCallRunning: an operator put aside while its step waits
// for a worker: the step is not sent.
func TestChangeOfflineCallRunning(t *testing.T) {
	h := changeHarness(t, Spread{{Lang: "en", Count: 1}}, 10)
	h.send(event.TypeOffered, customerID(1), operatorID(10), offerID(1))
	h.clock.Set(h.e.tl.machine(10 * time.Second))
	due := h.e.due(h.clock.Now())
	if len(due) != 1 || due[0].kind != kindAccept {
		t.Fatalf("due %v, want the accept", due)
	}
	h.e.applySim(h.simWith(9, 4))
	for _, task := range due {
		if err := h.e.execute(t.Context(), h.api, task); err != nil {
			t.Fatal(err)
		}
	}
	h.until(h.model())
	if n := len(h.api.callsOf(actionAccept)); n != 0 {
		t.Errorf("%d accepts sent for an operator put aside", n)
	}
}

// TestChangeMore: operators put aside come back first, lowest numbers
// first, with a put and then the online call; then new ones are created
// with the next numbers and the capacity in effect.
func TestChangeMore(t *testing.T) {
	h := changeHarness(t, Spread{{Lang: "en", Count: 1}}, 15)
	h.apply(h.simWith(10, 4))
	h.checkState(10)
	before := len(h.api.callsOf(""))
	r := h.apply(h.simWith(17, 4))
	if r.back != 5 || r.created != 2 {
		t.Errorf("result %+v, want 5 back and 2 created", r)
	}
	var got []string
	for _, c := range h.api.callsOf("")[before:] {
		got = append(got, c.action+" "+c.op)
	}
	var want []string
	for i := 11; i <= 17; i++ {
		want = append(want, "put_operator "+operatorID(i))
	}
	for i := 11; i <= 17; i++ {
		want = append(want, "online "+operatorID(i))
	}
	if !slices.Equal(got, want) {
		t.Errorf("calls %v, want %v", got, want)
	}
	h.checkState(17)
}

// TestChangeBackAndForth: counts up and down end in the right state each
// time, and operators are reused before new ones are created.
func TestChangeBackAndForth(t *testing.T) {
	h := changeHarness(t, Spread{{Lang: "en", Count: 3}, {Lang: "ja", Count: 1}}, 20)
	for _, n := range []int{12, 20, 25, 12, 25, 10, 20} {
		h.apply(h.simWith(n, 4))
		h.until(h.model() + time.Minute)
		h.checkState(n)
	}
	if len(h.e.opList) != 25 {
		t.Errorf("%d operators registered, want 25", len(h.e.opList))
	}
}

// TestChangeCapacity: a new capacity is sent to every active operator with
// its languages, not to the operators put aside; an operator that comes
// back gets the capacity in effect. Presence does not change.
func TestChangeCapacity(t *testing.T) {
	h := changeHarness(t, Spread{{Lang: "en", Count: 2}, {Lang: "ja", Count: 1}}, 12)
	h.apply(h.simWith(10, 4))
	before := len(h.api.callsOf(""))
	r := h.apply(h.simWith(10, 7))
	if r.capacityPuts != 10 {
		t.Errorf("capacity puts %d, want 10", r.capacityPuts)
	}
	calls := h.api.callsOf("")[before:]
	if len(calls) != 10 {
		t.Fatalf("calls %v, want 10 puts", calls)
	}
	active := map[string]bool{}
	for _, ids := range h.activeByLang() {
		for _, id := range ids {
			active[id] = true
		}
	}
	for _, c := range calls {
		if c.action != actionPut || c.capacity != 7 || !active[c.op] {
			t.Errorf("call %v capacity %d, want a put with 7 of an active operator", c, c.capacity)
		}
		if c.langs != "en,und" && c.langs != "ja,und" {
			t.Errorf("%s sent with languages %q", c.op, c.langs)
		}
	}
	h.checkState(10)
	// The operators put aside come back with 7.
	before = len(h.api.callsOf(""))
	h.apply(h.simWith(12, 7))
	for _, c := range h.api.callsOf(actionPut)[len(h.api.callsOf(actionPut))-2:] {
		if c.capacity != 7 {
			t.Errorf("%s came back with capacity %d, want 7", c.op, c.capacity)
		}
	}
	if n := len(h.api.callsOf("")[before:]); n != 4 {
		t.Errorf("%d calls to bring back 2 operators, want 4", n)
	}
	h.checkState(12)
}

// TestChangeCapacityTwice: two capacity changes before any put ran send
// only the newest capacity.
func TestChangeCapacityTwice(t *testing.T) {
	h := changeHarness(t, Spread{{Lang: "en", Count: 1}}, 10)
	h.e.applySim(h.simWith(10, 5))
	h.e.applySim(h.simWith(10, 6))
	h.until(h.model())
	puts := h.api.callsOf(actionPut)
	if len(puts) != 10 {
		t.Fatalf("%d puts, want 10", len(puts))
	}
	for _, c := range puts {
		if c.capacity != 6 {
			t.Errorf("%s got capacity %d, want only 6", c.op, c.capacity)
		}
	}
}

// TestChangeTwoInARow: a second change that comes while the calls of the
// first wait for their workers ends in the state of the second.
func TestChangeTwoInARow(t *testing.T) {
	h := changeHarness(t, Spread{{Lang: "en", Count: 1}}, 20)
	h.e.applySim(h.simWith(10, 4))
	// The scheduler took the offline calls; they wait for the workers.
	due := h.e.due(h.clock.Now())
	if len(due) != 10 {
		t.Fatalf("%d due, want 10 offline calls", len(due))
	}
	h.e.applySim(h.simWith(15, 5))
	for _, task := range due {
		if err := h.e.execute(t.Context(), h.api, task); err != nil {
			t.Fatal(err)
		}
	}
	h.until(h.model() + time.Minute)
	h.checkState(15)
	// The operators that came back went offline first: the router returned
	// their conversations before they went online again.
	for i := 11; i <= 15; i++ {
		var got []string
		for _, c := range h.api.callsOf("") {
			if c.op == operatorID(i) {
				got = append(got, c.action)
			}
		}
		want := []string{actionOnline, actionOffline, actionPut, actionOnline}
		if !slices.Equal(got, want) {
			t.Errorf("%s calls %v, want %v", operatorID(i), got, want)
		}
	}
	for _, c := range h.api.callsOf(actionPut) {
		if c.capacity != 5 {
			t.Errorf("%s put with %d, want 5", c.op, c.capacity)
		}
	}
	if n := len(h.api.callsOf(actionPut)); n != 15 {
		t.Errorf("%d puts, want 15", n)
	}
}

// TestChangeComeBackWaitsForOffline: an operator that comes back while its
// offline call gets no answer goes online only after that call had an
// answer.
func TestChangeComeBackWaitsForOffline(t *testing.T) {
	h := changeHarness(t, Spread{{Lang: "en", Count: 1}}, 11)
	down := true
	h.api.answer = func(c call) (opapi.Result, error) {
		if down && c.action == actionOffline {
			return "", errNoAnswer
		}
		return "", nil
	}
	h.apply(h.simWith(10, 4))
	h.apply(h.simWith(11, 4))
	h.until(h.model() + 5*time.Second)
	if n := len(h.api.callsOf(actionOnline)); n != 11 {
		t.Errorf("%d online calls while the offline call fails, want only the 11 of the start", n)
	}
	down = false
	h.until(h.model() + 10*time.Second)
	// The put may go out while the offline call is retried; the online
	// call only after the offline call had its answer.
	var got []string
	for _, c := range h.api.callsOf("") {
		if c.op == operatorID(11) && (len(got) == 0 || got[len(got)-1] != c.action) {
			got = append(got, c.action)
		}
	}
	want := []string{actionOnline, actionOffline, actionPut, actionOffline, actionOnline}
	if !slices.Equal(got, want) {
		t.Errorf("calls of sim-op-0011 %v, want %v", got, want)
	}
	h.checkState(11)
}

// TestChangeInOfflinePart: an operator put aside in the offline part of its
// shift does not come online at the end of that part. Brought back, it
// keeps the rhythm of its own cycle: online when its offline part ends.
func TestChangeInOfflinePart(t *testing.T) {
	b := testBehaviour()
	b.Languages = Spread{{Lang: "en", Count: 1}}
	b.Operators = 10
	b.Capacity = 4
	b.ShiftOnline, b.ShiftOffline = time.Hour, time.Hour
	h := newHarness(t, b, 1, 10)
	h.e.start()
	h.until(0)
	// The highest number in its offline part at some point is the one put
	// aside by 9 operators.
	op := h.e.ops[operatorID(10)]
	cycle := 2 * time.Hour
	// Move to the middle of its offline part: position 1.5h of its cycle.
	at := ((3*time.Hour/2-op.phase)%cycle + cycle) % cycle
	h.until(at)
	// No call yet also means offline: a new operator is offline.
	if h.presence()[op.id] == actionOnline {
		t.Fatalf("%s is not in its offline part at %s", op.id, at)
	}
	h.apply(h.simWith(9, 4))
	// Two hours later it is again in the middle of its offline part; on the
	// way it passed an online part.
	back := at + 4*time.Hour
	h.until(back)
	if got := h.presence()[op.id]; got != actionOffline {
		t.Errorf("%s last %s, want offline while put aside", op.id, got)
	}
	if n := len(h.api.callsOf(actionOnline)); n == 0 || slices.ContainsFunc(h.api.callsOf(actionOnline),
		func(c call) bool { return c.op == op.id && c.model > at }) {
		t.Errorf("%s went online while put aside", op.id)
	}
	before := len(h.api.callsOf(""))
	h.apply(h.simWith(10, 4))
	h.until(back + 20*time.Minute)
	var got []string
	for _, c := range h.api.callsOf("")[before:] {
		if c.op == op.id {
			got = append(got, c.action)
		}
	}
	if !slices.Equal(got, []string{actionPut}) {
		t.Errorf("calls %v in the offline part, want only the put", got)
	}
	h.until(back + 31*time.Minute)
	var online []call
	for _, c := range h.api.callsOf(actionOnline) {
		if c.op == op.id && c.model > back {
			online = append(online, c)
		}
	}
	// The fake API rounds call times to the millisecond.
	if len(online) != 1 || online[0].model != (back+30*time.Minute).Round(time.Millisecond) {
		t.Errorf("online calls after the return %v, want one at %s", online, back+30*time.Minute)
	}
}

// TestChangeTimes: a new handling median applies to accepts after the
// change and a new resolve median to replies after the change; planned
// steps keep their times. The draws are the same as without a change.
func TestChangeTimes(t *testing.T) {
	type times struct{ reply1, resolve1, reply2, resolve2 time.Duration }
	run := func(change func(h *harness, stage int)) times {
		h := changeHarness(t, Spread{{Lang: "en", Count: 1}}, 10)
		var out times
		find := func(action, offer string) time.Duration {
			for {
				for _, c := range h.api.callsOf(action) {
					if c.offer == offer {
						return c.model
					}
				}
				h.until(h.model() + time.Second)
			}
		}
		h.send(event.TypeOffered, customerID(1), op1, offerID(1))
		h.until(10 * time.Second)
		h.send(event.TypeAccepted, customerID(1), op1, offerID(1))
		change(h, 1) // reply 1 planned, its resolve not yet
		out.reply1 = find(actionReply, offerID(1))
		change(h, 2) // resolve 1 planned
		out.resolve1 = find(actionResolve, offerID(1))
		h.send(event.TypeOffered, customerID(2), op1, offerID(2))
		h.until(h.model() + 10*time.Second)
		h.send(event.TypeAccepted, customerID(2), op1, offerID(2))
		accepted2 := h.model()
		out.reply2 = find(actionReply, offerID(2)) - accepted2
		reply2 := find(actionReply, offerID(2))
		out.resolve2 = find(actionResolve, offerID(2)) - reply2
		return out
	}
	base := run(func(*harness, int) {})
	changed := run(func(h *harness, stage int) {
		v := h.e.inEffect()
		v.Capacity = 4
		switch stage {
		case 1:
			v.ResolveMedianUS = (30 * time.Minute).Microseconds()
			v.HandlingMedianUS = (10 * time.Minute).Microseconds()
		case 2:
			v.ResolveMedianUS = (10 * time.Second).Microseconds()
		}
		h.apply(v)
	})
	if changed.reply1 != base.reply1 {
		t.Errorf("planned reply moved from %s to %s", base.reply1, changed.reply1)
	}
	// Resolve 1 was planned after the first change (30m) and keeps that
	// time after the second one.
	ratio := func(a, b time.Duration) float64 { return float64(a) / float64(b) }
	if r := ratio(changed.resolve1-changed.reply1, base.resolve1-base.reply1); r < 9.99 || r > 10.01 {
		t.Errorf("resolve 1 delay ratio %.3f, want 10 (30m / 3m)", r)
	}
	if r := ratio(changed.reply2, base.reply2); r < 13.3 || r > 13.4 {
		t.Errorf("reply 2 delay ratio %.3f, want 13.33 (10m / 45s)", r)
	}
	if r := ratio(changed.resolve2, base.resolve2); r < 0.0554 || r > 0.0558 {
		t.Errorf("resolve 2 delay ratio %.4f, want 0.0556 (10s / 3m)", r)
	}
}

// TestChangeMissedShare: a new share applies to offers read after the
// change; an accept already planned stays.
func TestChangeMissedShare(t *testing.T) {
	h := changeHarness(t, Spread{{Lang: "en", Count: 1}}, 10)
	const n = 2000
	for i := range 100 {
		h.send(event.TypeOffered, customerID(i), op1, offerID(i))
	}
	v := h.e.inEffect()
	v.Capacity = 4
	v.MissedOfferShare = 0.5
	h.apply(v)
	for i := 100; i < 100+n; i++ {
		h.send(event.TypeOffered, customerID(i), op1, offerID(i))
	}
	h.until(h.model() + 10*time.Second)
	accepted := map[string]bool{}
	for _, c := range h.api.callsOf(actionAccept) {
		accepted[c.offer] = true
	}
	for i := range 100 {
		if !accepted[offerID(i)] {
			t.Fatalf("offer %d from before the change was missed", i)
		}
	}
	missed := n + 100 - len(accepted)
	if share := float64(missed) / n; share < 0.45 || share > 0.55 {
		t.Errorf("missed share %.3f after the change, want about 0.5", share)
	}
}

// TestChangeCountNotScalable: a count below the number of languages keeps
// the count in effect and applies the other values.
func TestChangeCountNotScalable(t *testing.T) {
	h := changeHarness(t, defaultSpread, 10)
	v := h.simWith(5, 9)
	r := h.apply(v)
	if r.countErr == nil {
		t.Fatal("no error for 5 operators of 7 languages")
	}
	if got := h.e.inEffect(); got.Operators != 10 || got.Capacity != 9 {
		t.Errorf("in effect %+v, want 10 operators with capacity 9", got)
	}
	h.checkState(10)
}
