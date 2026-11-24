package sim

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ZAUakaAlexey/threadroute/internal/defaults"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// boundSpreads are the spreads the bounds of langOrder are checked with:
// the default, one language, equal weights, one very small weight, and a
// few others.
func boundSpreads(t *testing.T) []Spread {
	t.Helper()
	var out []Spread
	for _, s := range []string{
		defaults.SimLanguages,
		"en:1",
		"en:1,ja:1,de:1,fr:1",
		"en:5000,ja:1",
		"ja:1,en:5000",
		"en:1,ja:3,de:3",
		"en:5,ja:3,de:2",
		"en:2,ja:1",
		"en:1,ja:1,de:1,fr:1,es:1,pt:1,ko:1,it:1,nl:1,pl:1,tr:1,zh:997",
	} {
		sp, err := ParseSpread(s)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, sp)
	}
	return out
}

// randomSpreads returns spreads of 1 to 12 languages with weights from 1 to
// 1000, from a fixed seed.
func randomSpreads(n int) []Spread {
	rng := rand.New(rand.NewPCG(5, 6))
	langs := []string{"en", "ja", "de", "fr", "es", "pt", "ko", "it", "nl", "pl", "tr", "zh"}
	out := make([]Spread, n)
	for i := range out {
		k := 1 + rng.IntN(len(langs))
		for _, l := range langs[:k] {
			w := 1 + rng.IntN(10)
			if rng.IntN(3) == 0 {
				w = 1 + rng.IntN(1000)
			}
			out[i] = append(out[i], LangShare{Lang: l, Count: w})
		}
	}
	return out
}

// checkLangOrder checks the bounds that langOrder states, for every count
// from 1 to upTo, in integers:
//
//   - count < share + 1, that is count*W - n*w < W;
//   - count > share - 1 - F, that is n*w - count*W < W + F*W, where F*W is
//     the sum over the languages of max(0, W - n*w);
//   - from n = k on, every language has an operator;
//   - for n = W, the counts are the spread.
func checkLangOrder(t *testing.T, s Spread, upTo int) {
	t.Helper()
	w := s.total()
	order := s.langOrder(upTo)
	if len(order) != upTo {
		t.Fatalf("%s: %d languages for %d operators", s, len(order), upTo)
	}
	at := map[string]int{}
	for i, l := range s {
		at[l.Lang] = i
	}
	counts := make([]int, len(s))
	for n := 1; n <= upTo; n++ {
		counts[at[order[n-1]]]++
		forced := 0
		for _, l := range s {
			forced += max(0, w-n*l.Count)
		}
		for j, l := range s {
			if over := counts[j]*w - n*l.Count; over >= w {
				t.Fatalf("%s, %d operators: %s has %d, 1 or more above its share %d/%d", s, n, l.Lang, counts[j], n*l.Count, w)
			}
			if under := n*l.Count - counts[j]*w; under >= w+forced {
				t.Fatalf("%s, %d operators: %s has %d, too far below its share %d/%d (forced %d/%d)",
					s, n, l.Lang, counts[j], n*l.Count, w, forced, w)
			}
			if n >= len(s) && counts[j] == 0 {
				t.Fatalf("%s, %d operators: %s has none", s, n, l.Lang)
			}
		}
		if n == w {
			for j, l := range s {
				if counts[j] != l.Count {
					t.Fatalf("%s, %d operators: %s has %d, want exactly %d", s, n, l.Lang, counts[j], l.Count)
				}
			}
		}
	}
}

// TestLangOrderDefault: the default stand has the counts of the spread, the
// first numbers go to every language once, largest first, and the bounds
// hold for every count up to MaxOperators.
func TestLangOrderDefault(t *testing.T) {
	def, err := ParseSpread(defaults.SimLanguages)
	if err != nil {
		t.Fatal(err)
	}
	if got := def.layout(defaults.SimOperators); got.String() != "en:176,ja:6,de:4,fr:4,es:4,pt:3,ko:3" {
		t.Errorf("%d operators: %s, want the spread", defaults.SimOperators, got)
	}
	if got, want := def.langOrder(7), []string{"en", "ja", "de", "fr", "es", "pt", "ko"}; !slices.Equal(got, want) {
		t.Errorf("first 7: %v, want %v", got, want)
	}
	checkLangOrder(t, def, MaxOperators)
}

// TestLangOrderBounds: the bounds hold for other spreads, up to 1000
// operators and up to the sum of the counts.
func TestLangOrderBounds(t *testing.T) {
	for _, s := range append(boundSpreads(t), randomSpreads(300)...) {
		checkLangOrder(t, s, min(max(1000, s.total()), MaxOperators))
	}
}

// TestLangOrderExact: when the count equals the sum of the counts, the
// operators have exactly the counts of the spread.
func TestLangOrderExact(t *testing.T) {
	for _, s := range append(boundSpreads(t), randomSpreads(300)...) {
		if w := s.total(); w <= MaxOperators {
			if got := s.layout(w); !slices.Equal(got, s) {
				t.Errorf("%d operators: %s, want %s", w, got, s)
			}
		}
	}
}

// TestLangOrderPrefix: the languages of the first m numbers are the same
// for every larger count.
func TestLangOrderPrefix(t *testing.T) {
	for _, s := range boundSpreads(t) {
		full := s.langOrder(MaxOperators)
		for _, m := range []int{0, 1, 2, 7, 13, 99, 120, 200, 201, 500, 1234, MaxOperators} {
			if got := s.langOrder(m); !slices.Equal(got, full[:m]) {
				t.Errorf("%s: the first %d differ from the longer order", s, m)
			}
		}
	}
}

// TestSpreadValidate: too few operators for the languages, and counts that
// add up to too much.
func TestSpreadValidate(t *testing.T) {
	def, _ := ParseSpread(defaults.SimLanguages)
	if err := def.Validate(6); err == nil {
		t.Error("7 languages for 6 operators without an error")
	}
	if err := def.Validate(7); err != nil {
		t.Errorf("7 languages for 7 operators: %v", err)
	}
	big := Spread{{Lang: "en", Count: maxSpreadTotal}, {Lang: "ja", Count: 1}}
	if err := big.Validate(10); err == nil {
		t.Error("counts above the maximum without an error")
	}
	if operatorID(1) != "sim-op-0001" || operatorID(MaxOperators) != "sim-op-9999" {
		t.Errorf("IDs %s, %s", operatorID(1), operatorID(MaxOperators))
	}
}

// historyRun is a stand with one fake router that lives across simulated
// restarts of the simulator.
type historyRun struct {
	t     *testing.T
	b     Behaviour
	api   *fakeAPI
	clock *fakeClock
	h     *harness
}

// restart runs the start of the simulator against the router with n
// operators: the puts of setOperators, the offline calls, the search for
// leftovers of an earlier run, then the shifts.
func (r *historyRun) restart(n int) {
	r.t.Helper()
	b := r.b
	b.Operators = n
	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		r.t.Fatal(err)
	}
	svc := &Service{
		cfg: Config{OutageLimit: time.Minute}, api: r.api, clock: r.clock,
		logger: slog.New(slog.DiscardHandler), m: m,
	}
	if err := svc.setOperators(b); err != nil {
		r.t.Fatal(err)
	}
	ctx := r.t.Context()
	for i, id := range svc.ids {
		if _, err := r.api.PutOperator(ctx, id, opapi.OperatorRequest{Languages: svc.langs[i], Capacity: b.Capacity}); err != nil {
			r.t.Fatal(err)
		}
	}
	e := newEngine(b, n, 1, time.Minute, r.clock, m, slog.New(slog.DiscardHandler))
	r.api.mu.Lock()
	r.api.tl = &e.tl
	r.api.mu.Unlock()
	for _, id := range svc.ids {
		if _, err := r.api.Offline(ctx, id); err != nil {
			r.t.Fatal(err)
		}
	}
	known := 0
	r.api.mu.Lock()
	for i := n + 1; r.api.known[operatorID(i)]; i++ {
		known++
	}
	r.api.mu.Unlock()
	last, err := svc.offlineLeftovers(ctx, n+1)
	if err != nil {
		r.t.Fatal(err)
	}
	e.addAside(last)
	if last != n+known {
		r.t.Errorf("restart at %d: leftovers up to %d, want %d", n, last, n+known)
	}
	if got := testutil.ToFloat64(m.operatorsAside); got != float64(known) {
		r.t.Errorf("restart at %d: put aside gauge %v, want %d", n, got, known)
	}
	if got := testutil.ToFloat64(m.calls.WithLabelValues(actionOffline, resultFailed)); got != 0 {
		r.t.Errorf("restart at %d: %v failed offline calls, want 0", n, got)
	}
	e.start()
	r.h = &harness{t: r.t, clock: r.clock, api: r.api, e: e, m: m, reg: reg, seq: map[string]uint64{}}
	r.h.until(r.h.model() + time.Minute)
}

func newHistoryRun(t *testing.T, spread Spread, n int) *historyRun {
	b := testBehaviour()
	b.Languages = spread
	b.Capacity = 4
	clock := newFakeClock()
	r := &historyRun{t: t, b: b, api: &fakeAPI{clock: clock}, clock: clock}
	r.restart(n)
	return r
}

// routerState returns the languages of the last put of every operator and
// the operators that the router has online, as the fake router saw them.
func (r *historyRun) routerState() (langs map[string]string, online []string) {
	langs = map[string]string{}
	for _, c := range r.api.callsOf(actionPut) {
		langs[c.op] = c.langs
	}
	for id, p := range r.h.presence() {
		if p == actionOnline {
			online = append(online, id)
		}
	}
	slices.Sort(online)
	return langs, online
}

// activeIDs returns the active operators of the engine, in number order.
func (r *historyRun) activeIDs() []string {
	var out []string
	for _, op := range r.h.e.opList {
		if op.active {
			out = append(out, op.id)
		}
	}
	return out
}

// TestLanguagesIndependentOfHistory: any row of count changes, with
// restarts in between, ends with the same number to language map and the
// same active operators as a fresh start at the final count.
func TestLanguagesIndependentOfHistory(t *testing.T) {
	spreads := boundSpreads(t)
	for seed := range uint64(12) {
		rng := rand.New(rand.NewPCG(seed, 99))
		spread := spreads[int(seed)%len(spreads)]
		k := len(spread)
		count := func() int { return k + rng.IntN(60) }
		run := newHistoryRun(t, spread, count())
		final := run.h.e.b.Operators
		var steps []string
		for range 25 {
			n := count()
			if rng.IntN(3) == 0 {
				run.restart(n)
				steps = append(steps, fmt.Sprintf("restart %d", n))
			} else {
				run.h.apply(run.h.simWith(n, 4))
				run.h.until(run.h.model() + time.Minute)
				steps = append(steps, fmt.Sprintf("%d", n))
			}
			final = n
			run.h.checkState(n)
		}
		fresh := newHistoryRun(t, spread, final)
		name := fmt.Sprintf("%s after %s", spread, strings.Join(steps, ", "))

		gotLangs, gotOnline := run.routerState()
		wantLangs, wantOnline := fresh.routerState()
		if !slices.Equal(gotOnline, wantOnline) {
			t.Errorf("%s: online %v, a fresh start %v", name, gotOnline, wantOnline)
		}
		if got, want := run.activeIDs(), fresh.activeIDs(); !slices.Equal(got, want) {
			t.Errorf("%s: active %v, a fresh start %v", name, got, want)
		}
		order := spread.langOrder(MaxOperators)
		for id, l := range gotLangs {
			var num int
			if _, err := fmt.Sscanf(id, "sim-op-%04d", &num); err != nil {
				t.Fatal(err)
			}
			if w, ok := wantLangs[id]; ok && w != l {
				t.Errorf("%s: %s has %s, a fresh start %s", name, id, l, w)
			}
			if l != order[num-1]+",und" {
				t.Errorf("%s: %s was last put with %s, want %s", name, id, l, order[num-1])
			}
		}
		// Every put of the whole history carried the language of the number.
		for _, c := range run.api.callsOf(actionPut) {
			var num int
			if _, err := fmt.Sscanf(c.op, "sim-op-%04d", &num); err != nil {
				t.Fatal(err)
			}
			if c.langs != order[num-1]+",und" {
				t.Fatalf("%s: %s put with %s, want %s", name, c.op, c.langs, order[num-1])
			}
		}
		// The shift phases depend on the number only.
		for _, op := range fresh.h.e.opList {
			if got := run.h.e.ops[op.id]; got == nil || got.phase != op.phase {
				t.Errorf("%s: %s has another phase than in a fresh start", name, op.id)
			}
		}
	}
}
