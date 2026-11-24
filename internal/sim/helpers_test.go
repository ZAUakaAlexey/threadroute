package sim

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// fakeClock is a manual clock. After channels fire when Set reaches them.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []waiter
	// added is signalled when a waiter is added.
	added *sync.Cond
}

type waiter struct {
	at time.Time
	ch chan time.Time
}

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newFakeClock() *fakeClock {
	c := &fakeClock{now: epoch}
	c.added = sync.NewCond(&c.mu)
	return c
}

// waitWaiters blocks until at least n waiters wait for a later time, so a
// test moves the clock only once the code under test is waiting.
func (c *fakeClock) waitWaiters(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.waiters) < n {
		c.added.Wait()
	}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	at := c.now.Add(d)
	if !at.After(c.now) {
		ch <- c.now
		return ch
	}
	c.waiters = append(c.waiters, waiter{at: at, ch: ch})
	c.added.Broadcast()
	return ch
}

// Set moves the clock to t, never back, and fires the waiters that are due.
func (c *fakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.After(c.now) {
		c.now = t
	}
	kept := c.waiters[:0]
	for _, w := range c.waiters {
		if w.at.After(c.now) {
			kept = append(kept, w)
			continue
		}
		w.ch <- c.now
	}
	c.waiters = kept
}

func (c *fakeClock) Advance(d time.Duration) { c.Set(c.Now().Add(d)) }

// call is one call to the fake API.
type call struct {
	action string
	op     string
	offer  string
	// capacity and langs are the capacity and the languages of a put.
	capacity int
	langs    string
	// model is the model time of the call, rounded to the millisecond.
	model time.Duration
}

func (c call) String() string {
	return fmt.Sprintf("%s %s %s @%s", c.action, c.op, c.offer, c.model)
}

// fakeAPI records calls and answers with answer, or "done" when answer is
// nil or returns a zero result and no error.
type fakeAPI struct {
	mu     sync.Mutex
	clock  Clock
	tl     *timeline
	calls  []call
	answer func(c call) (opapi.Result, error)
	// settings is the answer of Settings.
	settings opapi.Settings
	// settingsErr is returned by Settings when set.
	settingsErr error
	// settingsFn answers Settings when set, instead of the two fields.
	settingsFn func() (opapi.Settings, error)
	// simDefaults answers PutSimDefaults. nil answers CodeSettingsOff, as
	// a router from before decision 0014.
	simDefaults func(req opapi.SimDefaultsRequest) (opapi.SimDefaultsResponse, error)
	// reports are the bodies of PutSimDefaults.
	reports []opapi.SimSettings
	// anyOperator makes Offline answer for an operator that was never put.
	// Without it such a call gets CodeOperatorNotFound, as in the router.
	anyOperator bool
	// known are the operators that were put.
	known map[string]bool
}

func (a *fakeAPI) record(action, op, offer string) (opapi.Result, error) {
	return a.recordCall(call{action: action, op: op, offer: offer})
}

func (a *fakeAPI) recordCall(c call) (opapi.Result, error) {
	a.mu.Lock()
	if a.tl != nil {
		c.model = a.tl.model(a.clock.Now()).Round(time.Millisecond)
	}
	a.calls = append(a.calls, c)
	answer := a.answer
	unknown := c.action == actionOffline && !a.anyOperator && !a.known[c.op]
	a.mu.Unlock()
	if unknown {
		return "", apiError(opapi.CodeOperatorNotFound)
	}
	if answer != nil {
		if res, err := answer(c); res != "" || err != nil {
			return res, err
		}
	}
	return opapi.ResultDone, nil
}

func (a *fakeAPI) Settings(context.Context) (opapi.Settings, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.settingsFn != nil {
		return a.settingsFn()
	}
	if a.settingsErr != nil {
		return opapi.Settings{}, a.settingsErr
	}
	return a.settings, nil
}

// setSettings changes the answer of Settings.
func (a *fakeAPI) setSettings(s opapi.Settings, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.settings, a.settingsErr = s, err
}

func (a *fakeAPI) PutSimDefaults(_ context.Context, req opapi.SimDefaultsRequest) (opapi.SimDefaultsResponse, error) {
	a.mu.Lock()
	a.reports = append(a.reports, req.Sim)
	f := a.simDefaults
	a.mu.Unlock()
	if f == nil {
		return opapi.SimDefaultsResponse{}, settingsError(opapi.CodeSettingsOff)
	}
	return f(req)
}

func (a *fakeAPI) PutOperator(_ context.Context, id string, req opapi.OperatorRequest) (opapi.OperatorResponse, error) {
	res, err := a.recordCall(call{action: actionPut, op: id, capacity: req.Capacity, langs: strings.Join(req.Languages, ",")})
	if err == nil {
		a.mu.Lock()
		if a.known == nil {
			a.known = map[string]bool{}
		}
		a.known[id] = true
		a.mu.Unlock()
	}
	return opapi.OperatorResponse{Result: res}, err
}

func (a *fakeAPI) Online(_ context.Context, id string) (opapi.Result, error) {
	return a.record(actionOnline, id, "")
}

func (a *fakeAPI) Offline(_ context.Context, id string) (opapi.Result, error) {
	return a.record(actionOffline, id, "")
}

func (a *fakeAPI) Accept(_ context.Context, op, offer string) (opapi.Result, error) {
	return a.record(actionAccept, op, offer)
}

func (a *fakeAPI) Reply(_ context.Context, op, offer, _ string) (opapi.Result, error) {
	return a.record(actionReply, op, offer)
}

func (a *fakeAPI) Resolve(_ context.Context, op, offer string) (opapi.Result, error) {
	return a.record(actionResolve, op, offer)
}

// callsOf returns the calls of one action, or every call when action is "".
func (a *fakeAPI) callsOf(action string) []call {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []call
	for _, c := range a.calls {
		if action == "" || c.action == action {
			out = append(out, c)
		}
	}
	return out
}

// errNoAnswer stands for a network error: no answer from the router.
var errNoAnswer = errors.New("dial tcp: connection refused")

func apiError(code opapi.ErrorCode) error {
	return &opapi.Error{Status: code.Status(), ErrorResponse: opapi.ErrorResponse{Code: code, Message: "test"}}
}

func settingsError(code opapi.ErrorCode, fields ...opapi.FieldError) error {
	return &opapi.SettingsError{Status: code.Status(), SettingsErrorResponse: opapi.SettingsErrorResponse{
		ErrorResponse: opapi.ErrorResponse{Code: code, Message: "test"}, Fields: fields,
	}}
}

// harness drives an engine synchronously: events by hand, the fake clock
// moved to each task time, and the calls run inline.
type harness struct {
	t     *testing.T
	clock *fakeClock
	api   *fakeAPI
	e     *engine
	m     *metrics
	reg   *prometheus.Registry
	seq   map[string]uint64
	// fatal is the first error of execute.
	fatal error
}

// testBehaviour has no shifts that matter in a short test (one long online
// part, every operator online at once) and no missed offers.
func testBehaviour() Behaviour {
	b := DefaultBehaviour()
	b.ShiftOnline = 1000 * time.Hour
	b.ShiftOffline = time.Nanosecond
	b.MissedOfferShare = 0
	return b
}

func newHarness(t *testing.T, b Behaviour, speed float64, operators int) *harness {
	t.Helper()
	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	clock := newFakeClock()
	e := newEngine(b, operators, speed, time.Minute, clock, m, slog.New(slog.DiscardHandler))
	api := &fakeAPI{clock: clock, tl: &e.tl, anyOperator: true}
	return &harness{t: t, clock: clock, api: api, e: e, m: m, reg: reg, seq: map[string]uint64{}}
}

// run executes every task that is due at the current time.
func (h *harness) run() {
	for _, t := range h.e.due(h.clock.Now()) {
		if err := h.e.execute(context.Background(), h.api, t); err != nil && h.fatal == nil {
			h.fatal = err
		}
	}
}

// until moves the clock to the model time m, running each task at its time.
func (h *harness) until(m time.Duration) {
	target := h.e.tl.machine(m)
	for h.fatal == nil {
		at, ok := h.e.nextAt()
		if !ok || at.After(target) {
			break
		}
		h.clock.Set(at)
		h.run()
	}
	h.clock.Set(target)
}

// model returns the current model time.
func (h *harness) model() time.Duration { return h.e.tl.model(h.clock.Now()) }

// send gives the engine a conversation event with the next sequence number
// of its customer.
func (h *harness) send(typ event.RoutingType, customer, op, offer string) event.Routing {
	ev := event.Routing{
		Type: typ, CompanyID: "test-co-0001", CustomerID: customer, Session: 1,
		At: h.clock.Now(), Lang: "en", OperatorID: op, OfferID: offer,
	}
	key := string(ev.Key())
	h.seq[key]++
	ev.Seq = h.seq[key]
	h.e.handle(ev)
	return ev
}

func customerID(i int) string { return fmt.Sprintf("test-cu-%06d", i) }
func offerID(i int) string    { return fmt.Sprintf("test-offer-%06d", i) }
