package sim

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// API is the operator API as the simulator uses it. *opclient.Client
// implements it.
type API interface {
	Settings(ctx context.Context) (opapi.Settings, error)
	PutOperator(ctx context.Context, operatorID string, req opapi.OperatorRequest) (opapi.OperatorResponse, error)
	Online(ctx context.Context, operatorID string) (opapi.Result, error)
	Offline(ctx context.Context, operatorID string) (opapi.Result, error)
	Accept(ctx context.Context, operatorID, offerID string) (opapi.Result, error)
	Reply(ctx context.Context, operatorID, offerID, replyID string) (opapi.Result, error)
	Resolve(ctx context.Context, operatorID, offerID string) (opapi.Result, error)
}

// Backoff of a call that got a 503 or no answer, in machine time.
const (
	minBackoff = 100 * time.Millisecond
	maxBackoff = 5 * time.Second
)

// replyID is the reply ID of every reply marker. Each assignment gets one
// reply, so one fixed ID is unique within the assignment, and a repeated
// call after a lost answer gives already_done.
const replyID = "sim-reply-1"

// Pruning of the seen sequence numbers.
const (
	// seenKeep is how long a key stays in the seen map after its last
	// event, in machine time. The router sends an event again only after
	// a restart of its publisher, seconds later, so an hour is plenty.
	seenKeep = time.Hour
	// pruneEvery is the number of events between two prunes.
	pruneEvery = 4096
)

// ErrOutage is wrapped by the error that stops the simulator when the
// router has not answered for longer than the outage limit.
var ErrOutage = errors.New("router unreachable for longer than the outage limit")

type taskKind uint8

const (
	kindAccept taskKind = iota
	kindReply
	kindResolve
	kindPresence
)

// task is one planned action. It sits in the heap until its time, then it
// is one API call. A call that must be repeated goes back into the heap.
type task struct {
	kind taskKind
	// at is the model time of the action, from the start of the engine.
	at time.Duration
	// seq orders tasks with the same time by the order they were planned.
	seq uint64
	// index is the position in the heap, or -1 when the task is not in it.
	index int
	op    *operator
	// offer is set for accept, reply and resolve.
	offer *offer
	// online is the target of a presence task.
	online bool
	// toggle marks a shift change: when it is due it plans the next one
	// at next. A presence retry is not a toggle.
	toggle bool
	next   time.Duration
	// gen is the shift generation of a presence call. A retry of an older
	// generation is dropped.
	gen     uint64
	attempt int
}

func (t *task) action() string {
	switch t.kind {
	case kindAccept:
		return actionAccept
	case kindReply:
		return actionReply
	case kindResolve:
		return actionResolve
	}
	if t.online {
		return actionOnline
	}
	return actionOffline
}

// taskHeap orders tasks by time, then by the order they were planned.
type taskHeap []*task

func (h taskHeap) Len() int { return len(h) }
func (h taskHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}

func (h taskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *taskHeap) Push(x any) {
	t := x.(*task)
	t.index = len(*h)
	*h = append(*h, t)
}

func (h *taskHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	old[n-1] = nil
	t.index = -1
	*h = old[:n-1]
	return t
}

// operator is one simulated operator.
type operator struct {
	id string
	// idx is the 0-based index, used to pick the call worker.
	idx int
	// phase is the position in the shift cycle at the start, drawn once.
	phase time.Duration
	// gen counts the shift changes.
	gen uint64
	// online is the presence the router last confirmed.
	online bool
}

// offer is one offer or assignment of a simulated operator, with its
// planned work.
type offer struct {
	id  string
	key string
	op  *operator
	// task is the planned or running action of the offer, or nil.
	task *task
	// accepted is set when the "accepted" event was seen.
	accepted bool
	// resolveDelay is the model time from the reply to the resolve, drawn
	// at the accept.
	resolveDelay time.Duration
	// done is set when the offer was cancelled or finished. Its tasks are
	// dropped.
	done bool
}

type seenKey struct {
	seq uint64
	at  time.Time
}

// engine holds the state of the simulated operators: the planned actions,
// the offers they work on and the sequence numbers already seen. It never
// blocks on the network: the service runs the calls of due tasks in
// workers and reports each answer back with complete.
//
// Every random draw happens in newEngine (shift phases, in operator order)
// and in handle (accept delay and missed offers on "offered", handling and
// resolve time on "accepted"). So the same seed and the same event order
// give the same decisions, whatever the timing of the API answers.
type engine struct {
	mu          sync.Mutex
	b           Behaviour
	tl          timeline
	clock       Clock
	rng         *rand.Rand
	m           *metrics
	logger      *slog.Logger
	outageLimit time.Duration

	ops    map[string]*operator
	opList []*operator
	offers map[string]*offer
	byKey  map[string]*offer
	seen   map[string]seenKey
	events int

	heap taskHeap
	seq  uint64
	// wake receives a signal when a task was planned, so the scheduler
	// can arm its timer again.
	wake chan struct{}

	// outageSince is the time of the first failed call since the last
	// answer of the router, or zero.
	outageSince time.Time
	online      int
	started     bool
}

// newEngine builds the engine for the operators ids. speed is the speed
// factor of the router.
func newEngine(b Behaviour, ids []string, speed float64, outageLimit time.Duration,
	clock Clock, m *metrics, logger *slog.Logger,
) *engine {
	e := &engine{
		b: b,
		// Model time starts now, so events that arrive before start
		// already have a time.
		tl:          timeline{start: clock.Now(), speed: speed},
		clock:       clock,
		rng:         newRand(b.Seed),
		m:           m,
		logger:      logger,
		outageLimit: outageLimit,
		ops:         make(map[string]*operator, len(ids)),
		offers:      map[string]*offer{},
		byKey:       map[string]*offer{},
		seen:        map[string]seenKey{},
		wake:        make(chan struct{}, 1),
	}
	cycle := b.ShiftOnline + b.ShiftOffline
	for i, id := range ids {
		op := &operator{id: id, idx: i, phase: uniform(e.rng, 0, cycle-1)}
		e.ops[id] = op
		e.opList = append(e.opList, op)
	}
	return e
}

// start plans the first shift change of every operator. Every operator is
// offline at this point. An operator whose phase is in the online part of
// the shift goes online at once and offline when that part ends. The
// others go online when the offline part ends.
func (e *engine) start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started {
		return
	}
	e.started = true
	base := e.tl.model(e.clock.Now())
	on, cycle := e.b.ShiftOnline, e.b.ShiftOnline+e.b.ShiftOffline
	for _, op := range e.opList {
		if op.phase < on {
			e.push(&task{kind: kindPresence, op: op, online: true, toggle: true, at: base, next: base + on - op.phase})
			continue
		}
		at := base + cycle - op.phase
		e.push(&task{kind: kindPresence, op: op, online: true, toggle: true, at: at, next: at + on})
	}
}

// push plans a task.
func (e *engine) push(t *task) *task {
	e.seq++
	t.seq = e.seq
	heap.Push(&e.heap, t)
	e.m.planned.Set(float64(len(e.heap)))
	select {
	case e.wake <- struct{}{}:
	default:
	}
	return t
}

// remove takes a planned task out of the heap. A task that is running is
// not in the heap and is left alone.
func (e *engine) remove(t *task) {
	if t == nil || t.index < 0 {
		return
	}
	heap.Remove(&e.heap, t.index)
	e.m.planned.Set(float64(len(e.heap)))
}

// nextAt returns the machine time of the next planned task.
func (e *engine) nextAt() (time.Time, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.heap) == 0 {
		return time.Time{}, false
	}
	return e.tl.machine(e.heap[0].at), true
}

// due takes every task whose time has come at the machine time now, in
// order. A due shift change also plans the next one.
func (e *engine) due(now time.Time) []*task {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []*task
	for len(e.heap) > 0 && !e.tl.machine(e.heap[0].at).After(now) {
		t := heap.Pop(&e.heap).(*task)
		if t.toggle {
			t.op.gen++
			t.gen = t.op.gen
			dur := e.b.ShiftOffline
			if !t.online {
				dur = e.b.ShiftOnline
			}
			e.push(&task{kind: kindPresence, op: t.op, online: !t.online, toggle: true, at: t.next, next: t.next + dur})
		}
		out = append(out, t)
	}
	e.m.planned.Set(float64(len(e.heap)))
	return out
}

// handle applies one routing event. It skips an event whose sequence
// number was already seen for its key (decision 0010), and reacts only to
// events about the simulated operators, or about a conversation one of
// them holds.
//
// The routing topic holds three key families (decision 0011). Only
// conversation events reach the map of seen sequence numbers, so a company
// key or an operator key can never hide a conversation key.
func (e *engine) handle(ev event.Routing) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.m.events.WithLabelValues(string(ev.Type)).Inc()
	if ev.Type.IsTeam() {
		// Team membership changes nothing for the simulated operators:
		// the router picks who gets an offer.
		return
	}
	if ev.Type.IsOperator() {
		// Operator events are snapshots of presence and settings. For a
		// simulated operator they echo the simulator's own calls, whose
		// answers it already applied, and other operators do not concern
		// it.
		return
	}
	now := e.clock.Now()
	key := string(ev.Key())
	if s, ok := e.seen[key]; ok && ev.Seq <= s.seq {
		e.m.eventsSkipped.Inc()
		return
	}
	e.seen[key] = seenKey{seq: ev.Seq, at: now}
	e.prune(now)

	nowM := e.tl.model(now)
	op := e.ops[ev.OperatorID]
	// An event about another offer of a conversation that a simulated
	// operator holds means the held offer has ended, even if its own end
	// event was missed.
	if prev := e.byKey[key]; prev != nil && ev.OfferID != "" && ev.OfferID != prev.id {
		e.cancel(prev)
	}
	switch ev.Type {
	case event.TypeOffered:
		if op != nil {
			e.offered(key, op, ev.OfferID, nowM)
		}
	case event.TypeAccepted:
		if op != nil {
			e.accepted(key, op, ev.OfferID, nowM)
		}
	case event.TypeOfferExpired, event.TypeResolved, event.TypeEnded:
		if o := e.offers[ev.OfferID]; o != nil {
			e.cancel(o)
		}
		if ev.Type == event.TypeEnded {
			e.cancelKey(key)
		}
	case event.TypeQueued:
		// The conversation is back in a queue, so the offer or assignment
		// it had has ended. When an operator goes offline, an accepted
		// conversation gets only this event, with no operator ID.
		e.cancelKey(key)
	}
}

// offered plans the answer to an offer: an accept after a random delay,
// or, with the missed offer share, nothing at all.
func (e *engine) offered(key string, op *operator, offerID string, nowM time.Duration) {
	if _, ok := e.offers[offerID]; ok {
		return
	}
	// Both values are drawn for every offer, so the draws of later events
	// do not depend on whether this offer was missed.
	miss := e.rng.Float64() < e.b.MissedOfferShare
	delay := uniform(e.rng, e.b.AcceptDelayMin, e.b.AcceptDelayMax)
	e.m.offersSeen.Inc()
	o := e.track(key, op, offerID)
	if miss {
		e.m.offersMissed.Inc()
		return
	}
	o.task = e.push(&task{kind: kindAccept, op: op, offer: o, at: nowM + delay})
}

// accepted plans the reply marker after a random handling time. The
// resolve time is drawn now too and planned when the reply is done.
func (e *engine) accepted(key string, op *operator, offerID string, nowM time.Duration) {
	o := e.offers[offerID]
	if o == nil {
		o = e.track(key, op, offerID)
	}
	if o.accepted {
		return
	}
	o.accepted = true
	handling := logNormal(e.rng, e.b.HandlingMedian)
	o.resolveDelay = logNormal(e.rng, e.b.ResolveMedian)
	// An accept that is still planned (a retry) is not needed any more.
	e.remove(o.task)
	o.task = e.push(&task{kind: kindReply, op: op, offer: o, at: nowM + handling})
}

func (e *engine) track(key string, op *operator, offerID string) *offer {
	o := &offer{id: offerID, key: key, op: op}
	e.offers[offerID] = o
	e.byKey[key] = o
	return o
}

// cancel drops the planned work of an offer because of a routing event.
func (e *engine) cancel(o *offer) {
	if o.done {
		return
	}
	e.m.dropped.Inc()
	e.forget(o)
}

func (e *engine) cancelKey(key string) {
	if o := e.byKey[key]; o != nil {
		e.cancel(o)
	}
}

// forget removes an offer and its planned task.
func (e *engine) forget(o *offer) {
	if o.done {
		return
	}
	o.done = true
	e.remove(o.task)
	delete(e.offers, o.id)
	if e.byKey[o.key] == o {
		delete(e.byKey, o.key)
	}
}

// prune drops seen keys that had no event for seenKeep and whose
// conversation no simulated operator holds. It runs every pruneEvery
// events, so the map does not grow with every conversation of the day.
func (e *engine) prune(now time.Time) {
	e.events++
	if e.events < pruneEvery {
		return
	}
	e.events = 0
	for k, s := range e.seen {
		if now.Sub(s.at) > seenKeep && e.byKey[k] == nil {
			delete(e.seen, k)
		}
	}
}

// call runs the API call of a task. It holds no lock: the fields it reads
// do not change after the task was planned.
func (e *engine) call(ctx context.Context, api API, t *task) (opapi.Result, error) {
	switch t.kind {
	case kindAccept:
		return api.Accept(ctx, t.op.id, t.offer.id)
	case kindReply:
		return api.Reply(ctx, t.op.id, t.offer.id, replyID)
	case kindResolve:
		return api.Resolve(ctx, t.op.id, t.offer.id)
	}
	if t.online {
		return api.Online(ctx, t.op.id)
	}
	return api.Offline(ctx, t.op.id)
}

// execute runs the call of a due task and applies the answer. It returns
// an error that wraps ErrOutage when the router has not answered for
// longer than the outage limit. A call cut short by the end of ctx is
// ignored.
func (e *engine) execute(ctx context.Context, api API, t *task) error {
	res, err := e.call(ctx, api, t)
	if ctx.Err() != nil {
		return nil
	}
	return e.complete(t, res, err)
}

// outcome is how a call ended.
type outcome uint8

const (
	outcomeSuccess outcome = iota
	outcomeRejected
	outcomeRetry
	outcomeFailed
)

// classify sorts an answer: done and already_done are success, a 409 is a
// rejection, a 503 or no answer is retried, and any other error answer is
// a failure that is not retried.
func classify(res opapi.Result, err error) (label string, code opapi.ErrorCode, o outcome) {
	if err == nil {
		return string(res), "", outcomeSuccess
	}
	ae, ok := errors.AsType[*opapi.Error](err)
	if !ok {
		return resultUnavailable, "", outcomeRetry
	}
	switch {
	case ae.Code.Rejected():
		return resultRejected, ae.Code, outcomeRejected
	case ae.Code == opapi.CodeUnavailable:
		return resultUnavailable, ae.Code, outcomeRetry
	}
	return resultFailed, ae.Code, outcomeFailed
}

// complete applies the answer of a call.
func (e *engine) complete(t *task, res opapi.Result, err error) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	label, code, out := classify(res, err)
	action := t.action()
	e.m.calls.WithLabelValues(action, label).Inc()
	now := e.clock.Now()
	if out != outcomeRetry {
		// The router answered.
		e.outageSince = time.Time{}
	}
	switch out {
	case outcomeSuccess:
		e.succeeded(t, now)
	case outcomeRejected:
		// The state moved on: drop what was planned for this offer.
		e.m.rejections.WithLabelValues(action, string(code)).Inc()
		if t.offer != nil {
			e.forget(t.offer)
		}
	case outcomeFailed:
		e.m.failures.WithLabelValues(action, string(code)).Inc()
		e.logger.Warn("operator API call failed, not retried", "action", action, "operator_id", t.op.id, "error", err)
		if t.offer != nil {
			e.forget(t.offer)
		}
	case outcomeRetry:
		if e.outageSince.IsZero() {
			e.outageSince = now
		}
		if failing := now.Sub(e.outageSince); failing >= e.outageLimit {
			return fmt.Errorf("%w: no answer for %s (limit %s): %w",
				ErrOutage, failing.Round(time.Millisecond), e.outageLimit, err)
		}
		if e.stale(t) {
			return nil
		}
		backoff := min(minBackoff<<min(t.attempt, 16), maxBackoff)
		t.attempt++
		t.toggle = false
		t.at = e.tl.model(now) + e.tl.toModel(backoff)
		e.push(t)
		e.m.retries.WithLabelValues(action).Inc()
		if t.attempt == 1 {
			e.logger.Warn("operator API not available, retrying", "action", action, "operator_id", t.op.id, "error", err)
		}
	}
	return nil
}

// stale reports whether a task is no longer wanted: its offer was
// cancelled or moved on to another action, or a newer shift change
// replaced the presence call.
func (e *engine) stale(t *task) bool {
	if t.offer != nil {
		return t.offer.done || t.offer.task != t
	}
	return t.gen != t.op.gen
}

func (e *engine) succeeded(t *task, now time.Time) {
	o := t.offer
	switch t.kind {
	case kindAccept:
		e.m.accepts.Inc()
		if o.task == t {
			o.task = nil
		}
	case kindReply:
		e.m.replies.Inc()
		if !o.done && o.task == t {
			o.task = e.push(&task{kind: kindResolve, op: t.op, offer: o, at: e.tl.model(now) + o.resolveDelay})
		}
	case kindResolve:
		e.m.resolves.Inc()
		e.forget(o)
	case kindPresence:
		if t.op.online != t.online {
			t.op.online = t.online
			if t.online {
				e.online++
			} else {
				e.online--
			}
			e.m.operatorsOnline.Set(float64(e.online))
		}
	}
}

// pending returns the number of planned tasks, for tests.
func (e *engine) pending() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.heap)
}
