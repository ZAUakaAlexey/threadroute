package sim

import (
	"fmt"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// Changes of the sim part of the stand settings while the engine runs
// (decision 0014, point 3A). A change only updates the state of the engine
// and plans tasks: every API call it needs goes through the heap and the
// workers like any other call, so the engine lock is held for a short scan
// of the operators and never for a network call. The calls of one operator
// stay in order because they go to the same worker.
//
// A change is computed from the state of the engine, not from what the
// router confirmed. So a second change that comes while the calls of the
// first one still run is computed from the result the first one aimed at,
// and the generations of the operators drop the calls that the second one
// replaced. The final state is the one of the newest set.

// changeResult says what a change did, for the log.
type changeResult struct {
	// putAside, back and created count operators.
	putAside, back, created int
	// capacityPuts counts operators sent again with the new capacity.
	capacityPuts int
	// countErr is set when the operator count could not be applied: the
	// language spread cannot be scaled to it. The count in effect stays.
	countErr error
	// full is set when an operator could not be created because the
	// numbers ran out (MaxOperators).
	full bool
}

// applySim applies a sim part of the stand settings.
//
//   - Times and the missed share apply to the steps planned from now on.
//   - The language spread of the configuration, scaled to the new count,
//     gives a target for each language. In a language above its target,
//     the operators with the highest numbers are put aside. In a language
//     below it, operators put aside come back first, lowest numbers first,
//     then new operators are created with the next free numbers.
//   - A new capacity is sent to every active operator. Operators that come
//     back or are created get it with their own put.
func (e *engine) applySim(s opapi.SimSettings) changeResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.tl.model(e.clock.Now())
	var r changeResult
	e.b.HandlingMedian = s.HandlingMedian()
	e.b.ResolveMedian = s.ResolveMedian()
	e.b.MissedOfferShare = s.MissedOfferShare
	capChanged := s.Capacity != e.b.Capacity
	e.b.Capacity = s.Capacity

	// joined holds the operators that get a put of their own below, aside
	// the ones put aside now.
	joined := map[*operator]bool{}
	leaving := map[*operator]bool{}
	targets, err := e.b.Languages.Scale(s.Operators)
	if err != nil {
		r.countErr = fmt.Errorf("languages %s cannot be scaled to %d operators: %w", e.b.Languages, s.Operators, err)
	} else {
		e.b.Operators = s.Operators
		for _, l := range targets {
			var active, aside []*operator
			for _, op := range e.opList {
				switch {
				case op.lang != l.Lang:
				case op.active:
					active = append(active, op)
				default:
					aside = append(aside, op)
				}
			}
			if len(active) > l.Count {
				for _, op := range active[l.Count:] {
					e.putAside(op, now)
					leaving[op] = true
					r.putAside++
				}
				continue
			}
			need := l.Count - len(active)
			for _, op := range aside {
				if need == 0 {
					break
				}
				e.bringBack(op, now)
				joined[op] = true
				r.back++
				need--
			}
			for ; need > 0; need-- {
				op := e.create(l.Lang, now)
				if op == nil {
					r.full = true
					break
				}
				joined[op] = true
				r.created++
			}
		}
	}
	if len(leaving) > 0 {
		// One pass over the offers for every operator put aside.
		for _, o := range e.offers {
			if leaving[o.op] {
				e.forget(o)
			}
		}
	}
	if capChanged {
		for _, op := range e.opList {
			if op.active && !joined[op] {
				e.planPut(op, now)
				r.capacityPuts++
			}
		}
	}
	e.countOperators()
	return r
}

// putAside takes an operator out of the shift cycle and puts it offline.
// The router returns the conversations it holds to the queues (decision
// 0010), so the caller drops every offer of the operator in the same lock:
// no step is sent for a conversation the operator no longer holds. The
// caller holds e.mu.
func (e *engine) putAside(op *operator, now time.Duration) {
	op.active = false
	op.starting = false
	op.waitPut = false
	// Drops every presence call planned or waiting for a worker.
	op.gen++
	e.remove(op.shift)
	op.shift = nil
	if !op.registered {
		// The router does not have it yet: a put that still runs creates
		// it offline, holding nothing.
		return
	}
	op.waitOffline = true
	e.push(&task{kind: kindPresence, op: op, gen: op.gen, at: now})
}

// bringBack returns an operator that was put aside to the shift cycle. It
// is sent again first, so it has the capacity in effect. When its offline
// call has no final answer yet, it waits for it, and the generation is
// kept so that call is not dropped. The caller holds e.mu.
func (e *engine) bringBack(op *operator, now time.Duration) {
	op.active = true
	op.starting = true
	e.planPut(op, now)
}

// create makes a new operator with the next free number for a language and
// plans its put. Its phase is drawn now, and it takes its place in the
// shift cycle when the put succeeds. It returns nil when the numbers ran
// out. The caller holds e.mu.
func (e *engine) create(lang string, now time.Duration) *operator {
	if e.nextNum > MaxOperators {
		return nil
	}
	num := e.nextNum
	e.nextNum++
	cycle := e.b.ShiftOnline + e.b.ShiftOffline
	op := &operator{
		id: operatorID(num), idx: num - 1, lang: lang, langs: []string{lang, event.LangUndetermined},
		phase: uniform(e.rng, 0, cycle-1), base: now, active: true, starting: true,
	}
	e.ops[op.id] = op
	e.opList = append(e.opList, op)
	e.planPut(op, now)
	return op
}

// planPut plans a put of the operator with the capacity in effect. A put
// planned before it is dropped. The caller holds e.mu.
func (e *engine) planPut(op *operator, now time.Duration) {
	op.putGen++
	if op.starting {
		op.waitPut = true
	}
	e.push(&task{kind: kindPut, op: op, capacity: e.b.Capacity, putGen: op.putGen, at: now})
}

// maybeStart plans the shift of an operator that joins the cycle once it
// waits for nothing. now is machine time. The caller holds e.mu.
func (e *engine) maybeStart(op *operator, now time.Time) {
	if !op.active || !op.starting || op.waitPut || op.waitOffline {
		return
	}
	op.starting = false
	e.planShift(op, e.tl.model(now))
}

// countOperators sets the gauges of active operators and operators put
// aside. The caller holds e.mu.
func (e *engine) countOperators() {
	active := 0
	for _, op := range e.opList {
		if op.active {
			active++
		}
	}
	e.m.operatorsActive.Set(float64(active))
	e.m.operatorsAside.Set(float64(len(e.opList) - active))
}

// inEffect returns the sim values the engine runs with. Capacity is 0 when
// the operators take the router default.
func (e *engine) inEffect() opapi.SimSettings {
	e.mu.Lock()
	defer e.mu.Unlock()
	return simOf(e.b, 0)
}
