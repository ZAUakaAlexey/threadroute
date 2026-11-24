package gateway

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
)

// hub fans the changes of the view out to the clients (decision 0011,
// point 4B). It keeps no queue of messages: each client has a set of
// changed keys, and on every tick the client sends the current state of
// those keys. A client that is still writing when the tick comes simply
// gets one larger update later.
type hub struct {
	view *View
	ps   *Pseudonyms
	m    *metrics

	mu      sync.RWMutex
	clients map[*client]struct{}
	status  gwapi.Status

	// totals are computed once per tick and shared by every client.
	totals atomic.Pointer[gwapi.Totals]
}

func newHub(view *View, ps *Pseudonyms, m *metrics) *hub {
	h := &hub{view: view, ps: ps, m: m, clients: map[*client]struct{}{},
		status: gwapi.Status{Stale: true, Reason: gwapi.StaleCatchingUp}}
	h.totals.Store(&gwapi.Totals{})
	return h
}

// add registers a client; it gets every change from now on.
func (h *hub) add(c *client) gwapi.Status {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c] = struct{}{}
	h.m.connections.WithLabelValues(string(c.aud.View), string(c.aud.Level)).Inc()
	return h.status
}

func (h *hub) remove(c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		h.m.connections.WithLabelValues(string(c.aud.View), string(c.aud.Level)).Dec()
	}
}

func (h *hub) each(fn func(c *client)) {
	h.mu.RLock()
	list := make([]*client, 0, len(h.clients))
	for c := range h.clients {
		list = append(list, c)
	}
	h.mu.RUnlock()
	for _, c := range list {
		fn(c)
	}
}

func (h *hub) count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// publish hands the changes of one batch to every client.
func (h *hub) publish(ch *changes) {
	if ch.empty() {
		return
	}
	h.each(func(c *client) { c.markChanges(ch) })
}

// tick computes the totals once and signals every client.
func (h *hub) tick(now time.Time) {
	start := time.Now()
	rotated := h.ps.Rotate(now)
	var t gwapi.Totals
	h.view.Read(func(rd Reader) { t = rd.Totals(now) })
	h.totals.Store(&t)
	h.each(func(c *client) {
		if rotated && c.aud.Level != gwapi.LevelFull {
			// Every pseudonym changed: a new snapshot, which ends every
			// watch.
			c.requestSnapshot()
		}
		select {
		case c.tickCh <- struct{}{}:
		default:
			h.m.slowTicks.Inc()
		}
	})
	h.m.tickDuration.Observe(time.Since(start).Seconds())
}

func (h *hub) currentTotals() gwapi.Totals { return *h.totals.Load() }

// setStatus stores the status and sends it to every client on a change.
func (h *hub) setStatus(st gwapi.Status) {
	h.mu.Lock()
	if h.status == st {
		h.mu.Unlock()
		return
	}
	h.status = st
	h.mu.Unlock()
	h.each(func(c *client) { c.sendStatus(st) })
}

func (h *hub) currentStatus() gwapi.Status {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.status
}

// closeAll closes every client with code.
func (h *hub) closeAll(code gwapi.CloseCode, reason string) {
	h.each(func(c *client) { c.closeWith(code, reason) })
}
