// Package gateway is the gateway service (decisions 0011, 0012 and 0013):
// it connects browsers to the pipeline over WebSocket with the protocol of
// internal/gwapi, and serves the public snapshot.
//
// # Parts
//
//   - View: the latest state of every key of the conversations and routing
//     topics, built only from the topics (decision 0011, point 1A).
//   - source: reads every partition of the two topics from the beginning,
//     without a consumer group, applies the records to the View, and
//     decides whether the data is fresh.
//   - hub and client: the WebSocket sessions. No queue of state messages
//     per client: a set of changed keys, sent as current state on every
//     tick (point 4B).
//   - registry: the human operators this gateway created through the
//     router API (decision 0012, point 4B).
//   - SiteChecker: the token check with the site API (decision 0012, point
//     1B).
//
// # Privacy
//
// Every server message is validated by gwapi.EncodeServer for the level
// of its connection before it is sent, so a demo connection never gets
// post text, a conversation key or an author ID. Post text is read from
// Redis when it is sent and is not kept. Logs and metric labels never hold
// text, tokens, keys or author IDs.
package gateway

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZAUakaAlexey/threadroute/internal/defaults"
	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// Deps are the dependencies of a Gateway.
type Deps struct {
	Config Config
	// View is a new empty view when nil.
	View *View
	API  OperatorAPI
	Auth TokenChecker
	Text TextSource
	// Clock is SystemClock when nil.
	Clock Clock
	// Logger may be nil.
	Logger *slog.Logger
	// Registerer gets the metrics. Required.
	Registerer prometheus.Registerer
}

// Gateway serves WebSocket sessions on a View.
type Gateway struct {
	cfg    Config
	view   *View
	api    OperatorAPI
	auth   TokenChecker
	text   TextSource
	clock  Clock
	logger *slog.Logger
	m      *metrics
	ps     *Pseudonyms
	hub    *hub
	ops    *registry
	limits *connLimits

	settings atomic.Pointer[opapi.Settings]
	// conns owns the goroutine of every connection.
	conns sync.WaitGroup
}

// New builds a Gateway.
func New(d Deps) (*Gateway, error) {
	if d.API == nil || d.Auth == nil || d.Text == nil || d.Registerer == nil {
		return nil, errors.New("new gateway: API, Auth, Text and Registerer are required")
	}
	if err := d.Config.Validate(); err != nil {
		return nil, err
	}
	if d.Clock == nil {
		d.Clock = SystemClock{}
	}
	if d.Logger == nil {
		d.Logger = slog.New(slog.DiscardHandler)
	}
	if d.View == nil {
		d.View = NewView(d.Config.ForgetAfter)
	}
	m, err := newMetrics(d.Registerer)
	if err != nil {
		return nil, err
	}
	ps := NewPseudonyms(d.Clock.Now())
	g := &Gateway{
		cfg: d.Config, view: d.View, api: d.API, auth: d.Auth, text: d.Text, clock: d.Clock,
		logger: d.Logger, m: m, ps: ps, hub: newHub(d.View, ps, m),
		limits: newConnLimits(d.Config.MaxConnections, d.Config.MaxConnsPerAddr),
	}
	g.ops = newRegistry(d.API, d.Clock, d.Config.OfflineGrace, d.Config.DemoOperators, d.Logger, m)
	return g, nil
}

// SetSettings stores the router settings.
func (g *Gateway) SetSettings(s opapi.Settings) { g.settings.Store(&s) }

// timing returns what the view needs of the router settings.
func (g *Gateway) timing() Timing {
	s := g.settings.Load()
	if s == nil {
		return Timing{SpeedFactor: 1, FirstResponseTarget: defaults.FirstResponseTarget}
	}
	t := Timing{SpeedFactor: s.SpeedFactor, FirstResponseTarget: time.Duration(s.FirstResponseTargetUS) * time.Microsecond}
	if t.FirstResponseTarget <= 0 {
		t.FirstResponseTarget = defaults.FirstResponseTarget
	}
	return t
}

// welcome returns the welcome of a connection, or false while the router
// settings are not known.
func (g *Gateway) welcome(aud gwapi.Audience) (gwapi.Welcome, bool) {
	s := g.settings.Load()
	if s == nil {
		return gwapi.Welcome{}, false
	}
	w := gwapi.Welcome{
		Version: gwapi.ProtocolVersion, Level: aud.Level, View: aud.View,
		ServerTimeUS: g.clock.Now().UnixMicro(), SpeedFactor: s.SpeedFactor,
		AcceptTimeoutUS: s.AcceptTimeoutUS, AssignmentTargetUS: s.AssignmentTargetUS,
		FirstResponseTargetUS: s.FirstResponseTargetUS,
		TickMS:                int(g.cfg.Tick.Milliseconds()), Limits: gwapi.DefaultLimits(),
	}
	// A router from before stage 4 does not send the targets.
	if w.AssignmentTargetUS <= 0 {
		w.AssignmentTargetUS = defaults.AssignmentTarget.Microseconds()
	}
	if w.FirstResponseTargetUS <= 0 {
		w.FirstResponseTargetUS = defaults.FirstResponseTarget.Microseconds()
	}
	return w, true
}

// Tick runs one tick of the fan-out. Run calls it every Config.Tick;
// tests call it with a fake time.
func (g *Gateway) Tick(now time.Time) { g.hub.tick(now) }

// Publish hands the changes of the view to the clients.
func (g *Gateway) Publish() { g.hub.publish(g.view.takeChanges()) }

// SetStatus sets the freshness of the data.
func (g *Gateway) SetStatus(st gwapi.Status) {
	g.hub.setStatus(st)
	for _, r := range []string{"fresh", string(gwapi.StaleCatchingUp), string(gwapi.StaleSourceUnavailable), string(gwapi.StaleLagging)} {
		v := 0.0
		if (st.Stale && r == string(st.Reason)) || (!st.Stale && r == "fresh") {
			v = 1
		}
		g.m.stale.WithLabelValues(r).Set(v)
	}
}

// Status returns the current freshness.
func (g *Gateway) Status() gwapi.Status { return g.hub.currentStatus() }

// Serve runs one WebSocket connection until it ends. ctx ends every
// connection with "going away". addr is the client address for the
// limits; Serve releases it at the end.
func (g *Gateway) Serve(ctx context.Context, conn Conn, addr string) {
	defer g.limits.release(addr)
	ok, code, reason := g.limits.check(addr)
	if !ok {
		g.m.closed.WithLabelValues(reason).Inc()
		_ = conn.Close(code)
		return
	}
	hello, code, reason := g.readHello(ctx, conn)
	if code != 0 {
		g.m.closed.WithLabelValues(reason).Inc()
		_ = conn.Close(code)
		return
	}

	ident, outcome := g.auth.Check(ctx, hello.Token)
	g.m.siteCalls.WithLabelValues(string(outcome)).Inc()
	aud := gwapi.Audience{Level: ident.Level, View: hello.View}
	welcome, ok := g.welcome(aud)
	if !ok {
		g.m.closed.WithLabelValues(reasonTryLater).Inc()
		_ = conn.Close(gwapi.CloseTryAgainLater)
		return
	}
	c := g.newClient(conn, aud, ident)

	if hello.View == gwapi.ViewOperator {
		ar, err := g.ops.Attach(ctx, ident, hello.Languages, hello.ResumeToken, c)
		switch {
		case errors.Is(err, ErrNoSeat):
			g.m.closed.WithLabelValues(reasonNoSeat).Inc()
			_ = conn.Close(gwapi.CloseNoOperatorSeat)
			return
		case err != nil:
			g.logger.Warn("operator not created", "error", err)
			g.m.closed.WithLabelValues(reasonTryLater).Inc()
			_ = conn.Close(gwapi.CloseTryAgainLater)
			return
		}
		c.sess = ar.sess
		c.b.operatorID = ar.sess.id
		c.b.me = func() gwapi.OperatorRow { return g.ops.Row(ar.sess) }
		welcome.OperatorID, welcome.ResumeToken = ar.sess.id, ar.token
		if ar.replaced != nil {
			ar.replaced.closeWith(gwapi.CloseReplaced, reasonReplaced)
		}
		defer g.ops.Detach(ar.sess, c)
	}

	// Registered before the snapshot is read, so no change is lost.
	c.needSnap = true
	status := g.hub.add(c)
	defer g.hub.remove(c)
	if !c.writeMsg(welcome) || !c.writeMsg(status) || !c.flushState() {
		c.closeWith(gwapi.CloseGoingAway, reasonGone)
		c.wg.Wait()
		return
	}

	c.wg.Go(c.writeLoop)
	c.wg.Go(c.pingLoop)
	// A stop of the gateway closes with "going away" before anything else
	// ends.
	stopGoing := context.AfterFunc(ctx, func() { c.closeWith(gwapi.CloseGoingAway, reasonGoingAway) })
	defer stopGoing()
	actx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c.wg.Go(func() {
		<-c.done
		cancel()
	})
	c.readLoop(actx)
	cancel()
	c.wg.Wait()
}

// readHello reads the first message within gwapi.HelloTimeout. A non-zero
// code closes the connection.
func (g *Gateway) readHello(ctx context.Context, conn Conn) (gwapi.Hello, gwapi.CloseCode, string) {
	// The read has no deadline of its own: coder/websocket would drop the
	// socket without a close frame. A timer sends the close frame, which
	// ends the read.
	var timedOut, stopped atomic.Bool
	timer := time.AfterFunc(gwapi.HelloTimeout, func() {
		timedOut.Store(true)
		_ = conn.Close(gwapi.CloseHelloTimeout)
	})
	stopGoing := context.AfterFunc(ctx, func() {
		stopped.Store(true)
		_ = conn.Close(gwapi.CloseGoingAway)
	})
	b, err := conn.Read(context.Background())
	timer.Stop()
	stopGoing()
	switch {
	case timedOut.Load():
		return gwapi.Hello{}, gwapi.CloseHelloTimeout, reasonHelloTimeout
	case stopped.Load():
		return gwapi.Hello{}, gwapi.CloseGoingAway, reasonGoingAway
	case errors.Is(err, ErrTooBig):
		return gwapi.Hello{}, gwapi.CloseMessageTooBig, reasonTooBig
	case err != nil:
		return gwapi.Hello{}, gwapi.CloseNormal, reasonGone
	}
	m, err := gwapi.DecodeClient(b)
	switch {
	case errors.Is(err, gwapi.ErrUnsupportedVersion):
		return gwapi.Hello{}, gwapi.CloseUnsupportedVersion, reasonVersion
	case err != nil:
		return gwapi.Hello{}, gwapi.CloseBadMessage, reasonBadMessage
	}
	h, ok := m.(gwapi.Hello)
	if !ok {
		return gwapi.Hello{}, gwapi.CloseBadMessage, reasonBadMessage
	}
	return h, 0, ""
}

// connLimits counts connections in total and per client address.
type connLimits struct {
	mu      sync.Mutex
	total   int
	perAddr map[string]int
	max     int
	maxAddr int
}

func newConnLimits(maxTotal, maxAddr int) *connLimits {
	return &connLimits{perAddr: map[string]int{}, max: maxTotal, maxAddr: maxAddr}
}

// check counts the connection and reports whether it is within the
// limits. Every call needs a release.
func (l *connLimits) check(addr string) (bool, gwapi.CloseCode, string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.total++
	l.perAddr[addr]++
	switch {
	case l.perAddr[addr] > l.maxAddr:
		return false, gwapi.CloseTooManyConnections, reasonTooMany
	case l.total > l.max:
		return false, gwapi.CloseTryAgainLater, reasonTryLater
	}
	return true, 0, ""
}

func (l *connLimits) release(addr string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.total--
	if l.perAddr[addr]--; l.perAddr[addr] <= 0 {
		delete(l.perAddr, addr)
	}
}
