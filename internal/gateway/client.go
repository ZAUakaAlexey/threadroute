package gateway

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/ZAUakaAlexey/threadroute/internal/conv"
	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// Conn is one WebSocket connection. wsConn wraps coder/websocket; tests use
// a fake.
type Conn interface {
	// Read returns the next text message. It fails with ErrTooBig for a
	// message over gwapi.MaxClientMessageBytes.
	Read(ctx context.Context) ([]byte, error)
	// Write sends one text message.
	Write(ctx context.Context, b []byte) error
	// Ping sends a ping and waits for the pong.
	Ping(ctx context.Context) error
	// Close sends a close frame with the code and its name as reason.
	Close(code gwapi.CloseCode) error
}

// ErrTooBig is returned by Conn.Read for a message that is too large.
var ErrTooBig = errors.New("client message is too big")

// TextSource reads message text (conv.TextReader).
type TextSource interface {
	Messages(ctx context.Context, companyID, customerID string) ([]conv.StoredMessage, error)
	Message(ctx context.Context, companyID, customerID, messageID string) (conv.StoredMessage, bool, error)
}

// outBuffer bounds the queue of results, statuses and message events of
// one client. These are answers, not state; a client that lets it fill up
// is closed as slow.
const outBuffer = 64

// textTimeout bounds one read of text from Redis.
const textTimeout = 3 * time.Second

// maxPendingMessages bounds the new message IDs of one watch between two
// ticks. Above it the watch gets a snapshot instead.
const maxPendingMessages = 64

// outItem is one entry of the queue of a client: an encoded message, or
// the order to send the messages snapshots of new watches at this point.
type outItem struct {
	b          []byte
	typ        gwapi.ServerType
	watchFlush bool
}

// watch is one watched conversation of a client.
type watch struct {
	key      string
	company  string
	customer string
	// id is the conversation ID as this client sees it.
	id       string
	snapshot bool
	pending  map[string]struct{}
}

// client is one connection after the hello.
type client struct {
	g     *Gateway
	conn  Conn
	aud   gwapi.Audience
	ident Identity
	sess  *opSession
	b     *builder
	lim   *limiter

	tickCh chan struct{}
	out    chan outItem
	done   chan struct{}

	closeOnce sync.Once
	closeCode gwapi.CloseCode

	mu       sync.Mutex
	d        *dirty
	needSnap bool
	watches  map[string]*watch

	// seq is used by the writer only.
	seq uint64
	// wg owns the writer, the pinger and the request goroutines.
	wg sync.WaitGroup
}

func (g *Gateway) newClient(conn Conn, aud gwapi.Audience, ident Identity) *client {
	c := &client{
		g: g, conn: conn, aud: aud, ident: ident,
		lim:    newLimiter(g.clock.Now(), gwapi.ActionBurst, gwapi.ActionsPerSecond),
		tickCh: make(chan struct{}, 1),
		out:    make(chan outItem, outBuffer),
		done:   make(chan struct{}),
		d:      newDirty(), watches: map[string]*watch{},
	}
	c.b = &builder{aud: aud, ps: g.ps, frameRows: g.cfg.SnapshotFrameRows, timing: g.timing()}
	return c
}

// markChanges adds the view changes this client may see.
func (c *client) markChanges(ch *changes) {
	c.mu.Lock()
	c.d.merge(ch, c.aud.View, c.b.operatorID)
	for _, a := range ch.added {
		if w := c.watches[a.key]; w != nil && !w.snapshot {
			if len(w.pending) >= maxPendingMessages {
				w.snapshot, w.pending = true, nil
				continue
			}
			if w.pending == nil {
				w.pending = map[string]struct{}{}
			}
			w.pending[a.messageID] = struct{}{}
		}
	}
	var removed []gwapi.MessageRemoved
	for _, rm := range ch.removed {
		if w := c.watches[rm.key]; w != nil {
			delete(w.pending, rm.messageID)
			removed = append(removed, gwapi.MessageRemoved{
				ConversationID: w.id, MessageID: c.b.id(rm.messageID), Reason: rm.reason,
			})
		}
	}
	c.mu.Unlock()
	// The text must leave the screen at once (decision 0004), so this is
	// not left for the next tick.
	for _, m := range removed {
		c.send(m)
	}
}

func (c *client) requestSnapshot() {
	c.mu.Lock()
	c.needSnap = true
	c.mu.Unlock()
}

// send encodes m for this client and queues it. A full queue closes the
// client as slow.
func (c *client) send(m gwapi.ServerMessage) {
	b, err := gwapi.EncodeServer(m, c.aud)
	if err != nil {
		c.g.logger.Error("server message failed validation", "type", m.ServerType(), "error", err)
		c.closeWith(gwapi.CloseInternal, reasonInternal)
		return
	}
	c.enqueue(outItem{b: b, typ: m.ServerType()})
}

func (c *client) enqueue(it outItem) {
	select {
	case <-c.done:
	case c.out <- it:
	default:
		c.closeWith(gwapi.CloseSlowClient, reasonSlow)
	}
}

func (c *client) sendStatus(st gwapi.Status) { c.send(st) }

// closeWith closes the connection once, with code.
func (c *client) closeWith(code gwapi.CloseCode, reason string) {
	c.closeOnce.Do(func() {
		c.closeCode = code
		c.g.m.closed.WithLabelValues(reason).Inc()
		close(c.done)
		// Close waits for the close handshake, so it runs on its own.
		c.wg.Go(func() { _ = c.conn.Close(code) })
	})
}

// write sends one encoded frame within the write timeout.
func (c *client) write(b []byte, typ gwapi.ServerType) bool {
	ctx, cancel := context.WithTimeout(context.Background(), c.g.cfg.WriteTimeout)
	defer cancel()
	if err := c.conn.Write(ctx, b); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			c.closeWith(gwapi.CloseSlowClient, reasonSlow)
		} else {
			c.closeWith(gwapi.CloseGoingAway, reasonGone)
		}
		return false
	}
	c.g.m.sent.WithLabelValues(string(typ)).Inc()
	c.g.m.sentBytes.WithLabelValues(string(typ)).Add(float64(len(b)))
	return true
}

func (c *client) writeMsg(m gwapi.ServerMessage) bool {
	b, err := gwapi.EncodeServer(m, c.aud)
	if err != nil {
		c.g.logger.Error("server message failed validation", "type", m.ServerType(), "error", err)
		c.closeWith(gwapi.CloseInternal, reasonInternal)
		return false
	}
	return c.write(b, m.ServerType())
}

// writeLoop is the only writer of the connection after the handshake.
func (c *client) writeLoop() {
	for {
		select {
		case <-c.done:
			return
		case it := <-c.out:
			if it.watchFlush {
				c.flushMessages()
				continue
			}
			if !c.write(it.b, it.typ) {
				return
			}
		case <-c.tickCh:
			if !c.flushState() {
				return
			}
			c.flushMessages()
		}
	}
}

// pingLoop pings the client every PingInterval and closes it when no pong
// comes within the write timeout.
func (c *client) pingLoop() {
	t := time.NewTicker(c.g.cfg.PingInterval)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), c.g.cfg.WriteTimeout)
			err := c.conn.Ping(ctx)
			cancel()
			if err != nil {
				c.closeWith(gwapi.CloseGoingAway, reasonPing)
				return
			}
		}
	}
}

// takeDirty returns the changed keys and whether a snapshot is due, and
// starts a new set. A snapshot ends every watch.
func (c *client) takeDirty() (*dirty, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	d, snap := c.d, c.needSnap
	c.d, c.needSnap = newDirty(), false
	if snap {
		clear(c.watches)
	}
	return d, snap
}

// flushState sends the state of the changed keys, or a snapshot.
func (c *client) flushState() bool {
	d, snap := c.takeDirty()
	if !snap && d.empty() {
		return true
	}
	totals := c.g.hub.currentTotals()
	c.b.timing = c.g.timing()
	var frames []gwapi.State
	c.g.view.Read(func(rd Reader) {
		if snap {
			frames = c.b.snapshot(rd, totals)
			return
		}
		if st, ok := c.b.update(rd, d, totals); ok {
			frames = []gwapi.State{st}
		}
	})
	stamp(frames, &c.seq, c.g.clock.Now())
	for _, f := range frames {
		if !c.writeMsg(f) {
			return false
		}
	}
	return true
}

type textJob struct {
	w   watch
	ids []string
}

// flushMessages sends the messages snapshots of new watches and the new
// messages of watched conversations. The text is read from Redis now and
// not kept (decision 0011, point 2).
func (c *client) flushMessages() {
	c.mu.Lock()
	var jobs []textJob
	for _, w := range c.watches {
		switch {
		case w.snapshot:
			jobs = append(jobs, textJob{w: *w})
			w.snapshot, w.pending = false, nil
		case len(w.pending) > 0:
			jobs = append(jobs, textJob{w: *w, ids: sortedKeys(w.pending)})
			w.pending = nil
		}
	}
	c.mu.Unlock()
	for _, j := range jobs {
		if !c.mayWatch(j.w.key) {
			c.endWatch(j.w.key)
			continue
		}
		m, ok := c.readText(j)
		if !ok {
			continue
		}
		if !c.writeMsg(m) {
			return
		}
	}
	c.endLostWatches()
}

// endLostWatches ends the watches of conversations this client may no
// longer watch (an operator who lost the assignment).
func (c *client) endLostWatches() {
	c.mu.Lock()
	keys := make([]string, 0, len(c.watches))
	for k := range c.watches {
		keys = append(keys, k)
	}
	c.mu.Unlock()
	for _, k := range keys {
		if !c.mayWatch(k) {
			c.endWatch(k)
		}
	}
}

func (c *client) endWatch(key string) {
	c.mu.Lock()
	delete(c.watches, key)
	c.mu.Unlock()
}

// mayWatch reports whether the client may watch the conversation now: an
// operator while it holds the assignment, a supervisor while the view
// knows the conversation.
func (c *client) mayWatch(key string) bool {
	ok := false
	c.g.view.Read(func(rd Reader) {
		if c.aud.View == gwapi.ViewOperator {
			held, offered := rd.Holds(c.b.operatorID, key)
			ok = held && !offered
			return
		}
		ok = rd.Known(key)
	})
	return ok
}

func (c *client) readText(j textJob) (gwapi.Messages, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), textTimeout)
	defer cancel()
	out := gwapi.Messages{ConversationID: j.w.id, Snapshot: j.ids == nil, Messages: []gwapi.TextMessage{}}
	if out.Snapshot {
		list, err := c.g.text.Messages(ctx, j.w.company, j.w.customer)
		if err != nil {
			c.g.m.textReads.WithLabelValues("conversation", "error").Inc()
			// The error can name a message ID; it is not logged.
			c.g.logger.Warn("text read failed", "kind", "conversation")
			return out, false
		}
		c.g.m.textReads.WithLabelValues("conversation", "ok").Inc()
		for _, sm := range list {
			out.Messages = append(out.Messages, c.textMessage(sm))
		}
		return out, true
	}
	for _, id := range j.ids {
		sm, ok, err := c.g.text.Message(ctx, j.w.company, j.w.customer, id)
		switch {
		case err != nil:
			c.g.m.textReads.WithLabelValues("message", "error").Inc()
			c.g.logger.Warn("text read failed", "kind", "message")
			continue
		case !ok:
			c.g.m.textReads.WithLabelValues("message", "absent").Inc()
			continue
		}
		c.g.m.textReads.WithLabelValues("message", "ok").Inc()
		out.Messages = append(out.Messages, c.textMessage(sm))
	}
	return out, len(out.Messages) > 0
}

// textMessage turns a stored message into a protocol row for this
// client: with text at the full level, without it below.
func (c *client) textMessage(sm conv.StoredMessage) gwapi.TextMessage {
	m := gwapi.TextMessage{
		ID: c.b.id(sm.ID), Direction: sm.Direction, SourceUS: sm.SourceTime.UnixMicro(),
		Length: utf8.RuneCountInString(sm.Text), Edited: sm.Edited,
	}
	if c.aud.Level == gwapi.LevelFull {
		text := sm.Text
		m.Text = &text
	}
	return m
}

// readLoop reads client requests until the connection ends. ctx is the
// context of the requests; it ends when the client ends.
//
// The read itself has no context that can end: coder/websocket drops the
// socket without a close frame when the context of a blocked read ends.
// Every close goes through closeWith, whose Close sends the close frame
// and then ends the read.
func (c *client) readLoop(ctx context.Context) {
	for {
		b, err := c.conn.Read(context.Background())
		if err != nil {
			if errors.Is(err, ErrTooBig) {
				c.closeWith(gwapi.CloseMessageTooBig, reasonTooBig)
			} else {
				c.closeWith(gwapi.CloseNormal, reasonNormal)
			}
			return
		}
		m, err := gwapi.DecodeClient(b)
		if err != nil {
			c.closeWith(gwapi.CloseBadMessage, reasonBadMessage)
			return
		}
		switch m := m.(type) {
		case gwapi.Hello:
			c.closeWith(gwapi.CloseBadMessage, reasonBadMessage)
			return
		case gwapi.Watch:
			c.handleWatch(m)
		case gwapi.Unwatch:
			c.handleUnwatch(m)
		case gwapi.Action:
			c.handleAction(ctx, m)
		case gwapi.ChangeSettings:
			c.handleChangeSettings(ctx, m)
		}
	}
}

func (c *client) reject(typ, requestID string, code gwapi.ErrorCode, message string) {
	c.g.m.actions.WithLabelValues(typ, string(code)).Inc()
	c.send(gwapi.Result{RequestID: requestID, Outcome: gwapi.OutcomeRejected, Code: code, Message: message})
}

func (c *client) answer(typ, requestID string, o gwapi.Outcome) {
	c.g.m.actions.WithLabelValues(typ, string(o)).Inc()
	c.send(gwapi.Result{RequestID: requestID, Outcome: o})
}

func (c *client) handleWatch(w gwapi.Watch) {
	if !c.lim.allow(c.g.clock.Now()) {
		c.reject("watch", w.RequestID, gwapi.CodeRateLimited, rateLimitedMessage)
		return
	}
	key, company, customer, ok := c.resolveWatch(w.ConversationID)
	if !ok {
		c.reject("watch", w.RequestID, gwapi.CodeUnknownConversation, "the conversation is not in this view or may not be watched")
		return
	}
	c.mu.Lock()
	outcome := gwapi.OutcomeDone
	if cur := c.watches[key]; cur != nil {
		outcome = gwapi.OutcomeAlreadyDone
		cur.snapshot, cur.pending = true, nil
	} else {
		if len(c.watches) >= gwapi.MaxWatched {
			c.mu.Unlock()
			c.reject("watch", w.RequestID, gwapi.CodeTooManyWatched, "4 conversations are watched already")
			return
		}
		// The watch is registered before the text is read, so a removal
		// in between is still pushed.
		c.watches[key] = &watch{key: key, company: company, customer: customer, id: w.ConversationID, snapshot: true}
	}
	c.mu.Unlock()
	c.answer("watch", w.RequestID, outcome)
	c.enqueue(outItem{watchFlush: true})
}

// resolveWatch finds the conversation key of an ID as this client sees
// it, and checks that the client may watch it.
func (c *client) resolveWatch(id string) (key, company, customer string, ok bool) {
	c.g.view.Read(func(rd Reader) {
		match := func(k string) bool { return c.b.id(k) == id }
		if c.aud.View == gwapi.ViewOperator {
			for _, k := range rd.HeldBy(c.b.operatorID) {
				if held, offered := rd.Holds(c.b.operatorID, k); held && !offered && match(k) {
					key, ok = k, true
					return
				}
			}
			return
		}
		if c.aud.Level == gwapi.LevelFull {
			if _, vis := rd.Conversation(id); vis {
				key, ok = id, true
			}
			return
		}
		rd.Visible(func(k string) {
			if !ok && match(k) {
				key, ok = k, true
			}
		})
	})
	if ok {
		var err error
		company, customer, err = event.ParseConversationKey([]byte(key))
		ok = err == nil
	}
	return key, company, customer, ok
}

func (c *client) handleUnwatch(u gwapi.Unwatch) {
	if !c.lim.allow(c.g.clock.Now()) {
		c.reject("unwatch", u.RequestID, gwapi.CodeRateLimited, rateLimitedMessage)
		return
	}
	c.mu.Lock()
	found := false
	for k, w := range c.watches {
		if w.id == u.ConversationID {
			delete(c.watches, k)
			found = true
		}
	}
	c.mu.Unlock()
	if found {
		c.answer("unwatch", u.RequestID, gwapi.OutcomeDone)
		return
	}
	c.answer("unwatch", u.RequestID, gwapi.OutcomeAlreadyDone)
}

const rateLimitedMessage = "more than 5 requests per second"

func (c *client) handleAction(ctx context.Context, a gwapi.Action) {
	typ := "action_" + string(a.Action)
	if c.aud.View != gwapi.ViewOperator || c.sess == nil {
		c.reject(typ, a.RequestID, gwapi.CodeNotAllowed, "actions are allowed on an operator connection only")
		return
	}
	if !c.lim.allow(c.g.clock.Now()) {
		c.reject(typ, a.RequestID, gwapi.CodeRateLimited, rateLimitedMessage)
		return
	}
	c.wg.Go(func() {
		res := c.g.doAction(ctx, c, a)
		code := string(res.Outcome)
		if res.Outcome == gwapi.OutcomeRejected {
			code = string(res.Code)
		}
		c.g.m.actions.WithLabelValues(typ, code).Inc()
		c.send(res)
	})
}

// codeTexts are the messages of rejected actions. The gateway writes its
// own text instead of passing on the router's, which could hold IDs.
var codeTexts = map[gwapi.ErrorCode]string{
	gwapi.CodeInvalidRequest:   "the request is not valid",
	gwapi.CodeOperatorNotFound: "the operator is not known",
	gwapi.CodeOfferNotFound:    "the offer is not known",
	gwapi.CodeOfferExpired:     "the offer is expired",
	gwapi.CodeOfferRefused:     "the offer was refused before",
	gwapi.CodeOfferAccepted:    "the offer was accepted before",
	gwapi.CodeOfferEnded:       "the offer ended",
	gwapi.CodeOfferNotAccepted: "the offer is not accepted yet",
	gwapi.CodeAssignmentEnded:  "the assignment ended",
	gwapi.CodeUnavailable:      "the router cannot answer now",
	gwapi.CodeInternal:         "unexpected error",
}

const (
	staleOwnerMessage = "the operator moved to another connection"
	staleDataMessage  = "the gateway data is stale"
)

func rejected(a gwapi.Action, code gwapi.ErrorCode, text string) gwapi.Result {
	if text == "" {
		text = codeTexts[code]
	}
	return gwapi.Result{RequestID: a.RequestID, Outcome: gwapi.OutcomeRejected, Code: code, Message: string(a.Action) + ": " + text}
}

// doAction runs one operator action and returns its result (decision
// 0011, "Actions").
func (g *Gateway) doAction(ctx context.Context, c *client, a gwapi.Action) gwapi.Result {
	s := c.sess
	if !g.ops.Owns(s, c) {
		return rejected(a, gwapi.CodeStaleConnection, staleOwnerMessage)
	}
	if a.Action.NeedsOffer() {
		if g.hub.currentStatus().Stale {
			return rejected(a, gwapi.CodeStaleConnection, staleDataMessage)
		}
		var ok bool
		g.view.Read(func(rd Reader) { _, ok = rd.OfferKey(s.id, a.OfferID) })
		if !ok {
			return rejected(a, gwapi.CodeUnknownOffer, "the offer is not a live offer or assignment of this operator")
		}
	}
	var res opapi.Result
	err := g.ops.Call(s, func() error {
		var err error
		switch a.Action {
		case gwapi.ActionOnline:
			res, err = g.api.Online(ctx, s.id)
		case gwapi.ActionOffline:
			res, err = g.api.Offline(ctx, s.id)
		case gwapi.ActionSetLanguages:
			langs := slices.Sorted(slices.Values(a.Languages))
			var resp opapi.OperatorResponse
			resp, err = g.api.PutOperator(ctx, s.id, humanOperator(langs))
			if err == nil {
				g.ops.setRow(s, resp.Operator)
			}
			res = resp.Result
		case gwapi.ActionAccept:
			res, err = g.api.Accept(ctx, s.id, a.OfferID)
		case gwapi.ActionRefuse:
			res, err = g.api.Refuse(ctx, s.id, a.OfferID)
		case gwapi.ActionReply:
			// The request ID is the reply ID, so a resend records no
			// second reply.
			res, err = g.api.Reply(ctx, s.id, a.OfferID, a.RequestID)
		case gwapi.ActionResolve:
			res, err = g.api.Resolve(ctx, s.id, a.OfferID)
		}
		return err
	})
	switch {
	case errors.Is(err, ErrEnded):
		return rejected(a, gwapi.CodeStaleConnection, staleOwnerMessage)
	case err != nil:
		if e, ok := errors.AsType[*opapi.Error](err); ok && gwapi.FromOpapi(e.Code).Valid() {
			return rejected(a, gwapi.FromOpapi(e.Code), "")
		}
		g.logger.Warn("operator API call failed", "action", a.Action, "error", err)
		return rejected(a, gwapi.CodeUnavailable, "")
	}
	if res == opapi.ResultAlreadyDone {
		return gwapi.Result{RequestID: a.RequestID, Outcome: gwapi.OutcomeAlreadyDone}
	}
	return gwapi.Result{RequestID: a.RequestID, Outcome: gwapi.OutcomeDone}
}

// limiter is the token bucket of the requests of one connection.
type limiter struct {
	mu     sync.Mutex
	tokens float64
	burst  float64
	rate   float64
	last   time.Time
}

func newLimiter(now time.Time, burst, perSecond int) *limiter {
	return &limiter{tokens: float64(burst), burst: float64(burst), rate: float64(perSecond), last: now}
}

func (l *limiter) allow(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if el := now.Sub(l.last).Seconds(); el > 0 {
		l.tokens = min(l.burst, l.tokens+el*l.rate)
		l.last = now
	}
	if l.tokens < 1 {
		return false
	}
	l.tokens--
	return true
}
