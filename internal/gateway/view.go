package gateway

import (
	"slices"
	"sync"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
)

// The view (decision 0011, point 1A) is the latest state of every key of
// the conversations and routing topics, kept in memory. It is built only
// from the two topics: the gateway reads them from the beginning at every
// start.
//
// # Keys and order
//
// Four families have their own sequence numbers: conversation events per
// conversation key (conversations topic), routing conversation events per
// conversation key, team events per company and operator events per
// operator. An event whose number is not above the last applied number of
// its family and key is skipped. Order holds only within one family and
// key, so every event type can create the row of its key, and every field
// that the protocol requires has a fallback when the first events of a key
// are missing (they expired from the 24 hour topics).
//
// # Sessions
//
// A conversation row has a conv part and a routing part. The current
// session is the higher session of the two parts. The routing part counts
// only when its session is the current session; otherwise the row is
// "open" (not in a queue yet), so a reopened conversation never shows the
// operator of the previous session.
//
// # Memory
//
// Rows the protocol no longer shows are forgotten (see sweep): a closed
// conversation, a released team and an offline human operator without load
// stay ForgetAfter after their last event, so repeated events are still
// recognised, and are then deleted. An open conversation without any event
// for staleOpenAfter is deleted too (conv lost its state, decision 0009).
// All times of these rules are record times of the topics (log append
// time, machine clock), never the time of reading, so a replay after a
// restart gives the same view.

// staleOpenAfter forgets an open conversation that had no event for this
// long. The topics keep 24 hours, so after a restart such a row would not
// exist anyway.
const staleOpenAfter = 26 * time.Hour

// sweepEvery is the record time between two sweeps.
const sweepEvery = 5 * time.Minute

// windowKeep is how long the samples of the one-hour totals are kept.
const windowKeep = time.Hour + 5*time.Minute

// rateWindow is the window of messages_per_second.
const rateWindow = 10

// phase is the routing phase of the current session.
type phase uint8

const (
	phaseNone phase = iota
	phaseWaiting
	phaseOffered
	phaseAssigned
	phaseResolved
	phaseEnded
)

// convRow is one conversation key.
type convRow struct {
	company, customer string

	// conv part.
	convKnown       bool
	convSeq         uint64
	convSession     uint64
	closed          bool
	convLang        string
	openedSourceUS  int64
	customerMsgs    uint64
	companyMsgs     uint64
	lastMsgSourceUS int64

	// routing part.
	rtKnown             bool
	rtSeq               uint64
	rtSession           uint64
	phase               phase
	rtLang              string
	teamQueue           bool
	priority            uint32
	queuedAtUS          int64
	firstQueuedAtUS     int64
	operatorID, offerID string
	acceptByUS          int64
	acceptedAtUS        int64
	replied             bool
	assignmentMissed    bool
	firstResponseMissed bool

	// lastUS is the record time of the last applied event.
	lastUS int64
}

func (r *convRow) session() uint64 { return max(r.convSession, r.rtSession) }

// routingCounts reports whether the routing part is about the current
// session.
func (r *convRow) routingCounts() bool {
	return r.rtKnown && r.rtSession == r.session() && r.phase != phaseNone
}

// open reports whether the conversation is open: not closed by conv in
// the current session and not ended by the router in it.
func (r *convRow) open() bool {
	s := r.session()
	if r.convKnown && r.convSession == s && r.closed {
		return false
	}
	if r.rtKnown && r.rtSession == s && r.phase == phaseEnded {
		return false
	}
	return true
}

// derived is what the indexes need of a row.
type derived struct {
	exists  bool
	open    bool
	visible bool // in the supervisor list
	state   gwapi.ConversationState
	lang    string
	holder  string
	offered bool
	inQueue bool
	company string
}

func derive(r *convRow) derived {
	if r == nil {
		return derived{}
	}
	d := derived{exists: true, open: r.open(), company: r.company, state: gwapi.StateOpen}
	d.lang = r.convLang
	if r.routingCounts() {
		d.lang = r.rtLang
		switch r.phase {
		case phaseWaiting:
			d.state = gwapi.StateWaiting
		case phaseOffered:
			d.state = gwapi.StateOffered
		case phaseAssigned:
			d.state = gwapi.StateAssigned
		case phaseResolved:
			d.state = gwapi.StateResolved
		}
	}
	if d.lang == "" {
		d.lang = r.rtLang
	}
	if !d.open {
		return d
	}
	// A row needs the opened time of conv for the supervisor list.
	d.visible = r.convKnown && r.openedSourceUS > 0
	if d.state == gwapi.StateOffered || d.state == gwapi.StateAssigned {
		d.holder = r.operatorID
		d.offered = d.state == gwapi.StateOffered
	}
	d.inQueue = d.state == gwapi.StateWaiting || d.state == gwapi.StateOffered
	return d
}

// opRow is one operator, from its last operator event.
type opRow struct {
	seq       uint64
	languages []string
	capacity  int
	presence  event.Presence
	team      string
	lastUS    int64
}

// teamRow is the team of one company.
type teamRow struct {
	seq        uint64
	members    []string
	activeLoad uint64
	formedAtUS int64
	released   bool
	lastUS     int64
}

type waitSample struct{ atUS, waitUS int64 }

// changes are the keys that changed since the last takeChanges, and the
// message events for watched conversations.
type changes struct {
	convs  map[string]struct{}
	ops    map[string]struct{}
	teams  map[string]struct{}
	queues map[string]struct{}
	totals bool
	// opConvs are the conversation keys that changed for an operator: it
	// held them before or holds them after the change.
	opConvs map[string]map[string]struct{}
	added   []messageRef
	removed []removedRef
}

type messageRef struct{ key, messageID string }

type removedRef struct {
	key, messageID string
	reason         event.RemoveReason
}

func newChanges() *changes {
	return &changes{
		convs: map[string]struct{}{}, ops: map[string]struct{}{}, teams: map[string]struct{}{},
		queues: map[string]struct{}{}, opConvs: map[string]map[string]struct{}{},
	}
}

func (c *changes) empty() bool {
	return len(c.convs) == 0 && len(c.ops) == 0 && len(c.teams) == 0 && len(c.queues) == 0 &&
		!c.totals && len(c.opConvs) == 0 && len(c.added) == 0 && len(c.removed) == 0
}

func (c *changes) opConv(op, key string) {
	if op == "" {
		return
	}
	m := c.opConvs[op]
	if m == nil {
		m = map[string]struct{}{}
		c.opConvs[op] = m
	}
	m[key] = struct{}{}
}

// ApplyResult says what happened to one event.
type ApplyResult string

// Apply results.
const (
	ResultApplied ApplyResult = "applied"
	ResultSkipped ApplyResult = "skipped"
)

// View is the in-memory state of the topics. It is safe for concurrent
// use: one writer applies events, many readers build states.
type View struct {
	mu          sync.RWMutex
	forgetAfter time.Duration

	convs map[string]*convRow
	ops   map[string]*opRow
	teams map[string]*teamRow

	// held maps an operator to the conversation keys it holds; the value
	// is true for an offer.
	held map[string]map[string]bool
	// queued maps a language to the open conversations that wait or are
	// offered in it.
	queued map[string]map[string]struct{}
	// opLangs counts the known operators per language.
	opLangs map[string]int
	// counts are the supervisor-visible rows per state.
	counts map[gwapi.ConversationState]uint64
	// companies counts the supervisor-visible rows per company.
	companies map[string]int

	buckets      [16]struct{ sec, n int64 }
	missedAssign []int64
	missedFirst  []int64
	waits        []waitSample

	recordUS    int64
	lastSweepUS int64

	ch *changes
}

// NewView returns an empty view.
func NewView(forgetAfter time.Duration) *View {
	if forgetAfter <= 0 {
		forgetAfter = DefaultForgetAfter
	}
	return &View{
		forgetAfter: forgetAfter,
		convs:       map[string]*convRow{},
		ops:         map[string]*opRow{},
		teams:       map[string]*teamRow{},
		held:        map[string]map[string]bool{},
		queued:      map[string]map[string]struct{}{},
		opLangs:     map[string]int{},
		counts:      map[gwapi.ConversationState]uint64{},
		companies:   map[string]int{},
		ch:          newChanges(),
	}
}

// takeChanges returns the changes since the last call. The caller holds
// no lock.
func (v *View) takeChanges() *changes {
	v.mu.Lock()
	defer v.mu.Unlock()
	c := v.ch
	v.ch = newChanges()
	return c
}

// Item is one decoded record for ApplyBatch: exactly one of Conv and
// Routing is set.
type Item struct {
	Conv    *event.Conversation
	Routing *event.Routing
	// Time is the record time (log append time).
	Time time.Time
}

// ApplyBatch applies records under one lock and returns what happened to
// each.
func (v *View) ApplyBatch(items []Item) []ApplyResult {
	v.mu.Lock()
	defer v.mu.Unlock()
	res := make([]ApplyResult, len(items))
	for i, it := range items {
		switch {
		case it.Conv != nil:
			res[i] = v.applyConversation(*it.Conv, it.Time)
		case it.Routing != nil:
			res[i] = v.applyRouting(*it.Routing, it.Time)
		}
		v.maybeSweep()
	}
	return res
}

// ApplyConversation applies one event of the conversations topic. ts is
// the record time.
func (v *View) ApplyConversation(ev event.Conversation, ts time.Time) ApplyResult {
	v.mu.Lock()
	defer v.mu.Unlock()
	res := v.applyConversation(ev, ts)
	v.maybeSweep()
	return res
}

// ApplyRouting applies one event of the routing topic. ts is the record
// time.
func (v *View) ApplyRouting(ev event.Routing, ts time.Time) ApplyResult {
	v.mu.Lock()
	defer v.mu.Unlock()
	res := v.applyRouting(ev, ts)
	v.maybeSweep()
	return res
}

func (v *View) maybeSweep() {
	if v.recordUS-v.lastSweepUS >= sweepEvery.Microseconds() {
		v.sweepLocked(v.recordUS)
	}
}

func (v *View) clock(ts time.Time) int64 {
	us := ts.UnixMicro()
	if us > v.recordUS {
		v.recordUS = us
	}
	if v.lastSweepUS == 0 {
		v.lastSweepUS = us
	}
	return us
}

// updateConv runs mutate on the row of key, creating it, and keeps the
// indexes and the change sets in step.
func (v *View) updateConv(key, company, customer string, mutate func(r *convRow)) {
	r := v.convs[key]
	before := derive(r)
	if r == nil {
		r = &convRow{company: company, customer: customer}
		v.convs[key] = r
	}
	mutate(r)
	v.reindex(key, before, derive(r))
}

func (v *View) reindex(key string, before, after derived) {
	ch := v.ch
	if before.holder != after.holder || before.offered != after.offered {
		if before.holder != "" {
			if m := v.held[before.holder]; m != nil {
				delete(m, key)
				if len(m) == 0 {
					delete(v.held, before.holder)
				}
			}
			ch.ops[before.holder] = struct{}{}
		}
		if after.holder != "" {
			m := v.held[after.holder]
			if m == nil {
				m = map[string]bool{}
				v.held[after.holder] = m
			}
			m[key] = after.offered
			ch.ops[after.holder] = struct{}{}
		}
	}
	ch.opConv(before.holder, key)
	ch.opConv(after.holder, key)

	if before.inQueue && (!after.inQueue || before.lang != after.lang) {
		if m := v.queued[before.lang]; m != nil {
			delete(m, key)
			if len(m) == 0 {
				delete(v.queued, before.lang)
			}
		}
	}
	if after.inQueue {
		m := v.queued[after.lang]
		if m == nil {
			m = map[string]struct{}{}
			v.queued[after.lang] = m
		}
		m[key] = struct{}{}
	}
	if before.inQueue {
		ch.queues[before.lang] = struct{}{}
	}
	if after.inQueue {
		ch.queues[after.lang] = struct{}{}
	}

	if before.visible {
		v.counts[before.state]--
		if v.companies[before.company]--; v.companies[before.company] <= 0 {
			delete(v.companies, before.company)
		}
	}
	if after.visible {
		v.counts[after.state]++
		v.companies[after.company]++
	}
	if before.visible || after.visible {
		ch.convs[key] = struct{}{}
	}
	ch.totals = true
}

func (v *View) applyConversation(ev event.Conversation, ts time.Time) ApplyResult {
	us := v.clock(ts)
	key := string(ev.Key())
	r := v.convs[key]
	reset := false
	if r != nil && r.convKnown && ev.Seq <= r.convSeq {
		// conv forgets a conversation 24 hours after its close and starts
		// again at session 1, sequence 1 (decision 0009).
		if ev.Type != event.TypeOpened || ev.Session != 1 || ev.Seq != 1 || r.convSession != r.session() || !r.closed {
			return ResultSkipped
		}
		reset = true
	}
	v.updateConv(key, ev.CompanyID, ev.CustomerID, func(r *convRow) {
		if reset {
			*r = convRow{company: r.company, customer: r.customer, rtKnown: r.rtKnown, rtSeq: r.rtSeq}
		}
		newSession := !r.convKnown || ev.Session > r.convSession
		r.convKnown = true
		r.convSeq = ev.Seq
		if ev.Session >= r.convSession {
			r.convSession = ev.Session
			r.convLang = ev.Lang
			r.customerMsgs = ev.CustomerMessages
			r.companyMsgs = ev.CompanyMessages
			if ou := ev.OpenedAt.UnixMicro(); ou > 0 {
				r.openedSourceUS = ou
			}
		}
		switch ev.Type {
		case event.TypeClosed:
			if ev.Session >= r.convSession {
				r.closed = true
			}
		case event.TypeMessageRemoved:
			// It does not say whether the session is closed. A newer
			// session means it opened.
			if newSession {
				r.closed = false
			}
		default:
			if newSession || ev.Session == r.convSession {
				r.closed = false
			}
			if su := ev.SourceTime.UnixMicro(); su > r.lastMsgSourceUS {
				r.lastMsgSourceUS = su
			}
		}
		r.lastUS = us
	})
	switch ev.Type {
	case event.TypeOpened, event.TypeMessageAdded:
		v.countMessage(us)
		v.ch.added = append(v.ch.added, messageRef{key: key, messageID: ev.MessageID})
	case event.TypeAnswered:
		v.ch.added = append(v.ch.added, messageRef{key: key, messageID: ev.MessageID})
	case event.TypeMessageRemoved:
		v.ch.removed = append(v.ch.removed, removedRef{key: key, messageID: ev.MessageID, reason: ev.RemoveReason})
	}
	return ResultApplied
}

func (v *View) countMessage(us int64) {
	sec := us / 1_000_000
	b := &v.buckets[sec%int64(len(v.buckets))]
	switch {
	case sec < b.sec:
		// Older than the bucket holds: outside the window anyway.
		return
	case sec > b.sec:
		b.sec, b.n = sec, 0
	}
	b.n++
}

func (v *View) applyRouting(ev event.Routing, ts time.Time) ApplyResult {
	us := v.clock(ts)
	switch {
	case ev.Type.IsOperator():
		return v.applyOperator(ev, us)
	case ev.Type.IsTeam():
		return v.applyTeam(ev, us)
	}
	key := string(ev.Key())
	if r := v.convs[key]; r != nil && r.rtKnown && ev.Seq <= r.rtSeq {
		return ResultSkipped
	}
	at := ev.At.UnixMicro()
	v.updateConv(key, ev.CompanyID, ev.CustomerID, func(r *convRow) {
		r.rtKnown = true
		r.rtSeq = ev.Seq
		r.lastUS = us
		if ev.Session < r.rtSession {
			// An event of an older session after a newer one: only the
			// number moves.
			return
		}
		if ev.Session > r.rtSession {
			r.rtSession = ev.Session
			r.phase = phaseNone
			r.replied, r.assignmentMissed, r.firstResponseMissed = false, false, false
			r.firstQueuedAtUS, r.queuedAtUS, r.acceptedAtUS, r.acceptByUS = 0, 0, 0, 0
			r.operatorID, r.offerID = "", ""
		}
		r.rtLang = ev.Lang
		r.teamQueue = ev.TeamQueue
		r.priority = ev.Priority
		if q := ev.QueuedAt.UnixMicro(); !ev.QueuedAt.IsZero() && q > 0 {
			r.queuedAtUS = q
		}
		switch ev.Type {
		case event.TypeQueued:
			r.phase = phaseWaiting
			r.operatorID, r.offerID, r.acceptByUS, r.acceptedAtUS = "", "", 0, 0
			switch ev.QueueReason {
			case event.QueueNew, event.QueueReopened, event.QueueCatchUp:
				r.firstQueuedAtUS = r.queuedAtUS
			}
			if r.firstQueuedAtUS == 0 {
				r.firstQueuedAtUS = r.queuedAtUS
			}
		case event.TypeOffered:
			r.phase = phaseOffered
			r.operatorID, r.offerID = ev.OperatorID, ev.OfferID
			r.acceptByUS = ev.AcceptBy.UnixMicro()
			r.acceptedAtUS = 0
		case event.TypeAccepted:
			r.phase = phaseAssigned
			r.operatorID, r.offerID = ev.OperatorID, ev.OfferID
			r.acceptByUS = 0
			r.acceptedAtUS = at
			v.waits = append(v.waits, waitSample{atUS: at, waitUS: ev.Wait.Microseconds()})
		case event.TypeOfferExpired:
			r.phase = phaseWaiting
			r.operatorID, r.offerID, r.acceptByUS = "", "", 0
		case event.TypeReplied:
			r.replied = true
			if r.phase != phaseAssigned || r.offerID != ev.OfferID {
				// The accept is missing: the reply shows the assignment.
				r.phase = phaseAssigned
				r.operatorID, r.offerID, r.acceptByUS = ev.OperatorID, ev.OfferID, 0
				if r.acceptedAtUS == 0 {
					r.acceptedAtUS = at
				}
			}
		case event.TypeResolved:
			r.phase = phaseResolved
			r.operatorID, r.offerID, r.acceptByUS = "", "", 0
		case event.TypeSLAMissed:
			if ev.SLATarget == event.SLAAssignment {
				r.assignmentMissed = true
				v.missedAssign = append(v.missedAssign, at)
			} else {
				r.firstResponseMissed = true
				v.missedFirst = append(v.missedFirst, at)
			}
			if r.phase == phaseNone && ev.OperatorID == "" {
				r.phase = phaseWaiting
			}
		case event.TypeEnded:
			r.phase = phaseEnded
			r.operatorID, r.offerID, r.acceptByUS = "", "", 0
		}
		if r.phase != phaseNone && r.phase != phaseEnded && r.phase != phaseResolved && r.queuedAtUS == 0 {
			// The queue time of a session whose first events are gone.
			r.queuedAtUS = at
		}
		if r.firstQueuedAtUS == 0 && r.queuedAtUS > 0 {
			r.firstQueuedAtUS = r.queuedAtUS
		}
	})
	return ResultApplied
}

func (v *View) applyOperator(ev event.Routing, us int64) ApplyResult {
	o := v.ops[ev.OperatorID]
	if o != nil && ev.Seq <= o.seq {
		return ResultSkipped
	}
	if o == nil {
		o = &opRow{}
		v.ops[ev.OperatorID] = o
	} else {
		v.dropLangs(o.languages)
	}
	o.seq = ev.Seq
	o.languages = slices.Clone(ev.Languages)
	o.capacity = ev.Capacity
	o.presence = ev.Presence
	o.team = ev.DedicatedCompanyID
	o.lastUS = us
	for _, l := range o.languages {
		v.opLangs[l]++
		v.ch.queues[l] = struct{}{}
	}
	v.ch.ops[ev.OperatorID] = struct{}{}
	v.ch.totals = true
	return ResultApplied
}

func (v *View) dropLangs(langs []string) {
	for _, l := range langs {
		if v.opLangs[l]--; v.opLangs[l] <= 0 {
			delete(v.opLangs, l)
		}
		v.ch.queues[l] = struct{}{}
	}
}

func (v *View) applyTeam(ev event.Routing, us int64) ApplyResult {
	t := v.teams[ev.CompanyID]
	if t != nil && ev.Seq <= t.seq {
		return ResultSkipped
	}
	if t == nil {
		t = &teamRow{}
		v.teams[ev.CompanyID] = t
	}
	t.seq = ev.Seq
	t.lastUS = us
	t.members = slices.Clone(ev.Members)
	t.activeLoad = ev.ActiveLoad
	switch ev.Type {
	case event.TypeTeamFormed:
		t.formedAtUS = ev.At.UnixMicro()
		t.released = false
	case event.TypeTeamResized:
		t.released = false
		if t.formedAtUS == 0 {
			t.formedAtUS = ev.At.UnixMicro()
		}
	case event.TypeTeamReleased:
		t.released = true
	}
	v.ch.teams[ev.CompanyID] = struct{}{}
	return ResultApplied
}

// Sweep forgets old rows at the record time nowUS. The service calls it
// also when no record arrives, with the larger of the newest record time
// and the machine clock.
func (v *View) Sweep(now time.Time) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.sweepLocked(max(now.UnixMicro(), v.recordUS))
}

func (v *View) sweepLocked(nowUS int64) {
	v.lastSweepUS = nowUS
	forget := nowUS - v.forgetAfter.Microseconds()
	stale := nowUS - staleOpenAfter.Microseconds()
	for key, r := range v.convs {
		open := r.open()
		if (!open && r.lastUS < forget) || (open && r.lastUS < stale) {
			v.reindex(key, derive(r), derived{})
			delete(v.convs, key)
		}
	}
	for co, t := range v.teams {
		if t.released && t.lastUS < forget {
			delete(v.teams, co)
		}
	}
	for id, o := range v.ops {
		if gwapi.KindOf(id) == gwapi.KindHuman && o.presence == event.PresenceOffline &&
			len(v.held[id]) == 0 && o.lastUS < forget {
			v.dropLangs(o.languages)
			delete(v.ops, id)
			v.ch.ops[id] = struct{}{}
			v.ch.totals = true
		}
	}
	keep := nowUS - windowKeep.Microseconds()
	v.missedAssign = pruneTimes(v.missedAssign, keep)
	v.missedFirst = pruneTimes(v.missedFirst, keep)
	v.waits = slices.DeleteFunc(v.waits, func(w waitSample) bool { return w.atUS < keep })
}

func pruneTimes(ts []int64, keep int64) []int64 {
	return slices.DeleteFunc(ts, func(t int64) bool { return t < keep })
}

// Sizes returns the number of rows by kind, for the metrics.
func (v *View) Sizes() map[string]int {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return map[string]int{
		"conversations": len(v.convs),
		"operators":     len(v.ops),
		"teams":         len(v.teams),
		"queues":        len(v.queueLangsLocked()),
	}
}
