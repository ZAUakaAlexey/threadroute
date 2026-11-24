package gateway

import (
	"maps"
	"slices"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
)

// The read side of the view. Every method here is called with the read
// lock held (Read) and returns protocol objects at the full level, with
// real IDs. The state builder replaces the IDs with pseudonyms below the
// full level.

// Timing are the router settings that the view needs to derive deadlines.
type Timing struct {
	// SpeedFactor is model time per machine time.
	SpeedFactor float64
	// FirstResponseTarget is model time.
	FirstResponseTarget time.Duration
}

// machine turns a model-time duration into machine time.
func (t Timing) machine(d time.Duration) time.Duration {
	if t.SpeedFactor <= 0 {
		return d
	}
	return time.Duration(float64(d) / t.SpeedFactor)
}

// Read runs fn with the read lock held.
func (v *View) Read(fn func(r Reader)) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	fn(Reader{v: v})
}

// Reader reads the view. It is valid only inside View.Read.
type Reader struct{ v *View }

// Conversation returns the supervisor row of key, if the row is in the
// supervisor list.
func (rd Reader) Conversation(key string) (gwapi.ConversationRow, bool) {
	r := rd.v.convs[key]
	d := derive(r)
	if !d.visible {
		return gwapi.ConversationRow{}, false
	}
	row := gwapi.ConversationRow{
		ID: key, Company: r.company, Customer: r.customer,
		Lang: d.lang, Session: r.session(), State: d.state,
		AssignmentMissed: r.assignmentMissed && r.routingCounts(), FirstResponseMissed: r.firstResponseMissed && r.routingCounts(),
		CustomerMessages: r.customerMsgs, CompanyMessages: r.companyMsgs,
		OpenedSourceUS: r.openedSourceUS,
	}
	if r.routingCounts() {
		row.TeamQueue = r.teamQueue
		row.Priority = r.priority
	}
	switch d.state {
	case gwapi.StateOffered, gwapi.StateAssigned:
		row.OperatorID = r.operatorID
		row.QueuedAtUS = r.queuedAtUS
	case gwapi.StateWaiting:
		row.QueuedAtUS = r.queuedAtUS
	}
	return row, true
}

// Visible calls fn for every key of the supervisor list.
func (rd Reader) Visible(fn func(key string)) {
	for key, r := range rd.v.convs {
		if derive(r).visible {
			fn(key)
		}
	}
}

// Known reports whether the view has the conversation key at all.
func (rd Reader) Known(key string) bool { return rd.v.convs[key] != nil }

// Holds reports whether operator op holds key, and whether as an offer.
func (rd Reader) Holds(op, key string) (held, offered bool) {
	offered, held = rd.v.held[op][key]
	return held, offered
}

// HeldBy returns the keys that operator op holds.
func (rd Reader) HeldBy(op string) []string {
	return slices.Collect(maps.Keys(rd.v.held[op]))
}

// OfferKey returns the key of the live offer or assignment offerID of
// operator op.
func (rd Reader) OfferKey(op, offerID string) (string, bool) {
	for key := range rd.v.held[op] {
		if r := rd.v.convs[key]; r != nil && r.offerID == offerID {
			return key, true
		}
	}
	return "", false
}

// OperatorConversation returns the row of key for operator op, if op
// holds it.
func (rd Reader) OperatorConversation(op, key string, t Timing) (gwapi.OperatorConversation, bool) {
	r := rd.v.convs[key]
	d := derive(r)
	if d.holder != op || op == "" {
		return gwapi.OperatorConversation{}, false
	}
	row := gwapi.OperatorConversation{
		ID: key, Company: r.company, Customer: r.customer,
		Lang: d.lang, Session: r.session(), State: d.state, OfferID: r.offerID,
		QueuedAtUS: r.queuedAtUS, FirstResponseMissed: r.firstResponseMissed, Replied: r.replied,
		Priority: r.priority, CustomerMessages: r.customerMsgs, CompanyMessages: r.companyMsgs,
		LastMessageSourceUS: r.lastMsgSourceUS,
	}
	if d.state == gwapi.StateOffered {
		row.AcceptByUS = r.acceptByUS
	} else {
		row.AcceptedAtUS = r.acceptedAtUS
	}
	if !r.replied && !r.firstResponseMissed && r.firstQueuedAtUS > 0 && t.FirstResponseTarget > 0 {
		row.FirstResponseDueUS = r.firstQueuedAtUS + t.machine(t.FirstResponseTarget).Microseconds()
	}
	return row, true
}

// Operator returns the row of operator id.
func (rd Reader) Operator(id string) (gwapi.OperatorRow, bool) {
	o := rd.v.ops[id]
	if o == nil {
		return gwapi.OperatorRow{}, false
	}
	row := gwapi.OperatorRow{
		ID: id, Kind: gwapi.KindOf(id), Languages: slices.Clone(o.languages), Capacity: o.capacity,
		Presence: gwapi.Presence(o.presence), Team: o.team,
	}
	row.Load, row.Offered = rd.load(id)
	return row, true
}

// Load returns the load and the offers of operator id.
func (rd Reader) load(id string) (load, offered int) {
	for _, off := range rd.v.held[id] {
		load++
		if off {
			offered++
		}
	}
	return load, offered
}

// LoadOf is load for callers outside the view.
func (rd Reader) LoadOf(id string) (load, offered int) { return rd.load(id) }

// Operators calls fn for every operator ID.
func (rd Reader) Operators(fn func(id string)) {
	for id := range rd.v.ops {
		fn(id)
	}
}

// OnlineHumans returns the IDs of human operators that are online.
func (rd Reader) OnlineHumans() []string {
	var out []string
	for id, o := range rd.v.ops {
		if o.presence == event.PresenceOnline && gwapi.KindOf(id) == gwapi.KindHuman {
			out = append(out, id)
		}
	}
	return out
}

// Queue returns the queue row of a language, if the language has a queue
// row: an operator knows it, or a conversation waits or is offered in it.
func (rd Reader) Queue(lang string) (gwapi.QueueRow, bool) {
	keys := rd.v.queued[lang]
	if len(keys) == 0 && rd.v.opLangs[lang] == 0 {
		return gwapi.QueueRow{}, false
	}
	q := gwapi.QueueRow{ID: lang}
	for key := range keys {
		r := rd.v.convs[key]
		d := derive(r)
		if d.state == gwapi.StateOffered {
			q.Offered++
		} else {
			q.Waiting++
			if r.teamQueue {
				q.TeamWaiting++
			}
		}
		if r.assignmentMissed {
			q.MissedAssignment++
		}
		if q.OldestQueuedAtUS == 0 || r.queuedAtUS < q.OldestQueuedAtUS {
			q.OldestQueuedAtUS = r.queuedAtUS
		}
	}
	return q, true
}

func (v *View) queueLangsLocked() []string {
	set := map[string]struct{}{}
	for l := range v.queued {
		set[l] = struct{}{}
	}
	for l := range v.opLangs {
		set[l] = struct{}{}
	}
	return slices.Sorted(maps.Keys(set))
}

// QueueLangs returns the languages of the queue rows, sorted.
func (rd Reader) QueueLangs() []string { return rd.v.queueLangsLocked() }

// Team returns the team row of a company, if it has a team.
func (rd Reader) Team(company string) (gwapi.TeamRow, bool) {
	t := rd.v.teams[company]
	if t == nil || t.released {
		return gwapi.TeamRow{}, false
	}
	return gwapi.TeamRow{ID: company, Members: slices.Clone(t.members), ActiveLoad: t.activeLoad, FormedAtUS: t.formedAtUS}, true
}

// Teams calls fn for every company with a team.
func (rd Reader) Teams(fn func(company string)) {
	for co, t := range rd.v.teams {
		if !t.released {
			fn(co)
		}
	}
}

// Totals returns the totals at the machine time now.
func (rd Reader) Totals(now time.Time) gwapi.Totals {
	v := rd.v
	nowUS := now.UnixMicro()
	t := gwapi.Totals{
		Conversations: gwapi.StateCounts{
			Open: v.counts[gwapi.StateOpen], Waiting: v.counts[gwapi.StateWaiting],
			Offered: v.counts[gwapi.StateOffered], Assigned: v.counts[gwapi.StateAssigned],
			Resolved: v.counts[gwapi.StateResolved],
		},
		CompaniesActive: uint64(len(v.companies)),
	}
	sec := nowUS / 1_000_000
	var n int64
	for _, b := range v.buckets {
		if b.sec > sec-rateWindow && b.sec <= sec {
			n += b.n
		}
	}
	t.MessagesPerSecond = float64(n) / rateWindow
	for id, o := range v.ops {
		if o.presence != event.PresenceOnline {
			continue
		}
		t.OperatorsOnline++
		if gwapi.KindOf(id) == gwapi.KindHuman {
			t.HumansOnline++
		}
		t.SlotsCapacity += uint64(o.capacity)
		load, _ := rd.load(id)
		t.SlotsUsed += uint64(load)
	}
	hourAgo := nowUS - time.Hour.Microseconds()
	for _, at := range v.missedAssign {
		if at > hourAgo {
			t.MissedAssignment++
		}
	}
	for _, at := range v.missedFirst {
		if at > hourAgo {
			t.MissedFirstResponse++
		}
	}
	var waits []int64
	for _, w := range v.waits {
		if w.atUS > hourAgo {
			waits = append(waits, w.waitUS)
		}
	}
	if len(waits) > 0 {
		slices.Sort(waits)
		t.WaitP50US = percentile(waits, 50)
		t.WaitP90US = percentile(waits, 90)
	}
	return t
}

// percentile returns the nearest-rank percentile of sorted values.
func percentile(sorted []int64, p int) int64 {
	rank := (p*len(sorted) + 99) / 100
	if rank < 1 {
		rank = 1
	}
	return sorted[rank-1]
}
