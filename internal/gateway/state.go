package gateway

import (
	"slices"
	"strings"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
)

// dirty is the set of keys that changed for one client since its last
// state message (decision 0011, point 4B). It holds keys, never messages,
// so its size is bounded by the number of keys the client may see.
type dirty struct {
	convs  map[string]struct{}
	ops    map[string]struct{}
	teams  map[string]struct{}
	queues map[string]struct{}
	totals bool
	me     bool
}

func newDirty() *dirty {
	return &dirty{
		convs: map[string]struct{}{}, ops: map[string]struct{}{},
		teams: map[string]struct{}{}, queues: map[string]struct{}{},
	}
}

func (d *dirty) empty() bool {
	return len(d.convs) == 0 && len(d.ops) == 0 && len(d.teams) == 0 && len(d.queues) == 0 && !d.totals && !d.me
}

// merge adds the view changes that the client may see.
func (d *dirty) merge(c *changes, view gwapi.View, operatorID string) {
	if view == gwapi.ViewSupervisor {
		for k := range c.convs {
			d.convs[k] = struct{}{}
		}
		for k := range c.ops {
			d.ops[k] = struct{}{}
		}
		for k := range c.teams {
			d.teams[k] = struct{}{}
		}
		for k := range c.queues {
			d.queues[k] = struct{}{}
		}
		d.totals = d.totals || c.totals
		return
	}
	if _, ok := c.ops[operatorID]; ok {
		d.me = true
	}
	for k := range c.opConvs[operatorID] {
		d.convs[k] = struct{}{}
	}
}

// builder makes state messages for one audience.
type builder struct {
	aud        gwapi.Audience
	operatorID string
	ps         *Pseudonyms
	timing     Timing
	frameRows  int
	// me is the operator row while the view has no event of the operator
	// yet: the answer of the router when the gateway created it.
	me func() gwapi.OperatorRow
}

func (b *builder) id(s string) string {
	if b.aud.Level == gwapi.LevelFull || s == "" {
		return s
	}
	return b.ps.Of(s)
}

func (b *builder) convRow(r gwapi.ConversationRow) gwapi.ConversationRow {
	r.ID, r.Company, r.Customer = b.id(r.ID), b.id(r.Company), b.id(r.Customer)
	return r
}

func (b *builder) opConv(r gwapi.OperatorConversation) gwapi.OperatorConversation {
	r.ID, r.Company, r.Customer = b.id(r.ID), b.id(r.Company), b.id(r.Customer)
	return r
}

func (b *builder) opRow(r gwapi.OperatorRow) gwapi.OperatorRow {
	r.Team = b.id(r.Team)
	return r
}

func (b *builder) meRow(rd Reader) gwapi.OperatorRow {
	if r, ok := rd.Operator(b.operatorID); ok {
		return b.opRow(r)
	}
	r := b.me()
	r.Load, r.Offered = rd.LoadOf(b.operatorID)
	return b.opRow(r)
}

// snapshot builds the frames of a snapshot. The first one is the
// snapshot; the rest add rows of the large collection (decision 0011,
// "A large first state"). Seq and ServerTimeUS are set by the caller.
func (b *builder) snapshot(rd Reader, totals gwapi.Totals) []gwapi.State {
	first := gwapi.State{Snapshot: true}
	if b.aud.View == gwapi.ViewOperator {
		me := b.meRow(rd)
		first.Me = &me
		keys := rd.HeldBy(b.operatorID)
		var rows []gwapi.OperatorConversation
		for _, k := range keys {
			if row, ok := rd.OperatorConversation(b.operatorID, k, b.timing); ok {
				rows = append(rows, b.opConv(row))
			}
		}
		sortRows(rows)
		if len(rows) > 0 {
			first.MyConversations = &gwapi.Changes[gwapi.OperatorConversation]{Set: rows}
		}
		return []gwapi.State{first}
	}

	var ops []gwapi.OperatorRow
	rd.Operators(func(id string) {
		if r, ok := rd.Operator(id); ok {
			ops = append(ops, b.opRow(r))
		}
	})
	sortRows(ops)
	var queues []gwapi.QueueRow
	for _, l := range rd.QueueLangs() {
		if q, ok := rd.Queue(l); ok {
			queues = append(queues, q)
		}
	}
	var teams []gwapi.TeamRow
	rd.Teams(func(co string) {
		if t, ok := rd.Team(co); ok {
			t.ID = b.id(t.ID)
			teams = append(teams, t)
		}
	})
	sortRows(teams)
	var convs []gwapi.ConversationRow
	rd.Visible(func(key string) {
		if r, ok := rd.Conversation(key); ok {
			convs = append(convs, b.convRow(r))
		}
	})
	sortRows(convs)

	first.Totals = &totals
	if len(ops) > 0 {
		first.Operators = &gwapi.Changes[gwapi.OperatorRow]{Set: ops}
	}
	if len(queues) > 0 {
		first.Queues = &gwapi.Changes[gwapi.QueueRow]{Set: queues}
	}
	if len(teams) > 0 {
		first.Teams = &gwapi.Changes[gwapi.TeamRow]{Set: teams}
	}
	frames := []gwapi.State{first}
	for i := 0; i < len(convs); i += b.frameRows {
		part := convs[i:min(i+b.frameRows, len(convs))]
		set := &gwapi.Changes[gwapi.ConversationRow]{Set: part}
		if i == 0 {
			frames[0].Conversations = set
			continue
		}
		frames = append(frames, gwapi.State{Conversations: set})
	}
	return frames
}

// update builds one update from the dirty keys. ok is false when nothing
// is left to send.
func (b *builder) update(rd Reader, d *dirty, totals gwapi.Totals) (gwapi.State, bool) {
	var st gwapi.State
	if b.aud.View == gwapi.ViewOperator {
		if d.me {
			me := b.meRow(rd)
			st.Me = &me
		}
		ch := &gwapi.Changes[gwapi.OperatorConversation]{}
		for _, k := range sortedKeys(d.convs) {
			if row, ok := rd.OperatorConversation(b.operatorID, k, b.timing); ok {
				ch.Set = append(ch.Set, b.opConv(row))
			} else {
				ch.Remove = append(ch.Remove, b.id(k))
			}
		}
		if len(ch.Set)+len(ch.Remove) > 0 {
			st.MyConversations = ch
		}
		return st, st.Me != nil || st.MyConversations != nil
	}

	convs := &gwapi.Changes[gwapi.ConversationRow]{}
	for _, k := range sortedKeys(d.convs) {
		if r, ok := rd.Conversation(k); ok {
			convs.Set = append(convs.Set, b.convRow(r))
		} else {
			convs.Remove = append(convs.Remove, b.id(k))
		}
	}
	ops := &gwapi.Changes[gwapi.OperatorRow]{}
	for _, id := range sortedKeys(d.ops) {
		if r, ok := rd.Operator(id); ok {
			ops.Set = append(ops.Set, b.opRow(r))
		} else {
			ops.Remove = append(ops.Remove, id)
		}
	}
	queues := &gwapi.Changes[gwapi.QueueRow]{}
	for _, l := range sortedKeys(d.queues) {
		if q, ok := rd.Queue(l); ok {
			queues.Set = append(queues.Set, q)
		} else {
			queues.Remove = append(queues.Remove, l)
		}
	}
	teams := &gwapi.Changes[gwapi.TeamRow]{}
	for _, co := range sortedKeys(d.teams) {
		if t, ok := rd.Team(co); ok {
			t.ID = b.id(t.ID)
			teams.Set = append(teams.Set, t)
		} else {
			teams.Remove = append(teams.Remove, b.id(co))
		}
	}
	changed := false
	if len(convs.Set)+len(convs.Remove) > 0 {
		st.Conversations, changed = convs, true
	}
	if len(ops.Set)+len(ops.Remove) > 0 {
		st.Operators, changed = ops, true
	}
	if len(queues.Set)+len(queues.Remove) > 0 {
		st.Queues, changed = queues, true
	}
	if len(teams.Set)+len(teams.Remove) > 0 {
		st.Teams, changed = teams, true
	}
	if d.totals {
		st.Totals, changed = &totals, true
	}
	return st, changed
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func sortRows[T gwapi.Row](rows []T) {
	slices.SortFunc(rows, func(a, b T) int { return strings.Compare(a.RowID(), b.RowID()) })
}

// stamp sets the sequence numbers and the server time of frames.
func stamp(frames []gwapi.State, seq *uint64, now time.Time) {
	for i := range frames {
		*seq++
		frames[i].Seq = *seq
		frames[i].ServerTimeUS = now.UnixMicro()
	}
}
