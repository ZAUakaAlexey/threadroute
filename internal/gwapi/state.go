package gwapi

import (
	"fmt"
	"unicode/utf8"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

// The state objects of the State message. Each is the current state of one
// key, never a step (decision 0011, point 4): a client replaces what it
// holds for the key with the new object.
//
// # Clocks
//
// Fields that end in "_at_us" or "_due_us" are machine times of the router
// and the gateway (decision 0009, "Clock"). The client turns them into
// timers with the offset between its clock and Welcome.ServerTimeUS (or
// State.ServerTimeUS). Fields that end in "_source_us" are source times of
// the messages (decision 0007). Durations in "_us" fields are model time:
// machine time multiplied by Welcome.SpeedFactor, the unit of the SLA
// targets. The screens show model time (decision 0013).

// ConversationState is the routing state of a conversation in the view of
// the gateway.
type ConversationState string

// Conversation states. A closed conversation is removed from the view.
const (
	// StateOpen: open in conv, not in a queue yet (for example while the
	// router catches up).
	StateOpen ConversationState = "open"
	// StateWaiting: in a queue without an offer.
	StateWaiting ConversationState = "waiting"
	// StateOffered: an offer is out and waits for an accept.
	StateOffered ConversationState = "offered"
	// StateAssigned: an operator accepted it.
	StateAssigned ConversationState = "assigned"
	// StateResolved: the operator resolved it; the conversation is still
	// open in conv.
	StateResolved ConversationState = "resolved"
)

func (s ConversationState) valid() bool {
	switch s {
	case StateOpen, StateWaiting, StateOffered, StateAssigned, StateResolved:
		return true
	}
	return false
}

// Row is a state object with a key.
type Row interface {
	// RowID returns the key of the object within its collection.
	RowID() string
	validate(errs *fieldErrors, a Audience)
}

// Changes is one collection in a State message: objects to add or replace
// by their ID, and IDs to remove. In a snapshot Set holds the whole
// collection and Remove is empty.
type Changes[T Row] struct {
	Set    []T      `json:"set,omitempty"`
	Remove []string `json:"remove,omitempty"`
}

func (c *Changes[T]) validate(errs *fieldErrors, a Audience, name string, snapshot bool) {
	if c == nil {
		return
	}
	seen := make(map[string]bool, len(c.Set)+len(c.Remove))
	for i, r := range c.Set {
		errs.within(fmt.Sprintf("%s.set[%d]", name, i), func(e *fieldErrors) { r.validate(e, a) })
		if id := r.RowID(); id != "" {
			if seen[id] {
				errs.invalid(name, "ID at set[%d] is listed twice", i)
			}
			seen[id] = true
		}
	}
	if snapshot && len(c.Remove) > 0 {
		errs.invalid(name+".remove", "must be empty in a snapshot")
	}
	for i, id := range c.Remove {
		switch {
		case id == "":
			errs.invalid(name+".remove", "item %d is empty", i)
		case seen[id]:
			errs.invalid(name+".remove", "item %d is also set or listed twice", i)
		}
		seen[id] = true
	}
}

// OperatorConversation is a conversation as its operator sees it: an offer
// or an assignment of the operator of the connection (decision 0013,
// "Operator"). Collection "my_conversations", ViewOperator only. The row is
// removed when the offer or assignment ends: accepted by nobody, refused,
// expired, resolved, returned to the queue, or the conversation closed.
type OperatorConversation struct {
	// ID is the conversation key at LevelFull and its pseudonym at
	// LevelDemo. Watch takes it.
	ID string `json:"id"`
	// Company and Customer are the IDs at LevelFull, and pseudonyms at
	// LevelDemo.
	Company  string `json:"company"`
	Customer string `json:"customer"`
	// Lang is the queue language of the session.
	Lang string `json:"lang"`
	// Session is the session number from conv.
	Session uint64 `json:"session"`
	// State is StateOffered or StateAssigned.
	State ConversationState `json:"state"`
	// OfferID names the offer, and after the accept the assignment. The
	// actions take it.
	OfferID string `json:"offer_id"`
	// AcceptByUS is the accept deadline, only for StateOffered.
	AcceptByUS int64 `json:"accept_by_us,omitempty"`
	// AcceptedAtUS is when the operator accepted, only for StateAssigned.
	AcceptedAtUS int64 `json:"accepted_at_us,omitempty"`
	// QueuedAtUS is when the conversation entered its queue; the waiting
	// time runs from it.
	QueuedAtUS int64 `json:"queued_at_us"`
	// FirstResponseDueUS is when the "time to first response" target is
	// missed. 0 when the target is met or missed already.
	FirstResponseDueUS int64 `json:"first_response_due_us,omitempty"`
	// FirstResponseMissed reports that the target was missed.
	FirstResponseMissed bool `json:"first_response_missed"`
	// Replied reports that the operator sent a reply marker in this
	// session.
	Replied bool `json:"replied"`
	// Priority is the queue priority, 0 or higher.
	Priority uint32 `json:"priority"`
	// CustomerMessages and CompanyMessages count the messages of the
	// session (decision 0007). A removed message still counts.
	CustomerMessages uint64 `json:"customer_messages"`
	CompanyMessages  uint64 `json:"company_messages"`
	// LastMessageSourceUS is the source time of the newest message.
	LastMessageSourceUS int64 `json:"last_message_source_us,omitempty"`
}

// RowID implements Row.
func (c OperatorConversation) RowID() string { return c.ID }

func (c OperatorConversation) validate(errs *fieldErrors, a Audience) {
	checkConversationID(errs, a.Level, c.ID, c.Company, c.Customer)
	checkLang(errs, "lang", c.Lang)
	if c.Session == 0 {
		errs.missing("session")
	}
	switch c.State {
	case StateOffered:
		if c.AcceptByUS <= 0 {
			errs.missing("accept_by_us")
		}
		if c.AcceptedAtUS != 0 {
			errs.invalid("accepted_at_us", "must be empty for state %q", c.State)
		}
	case StateAssigned:
		if c.AcceptedAtUS <= 0 {
			errs.missing("accepted_at_us")
		}
		if c.AcceptByUS != 0 {
			errs.invalid("accept_by_us", "must be empty for state %q", c.State)
		}
	case "":
		errs.missing("state")
	default:
		errs.invalid("state", "%q is not %q or %q", c.State, StateOffered, StateAssigned)
	}
	switch {
	case c.OfferID == "":
		errs.missing("offer_id")
	case !event.ValidRoutingID(c.OfferID):
		errs.invalid("offer_id", "%q is not a valid ID", c.OfferID)
	}
	if c.QueuedAtUS <= 0 {
		errs.missing("queued_at_us")
	}
	if c.FirstResponseDueUS < 0 || c.LastMessageSourceUS < 0 {
		errs.invalid("first_response_due_us or last_message_source_us", "is negative")
	}
}

// ConversationRow is a conversation in the supervisor list (decision 0013,
// "Supervisor"). Collection "conversations", ViewSupervisor only. It holds
// every open conversation the gateway knows; a closed one is removed.
type ConversationRow struct {
	// ID, Company and Customer are as in OperatorConversation.
	ID       string `json:"id"`
	Company  string `json:"company"`
	Customer string `json:"customer"`
	Lang     string `json:"lang"`
	Session  uint64 `json:"session"`
	// State is the routing state.
	State ConversationState `json:"state"`
	// TeamQueue reports that the queue is the team queue of the company.
	TeamQueue bool   `json:"team_queue"`
	Priority  uint32 `json:"priority"`
	// OperatorID is the operator of the offer or the assignment, for
	// StateOffered and StateAssigned only.
	OperatorID string `json:"operator_id,omitempty"`
	// QueuedAtUS is when the conversation entered its queue, for
	// StateWaiting, StateOffered and StateAssigned.
	QueuedAtUS int64 `json:"queued_at_us,omitempty"`
	// AssignmentMissed and FirstResponseMissed report missed SLA targets
	// of the session.
	AssignmentMissed    bool `json:"assignment_missed"`
	FirstResponseMissed bool `json:"first_response_missed"`
	// CustomerMessages and CompanyMessages count the messages of the
	// session.
	CustomerMessages uint64 `json:"customer_messages"`
	CompanyMessages  uint64 `json:"company_messages"`
	// OpenedSourceUS is the source time at which the conversation opened.
	OpenedSourceUS int64 `json:"opened_source_us"`
}

// RowID implements Row.
func (c ConversationRow) RowID() string { return c.ID }

func (c ConversationRow) validate(errs *fieldErrors, a Audience) {
	checkConversationID(errs, a.Level, c.ID, c.Company, c.Customer)
	checkLang(errs, "lang", c.Lang)
	if c.Session == 0 {
		errs.missing("session")
	}
	switch {
	case c.State == "":
		errs.missing("state")
	case !c.State.valid():
		errs.invalid("state", "%q is not a known state", c.State)
	}
	held := c.State == StateOffered || c.State == StateAssigned
	switch {
	case held:
		checkOperatorID(errs, "operator_id", c.OperatorID)
	case c.OperatorID != "":
		errs.invalid("operator_id", "must be empty for state %q", c.State)
	}
	if (held || c.State == StateWaiting) && c.QueuedAtUS <= 0 {
		errs.missing("queued_at_us")
	}
	if c.OpenedSourceUS <= 0 {
		errs.missing("opened_source_us")
	}
}

// OperatorRow is an operator (decision 0013, "Operators"). Collection
// "operators" for ViewSupervisor, and the object "me" for ViewOperator.
type OperatorRow struct {
	// ID is the operator ID. Operator IDs are synthetic and shown at every
	// level.
	ID string `json:"id"`
	// Kind is KindHuman for an ID with HumanOperatorPrefix, else
	// KindSimulated.
	Kind Kind `json:"kind"`
	// Languages are sorted.
	Languages []string `json:"languages"`
	Capacity  int      `json:"capacity"`
	Presence  Presence `json:"presence"`
	// Load is the number of live offers and assignments. It can be higher
	// than Capacity after the capacity was lowered (decision 0009).
	Load int `json:"load"`
	// Offered is the part of Load that waits for an accept.
	Offered int `json:"offered"`
	// Team is the company of the dedicated team the operator is in, or
	// empty: the company ID at LevelFull, its pseudonym at LevelDemo.
	Team string `json:"team,omitempty"`
}

// RowID implements Row.
func (o OperatorRow) RowID() string { return o.ID }

func (o OperatorRow) validate(errs *fieldErrors, a Audience) {
	checkOperatorID(errs, "id", o.ID)
	switch {
	case o.Kind == "":
		errs.missing("kind")
	case o.ID != "" && o.Kind != KindOf(o.ID):
		errs.invalid("kind", "%q does not match the ID", o.Kind)
	}
	checkLanguages(errs, "languages", o.Languages, true)
	if o.Capacity <= 0 {
		errs.invalid("capacity", "is not positive")
	}
	switch {
	case o.Presence == "":
		errs.missing("presence")
	case !o.Presence.valid():
		errs.invalid("presence", "%q is not %q or %q", o.Presence, PresenceOnline, PresenceOffline)
	}
	if o.Load < 0 || o.Offered < 0 || o.Offered > o.Load {
		errs.invalid("load and offered", "need 0 <= offered <= load")
	}
	if o.Team != "" {
		checkLabel(errs, a.Level, "team", o.Team)
	}
}

// QueueRow is the queue of one language (decision 0013, "Queues by
// language"): the shared queue of the language and the team queues of that
// language together. Collection "queues", ViewSupervisor only. ID is the
// language.
type QueueRow struct {
	ID string `json:"id"`
	// Waiting counts conversations in the queue without an offer.
	Waiting uint64 `json:"waiting"`
	// TeamWaiting is the part of Waiting in team queues.
	TeamWaiting uint64 `json:"team_waiting"`
	// Offered counts conversations with an offer that waits for an
	// accept.
	Offered uint64 `json:"offered"`
	// OldestQueuedAtUS is the queue time of the conversation that has
	// waited longest among Waiting and Offered, or 0 when there is none.
	OldestQueuedAtUS int64 `json:"oldest_queued_at_us,omitempty"`
	// MissedAssignment counts the conversations of Waiting and Offered
	// that missed the "time to assignment" target.
	MissedAssignment uint64 `json:"missed_assignment"`
}

// RowID implements Row.
func (q QueueRow) RowID() string { return q.ID }

func (q QueueRow) validate(errs *fieldErrors, _ Audience) {
	checkLang(errs, "id", q.ID)
	if q.TeamWaiting > q.Waiting {
		errs.invalid("team_waiting", "is more than waiting")
	}
	if q.MissedAssignment > q.Waiting+q.Offered {
		errs.invalid("missed_assignment", "is more than waiting plus offered")
	}
	if q.OldestQueuedAtUS < 0 || (q.Waiting+q.Offered > 0) != (q.OldestQueuedAtUS > 0) {
		errs.invalid("oldest_queued_at_us", "must be set exactly when a conversation waits or is offered")
	}
}

// TeamRow is the dedicated team of one company (decision 0013, "Teams").
// Collection "teams", ViewSupervisor only. ID is the company ID at
// LevelFull and its pseudonym at LevelDemo. A released team is removed.
type TeamRow struct {
	ID string `json:"id"`
	// Members are the operator IDs of the team.
	Members []string `json:"members"`
	// ActiveLoad is the active load of the company (decision 0009).
	ActiveLoad uint64 `json:"active_load"`
	// FormedAtUS is when the team formed.
	FormedAtUS int64 `json:"formed_at_us"`
}

// RowID implements Row.
func (t TeamRow) RowID() string { return t.ID }

func (t TeamRow) validate(errs *fieldErrors, a Audience) {
	checkLabel(errs, a.Level, "id", t.ID)
	if len(t.Members) == 0 {
		errs.missing("members")
	}
	for i, m := range t.Members {
		checkOperatorID(errs, fmt.Sprintf("members[%d]", i), m)
	}
	if t.FormedAtUS <= 0 {
		errs.missing("formed_at_us")
	}
}

// TextMessage is one message of a watched conversation (decision 0011,
// point 2). At LevelFull it carries the text; at LevelDemo the same row
// without text, so the screen can show a placeholder with length and time
// (decision 0013).
type TextMessage struct {
	// ID is the message ID at LevelFull and its pseudonym at LevelDemo.
	ID string `json:"id"`
	// Direction says whether the customer or the company wrote it. The
	// author is the Customer or the Company of the conversation.
	Direction event.Direction `json:"direction"`
	// SourceUS is the source time of the message.
	SourceUS int64 `json:"source_us"`
	// Length is the length of the text in Unicode code points (in
	// JavaScript: Array.from(text).length).
	Length int `json:"length"`
	// Text is the post text, at LevelFull only, where it is always set
	// (it can be ""). It must not be stored anywhere but in memory
	// (decision 0013).
	Text *string `json:"text,omitempty"`
	// Edited reports that the author edited the message.
	Edited bool `json:"edited"`
}

func (m TextMessage) validate(errs *fieldErrors, a Audience) {
	checkLabel(errs, a.Level, "id", m.ID)
	switch m.Direction {
	case event.FromCustomer, event.FromCompany:
	case "":
		errs.missing("direction")
	default:
		errs.invalid("direction", "%q is not %q or %q", m.Direction, event.FromCustomer, event.FromCompany)
	}
	if m.SourceUS <= 0 {
		errs.missing("source_us")
	}
	if m.Length < 0 {
		errs.invalid("length", "is negative")
	}
	switch {
	case a.Level != LevelFull && m.Text != nil:
		errs.invalid("text", "must be empty at level %q", a.Level)
	case a.Level == LevelFull && m.Text == nil:
		errs.missing("text")
	case m.Text != nil && utf8.RuneCountInString(*m.Text) != m.Length:
		// The error never contains the text (decision 0004).
		errs.invalid("length", "does not match the text")
	}
}

// Totals are the numbers of the flow and of the whole system (decision
// 0013, "Supervisor" and "Public page"). They are in the State message of
// ViewSupervisor and in PublicSnapshot, so they never hold a key or an ID.
type Totals struct {
	// MessagesPerSecond is the rate of messages added to conversations
	// (opened and message_added events) over the last 10 seconds of
	// machine time.
	MessagesPerSecond float64 `json:"messages_per_second"`
	// Conversations counts the open conversations by state.
	Conversations StateCounts `json:"conversations"`
	// CompaniesActive counts companies with at least one open
	// conversation.
	CompaniesActive uint64 `json:"companies_active"`
	// OperatorsOnline counts online operators, HumansOnline the human
	// ones among them.
	OperatorsOnline uint64 `json:"operators_online"`
	HumansOnline    uint64 `json:"humans_online"`
	// SlotsCapacity is the sum of the capacities of online operators, and
	// SlotsUsed the sum of their loads.
	SlotsCapacity uint64 `json:"slots_capacity"`
	SlotsUsed     uint64 `json:"slots_used"`
	// MissedAssignment and MissedFirstResponse count the missed SLA
	// targets of the last hour of machine time.
	MissedAssignment    uint64 `json:"missed_assignment"`
	MissedFirstResponse uint64 `json:"missed_first_response"`
	// WaitP50US and WaitP90US are the median and the 90th percentile of
	// the waiting time until accept (model time) of the accepts of the
	// last hour of machine time, 0 when there was none.
	WaitP50US int64 `json:"wait_p50_us"`
	WaitP90US int64 `json:"wait_p90_us"`
}

// StateCounts counts open conversations by ConversationState.
type StateCounts struct {
	Open     uint64 `json:"open"`
	Waiting  uint64 `json:"waiting"`
	Offered  uint64 `json:"offered"`
	Assigned uint64 `json:"assigned"`
	Resolved uint64 `json:"resolved"`
}

func (t Totals) validate(errs *fieldErrors) {
	if t.MessagesPerSecond < 0 {
		errs.invalid("messages_per_second", "is negative")
	}
	if t.HumansOnline > t.OperatorsOnline {
		errs.invalid("humans_online", "is more than operators_online")
	}
	if t.WaitP50US < 0 || t.WaitP90US < t.WaitP50US {
		errs.invalid("wait_p90_us", "need 0 <= wait_p50_us <= wait_p90_us")
	}
}

func checkLang(errs *fieldErrors, field, l string) {
	switch {
	case l == "":
		errs.missing(field)
	case !event.ValidLang(l):
		errs.invalid(field, "%q is not 2 or 3 lower case letters", l)
	}
}
