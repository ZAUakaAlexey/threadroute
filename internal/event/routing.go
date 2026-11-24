package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// RoutingType is the type of a routing event in the routing topic
// (decisions 0009 and 0010). The router writes routing events, and the
// simulator and the gateway read them.
//
// There are two families:
//
//   - Conversation events (TypeQueued to TypeEnded) are about one session of
//     one conversation. Their record key is ConversationKey(CompanyID,
//     CustomerID), the same key as in the conversations topic.
//   - Team events (TypeTeamFormed, TypeTeamResized, TypeTeamReleased) are
//     about the dedicated team of one company. Their record key is
//     CompanyKey(CompanyID).
type RoutingType string

// Routing event types of the conversation family.
const (
	// TypeQueued: the conversation entered a queue, or moved to another
	// queue. QueueReason and QueuedAt are set.
	TypeQueued RoutingType = "queued"
	// TypeOffered: the router reserved a slot of an operator and offered
	// the conversation to them. OperatorID, OfferID, QueuedAt and AcceptBy
	// are set.
	TypeOffered RoutingType = "offered"
	// TypeAccepted: the operator accepted the offer, the conversation is
	// assigned. OperatorID, OfferID, QueuedAt and Wait are set.
	TypeAccepted RoutingType = "accepted"
	// TypeOfferExpired: the offer ended without an accept. The slot is
	// free again. OperatorID, OfferID, QueuedAt and OfferEndReason are
	// set. A TypeQueued event follows.
	TypeOfferExpired RoutingType = "offer_expired"
	// TypeReplied: the operator sent a reply marker. OperatorID and
	// OfferID are set, and ResponseTime is set on the first reply of the
	// session.
	TypeReplied RoutingType = "replied"
	// TypeResolved: the operator resolved the conversation, the slot is
	// free again. OperatorID and OfferID are set.
	TypeResolved RoutingType = "resolved"
	// TypeSLAMissed: an SLA target was missed and the priority was raised.
	// SLATarget, Measured and Target are set. Priority is the new value.
	TypeSLAMissed RoutingType = "sla_missed"
	// TypeEnded: conv closed the session. Any offer or assignment ended
	// and the slot is free again. EndReason is set. OperatorID and OfferID
	// name the offer or assignment that ended, if there was one.
	TypeEnded RoutingType = "ended"
)

// Routing event types of the team family.
const (
	// TypeTeamFormed: a company got a dedicated team. Members and
	// ActiveLoad are set.
	TypeTeamFormed RoutingType = "team_formed"
	// TypeTeamResized: the team size changed with the load of the company.
	// Members is the new member list.
	TypeTeamResized RoutingType = "team_resized"
	// TypeTeamReleased: the team was released and its members went back
	// to the shared pool. Members is the list of the released operators.
	// ReleaseReason is set.
	TypeTeamReleased RoutingType = "team_released"
)

// IsTeam reports whether t is a type of the team family.
func (t RoutingType) IsTeam() bool {
	return t == TypeTeamFormed || t == TypeTeamResized || t == TypeTeamReleased
}

func (t RoutingType) known() bool {
	switch t {
	case TypeQueued, TypeOffered, TypeAccepted, TypeOfferExpired, TypeReplied,
		TypeResolved, TypeSLAMissed, TypeEnded:
		return true
	}
	return t.IsTeam()
}

// QueueReason says why a conversation entered a queue.
type QueueReason string

// Queue reasons.
const (
	// QueueNew: the first session of the conversation opened.
	QueueNew QueueReason = "new"
	// QueueReopened: a later session opened (decision 0007). The previous
	// operator is preferred.
	QueueReopened QueueReason = "reopened"
	// QueueCustomerMessage: a customer message arrived after the operator
	// resolved the conversation, in the same session. The same operator
	// is preferred.
	QueueCustomerMessage QueueReason = "customer_message_after_resolve"
	// QueueOfferExpired: the accept timeout of an offer passed.
	QueueOfferExpired QueueReason = "offer_expired"
	// QueueOfferRefused: the operator refused the offer.
	QueueOfferRefused QueueReason = "offer_refused"
	// QueueOperatorOffline: the operator who held the offer or the
	// conversation went offline.
	QueueOperatorOffline QueueReason = "operator_offline"
	// QueueTeamFormed: the company got a team, and the waiting
	// conversation moved from the shared queue to the team queue.
	QueueTeamFormed QueueReason = "team_formed"
	// QueueTeamReleased: the team of the company was released, and the
	// waiting conversation moved to the shared queue.
	QueueTeamReleased QueueReason = "team_released"
	// QueueCatchUp: the router finished its catch-up at start, and the
	// conversation was still open (decision 0009, "Catching up").
	QueueCatchUp QueueReason = "catch_up"
)

func (r QueueReason) valid() bool {
	switch r {
	case QueueNew, QueueReopened, QueueCustomerMessage, QueueOfferExpired, QueueOfferRefused,
		QueueOperatorOffline, QueueTeamFormed, QueueTeamReleased, QueueCatchUp:
		return true
	}
	return false
}

// OfferEndReason says why an offer ended without an accept.
type OfferEndReason string

// Offer end reasons.
const (
	// OfferTimeout: the operator did not answer within the accept timeout.
	OfferTimeout OfferEndReason = "timeout"
	// OfferRefused: the operator refused the offer.
	OfferRefused OfferEndReason = "refused"
	// OfferOperatorOffline: the operator went offline before answering.
	OfferOperatorOffline OfferEndReason = "operator_offline"
)

func (r OfferEndReason) valid() bool {
	return r == OfferTimeout || r == OfferRefused || r == OfferOperatorOffline
}

// SLATarget names one of the two SLA targets of a session (decision 0009).
type SLATarget string

// SLA targets.
const (
	// SLAAssignment: time from entering the queue until an operator
	// accepts.
	SLAAssignment SLATarget = "assignment"
	// SLAFirstResponse: time until the first reply of the operator.
	SLAFirstResponse SLATarget = "first_response"
)

func (t SLATarget) valid() bool {
	return t == SLAAssignment || t == SLAFirstResponse
}

// TeamReleaseReason says why a team was released (decision 0009).
type TeamReleaseReason string

// Team release reasons.
const (
	// TeamLowLoad: the active load stayed below the release threshold for
	// the set time.
	TeamLowLoad TeamReleaseReason = "low_load"
	// TeamAdmissionEnded: the admission of the company ended (decision
	// 0008).
	TeamAdmissionEnded TeamReleaseReason = "admission_ended"
)

func (r TeamReleaseReason) valid() bool {
	return r == TeamLowLoad || r == TeamAdmissionEnded
}

// PriorityNormal is the priority of a conversation that has missed no SLA
// target. A higher number is served first.
const PriorityNormal uint32 = 0

// MaxRoutingIDLen is the longest operator ID or offer ID.
const MaxRoutingIDLen = 64

// ValidRoutingID reports whether s can be an operator ID or an offer ID: 1
// to MaxRoutingIDLen ASCII letters, digits, ".", "_" or "-". Both IDs are
// synthetic (operators are simulated), and the operator API puts them into
// URL paths, so the rule keeps them free of characters that need escaping.
func ValidRoutingID(s string) bool {
	if s == "" || len(s) > MaxRoutingIDLen {
		return false
	}
	for i := range len(s) {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

// ValidLang reports whether s is a value NormalizeLang can return: 2 or 3
// lower case ASCII letters. LangUndetermined is one of them.
func ValidLang(s string) bool {
	return validLang(s)
}

// Routing is one event in the routing topic (decisions 0009 and 0010). It
// carries IDs, times, counters and reasons, never post text and never a
// message ID or URI (decision 0004).
//
// # Clocks
//
// The router runs on the machine clock with a speed factor (decision 0009,
// "Clock"). So in this event:
//
//   - Instants (At, QueuedAt, AcceptBy) are machine clock times, not source
//     times. A reader compares them with its own machine clock.
//   - Durations (Wait, ResponseTime, Measured, Target) are model time: the
//     machine time that passed, multiplied by the speed factor. They can be
//     compared directly with the configured SLA targets. With a speed
//     factor of 1 both are the same.
//
// # Order
//
// Seq is given in the same database transaction as the state change
// (decision 0009, "Order of routing events"). For conversation events it
// counts per conversation key, over all sessions, starting at 1. For team
// events it counts per company, starting at 1. A reader skips an event
// whose Seq it has already seen for that key.
//
// Order holds only per record key. Team events and conversation events have
// different keys and can sit in different partitions, so a reader can get
// a TypeQueued event with QueueTeamFormed before the TypeTeamFormed event
// of the same company, or the other way round.
type Routing struct {
	// Type is the kind of change.
	Type RoutingType
	// CompanyID is set for every event. CustomerID is set for conversation
	// events and empty for team events.
	CompanyID  string
	CustomerID string
	// Session is the session number from conv (decision 0007). It is set
	// for conversation events and 0 for team events.
	Session uint64
	// Seq is the sequence number of this event for its record key. See
	// "Order" above.
	Seq uint64
	// At is the machine time of the change.
	At time.Time

	// Lang is the language of the queue of the conversation: the session
	// language from conv. It is set for conversation events.
	Lang string
	// TeamQueue reports whether the queue is the team queue of the
	// company: the queue is then Lang plus CompanyID. When it is false the
	// queue is the shared queue of Lang (decision 0009, "Queues").
	TeamQueue bool
	// Priority is the queue priority of the conversation, PriorityNormal
	// or higher. It is set for conversation events.
	Priority uint32
	// QueuedAt is when the conversation entered its queue. It is kept when
	// an offer ends and the conversation goes back to the queue (decision
	// 0009, "Assignment", step 4), so the waiting time continues. It is
	// set for TypeQueued, TypeOffered, TypeAccepted and TypeOfferExpired,
	// and may be set for TypeSLAMissed.
	QueuedAt time.Time

	// OperatorID is the operator of the offer or the assignment.
	OperatorID string
	// OfferID identifies one offer. The operator API names it in accept
	// and refuse, so a late accept of an old offer cannot accept a newer
	// one. Once accepted, the same ID names the assignment in reply and
	// resolve. OperatorID and OfferID are always set together.
	OfferID string
	// AcceptBy is the machine time at which the offer expires. Set only
	// for TypeOffered, and never before At.
	AcceptBy time.Time
	// Wait is the waiting time from QueuedAt until the accept, in model
	// time. Set only for TypeAccepted, where it is always written, even
	// when it is 0.
	Wait time.Duration
	// QueueReason is set only for TypeQueued.
	QueueReason QueueReason
	// OfferEndReason is set only for TypeOfferExpired.
	OfferEndReason OfferEndReason
	// FirstReply reports that this is the first reply of the operator in
	// this session. Only for TypeReplied.
	FirstReply bool
	// ResponseTime is the time to first response, in model time. It is
	// set only for TypeReplied with FirstReply, where it is always
	// written, even when it is 0.
	ResponseTime time.Duration
	// SLATarget, Measured and Target are set only for TypeSLAMissed.
	// Measured is the time that had passed when the router saw the miss,
	// and Target is the configured target. Measured is never less than
	// Target.
	SLATarget SLATarget
	Measured  time.Duration
	Target    time.Duration
	// EndReason is the close reason from conv. Set only for TypeEnded.
	EndReason CloseReason

	// Members are the operator IDs of the team. Set only for team events,
	// never empty there. For TypeTeamReleased they are the operators that
	// go back to the shared pool.
	Members []string
	// ActiveLoad is the active load of the company that caused the change:
	// its conversations that wait in a queue or are held by an operator
	// (decision 0009, "Which conversations count for a team"). Only for
	// team events. It can be 0 on TypeTeamReleased.
	ActiveLoad uint64
	// ReleaseReason is set only for TypeTeamReleased.
	ReleaseReason TeamReleaseReason
}

// Key returns the record key: ConversationKey(CompanyID, CustomerID) for a
// conversation event and CompanyKey(CompanyID) for a team event.
func (r Routing) Key() []byte {
	if r.Type.IsTeam() {
		return CompanyKey(r.CompanyID)
	}
	return ConversationKey(r.CompanyID, r.CustomerID)
}

// routingJSON is the wire form of Routing.
type routingJSON struct {
	Type           RoutingType `json:"type"`
	CompanyID      string      `json:"company_id"`
	CustomerID     string      `json:"customer_id,omitempty"`
	Session        uint64      `json:"session,omitempty"`
	Seq            uint64      `json:"seq"`
	AtUS           int64       `json:"at_us"`
	Lang           string      `json:"lang,omitempty"`
	TeamQueue      *bool       `json:"team_queue,omitempty"`
	Priority       *uint32     `json:"priority,omitempty"`
	QueuedAtUS     int64       `json:"queued_at_us,omitempty"`
	OperatorID     string      `json:"operator_id,omitempty"`
	OfferID        string      `json:"offer_id,omitempty"`
	AcceptByUS     int64       `json:"accept_by_us,omitempty"`
	WaitUS         *int64      `json:"wait_us,omitempty"`
	QueueReason    string      `json:"queue_reason,omitempty"`
	OfferEndReason string      `json:"offer_end_reason,omitempty"`
	FirstReply     bool        `json:"first_reply,omitempty"`
	ResponseTimeUS *int64      `json:"response_time_us,omitempty"`
	SLATarget      string      `json:"sla_target,omitempty"`
	MeasuredUS     int64       `json:"measured_us,omitempty"`
	TargetUS       int64       `json:"target_us,omitempty"`
	EndReason      string      `json:"end_reason,omitempty"`
	Members        []string    `json:"members,omitempty"`
	ActiveLoad     uint64      `json:"active_load,omitempty"`
	ReleaseReason  string      `json:"release_reason,omitempty"`
}

// routingField is one optional group of fields. Each type allows a fixed
// set of groups, and Validate reports a group that is set on a type that
// does not allow it.
type routingField uint16

const (
	rfQueuedAt routingField = 1 << iota
	rfOffer                 // operator_id and offer_id
	rfAcceptBy
	rfWait
	rfQueueReason
	rfOfferEndReason
	rfReply // first_reply and response_time_us
	rfSLA   // sla_target, measured_us and target_us
	rfEndReason
	rfTeam // members, active_load
	rfReleaseReason
)

// routingFieldNames are the JSON names of each group, for error messages.
var routingFieldNames = []struct {
	field routingField
	name  string
}{
	{rfQueuedAt, "queued_at_us"},
	{rfOffer, "operator_id and offer_id"},
	{rfAcceptBy, "accept_by_us"},
	{rfWait, "wait_us"},
	{rfQueueReason, "queue_reason"},
	{rfOfferEndReason, "offer_end_reason"},
	{rfReply, "first_reply and response_time_us"},
	{rfSLA, "sla_target, measured_us and target_us"},
	{rfEndReason, "end_reason"},
	{rfTeam, "members and active_load"},
	{rfReleaseReason, "release_reason"},
}

// routingAllowed lists the optional groups each type may carry.
var routingAllowed = map[RoutingType]routingField{
	TypeQueued:       rfQueuedAt | rfQueueReason,
	TypeOffered:      rfQueuedAt | rfOffer | rfAcceptBy,
	TypeAccepted:     rfQueuedAt | rfOffer | rfWait,
	TypeOfferExpired: rfQueuedAt | rfOffer | rfOfferEndReason,
	TypeReplied:      rfOffer | rfReply,
	TypeResolved:     rfOffer,
	TypeSLAMissed:    rfQueuedAt | rfOffer | rfSLA,
	TypeEnded:        rfOffer | rfEndReason,
	TypeTeamFormed:   rfTeam,
	TypeTeamResized:  rfTeam,
	TypeTeamReleased: rfTeam | rfReleaseReason,
}

// present returns the optional groups that have a value. Wait and
// ResponseTime count as present only when not 0, because 0 is a valid value
// that Validate cannot tell from "not set".
func (r Routing) present() routingField {
	var f routingField
	set := func(ok bool, g routingField) {
		if ok {
			f |= g
		}
	}
	set(!r.QueuedAt.IsZero(), rfQueuedAt)
	set(r.OperatorID != "" || r.OfferID != "", rfOffer)
	set(!r.AcceptBy.IsZero(), rfAcceptBy)
	set(r.Wait != 0, rfWait)
	set(r.QueueReason != "", rfQueueReason)
	set(r.OfferEndReason != "", rfOfferEndReason)
	set(r.FirstReply || r.ResponseTime != 0, rfReply)
	set(r.SLATarget != "" || r.Measured != 0 || r.Target != 0, rfSLA)
	set(r.EndReason != "", rfEndReason)
	set(len(r.Members) > 0 || r.ActiveLoad != 0, rfTeam)
	set(r.ReleaseReason != "", rfReleaseReason)
	return f
}

// Validate reports every missing or invalid field.
func (r Routing) Validate() error {
	var errs fieldErrors
	errs.requireString("company_id", r.CompanyID)
	if r.Seq == 0 {
		errs.missing("seq")
	}
	errs.requireTime("at_us", r.At)

	switch {
	case r.Type == "":
		errs.missing("type")
		return errs.join()
	case !r.Type.known():
		errs = append(errs, fmt.Errorf("type %q: %w", r.Type, ErrUnknownKind))
		return errs.join()
	case r.Type.IsTeam():
		r.validateTeam(&errs)
	default:
		r.validateConversation(&errs)
	}

	extra := r.present() &^ routingAllowed[r.Type]
	for _, g := range routingFieldNames {
		if extra&g.field != 0 {
			errs.invalid(g.name, "must be empty for type %q", r.Type)
		}
	}
	return errs.join()
}

// validateTeam checks the fields of a team event.
func (r Routing) validateTeam(errs *fieldErrors) {
	if r.CustomerID != "" {
		errs.invalid("customer_id", "must be empty for type %q", r.Type)
	}
	if r.Session != 0 {
		errs.invalid("session", "must be empty for type %q", r.Type)
	}
	if r.Lang != "" {
		errs.invalid("lang", "must be empty for type %q", r.Type)
	}
	if r.TeamQueue {
		errs.invalid("team_queue", "must be empty for type %q", r.Type)
	}
	if r.Priority != PriorityNormal {
		errs.invalid("priority", "must be empty for type %q", r.Type)
	}
	if len(r.Members) == 0 {
		errs.missing("members")
	}
	seen := make(map[string]bool, len(r.Members))
	for _, m := range r.Members {
		switch {
		case !ValidRoutingID(m):
			errs.invalid("members", "%q is not a valid operator ID", m)
		case seen[m]:
			errs.invalid("members", "%q is listed twice", m)
		}
		seen[m] = true
	}
	if r.Type == TypeTeamReleased {
		switch {
		case r.ReleaseReason == "":
			errs.missing("release_reason")
		case !r.ReleaseReason.valid():
			errs.invalid("release_reason", "%q is not a known reason", r.ReleaseReason)
		}
	}
}

// validateConversation checks the fields of a conversation event.
func (r Routing) validateConversation(errs *fieldErrors) {
	errs.requireString("customer_id", r.CustomerID)
	if r.CompanyID != "" && r.CompanyID == r.CustomerID {
		errs.invalid("customer_id", "equals company_id")
	}
	if r.Session == 0 {
		errs.missing("session")
	}
	switch {
	case r.Lang == "":
		errs.missing("lang")
	case !validLang(r.Lang):
		errs.invalid("lang", "%q is not 2 or 3 lower case letters", r.Lang)
	}

	allowed := routingAllowed[r.Type]
	// The offer fields come as a pair and are required where the type
	// always has an operator.
	needOffer := r.Type == TypeOffered || r.Type == TypeAccepted || r.Type == TypeOfferExpired ||
		r.Type == TypeReplied || r.Type == TypeResolved
	if allowed&rfOffer != 0 && (needOffer || r.OperatorID != "" || r.OfferID != "") {
		switch {
		case r.OperatorID == "":
			errs.missing("operator_id")
		case !ValidRoutingID(r.OperatorID):
			errs.invalid("operator_id", "%q is not a valid ID", r.OperatorID)
		}
		switch {
		case r.OfferID == "":
			errs.missing("offer_id")
		case !ValidRoutingID(r.OfferID):
			errs.invalid("offer_id", "%q is not a valid ID", r.OfferID)
		}
	}
	if r.Type == TypeQueued || r.Type == TypeOffered || r.Type == TypeAccepted || r.Type == TypeOfferExpired {
		errs.requireTime("queued_at_us", r.QueuedAt)
	}
	if !r.QueuedAt.IsZero() && !r.At.IsZero() && r.QueuedAt.After(r.At) {
		errs.invalid("queued_at_us", "is after at_us")
	}

	switch r.Type {
	case TypeQueued:
		switch {
		case r.QueueReason == "":
			errs.missing("queue_reason")
		case !r.QueueReason.valid():
			errs.invalid("queue_reason", "%q is not a known reason", r.QueueReason)
		}
	case TypeOffered:
		errs.requireTime("accept_by_us", r.AcceptBy)
		if !r.AcceptBy.IsZero() && r.AcceptBy.Before(r.At) {
			errs.invalid("accept_by_us", "is before at_us")
		}
	case TypeAccepted:
		if r.Wait < 0 {
			errs.invalid("wait_us", "is negative")
		}
	case TypeOfferExpired:
		switch {
		case r.OfferEndReason == "":
			errs.missing("offer_end_reason")
		case !r.OfferEndReason.valid():
			errs.invalid("offer_end_reason", "%q is not a known reason", r.OfferEndReason)
		}
	case TypeReplied:
		if r.ResponseTime < 0 {
			errs.invalid("response_time_us", "is negative")
		}
		if !r.FirstReply && r.ResponseTime != 0 {
			errs.invalid("response_time_us", "must be empty when first_reply is false")
		}
	case TypeSLAMissed:
		switch {
		case r.SLATarget == "":
			errs.missing("sla_target")
		case !r.SLATarget.valid():
			errs.invalid("sla_target", "%q is not a known target", r.SLATarget)
		}
		if r.Target <= 0 {
			errs.invalid("target_us", "is not positive")
		}
		if r.Measured < r.Target {
			errs.invalid("measured_us", "is less than target_us")
		}
	case TypeEnded:
		switch {
		case r.EndReason == "":
			errs.missing("end_reason")
		case !r.EndReason.valid():
			errs.invalid("end_reason", "%q is not a known reason", r.EndReason)
		}
	}
}

// MarshalRouting validates and encodes a routing event. Times and durations
// are written in whole microseconds. TeamQueue and Priority are always
// written for conversation events, Wait for TypeAccepted, and ResponseTime
// for TypeReplied with FirstReply, even when they are false or 0.
func MarshalRouting(r Routing) ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, fmt.Errorf("marshal routing event: %w", err)
	}
	j := routingJSON{
		Type:           r.Type,
		CompanyID:      r.CompanyID,
		CustomerID:     r.CustomerID,
		Session:        r.Session,
		Seq:            r.Seq,
		AtUS:           micros(r.At),
		Lang:           r.Lang,
		QueuedAtUS:     micros(r.QueuedAt),
		OperatorID:     r.OperatorID,
		OfferID:        r.OfferID,
		AcceptByUS:     micros(r.AcceptBy),
		QueueReason:    string(r.QueueReason),
		OfferEndReason: string(r.OfferEndReason),
		FirstReply:     r.FirstReply,
		SLATarget:      string(r.SLATarget),
		MeasuredUS:     r.Measured.Microseconds(),
		TargetUS:       r.Target.Microseconds(),
		EndReason:      string(r.EndReason),
		Members:        r.Members,
		ActiveLoad:     r.ActiveLoad,
		ReleaseReason:  string(r.ReleaseReason),
	}
	if !r.Type.IsTeam() {
		team, priority := r.TeamQueue, r.Priority
		j.TeamQueue, j.Priority = &team, &priority
	}
	if r.Type == TypeAccepted {
		us := r.Wait.Microseconds()
		j.WaitUS = &us
	}
	if r.Type == TypeReplied && r.FirstReply {
		us := r.ResponseTime.Microseconds()
		j.ResponseTimeUS = &us
	}
	b, err := json.Marshal(j)
	if err != nil {
		return nil, fmt.Errorf("marshal routing event: %w", err)
	}
	return b, nil
}

// UnmarshalRouting decodes and validates a routing event. An unknown type
// gives an error that wraps ErrUnknownKind. Unknown fields are ignored.
func UnmarshalRouting(b []byte) (Routing, error) {
	var j routingJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return Routing{}, fmt.Errorf("unmarshal routing event: %w", err)
	}
	if j.Type != "" && !j.Type.known() {
		return Routing{}, fmt.Errorf("unmarshal routing event: type %q: %w", j.Type, ErrUnknownKind)
	}
	r := Routing{
		Type:           j.Type,
		CompanyID:      j.CompanyID,
		CustomerID:     j.CustomerID,
		Session:        j.Session,
		Seq:            j.Seq,
		At:             fromMicros(j.AtUS),
		Lang:           j.Lang,
		QueuedAt:       fromMicros(j.QueuedAtUS),
		OperatorID:     j.OperatorID,
		OfferID:        j.OfferID,
		AcceptBy:       fromMicros(j.AcceptByUS),
		QueueReason:    QueueReason(j.QueueReason),
		OfferEndReason: OfferEndReason(j.OfferEndReason),
		FirstReply:     j.FirstReply,
		SLATarget:      SLATarget(j.SLATarget),
		Measured:       time.Duration(j.MeasuredUS) * time.Microsecond,
		Target:         time.Duration(j.TargetUS) * time.Microsecond,
		EndReason:      CloseReason(j.EndReason),
		Members:        j.Members,
		ActiveLoad:     j.ActiveLoad,
		ReleaseReason:  TeamReleaseReason(j.ReleaseReason),
	}
	if j.TeamQueue != nil {
		r.TeamQueue = *j.TeamQueue
	}
	if j.Priority != nil {
		r.Priority = *j.Priority
	}
	if j.WaitUS != nil {
		r.Wait = time.Duration(*j.WaitUS) * time.Microsecond
	}
	if j.ResponseTimeUS != nil {
		r.ResponseTime = time.Duration(*j.ResponseTimeUS) * time.Microsecond
	}
	err := r.Validate()
	if r.Type == TypeAccepted && j.WaitUS == nil {
		err = errors.Join(err, errors.New("wait_us is not set"))
	}
	if r.Type == TypeReplied && r.FirstReply && j.ResponseTimeUS == nil {
		err = errors.Join(err, errors.New("response_time_us is not set"))
	}
	if err != nil {
		return Routing{}, fmt.Errorf("unmarshal routing event: %w", err)
	}
	return r, nil
}
