package gwapi

import (
	"encoding/json"
	"fmt"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

// ServerType is the "type" of a server message.
type ServerType string

// Server message types.
const (
	TypeWelcome        ServerType = "welcome"
	TypeStatus         ServerType = "status"
	TypeState          ServerType = "state"
	TypeMessages       ServerType = "messages"
	TypeMessageRemoved ServerType = "message_removed"
	TypeResult         ServerType = "result"
)

// ServerMessage is a message from the gateway to the browser: Welcome,
// Status, State, Messages, MessageRemoved or Result.
//
// Order on one connection: Welcome first, then Status, then a State
// snapshot. After that State messages (at most one per Tick), Status on
// every change, and Messages, MessageRemoved and Result as they happen.
type ServerMessage interface {
	// ServerType returns the "type" of the message.
	ServerType() ServerType
	// Validate reports every missing or invalid field, and every field
	// that the audience may not get (decision 0012).
	Validate(a Audience) error
}

// Welcome answers a valid Hello.
type Welcome struct {
	// Version is ProtocolVersion.
	Version int `json:"version"`
	// Level is the access level the gateway gave the connection.
	Level Level `json:"level"`
	// View repeats the view of the Hello.
	View View `json:"view"`
	// OperatorID is the operator the gateway created for the connection,
	// for ViewOperator only. The client never sends an operator ID
	// (decision 0012, point 4). A new operator is offline.
	OperatorID string `json:"operator_id,omitempty"`
	// ResumeToken lets a reload of the page get the same operator back
	// within OfflineGrace (Hello.ResumeToken), for ViewOperator only. The
	// client keeps it for the browser tab only (session storage).
	ResumeToken string `json:"resume_token,omitempty"`
	// ServerTimeUS is the machine clock of the gateway when it sent the
	// message. The client keeps the offset to its own clock for the
	// timers (decision 0013, "Timers run in the browser").
	ServerTimeUS int64 `json:"server_time_us"`
	// SpeedFactor is the speed factor of the router (decision 0009).
	// Model time = machine time * SpeedFactor.
	SpeedFactor float64 `json:"speed_factor"`
	// AcceptTimeoutUS, AssignmentTargetUS and FirstResponseTargetUS are
	// the accept timeout and the SLA targets, in model time.
	AcceptTimeoutUS       int64 `json:"accept_timeout_us"`
	AssignmentTargetUS    int64 `json:"assignment_target_us"`
	FirstResponseTargetUS int64 `json:"first_response_target_us"`
	// TickMS is the shortest time between two State messages.
	TickMS int `json:"tick_ms"`
	// Limits are the limits of this connection.
	Limits Limits `json:"limits"`
}

// Limits are the limits the server enforces on one connection.
type Limits struct {
	MaxMessageBytes  int `json:"max_message_bytes"`
	ActionsPerSecond int `json:"actions_per_second"`
	ActionBurst      int `json:"action_burst"`
	MaxWatched       int `json:"max_watched"`
}

// DefaultLimits returns the limits of this package.
func DefaultLimits() Limits {
	return Limits{
		MaxMessageBytes:  MaxClientMessageBytes,
		ActionsPerSecond: ActionsPerSecond,
		ActionBurst:      ActionBurst,
		MaxWatched:       MaxWatched,
	}
}

// ServerType implements ServerMessage.
func (Welcome) ServerType() ServerType { return TypeWelcome }

// Validate implements ServerMessage.
func (w Welcome) Validate(a Audience) error {
	var errs fieldErrors
	if w.Version != ProtocolVersion {
		errs.invalid("version", "is %d, want %d", w.Version, ProtocolVersion)
	}
	if w.Level != a.Level || !w.Level.valid() {
		errs.invalid("level", "%q is not the level of the audience", w.Level)
	}
	if w.View != a.View || !w.View.valid() {
		errs.invalid("view", "%q is not the view of the audience", w.View)
	}
	switch w.View {
	case ViewOperator:
		checkOperatorID(&errs, "operator_id", w.OperatorID)
		if w.OperatorID != "" && KindOf(w.OperatorID) != KindHuman {
			errs.invalid("operator_id", "does not start with %q", HumanOperatorPrefix)
		}
		if !validToken(w.ResumeToken, MaxResumeTokenBytes) {
			errs.invalid("resume_token", "is not a valid resume token")
		}
	default:
		if w.OperatorID != "" || w.ResumeToken != "" {
			errs.invalid("operator_id and resume_token", "must be empty for view %q", w.View)
		}
	}
	if w.ServerTimeUS <= 0 {
		errs.missing("server_time_us")
	}
	if !(w.SpeedFactor > 0) {
		errs.invalid("speed_factor", "is not positive")
	}
	if w.AcceptTimeoutUS <= 0 || w.AssignmentTargetUS <= 0 || w.FirstResponseTargetUS <= 0 {
		errs.invalid("accept_timeout_us, assignment_target_us and first_response_target_us", "must be positive")
	}
	if w.TickMS <= 0 {
		errs.missing("tick_ms")
	}
	l := w.Limits
	if l.MaxMessageBytes <= 0 || l.ActionsPerSecond <= 0 || l.ActionBurst <= 0 || l.MaxWatched <= 0 {
		errs.invalid("limits", "every limit must be positive")
	}
	return errs.join()
}

// StaleReason says why the data of the gateway is stale.
type StaleReason string

// Stale reasons.
const (
	// StaleCatchingUp: the gateway reads the topics from the beginning
	// after its start and has not reached their end.
	StaleCatchingUp StaleReason = "catching_up"
	// StaleSourceUnavailable: the gateway cannot read the topics.
	StaleSourceUnavailable StaleReason = "source_unavailable"
	// StaleLagging: the newest event the gateway read is older than its
	// limit.
	StaleLagging StaleReason = "lagging"
)

func (r StaleReason) valid() bool {
	return r == StaleCatchingUp || r == StaleSourceUnavailable || r == StaleLagging
}

// Status says whether the data on the screen is current (decision 0011,
// "Outages", and decision 0013, "Stale data is shown as stale"). The server
// sends it after Welcome and on every change. While Stale is true the
// screens say so and stop the timers. State messages may still arrive.
type Status struct {
	Stale bool `json:"stale"`
	// Reason is set when Stale is true.
	Reason StaleReason `json:"reason,omitempty"`
	// LagUS is how far the gateway is behind the topics, in machine time,
	// when it knows it. Only when Stale is true.
	LagUS int64 `json:"lag_us,omitempty"`
}

// ServerType implements ServerMessage.
func (Status) ServerType() ServerType { return TypeStatus }

// Validate implements ServerMessage.
func (s Status) Validate(Audience) error {
	var errs fieldErrors
	switch {
	case s.Stale && s.Reason == "":
		errs.missing("reason")
	case s.Stale && !s.Reason.valid():
		errs.invalid("reason", "%q is not a known reason", s.Reason)
	case !s.Stale && (s.Reason != "" || s.LagUS != 0):
		errs.invalid("reason and lag_us", "must be empty when stale is false")
	}
	if s.LagUS < 0 {
		errs.invalid("lag_us", "is negative")
	}
	return errs.join()
}

// State carries the current state of the keys that changed since the last
// State message (decision 0011, point 4). It never carries a list of steps.
//
// With Snapshot true the client drops its whole view and builds it again
// from this message: a collection that is absent is empty, and no Remove
// list is set. The first State of a connection is a snapshot. The server
// sends another snapshot when it rebuilt its view, and at LevelDemo when
// the pseudonym key of the day changes (every ID changes then). A snapshot
// ends every Watch of the connection: the client sends Watch again for a
// conversation it still shows, under its ID in the snapshot.
//
// With Snapshot false the client applies each collection: it replaces the
// objects in Set by ID and drops the IDs in Remove. A collection that is
// absent did not change.
//
// ViewOperator gets Me and MyConversations. ViewSupervisor gets
// Conversations, Operators, Queues, Teams and Totals. A snapshot carries
// Me for ViewOperator and Totals for ViewSupervisor.
type State struct {
	// Seq counts the State messages of the connection, from 1. The first
	// one is a snapshot.
	Seq uint64 `json:"seq"`
	// Snapshot marks a full replacement of the view.
	Snapshot bool `json:"snapshot"`
	// ServerTimeUS is the machine clock of the gateway when it built the
	// message. The client may correct its clock offset with it.
	ServerTimeUS int64 `json:"server_time_us"`

	// Me is the operator of the connection, when it changed.
	Me *OperatorRow `json:"me,omitempty"`
	// MyConversations are the offers and assignments of the operator of
	// the connection.
	MyConversations *Changes[OperatorConversation] `json:"my_conversations,omitempty"`

	Conversations *Changes[ConversationRow] `json:"conversations,omitempty"`
	Operators     *Changes[OperatorRow]     `json:"operators,omitempty"`
	Queues        *Changes[QueueRow]        `json:"queues,omitempty"`
	Teams         *Changes[TeamRow]         `json:"teams,omitempty"`
	// Totals is set when a total changed.
	Totals *Totals `json:"totals,omitempty"`
}

// ServerType implements ServerMessage.
func (State) ServerType() ServerType { return TypeState }

// Validate implements ServerMessage.
func (s State) Validate(a Audience) error {
	var errs fieldErrors
	if s.Seq == 0 {
		errs.missing("seq")
	}
	if s.Seq == 1 && !s.Snapshot {
		errs.invalid("snapshot", "must be true for seq 1")
	}
	if s.ServerTimeUS <= 0 {
		errs.missing("server_time_us")
	}
	operator := s.Me != nil || s.MyConversations != nil
	supervisor := s.Conversations != nil || s.Operators != nil || s.Queues != nil || s.Teams != nil || s.Totals != nil
	switch a.View {
	case ViewOperator:
		if supervisor {
			errs.invalid("conversations, operators, queues, teams and totals", "must be empty for view %q", a.View)
		}
		if s.Snapshot && s.Me == nil {
			errs.missing("me")
		}
	case ViewSupervisor:
		if operator {
			errs.invalid("me and my_conversations", "must be empty for view %q", a.View)
		}
		if s.Snapshot && s.Totals == nil {
			errs.missing("totals")
		}
	default:
		errs.invalid("view", "%q is not a known view", a.View)
	}
	if s.Me != nil {
		errs.within("me", func(e *fieldErrors) { s.Me.validate(e, a) })
		if s.Me.Kind != KindHuman {
			errs.invalid("me.kind", "must be %q", KindHuman)
		}
	}
	s.MyConversations.validate(&errs, a, "my_conversations", s.Snapshot)
	s.Conversations.validate(&errs, a, "conversations", s.Snapshot)
	s.Operators.validate(&errs, a, "operators", s.Snapshot)
	s.Queues.validate(&errs, a, "queues", s.Snapshot)
	s.Teams.validate(&errs, a, "teams", s.Snapshot)
	if s.Totals != nil {
		errs.within("totals", s.Totals.validate)
	}
	return errs.join()
}

// Messages carries the messages of a watched conversation. With Snapshot
// true it is the whole list, ordered by source time, and the client
// replaces what it holds for the conversation: a late message can sort
// before messages the client already shows (decision 0007). With Snapshot
// false it adds or replaces single messages by ID (a new message, or an
// edit), and the client sorts them in by SourceUS. The first Messages after
// a Watch is a snapshot.
//
// The server reads the text when it sends this message and keeps no copy
// (decision 0011, point 2). A message whose text is gone is not in a
// snapshot at all.
type Messages struct {
	ConversationID string `json:"conversation_id"`
	Snapshot       bool   `json:"snapshot"`
	// Messages is never null; a snapshot without stored text is [].
	Messages []TextMessage `json:"messages"`
}

// ServerType implements ServerMessage.
func (Messages) ServerType() ServerType { return TypeMessages }

// Validate implements ServerMessage.
func (m Messages) Validate(a Audience) error {
	var errs fieldErrors
	checkConversationRef(&errs, a.Level, "conversation_id", m.ConversationID)
	switch {
	case m.Messages == nil:
		// JSON null; a snapshot of a conversation without stored text
		// is an empty list.
		errs.missing("messages")
	case !m.Snapshot && len(m.Messages) == 0:
		errs.invalid("messages", "must not be empty in an update")
	}
	seen := make(map[string]bool, len(m.Messages))
	for i, msg := range m.Messages {
		errs.within(fmt.Sprintf("messages[%d]", i), func(e *fieldErrors) { msg.validate(e, a) })
		if seen[msg.ID] {
			errs.invalid("messages", "ID at [%d] is listed twice", i)
		}
		seen[msg.ID] = true
		if m.Snapshot && i > 0 && msg.SourceUS < m.Messages[i-1].SourceUS {
			errs.invalid("messages", "are not ordered by source_us at [%d]", i)
		}
	}
	return errs.join()
}

// MessageRemoved tells the client that the author deleted a message of a
// watched conversation, or was removed (decisions 0004 and 0011). The
// client drops the text from memory at once and shows a placeholder
// (decision 0013). It is sent at every level.
type MessageRemoved struct {
	ConversationID string `json:"conversation_id"`
	MessageID      string `json:"message_id"`
	// Reason is why the message went.
	Reason event.RemoveReason `json:"reason"`
}

// ServerType implements ServerMessage.
func (MessageRemoved) ServerType() ServerType { return TypeMessageRemoved }

// Validate implements ServerMessage.
func (m MessageRemoved) Validate(a Audience) error {
	var errs fieldErrors
	checkConversationRef(&errs, a.Level, "conversation_id", m.ConversationID)
	checkLabel(&errs, a.Level, "message_id", m.MessageID)
	switch m.Reason {
	case event.RemoveDeleted, event.RemoveAuthorRemoved:
	case "":
		errs.missing("reason")
	default:
		errs.invalid("reason", "%q is not a known reason", m.Reason)
	}
	return errs.join()
}

// Outcome is the outcome of a request in a Result.
type Outcome string

// Outcomes (decision 0010, "Three outcomes").
const (
	// OutcomeDone: the request changed the state.
	OutcomeDone Outcome = "done"
	// OutcomeAlreadyDone: the same request was applied before; nothing
	// changed.
	OutcomeAlreadyDone Outcome = "already_done"
	// OutcomeRejected: the request was not applied. Code says why.
	OutcomeRejected Outcome = "rejected"
)

// Result answers one request (Action, Watch or Unwatch). Every request
// gets exactly one Result with its RequestID. Results can come in another
// order than the requests.
type Result struct {
	RequestID string  `json:"request_id"`
	Outcome   Outcome `json:"outcome"`
	// Code is set only for OutcomeRejected.
	Code ErrorCode `json:"code,omitempty"`
	// Message is for people and logs, only for OutcomeRejected. It never
	// holds post text, a token or an author ID.
	Message string `json:"message,omitempty"`
}

// ServerType implements ServerMessage.
func (Result) ServerType() ServerType { return TypeResult }

// Validate implements ServerMessage.
func (r Result) Validate(Audience) error {
	var errs fieldErrors
	checkRequestID(&errs, r.RequestID)
	switch r.Outcome {
	case OutcomeDone, OutcomeAlreadyDone:
		if r.Code != "" || r.Message != "" {
			errs.invalid("code and message", "must be empty for outcome %q", r.Outcome)
		}
	case OutcomeRejected:
		switch {
		case r.Code == "":
			errs.missing("code")
		case !r.Code.Valid():
			errs.invalid("code", "%q is not a known code", r.Code)
		}
	case "":
		errs.missing("outcome")
	default:
		errs.invalid("outcome", "%q is not a known outcome", r.Outcome)
	}
	return errs.join()
}

// EncodeServer validates a server message for its audience and encodes it
// with its "type". The gateway sends nothing that fails here.
func EncodeServer(m ServerMessage, a Audience) ([]byte, error) {
	if err := validateFor(m, a); err != nil {
		return nil, fmt.Errorf("encode server message %s: %w", m.ServerType(), err)
	}
	b, err := withType(string(m.ServerType()), m)
	if err != nil {
		return nil, fmt.Errorf("encode server message %s: %w", m.ServerType(), err)
	}
	return b, nil
}

// DecodeServer decodes and validates one server message for an audience.
// An unknown type gives an error that wraps ErrUnknownType, which a client
// skips. The gateway does not need it; tests and Go clients do.
func DecodeServer(b []byte, a Audience) (ServerMessage, error) {
	var env struct {
		Type ServerType `json:"type"`
	}
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, fmt.Errorf("decode server message: %w", err)
	}
	var m ServerMessage
	var err error
	switch env.Type {
	case TypeWelcome:
		m, err = decodeAs[Welcome](b)
	case TypeStatus:
		m, err = decodeAs[Status](b)
	case TypeState:
		m, err = decodeAs[State](b)
	case TypeMessages:
		m, err = decodeAs[Messages](b)
	case TypeMessageRemoved:
		m, err = decodeAs[MessageRemoved](b)
	case TypeResult:
		m, err = decodeAs[Result](b)
	case "":
		return nil, fmt.Errorf("decode server message: type is not set")
	default:
		return nil, fmt.Errorf("decode server message: type %q: %w", env.Type, ErrUnknownType)
	}
	if err != nil {
		return nil, fmt.Errorf("decode server message %s: %w", env.Type, err)
	}
	if err := validateFor(m, a); err != nil {
		return nil, fmt.Errorf("decode server message %s: %w", env.Type, err)
	}
	return m, nil
}

// validateFor checks the audience and then the message for it.
func validateFor(m ServerMessage, a Audience) error {
	var errs fieldErrors
	a.validate(&errs)
	if err := errs.join(); err != nil {
		return err
	}
	return m.Validate(a)
}
