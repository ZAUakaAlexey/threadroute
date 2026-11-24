package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ConversationType is the type of a conversation event (decision 0007).
//
// This package does not decide which messages produce which type. In
// particular, whether a company message that answers the customer gives one
// TypeAnswered event or a TypeMessageAdded event as well is up to conv. Every
// type that is caused by a message carries the message ID and direction, so
// both choices fit this format.
type ConversationType string

// Conversation event types.
const (
	// TypeOpened: the first customer message of a session.
	TypeOpened ConversationType = "opened"
	// TypeMessageAdded: a later message of the session.
	TypeMessageAdded ConversationType = "message_added"
	// TypeAnswered: the company answered the customer. ResponseTime is set.
	TypeAnswered ConversationType = "answered"
	// TypeClosed: the session ended. CloseReason is set.
	TypeClosed ConversationType = "closed"
	// TypeMessageRemoved: conv removed the stored text of one message
	// because its author deleted it or the author was removed (decisions
	// 0004 and 0011). MessageID, Direction and RemoveReason are set. The
	// event carries no text. The counters do not change (decision 0007).
	//
	// conv publishes it only when it dropped a text that was stored. A
	// deletion that arrives before its message, or a removal of a message
	// whose text was never stored, gives no event. Text stays in Redis for
	// a while after its session closed (decision 0007), so this event can
	// follow TypeClosed of the same session. Session and the other
	// conversation fields describe the conversation as conv holds it at
	// that moment: Session is the current session, which can be later than
	// the session of the removed message.
	TypeMessageRemoved ConversationType = "message_removed"
)

// RemoveReason says why the text of a message was removed.
type RemoveReason string

// Remove reasons.
const (
	// RemoveDeleted: the author deleted the message (a Deletion control
	// event).
	RemoveDeleted RemoveReason = "deleted"
	// RemoveAuthorRemoved: the author was removed (an AuthorRemoval control
	// event), so every stored message of the author went.
	RemoveAuthorRemoved RemoveReason = "author_removed"
)

func (r RemoveReason) valid() bool {
	return r == RemoveDeleted || r == RemoveAuthorRemoved
}

// CloseReason says why a session was closed.
type CloseReason string

// Close reasons.
const (
	// CloseInactivity: no new message for the inactivity timeout (decision
	// 0007).
	CloseInactivity CloseReason = "inactivity"
	// CloseCustomerRemoved: an author removal of the customer (decision
	// 0007).
	CloseCustomerRemoved CloseReason = "customer_removed"
	// CloseCompanyRemoved: an author removal of the company.
	CloseCompanyRemoved CloseReason = "company_removed"
)

func (r CloseReason) valid() bool {
	return r == CloseInactivity || r == CloseCustomerRemoved || r == CloseCompanyRemoved
}

// Conversation is one event in the conversations topic (decision 0007). Its
// record key is ConversationKey(CompanyID, CustomerID). It carries IDs,
// language, counters and times, never post text (decision 0004), so a
// consumer can act on it without reading Redis.
type Conversation struct {
	// Type is the kind of change.
	Type ConversationType
	// Channel is the source of the conversation.
	Channel Channel
	// CompanyID and CustomerID are the two parts of the conversation key.
	CompanyID  string
	CustomerID string
	// Session is the session number, starting at 1. A conversation that is
	// reopened after it closed starts a new session with a higher number
	// (decision 0007). See Reopened.
	Session uint64
	// Seq is the sequence number of this event in its conversation. It
	// starts at 1 and grows by exactly one with each event conv publishes
	// for the conversation, so a consumer can order events and find gaps
	// and repeats.
	Seq uint64
	// SourceTime is the source time of the change: the witnessed time of
	// the message that caused it, or for a close the source time at which
	// conv decided it.
	SourceTime time.Time
	// OpenedAt is the source time at which the conversation opened. Whether
	// this is the start of the current session or of the first session is
	// up to conv and is not decided yet.
	OpenedAt time.Time
	// Lang is the normalised language of the conversation, a value that
	// NormalizeLang can return.
	Lang string
	// CustomerMessages and CompanyMessages count the messages by direction.
	// A deletion does not lower them (decision 0007). Whether they count
	// the current session or every session is up to conv and is not
	// decided yet.
	CustomerMessages uint64
	CompanyMessages  uint64
	// MessageID and Direction name the message that caused the event, or
	// for TypeMessageRemoved the message whose text was removed. They are
	// set for every type except TypeClosed, and empty for TypeClosed.
	MessageID string
	Direction Direction
	// ResponseTime is the real response time: the time since the
	// customer's first unanswered message (decision 0007). It is set only
	// for TypeAnswered, with microsecond precision, and is never negative.
	ResponseTime time.Duration
	// CloseReason is set only for TypeClosed.
	CloseReason CloseReason
	// RemoveReason is set only for TypeMessageRemoved.
	RemoveReason RemoveReason
}

// Reopened reports whether the event belongs to a later session of the
// conversation, that is a session after the first one.
func (c Conversation) Reopened() bool {
	return c.Session > 1
}

// Key returns the record key: ConversationKey(CompanyID, CustomerID).
func (c Conversation) Key() []byte {
	return ConversationKey(c.CompanyID, c.CustomerID)
}

// conversationJSON is the wire form of Conversation.
type conversationJSON struct {
	Type             ConversationType `json:"type"`
	Channel          Channel          `json:"channel"`
	CompanyID        string           `json:"company_id"`
	CustomerID       string           `json:"customer_id"`
	Session          uint64           `json:"session"`
	Seq              uint64           `json:"seq"`
	SourceTimeUS     int64            `json:"source_time_us"`
	OpenedAtUS       int64            `json:"opened_at_us"`
	Lang             string           `json:"lang"`
	CustomerMessages uint64           `json:"customer_messages"`
	CompanyMessages  uint64           `json:"company_messages"`
	MessageID        string           `json:"message_id,omitempty"`
	Direction        string           `json:"direction,omitempty"`
	ResponseTimeUS   *int64           `json:"response_time_us,omitempty"`
	CloseReason      string           `json:"close_reason,omitempty"`
	RemoveReason     string           `json:"remove_reason,omitempty"`
}

// Validate reports every missing or invalid field.
func (c Conversation) Validate() error {
	var errs fieldErrors
	errs.requireString("channel", string(c.Channel))
	errs.requireString("company_id", c.CompanyID)
	errs.requireString("customer_id", c.CustomerID)
	if c.CompanyID != "" && c.CompanyID == c.CustomerID {
		errs.invalid("customer_id", "equals company_id")
	}
	if c.Session == 0 {
		errs.missing("session")
	}
	if c.Seq == 0 {
		errs.missing("seq")
	}
	errs.requireTime("source_time_us", c.SourceTime)
	errs.requireTime("opened_at_us", c.OpenedAt)
	switch {
	case c.Lang == "":
		errs.missing("lang")
	case !validLang(c.Lang):
		errs.invalid("lang", "%q is not 2 or 3 lower case letters", c.Lang)
	}
	if c.ResponseTime < 0 {
		errs.invalid("response_time_us", "is negative")
	}

	switch c.Type {
	case "":
		errs.missing("type")
	case TypeOpened:
		c.validateMessage(&errs, FromCustomer)
	case TypeMessageAdded:
		c.validateMessage(&errs, "")
	case TypeAnswered:
		c.validateMessage(&errs, FromCompany)
	case TypeClosed:
		if c.MessageID != "" {
			errs.invalid("message_id", "must be empty for type %q", TypeClosed)
		}
		if c.Direction != "" {
			errs.invalid("direction", "must be empty for type %q", TypeClosed)
		}
		switch {
		case c.CloseReason == "":
			errs.missing("close_reason")
		case !c.CloseReason.valid():
			errs.invalid("close_reason", "%q is not a known reason", c.CloseReason)
		}
	case TypeMessageRemoved:
		c.validateMessage(&errs, "")
		switch {
		case c.RemoveReason == "":
			errs.missing("remove_reason")
		case !c.RemoveReason.valid():
			errs.invalid("remove_reason", "%q is not a known reason", c.RemoveReason)
		}
	default:
		errs = append(errs, fmt.Errorf("type %q: %w", c.Type, ErrUnknownKind))
	}
	if c.Type != TypeMessageRemoved && c.RemoveReason != "" {
		errs.invalid("remove_reason", "must be empty for type %q", c.Type)
	}
	if c.Type != TypeAnswered && c.ResponseTime != 0 {
		errs.invalid("response_time_us", "must be empty for type %q", c.Type)
	}
	if c.Type != TypeClosed && c.CloseReason != "" {
		errs.invalid("close_reason", "must be empty for type %q", c.Type)
	}
	return errs.join()
}

// validateMessage checks the message fields of a type caused by a message.
// want is the only allowed direction, or empty for either.
func (c Conversation) validateMessage(errs *fieldErrors, want Direction) {
	errs.requireString("message_id", c.MessageID)
	switch {
	case c.Direction == "":
		errs.missing("direction")
	case !c.Direction.valid():
		errs.invalid("direction", "%q is not %q or %q", c.Direction, FromCustomer, FromCompany)
	case want != "" && c.Direction != want:
		errs.invalid("direction", "must be %q for type %q", want, c.Type)
	}
}

// MarshalConversation validates and encodes a conversation event.
// ResponseTime is written in whole microseconds, and only for TypeAnswered,
// where it is always written, even when it is 0.
func MarshalConversation(c Conversation) ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("marshal conversation event: %w", err)
	}
	var responseUS *int64
	if c.Type == TypeAnswered {
		us := c.ResponseTime.Microseconds()
		responseUS = &us
	}
	b, err := json.Marshal(conversationJSON{
		Type:             c.Type,
		Channel:          c.Channel,
		CompanyID:        c.CompanyID,
		CustomerID:       c.CustomerID,
		Session:          c.Session,
		Seq:              c.Seq,
		SourceTimeUS:     micros(c.SourceTime),
		OpenedAtUS:       micros(c.OpenedAt),
		Lang:             c.Lang,
		CustomerMessages: c.CustomerMessages,
		CompanyMessages:  c.CompanyMessages,
		MessageID:        c.MessageID,
		Direction:        string(c.Direction),
		ResponseTimeUS:   responseUS,
		CloseReason:      string(c.CloseReason),
		RemoveReason:     string(c.RemoveReason),
	})
	if err != nil {
		return nil, fmt.Errorf("marshal conversation event: %w", err)
	}
	return b, nil
}

// UnmarshalConversation decodes and validates a conversation event. An
// unknown type gives an error that wraps ErrUnknownKind. Unknown fields are
// ignored.
func UnmarshalConversation(b []byte) (Conversation, error) {
	var j conversationJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return Conversation{}, fmt.Errorf("unmarshal conversation event: %w", err)
	}
	switch j.Type {
	case TypeOpened, TypeMessageAdded, TypeAnswered, TypeClosed, TypeMessageRemoved, "":
		// An empty type is reported by Validate with the other fields.
	default:
		return Conversation{}, fmt.Errorf("unmarshal conversation event: type %q: %w", j.Type, ErrUnknownKind)
	}
	var response time.Duration
	if j.ResponseTimeUS != nil {
		response = time.Duration(*j.ResponseTimeUS) * time.Microsecond
	}
	c := Conversation{
		Type:             j.Type,
		Channel:          j.Channel,
		CompanyID:        j.CompanyID,
		CustomerID:       j.CustomerID,
		Session:          j.Session,
		Seq:              j.Seq,
		SourceTime:       fromMicros(j.SourceTimeUS),
		OpenedAt:         fromMicros(j.OpenedAtUS),
		Lang:             j.Lang,
		CustomerMessages: j.CustomerMessages,
		CompanyMessages:  j.CompanyMessages,
		MessageID:        j.MessageID,
		Direction:        Direction(j.Direction),
		ResponseTime:     response,
		CloseReason:      CloseReason(j.CloseReason),
		RemoveReason:     RemoveReason(j.RemoveReason),
	}
	err := c.Validate()
	if c.Type == TypeAnswered && j.ResponseTimeUS == nil {
		err = errors.Join(err, errors.New("response_time_us is not set"))
	}
	if err != nil {
		return Conversation{}, fmt.Errorf("unmarshal conversation event: %w", err)
	}
	return c, nil
}
