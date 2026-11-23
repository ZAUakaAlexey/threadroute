package event

import (
	"encoding/json"
	"fmt"
	"time"
)

// Direction says who wrote a message in a conversation.
type Direction string

// Directions of a message (decision 0006).
const (
	// FromCustomer is a reply by someone other than the thread author.
	FromCustomer Direction = "customer"
	// FromCompany is a reply by the thread author to another person.
	FromCompany Direction = "company"
)

func (d Direction) valid() bool {
	return d == FromCustomer || d == FromCompany
}

// Message is one message event in the events topic (decision 0006). Its
// record key is ConversationKey(CompanyID, CustomerID).
type Message struct {
	// EventID is the content-based ID of the source event.
	EventID string
	// Channel is the source of the message.
	Channel Channel
	// SourceTime is the witnessed time of the source event, with
	// microsecond precision. Every window after normalize uses it.
	SourceTime time.Time
	// MessageID identifies the message in its channel (for Bluesky, the
	// post URI). A Deletion names the same ID.
	MessageID string
	// CompanyID is the thread author.
	CompanyID string
	// CustomerID is the other person in the conversation: the reply author
	// for a customer message, or the author of the answered message for a
	// company message. It differs from CompanyID.
	CustomerID string
	// Direction says whether the customer or the company wrote it.
	Direction Direction
	// ParentID is the message ID of the message this one replies to. Every
	// message event is a reply, so it is always set.
	ParentID string
	// Text is the post text. It may be empty, for example for a reply with
	// only an image. It must never be logged.
	Text string
	// Langs are the language tags exactly as the author's client sent them.
	// The list may be empty.
	Langs []string
	// Lang is NormalizeLang(Langs) at the time normalize wrote the event.
	Lang string
}

// messageJSON is the wire form of Message.
type messageJSON struct {
	EventID      string   `json:"event_id"`
	Channel      Channel  `json:"channel"`
	SourceTimeUS int64    `json:"source_time_us"`
	MessageID    string   `json:"message_id"`
	CompanyID    string   `json:"company_id"`
	CustomerID   string   `json:"customer_id"`
	Direction    string   `json:"direction"`
	ParentID     string   `json:"parent_id"`
	Text         string   `json:"text"`
	Langs        []string `json:"langs"`
	Lang         string   `json:"lang"`
}

// Validate reports every missing or invalid field. It never includes the
// text.
func (m Message) Validate() error {
	var errs fieldErrors
	errs.requireString("event_id", m.EventID)
	errs.requireString("channel", string(m.Channel))
	errs.requireTime("source_time_us", m.SourceTime)
	errs.requireString("message_id", m.MessageID)
	errs.requireString("company_id", m.CompanyID)
	errs.requireString("customer_id", m.CustomerID)
	if m.CompanyID != "" && m.CompanyID == m.CustomerID {
		errs.invalid("customer_id", "equals company_id")
	}
	switch {
	case m.Direction == "":
		errs.missing("direction")
	case !m.Direction.valid():
		errs.invalid("direction", "%q is not %q or %q", m.Direction, FromCustomer, FromCompany)
	}
	errs.requireString("parent_id", m.ParentID)
	if m.ParentID != "" && m.ParentID == m.MessageID {
		errs.invalid("parent_id", "equals message_id")
	}
	switch {
	case m.Lang == "":
		errs.missing("lang")
	case !validLang(m.Lang):
		errs.invalid("lang", "%q is not 2 or 3 lower case letters", m.Lang)
	}
	return errs.join()
}

// MarshalMessage validates and encodes a message event. A nil Langs is
// written as an empty list.
func MarshalMessage(m Message) ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}
	langs := m.Langs
	if langs == nil {
		langs = []string{}
	}
	b, err := json.Marshal(messageJSON{
		EventID:      m.EventID,
		Channel:      m.Channel,
		SourceTimeUS: micros(m.SourceTime),
		MessageID:    m.MessageID,
		CompanyID:    m.CompanyID,
		CustomerID:   m.CustomerID,
		Direction:    string(m.Direction),
		ParentID:     m.ParentID,
		Text:         m.Text,
		Langs:        langs,
		Lang:         m.Lang,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}
	return b, nil
}

// UnmarshalMessage decodes and validates a message event. Unknown fields are
// ignored. A missing or null langs gives an empty Langs.
func UnmarshalMessage(b []byte) (Message, error) {
	var j messageJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return Message{}, fmt.Errorf("unmarshal message: %w", err)
	}
	m := Message{
		EventID:    j.EventID,
		Channel:    j.Channel,
		SourceTime: fromMicros(j.SourceTimeUS),
		MessageID:  j.MessageID,
		CompanyID:  j.CompanyID,
		CustomerID: j.CustomerID,
		Direction:  Direction(j.Direction),
		ParentID:   j.ParentID,
		Text:       j.Text,
		Langs:      j.Langs,
		Lang:       j.Lang,
	}
	if m.Langs == nil {
		m.Langs = []string{}
	}
	if err := m.Validate(); err != nil {
		return Message{}, fmt.Errorf("unmarshal message: %w", err)
	}
	return m, nil
}
