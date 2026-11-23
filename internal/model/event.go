// Package model holds the channel-agnostic types that flow through the
// pipeline. A connector turns source-specific data into these types, and no
// other package knows which channel an event came from.
package model

import "time"

// Kind selects which payload of an Event is set.
type Kind string

const (
	// KindMessage is a new or edited message in a conversation.
	KindMessage Kind = "message"
	// KindMessageDeleted means the author removed one message.
	KindMessageDeleted Kind = "message_deleted"
	// KindAccount is a change of an author's account status. A non-active
	// account means every message of that author must be removed.
	KindAccount Kind = "account"
)

// Event is one unit emitted by a connector. Exactly one of Message, Deletion
// and Account is set, selected by Kind.
type Event struct {
	// Channel names the connector, for example "bluesky".
	Channel string `json:"channel"`
	Kind    Kind   `json:"kind"`
	// Cursor is the position of this event in the source stream.
	Cursor Cursor `json:"cursor"`
	// DedupID identifies the event independently of the source host, so a
	// replay after a reconnect or a host change can be dropped as a duplicate.
	DedupID string `json:"dedup_id"`
	// Time is the source server time. Client-supplied timestamps are never
	// used, because a client can put any value there.
	Time time.Time `json:"time"`

	Message  *Message      `json:"message,omitempty"`
	Deletion *Deletion     `json:"deletion,omitempty"`
	Account  *AccountEvent `json:"account,omitempty"`
}

// Cursor is a resume position. Seq is only meaningful on the host that issued
// it. WitnessedAt works on any host and is used after a host change.
type Cursor struct {
	Host        string    `json:"host"`
	Seq         uint64    `json:"seq"`
	WitnessedAt time.Time `json:"witnessed_at"`
}

// IsZero reports whether the cursor is unset, which means "start from live".
func (c Cursor) IsZero() bool {
	return c.Seq == 0 && c.WitnessedAt.IsZero()
}

// Message is one message in a conversation.
type Message struct {
	// ID is unique within the channel.
	ID string `json:"id"`
	// ConversationID groups messages into one conversation. For the first
	// message of a conversation it equals ID.
	ConversationID string `json:"conversation_id"`
	// ParentID is the message this one answers. Empty for the first message.
	ParentID string `json:"parent_id,omitempty"`
	AuthorID string `json:"author_id"`
	Text     string `json:"text"`
	// Langs holds the language tags exactly as the source sent them. They are
	// set by the client and can be missing or wrong.
	Langs []string `json:"langs,omitempty"`
	// Edited is true when the event replaces an earlier version of the message.
	Edited bool `json:"edited,omitempty"`
}

// IsReply reports whether the message answers another message.
func (m Message) IsReply() bool {
	return m.ParentID != ""
}

// Deletion identifies one removed message. The source does not say which
// conversation it belonged to.
type Deletion struct {
	MessageID string `json:"message_id"`
	AuthorID  string `json:"author_id"`
}

// AccountEvent is a change of an author's account status.
type AccountEvent struct {
	AuthorID string `json:"author_id"`
	Active   bool   `json:"active"`
	// Status explains a non-active account, for example "deleted",
	// "takendown", "suspended" or "deactivated". Empty when Active is true.
	Status string `json:"status,omitempty"`
}
