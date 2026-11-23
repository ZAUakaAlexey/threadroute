package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ControlKind tells the control events in the events.control topic apart.
//
// Every control record is one JSON object with a "kind" field. A reader
// calls UnmarshalControl, which looks at "kind" first and returns a Deletion
// or an AuthorRemoval. A kind the reader does not know gives an error that
// wraps ErrUnknownKind, so the reader can skip the record and count it.
type ControlKind string

// Control event kinds.
const (
	KindDeletion      ControlKind = "deletion"
	KindAuthorRemoval ControlKind = "author_removal"
)

// Control is a control event: a Deletion or an AuthorRemoval. Its record key
// is AuthorKey(Author()).
type Control interface {
	// Kind returns the kind written in the "kind" field.
	Kind() ControlKind
	// Author returns the author ID, which is also the record key.
	Author() string
	// Validate reports every missing or invalid field.
	Validate() error

	isControl()
}

// Deletion says that an author deleted one message (decision 0006). It names
// the message but not its conversation. conv finds the conversation with its
// own index (decision 0006).
type Deletion struct {
	// EventID is the content-based ID of the source event.
	EventID string
	// Channel is the source of the deletion.
	Channel Channel
	// SourceTime is the witnessed time of the source event.
	SourceTime time.Time
	// MessageID is the ID of the deleted message, as in Message.MessageID.
	MessageID string
	// AuthorID is the author of the deleted message.
	AuthorID string
}

// RemovalReason says why all messages of an author are removed.
type RemovalReason string

// Reasons for an author removal (decisions 0003 and 0004).
const (
	// RemovalInactive is an account event with a status that is not active,
	// for example a deleted or suspended account.
	RemovalInactive RemovalReason = "inactive"
	// RemovalSync is a sync event: the author's repository was replaced, so
	// earlier records are treated as removed.
	RemovalSync RemovalReason = "sync"
)

// AuthorRemoval says that every stored message of an author must be removed
// (decision 0004).
type AuthorRemoval struct {
	// EventID is the content-based ID of the source event.
	EventID string
	// Channel is the source of the removal.
	Channel Channel
	// SourceTime is the witnessed time of the source event.
	SourceTime time.Time
	// AuthorID is the author whose messages are removed.
	AuthorID string
	// Reason says why.
	Reason RemovalReason
	// Status is the account status as the source sent it, for example
	// "deleted" or "takendown". It is set only for RemovalInactive, and it
	// may be empty there too, because a source can report a non-active
	// account without a status. The removal must still happen then.
	Status string
}

// Kind implements Control.
func (Deletion) Kind() ControlKind { return KindDeletion }

// Author implements Control.
func (d Deletion) Author() string { return d.AuthorID }

func (Deletion) isControl() {}

// Kind implements Control.
func (AuthorRemoval) Kind() ControlKind { return KindAuthorRemoval }

// Author implements Control.
func (r AuthorRemoval) Author() string { return r.AuthorID }

func (AuthorRemoval) isControl() {}

// Validate reports every missing or invalid field.
func (d Deletion) Validate() error {
	var errs fieldErrors
	errs.requireString("event_id", d.EventID)
	errs.requireString("channel", string(d.Channel))
	errs.requireTime("source_time_us", d.SourceTime)
	errs.requireString("message_id", d.MessageID)
	errs.requireString("author_id", d.AuthorID)
	return errs.join()
}

// Validate reports every missing or invalid field.
func (r AuthorRemoval) Validate() error {
	var errs fieldErrors
	errs.requireString("event_id", r.EventID)
	errs.requireString("channel", string(r.Channel))
	errs.requireTime("source_time_us", r.SourceTime)
	errs.requireString("author_id", r.AuthorID)
	switch r.Reason {
	case "":
		errs.missing("reason")
	case RemovalInactive:
	case RemovalSync:
		if r.Status != "" {
			errs.invalid("status", "must be empty for reason %q", RemovalSync)
		}
	default:
		errs.invalid("reason", "%q is not %q or %q", r.Reason, RemovalInactive, RemovalSync)
	}
	return errs.join()
}

// controlJSON is the wire form of both control events. Fields that a kind
// does not use are left out.
type controlJSON struct {
	Kind         ControlKind `json:"kind"`
	EventID      string      `json:"event_id"`
	Channel      Channel     `json:"channel"`
	SourceTimeUS int64       `json:"source_time_us"`
	MessageID    string      `json:"message_id,omitempty"`
	AuthorID     string      `json:"author_id"`
	Reason       string      `json:"reason,omitempty"`
	Status       string      `json:"status,omitempty"`
}

// MarshalControl validates and encodes a control event, with its kind in
// the "kind" field.
func MarshalControl(c Control) ([]byte, error) {
	// A pointer to either type also implements Control. Use its value.
	switch v := c.(type) {
	case *Deletion:
		if v == nil {
			return nil, errors.New("marshal control: event is nil")
		}
		c = *v
	case *AuthorRemoval:
		if v == nil {
			return nil, errors.New("marshal control: event is nil")
		}
		c = *v
	case nil:
		return nil, errors.New("marshal control: event is nil")
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("marshal control %s: %w", c.Kind(), err)
	}
	var j controlJSON
	switch v := c.(type) {
	case Deletion:
		j = controlJSON{
			Kind:         KindDeletion,
			EventID:      v.EventID,
			Channel:      v.Channel,
			SourceTimeUS: micros(v.SourceTime),
			MessageID:    v.MessageID,
			AuthorID:     v.AuthorID,
		}
	case AuthorRemoval:
		j = controlJSON{
			Kind:         KindAuthorRemoval,
			EventID:      v.EventID,
			Channel:      v.Channel,
			SourceTimeUS: micros(v.SourceTime),
			AuthorID:     v.AuthorID,
			Reason:       string(v.Reason),
			Status:       v.Status,
		}
	default:
		// Control is sealed and pointers are handled above, so this is not
		// reached.
		return nil, fmt.Errorf("marshal control: unsupported type %T", c)
	}
	b, err := json.Marshal(j)
	if err != nil {
		return nil, fmt.Errorf("marshal control: %w", err)
	}
	return b, nil
}

// UnmarshalControl decodes and validates a control event. It returns a
// Deletion or an AuthorRemoval value, so a reader uses a type switch. An
// unknown kind gives an error that wraps ErrUnknownKind. Unknown fields are
// ignored.
func UnmarshalControl(b []byte) (Control, error) {
	var j controlJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return nil, fmt.Errorf("unmarshal control: %w", err)
	}
	var c Control
	switch j.Kind {
	case KindDeletion:
		c = Deletion{
			EventID:    j.EventID,
			Channel:    j.Channel,
			SourceTime: fromMicros(j.SourceTimeUS),
			MessageID:  j.MessageID,
			AuthorID:   j.AuthorID,
		}
	case KindAuthorRemoval:
		c = AuthorRemoval{
			EventID:    j.EventID,
			Channel:    j.Channel,
			SourceTime: fromMicros(j.SourceTimeUS),
			AuthorID:   j.AuthorID,
			Reason:     RemovalReason(j.Reason),
			Status:     j.Status,
		}
	case "":
		return nil, errors.New("unmarshal control: kind is not set")
	default:
		return nil, fmt.Errorf("unmarshal control: kind %q: %w", j.Kind, ErrUnknownKind)
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("unmarshal control %s: %w", j.Kind, err)
	}
	return c, nil
}
