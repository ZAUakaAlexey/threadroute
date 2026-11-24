package gwapi

import (
	"encoding/json"
	"fmt"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

// ClientType is the "type" of a client message.
type ClientType string

// Client message types.
const (
	TypeHello   ClientType = "hello"
	TypeWatch   ClientType = "watch"
	TypeUnwatch ClientType = "unwatch"
	TypeAction  ClientType = "action"
	// TypeChangeSettings is the type of ChangeSettings (decision 0014).
	TypeChangeSettings ClientType = "change_settings"
)

// ClientMessage is a message from the browser to the gateway: Hello, Watch,
// Unwatch, Action or ChangeSettings.
type ClientMessage interface {
	// ClientType returns the "type" of the message.
	ClientType() ClientType
	// Validate reports every missing or invalid field.
	Validate() error
}

// Hello is the first message of every connection. The server answers with
// Welcome, or closes the connection (see CloseCode). Only one Hello is
// allowed; a second one closes the connection with CloseBadMessage.
type Hello struct {
	// Version is the protocol version of the client, ProtocolVersion.
	Version int `json:"version"`
	// View is the screen the connection serves.
	View View `json:"view"`
	// Token is the site token of a signed-in user, or empty. The gateway
	// checks it with the site API and decides the level from the answer
	// (decision 0012). It is never put into a URL. Since decision 0014
	// (point 7) the gateway also reads the site sign-in from a cookie of
	// the WebSocket opening request, and a local stand may give every
	// connection one role; both are gateway behaviour and change nothing
	// here. A token in Hello is still accepted.
	Token string `json:"token,omitempty"`
	// ResumeToken is the ResumeToken of an earlier Welcome, or empty. With
	// ViewOperator it gives the connection the same operator again while
	// that operator is still within OfflineGrace or held by another
	// connection (which is then closed with CloseReplaced). An unknown or
	// expired resume token is ignored: the connection gets a new operator.
	ResumeToken string `json:"resume_token,omitempty"`
	// Languages are the languages the operator handles, for ViewOperator
	// only, where they are required. Values that event.NormalizeLang can
	// return; add "und" for the general queue. Any order, no duplicates.
	Languages []string `json:"languages,omitempty"`
}

// ClientType implements ClientMessage.
func (Hello) ClientType() ClientType { return TypeHello }

// Validate implements ClientMessage. It does not compare Version with
// ProtocolVersion; DecodeClient does that first.
func (h Hello) Validate() error {
	var errs fieldErrors
	if h.Version <= 0 {
		errs.missing("version")
	}
	switch {
	case h.View == "":
		errs.missing("view")
	case !h.View.valid():
		errs.invalid("view", "%q is not %q or %q", h.View, ViewOperator, ViewSupervisor)
	}
	if len(h.Token) > MaxTokenBytes {
		errs.invalid("token", "is longer than %d bytes", MaxTokenBytes)
	}
	if h.ResumeToken != "" && !validToken(h.ResumeToken, MaxResumeTokenBytes) {
		errs.invalid("resume_token", "is not a valid resume token")
	}
	switch h.View {
	case ViewOperator:
		checkLanguages(&errs, "languages", h.Languages, false)
	case ViewSupervisor:
		if len(h.Languages) > 0 {
			errs.invalid("languages", "must be empty for view %q", h.View)
		}
		if h.ResumeToken != "" {
			errs.invalid("resume_token", "must be empty for view %q", h.View)
		}
	}
	return errs.join()
}

// validToken reports whether s is 1 to maxLen of the characters of
// base64url: letters, digits, "-" and "_", and ".".
func validToken(s string, maxLen int) bool {
	if s == "" || len(s) > maxLen {
		return false
	}
	for i := range len(s) {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_', c == '.':
		default:
			return false
		}
	}
	return true
}

// checkRequestID checks the request ID of a request. It follows
// event.ValidRoutingID, so it can be the reply ID of the operator API.
func checkRequestID(errs *fieldErrors, id string) {
	switch {
	case id == "":
		errs.missing("request_id")
	case !event.ValidRoutingID(id):
		errs.invalid("request_id", "%q is not 1 to %d letters, digits, '.', '_' or '-'", id, MaxRequestIDLen)
	}
}

// Watch asks for the messages of one conversation (decision 0011, point 2).
// The server answers with a Result, and on success at once with a Messages
// snapshot of the conversation, and later with Messages updates and
// MessageRemoved. An operator connection may watch the conversations it
// holds, that is OperatorConversation rows with StateAssigned (decision
// 0011, point 6); an offer shows no messages. A supervisor connection may
// watch any conversation in its view. A State snapshot ends every watch
// (see State).
type Watch struct {
	// RequestID is chosen by the client. The Result carries it.
	RequestID string `json:"request_id"`
	// ConversationID is the ID of a conversation as the server sent it.
	ConversationID string `json:"conversation_id"`
}

// ClientType implements ClientMessage.
func (Watch) ClientType() ClientType { return TypeWatch }

// Validate implements ClientMessage. The ID is checked only for being set:
// the client cannot know the level rules.
func (w Watch) Validate() error {
	var errs fieldErrors
	checkRequestID(&errs, w.RequestID)
	checkWatchID(&errs, w.ConversationID)
	return errs.join()
}

func checkWatchID(errs *fieldErrors, id string) {
	switch {
	case id == "":
		errs.missing("conversation_id")
	case len(id) > MaxIDLen:
		errs.invalid("conversation_id", "is longer than %d bytes", MaxIDLen)
	}
}

// Unwatch stops a Watch. The server answers with a Result and sends no
// more messages of that conversation. Messages already sent stay with the
// client, which drops them when it no longer shows them.
type Unwatch struct {
	RequestID      string `json:"request_id"`
	ConversationID string `json:"conversation_id"`
}

// ClientType implements ClientMessage.
func (Unwatch) ClientType() ClientType { return TypeUnwatch }

// Validate implements ClientMessage.
func (u Unwatch) Validate() error {
	var errs fieldErrors
	checkRequestID(&errs, u.RequestID)
	checkWatchID(&errs, u.ConversationID)
	return errs.join()
}

// ActionName names an operator action (decision 0010). Actions are allowed
// only on a ViewOperator connection, and act on the operator the gateway
// created for it.
type ActionName string

// Actions.
const (
	// ActionOnline puts the operator online. A new operator is offline.
	ActionOnline ActionName = "online"
	// ActionOffline puts the operator offline. Their offers end and their
	// conversations go back to the queue.
	ActionOffline ActionName = "offline"
	// ActionSetLanguages replaces the languages of the operator.
	// Languages is set.
	ActionSetLanguages ActionName = "set_languages"
	// ActionAccept accepts an offer. OfferID is set.
	ActionAccept ActionName = "accept"
	// ActionRefuse refuses an offer. OfferID is set.
	ActionRefuse ActionName = "refuse"
	// ActionReply records the "mark replied" marker of an assignment.
	// OfferID is set. The request ID is the reply ID, so sending the same
	// request again records no second reply.
	ActionReply ActionName = "reply"
	// ActionResolve resolves an assignment. OfferID is set.
	ActionResolve ActionName = "resolve"
)

// Actions lists every action.
var Actions = []ActionName{
	ActionOnline, ActionOffline, ActionSetLanguages, ActionAccept, ActionRefuse, ActionReply, ActionResolve,
}

// NeedsOffer reports whether the action names an offer.
func (a ActionName) NeedsOffer() bool {
	return a == ActionAccept || a == ActionRefuse || a == ActionReply || a == ActionResolve
}

func (a ActionName) valid() bool {
	for _, k := range Actions {
		if a == k {
			return true
		}
	}
	return false
}

// Action is an operator action. The server answers with exactly one Result
// with the same RequestID. The state change itself arrives with a later
// State message.
type Action struct {
	// RequestID is chosen by the client and follows event.ValidRoutingID.
	// It should be unique within the connection. For ActionReply it is
	// the reply ID, so a resend after a lost answer is safe.
	RequestID string     `json:"request_id"`
	Action    ActionName `json:"action"`
	// OfferID is the offer of ActionAccept, ActionRefuse, ActionReply and
	// ActionResolve, as OperatorConversation.OfferID gives it.
	OfferID string `json:"offer_id,omitempty"`
	// Languages are the new languages of ActionSetLanguages, as in Hello.
	Languages []string `json:"languages,omitempty"`
}

// ClientType implements ClientMessage.
func (Action) ClientType() ClientType { return TypeAction }

// Validate implements ClientMessage.
func (a Action) Validate() error {
	var errs fieldErrors
	checkRequestID(&errs, a.RequestID)
	switch {
	case a.Action == "":
		errs.missing("action")
	case !a.Action.valid():
		errs.invalid("action", "%q is not a known action", a.Action)
	}
	switch {
	case a.Action.NeedsOffer():
		switch {
		case a.OfferID == "":
			errs.missing("offer_id")
		case !event.ValidRoutingID(a.OfferID):
			errs.invalid("offer_id", "%q is not a valid ID", a.OfferID)
		}
	case a.OfferID != "":
		errs.invalid("offer_id", "must be empty for action %q", a.Action)
	}
	switch {
	case a.Action == ActionSetLanguages:
		checkLanguages(&errs, "languages", a.Languages, false)
	case len(a.Languages) > 0:
		errs.invalid("languages", "must be empty for action %q", a.Action)
	}
	return errs.join()
}

// DecodeClient decodes and validates one client message. It returns an
// error that wraps ErrUnsupportedVersion for a Hello with another version
// (checked before every other field), and one that wraps ErrUnknownType
// for an unknown type. A message larger than MaxClientMessageBytes is an
// error too; the server closes with CloseMessageTooBig before it decodes.
func DecodeClient(b []byte) (ClientMessage, error) {
	if len(b) > MaxClientMessageBytes {
		return nil, fmt.Errorf("decode client message: %d bytes are more than %d", len(b), MaxClientMessageBytes)
	}
	var env struct {
		Type    ClientType `json:"type"`
		Version int        `json:"version"`
	}
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, fmt.Errorf("decode client message: %w", err)
	}
	var m ClientMessage
	var err error
	switch env.Type {
	case TypeHello:
		if env.Version != ProtocolVersion {
			return nil, fmt.Errorf("decode client message: version %d: %w", env.Version, ErrUnsupportedVersion)
		}
		m, err = decodeAs[Hello](b)
	case TypeWatch:
		m, err = decodeAs[Watch](b)
	case TypeUnwatch:
		m, err = decodeAs[Unwatch](b)
	case TypeAction:
		m, err = decodeAs[Action](b)
	case TypeChangeSettings:
		m, err = decodeAs[ChangeSettings](b)
	case "":
		return nil, fmt.Errorf("decode client message: type is not set")
	default:
		return nil, fmt.Errorf("decode client message: type %q: %w", env.Type, ErrUnknownType)
	}
	if err != nil {
		return nil, fmt.Errorf("decode client message %s: %w", env.Type, err)
	}
	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("decode client message %s: %w", env.Type, err)
	}
	return m, nil
}

// EncodeClient validates and encodes one client message, with its "type".
// The gateway does not need it; tests and Go clients (the load test) do.
func EncodeClient(m ClientMessage) ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("encode client message %s: %w", m.ClientType(), err)
	}
	b, err := withType(string(m.ClientType()), m)
	if err != nil {
		return nil, fmt.Errorf("encode client message %s: %w", m.ClientType(), err)
	}
	return b, nil
}

// decodeAs decodes b into a T. Unknown fields, "type" among them, are
// ignored.
func decodeAs[T any](b []byte) (T, error) {
	var v T
	err := json.Unmarshal(b, &v)
	return v, err
}

// withType encodes v, a struct, as a JSON object whose first field is
// "type".
func withType(typ string, v any) ([]byte, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if len(body) < 2 || body[0] != '{' {
		return nil, fmt.Errorf("message is not a JSON object")
	}
	head, err := json.Marshal(typ)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(body)+len(head)+10)
	out = append(out, `{"type":`...)
	out = append(out, head...)
	if len(body) > 2 {
		out = append(out, ',')
	}
	return append(out, body[1:]...), nil
}
