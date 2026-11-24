// Package opapi is the contract of the operator API (decision 0010): the
// paths, the request and response bodies, and the error codes. The router
// serves the API, and the simulator (and later the gateway) calls it. This
// package holds no handler and no client, only what both sides must agree
// on.
//
// # Bodies
//
// Every request and response body is one JSON object of at most
// MaxBodyBytes. Durations are whole microseconds in fields that end in
// "_us", as in the event package.
//
// # Safe to repeat and safe when late
//
// Every action is checked against the current state, so a repeated or late
// call cannot break it (decision 0010). The caller tells three outcomes
// apart:
//
//   - Done now: HTTP 200 and ActionResponse.Result is ResultDone. The state
//     changed.
//   - Already done: HTTP 200 and Result is ResultAlreadyDone. The same
//     call was applied before. Nothing changed, and the outcome is the same
//     as for the first call.
//   - Rejected: HTTP 409 and an ErrorResponse with a code for which
//     ErrorCode.Rejected is true. The state moved on (for example the offer
//     expired), and the call cannot be applied. Repeating it gives the same
//     answer.
//
// Other errors are a bad request (400), an unknown operator or offer (404),
// or a router that cannot answer now (503, retry later), each with an
// ErrorResponse.
//
// # Offers and assignments
//
// An offer is named by its offer ID, the OfferID of the "offered" routing
// event. Accept and refuse name the offer, so a late accept of an old offer
// can never accept a newer offer of the same conversation. After an accept
// the same ID names the assignment, and reply and resolve use it, so a late
// resolve of an earlier assignment cannot resolve a later one (for example
// after the conversation was reopened).
package opapi

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

// Names of the path wildcards, for http.Request.PathValue.
const (
	ParamOperatorID = "operator_id"
	ParamOfferID    = "offer_id"
)

// Route patterns for http.ServeMux (Go 1.22 syntax: method, path and
// wildcards). Operator IDs and offer IDs follow event.ValidRoutingID, so
// they never need escaping in a path.
const (
	// PatternPutOperator creates an operator or updates its languages and
	// capacity. Body: OperatorRequest. Answer: OperatorResponse. The
	// simulator calls it for each of its operators at start. A new
	// operator is offline.
	PatternPutOperator = "PUT /v1/operators/{operator_id}"
	// PatternOnline puts the operator online, so they can get offers.
	// No body. Answer: ActionResponse.
	PatternOnline = "POST /v1/operators/{operator_id}/online"
	// PatternOffline puts the operator offline. Their open offers end and
	// the conversations they hold go back to the queue (decision 0010). No
	// body. Answer: ActionResponse.
	PatternOffline = "POST /v1/operators/{operator_id}/offline"
	// PatternAccept accepts the offer. No body. Answer: ActionResponse.
	// Rejected with CodeOfferExpired, CodeOfferRefused or CodeOfferEnded.
	PatternAccept = "POST /v1/operators/{operator_id}/offers/{offer_id}/accept"
	// PatternRefuse refuses the offer, which frees the slot at once. No
	// body. Answer: ActionResponse. Rejected with CodeOfferAccepted,
	// CodeOfferExpired or CodeOfferEnded.
	PatternRefuse = "POST /v1/operators/{operator_id}/offers/{offer_id}/refuse"
	// PatternReply records a reply marker for the assignment of an
	// accepted offer. No text is sent anywhere (decision 0010). Body:
	// ReplyRequest. Answer: ActionResponse. Rejected with
	// CodeOfferNotAccepted, CodeOfferExpired, CodeOfferRefused,
	// CodeOfferEnded or CodeAssignmentEnded.
	PatternReply = "POST /v1/operators/{operator_id}/offers/{offer_id}/reply"
	// PatternResolve resolves the assignment of an accepted offer, which
	// frees the slot. No body. Answer: ActionResponse. Resolving twice
	// gives ResultAlreadyDone. Rejected with CodeOfferNotAccepted,
	// CodeOfferExpired, CodeOfferRefused, CodeOfferEnded or
	// CodeAssignmentEnded.
	PatternResolve = "POST /v1/operators/{operator_id}/offers/{offer_id}/resolve"
	// PatternSettings reads the router settings that the simulator needs.
	// No body. Answer: Settings.
	PatternSettings = "GET /v1/settings"
)

// Patterns lists every route pattern of the API.
var Patterns = []string{
	PatternPutOperator, PatternOnline, PatternOffline, PatternAccept, PatternRefuse,
	PatternReply, PatternResolve, PatternSettings,
}

// OperatorPath returns the path of PatternPutOperator.
func OperatorPath(operatorID string) string {
	return "/v1/operators/" + url.PathEscape(operatorID)
}

// OnlinePath returns the path of PatternOnline.
func OnlinePath(operatorID string) string { return OperatorPath(operatorID) + "/online" }

// OfflinePath returns the path of PatternOffline.
func OfflinePath(operatorID string) string { return OperatorPath(operatorID) + "/offline" }

// AcceptPath returns the path of PatternAccept.
func AcceptPath(operatorID, offerID string) string { return offerPath(operatorID, offerID) + "/accept" }

// RefusePath returns the path of PatternRefuse.
func RefusePath(operatorID, offerID string) string { return offerPath(operatorID, offerID) + "/refuse" }

// ReplyPath returns the path of PatternReply.
func ReplyPath(operatorID, offerID string) string { return offerPath(operatorID, offerID) + "/reply" }

// ResolvePath returns the path of PatternResolve.
func ResolvePath(operatorID, offerID string) string {
	return offerPath(operatorID, offerID) + "/resolve"
}

// SettingsPath returns the path of PatternSettings.
func SettingsPath() string { return "/v1/settings" }

func offerPath(operatorID, offerID string) string {
	return OperatorPath(operatorID) + "/offers/" + url.PathEscape(offerID)
}

// MaxBodyBytes is the largest request or response body.
const MaxBodyBytes = 4096

// ContentType is the media type of every body.
const ContentType = "application/json"

// Limits of an operator.
const (
	// MaxLanguages is the most languages of one operator.
	MaxLanguages = 32
	// MaxCapacity is the largest capacity of one operator.
	MaxCapacity = 100
	// MaxReplyIDLen is the longest reply ID.
	MaxReplyIDLen = 64
)

// ValidID reports whether s can be an operator ID or an offer ID. It is
// event.ValidRoutingID. A handler checks both path values with it and
// answers CodeInvalidRequest when one fails.
func ValidID(s string) bool {
	return event.ValidRoutingID(s)
}

// Presence is the presence state of an operator (decision 0009).
type Presence string

// Presence states.
const (
	PresenceOnline  Presence = "online"
	PresenceOffline Presence = "offline"
)

// Valid reports whether p is a known presence state.
func (p Presence) Valid() bool {
	return p == PresenceOnline || p == PresenceOffline
}

// OperatorRequest is the body of PatternPutOperator.
type OperatorRequest struct {
	// Languages are the languages the operator handles, each a value that
	// event.NormalizeLang can return. Add event.LangUndetermined for the
	// general queue (decision 0009). Not empty, no duplicates, at most
	// MaxLanguages. The order does not matter.
	Languages []string `json:"languages"`
	// Capacity is how many conversations the operator handles at once,
	// from 1 to MaxCapacity. 0 or absent takes the router default
	// (Settings.CapacityDefault).
	Capacity int `json:"capacity,omitempty"`
}

// Validate reports every invalid field.
func (r OperatorRequest) Validate() error {
	var errs []error
	switch {
	case len(r.Languages) == 0:
		errs = append(errs, errors.New("languages is not set"))
	case len(r.Languages) > MaxLanguages:
		errs = append(errs, fmt.Errorf("languages: %d are more than %d", len(r.Languages), MaxLanguages))
	}
	seen := make(map[string]bool, len(r.Languages))
	for _, l := range r.Languages {
		switch {
		case !event.ValidLang(l):
			errs = append(errs, fmt.Errorf("languages: %q is not 2 or 3 lower case letters", l))
		case seen[l]:
			errs = append(errs, fmt.Errorf("languages: %q is listed twice", l))
		}
		seen[l] = true
	}
	if r.Capacity < 0 || r.Capacity > MaxCapacity {
		errs = append(errs, fmt.Errorf("capacity: %d is not between 0 and %d", r.Capacity, MaxCapacity))
	}
	return errors.Join(errs...)
}

// Operator is an operator as the router stores it.
type Operator struct {
	ID string `json:"id"`
	// Languages are sorted.
	Languages []string `json:"languages"`
	// Capacity is the effective capacity, after the default was applied.
	Capacity int      `json:"capacity"`
	Presence Presence `json:"presence"`
	// TeamCompanyID is the company of the dedicated team the operator is
	// in, or empty in the shared pool.
	TeamCompanyID string `json:"team_company_id,omitempty"`
}

// OperatorResponse is the answer of PatternPutOperator. Result is
// ResultDone when the operator was created or changed, and
// ResultAlreadyDone when it already had these languages and capacity.
type OperatorResponse struct {
	Result   Result   `json:"result"`
	Operator Operator `json:"operator"`
}

// ReplyRequest is the body of PatternReply.
type ReplyRequest struct {
	// ReplyID is chosen by the caller and is unique within the
	// assignment. A repeated call with the same ID gives
	// ResultAlreadyDone and records no second reply. It follows the rule
	// of ValidID, with at most MaxReplyIDLen characters.
	ReplyID string `json:"reply_id"`
}

// Validate reports every invalid field.
func (r ReplyRequest) Validate() error {
	switch {
	case r.ReplyID == "":
		return errors.New("reply_id is not set")
	case len(r.ReplyID) > MaxReplyIDLen || !ValidID(r.ReplyID):
		return fmt.Errorf("reply_id: %q is not a valid ID", r.ReplyID)
	}
	return nil
}

// Result tells "done now" from "already done" in a successful answer.
type Result string

// Results.
const (
	// ResultDone: the call changed the state.
	ResultDone Result = "done"
	// ResultAlreadyDone: the same call was applied before, nothing
	// changed.
	ResultAlreadyDone Result = "already_done"
)

// ActionResponse is the answer of an action that succeeded.
type ActionResponse struct {
	Result Result `json:"result"`
}

// Settings is the answer of PatternSettings: the router settings that the
// simulator must use (decisions 0009 and 0010).
type Settings struct {
	// SpeedFactor multiplies the machine clock of the router timers. The
	// simulator divides its delays by it. Positive, 1 for live traffic.
	SpeedFactor float64 `json:"speed_factor"`
	// AcceptTimeoutUS is the accept timeout in model time, in whole
	// microseconds. See AcceptTimeout.
	AcceptTimeoutUS int64 `json:"accept_timeout_us"`
	// CapacityDefault is the capacity of an operator that is created
	// without one.
	CapacityDefault int `json:"capacity_default"`
}

// AcceptTimeout returns AcceptTimeoutUS as a duration.
func (s Settings) AcceptTimeout() time.Duration {
	return time.Duration(s.AcceptTimeoutUS) * time.Microsecond
}

// Validate reports every invalid field. The simulator does not start with
// invalid settings (decision 0010).
func (s Settings) Validate() error {
	var errs []error
	if !(s.SpeedFactor > 0) {
		errs = append(errs, fmt.Errorf("speed_factor: %v is not positive", s.SpeedFactor))
	}
	if s.AcceptTimeoutUS <= 0 {
		errs = append(errs, fmt.Errorf("accept_timeout_us: %d is not positive", s.AcceptTimeoutUS))
	}
	if s.CapacityDefault < 1 || s.CapacityDefault > MaxCapacity {
		errs = append(errs, fmt.Errorf("capacity_default: %d is not between 1 and %d", s.CapacityDefault, MaxCapacity))
	}
	return errors.Join(errs...)
}

// ErrorCode is the machine-readable code of an error answer.
type ErrorCode string

// Error codes. Each has one HTTP status, see ErrorCode.Status.
const (
	// CodeInvalidRequest: the path or the body is not valid. 400.
	CodeInvalidRequest ErrorCode = "invalid_request"
	// CodeOperatorNotFound: no operator has this ID. Create it with
	// PatternPutOperator first. 404.
	CodeOperatorNotFound ErrorCode = "operator_not_found"
	// CodeOfferNotFound: no offer of this operator has this ID. An offer
	// made to another operator gives this code too. 404.
	CodeOfferNotFound ErrorCode = "offer_not_found"
	// CodeOfferExpired: the accept timeout of the offer passed before the
	// call. 409.
	CodeOfferExpired ErrorCode = "offer_expired"
	// CodeOfferRefused: the operator refused this offer before. 409.
	CodeOfferRefused ErrorCode = "offer_refused"
	// CodeOfferAccepted: the operator accepted this offer before, so it
	// cannot be refused. 409.
	CodeOfferAccepted ErrorCode = "offer_accepted"
	// CodeOfferEnded: the offer ended before an answer for another reason:
	// the conversation closed, or the operator went offline. 409.
	CodeOfferEnded ErrorCode = "offer_ended"
	// CodeOfferNotAccepted: reply or resolve for an offer that is still
	// waiting for an accept. 409.
	CodeOfferNotAccepted ErrorCode = "offer_not_accepted"
	// CodeAssignmentEnded: the assignment ended before the call: the
	// conversation closed, went back to the queue (operator offline), or,
	// for a reply, was resolved. 409.
	CodeAssignmentEnded ErrorCode = "assignment_ended"
	// CodeUnavailable: the router cannot answer now, for example the
	// database is not reachable. Retry later. 503.
	CodeUnavailable ErrorCode = "unavailable"
	// CodeInternal: an unexpected error. 500.
	CodeInternal ErrorCode = "internal"
)

// Codes lists every error code.
var Codes = []ErrorCode{
	CodeInvalidRequest, CodeOperatorNotFound, CodeOfferNotFound, CodeOfferExpired,
	CodeOfferRefused, CodeOfferAccepted, CodeOfferEnded, CodeOfferNotAccepted,
	CodeAssignmentEnded, CodeUnavailable, CodeInternal,
}

// Status returns the HTTP status of the code. An unknown code gives 500.
func (c ErrorCode) Status() int {
	switch c {
	case CodeInvalidRequest:
		return http.StatusBadRequest
	case CodeOperatorNotFound, CodeOfferNotFound:
		return http.StatusNotFound
	case CodeOfferExpired, CodeOfferRefused, CodeOfferAccepted, CodeOfferEnded,
		CodeOfferNotAccepted, CodeAssignmentEnded:
		return http.StatusConflict
	case CodeUnavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// Rejected reports whether the code means "rejected because the state moved
// on". Such a call must not be retried: it gives the same answer again.
func (c ErrorCode) Rejected() bool {
	return c.Status() == http.StatusConflict
}

// ErrorResponse is the body of every error answer.
type ErrorResponse struct {
	Code ErrorCode `json:"code"`
	// Message is for people and logs. It never holds post text, a DSN or
	// a secret.
	Message string `json:"message"`
}

// Error is an error answer as a Go error, for the caller side. Use
// errors.As to read the code.
type Error struct {
	// Status is the HTTP status of the answer.
	Status int
	ErrorResponse
}

func (e *Error) Error() string {
	return fmt.Sprintf("operator API: %d %s: %s", e.Status, e.Code, e.Message)
}
