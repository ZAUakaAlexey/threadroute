// Package opapi is the contract of the operator API (decision 0010): the
// paths, the request and response bodies, and the error codes. The router
// serves the API, and the simulator (and later the gateway) calls it. This
// package holds no handler and no client, only what both sides must agree
// on.
//
// # Bodies
//
// Every request and response body is one JSON object of at most
// MaxBodyBytes. Two answers may be larger, up to MaxSettingsBodyBytes: the
// answer of SettingsWithChangesPath and every answer of PatternPutSettings,
// because they carry the change log. Durations are whole microseconds in
// fields that end in "_us", as in the event package.
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
// # Stand settings
//
// The calls of SettingsPatterns read and change the stand settings of
// decision 0014. stand.go describes them. A router from before decision
// 0014 does not serve them: it answers PatternPutSettings with HTTP 405
// and PatternPutSimDefaults with HTTP 404, both without an ErrorResponse,
// and its PatternSettings answer has no "stand". A caller treats those
// three like CodeSettingsOff.
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
	// PatternPutOperator creates an operator or updates its languages,
	// capacity and priority. Body: OperatorRequest. Answer: OperatorResponse. The
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
	// PatternSettings reads the router settings that the simulator and the
	// gateway need, and the stand settings (Settings.Stand). No body.
	// Answer: Settings. Without a query the answer leaves out
	// StandState.Changes and stays below MaxBodyBytes, so a client built
	// before decision 0014 can still read it. With the query
	// QueryChanges=1 (SettingsWithChangesPath) it carries them and may be
	// up to MaxSettingsBodyBytes. Another value of the query is the same
	// as none.
	PatternSettings = "GET /v1/settings"

	// PatternPutSettings changes the stand settings. Body:
	// SettingsChangeRequest. The router checks, in this order, and the
	// first rule that fails gives the answer:
	//
	//  1. The body is a JSON object of the right shape: else HTTP 400,
	//     CodeInvalidRequest without fields.
	//  2. The router offers stand settings: else HTTP 501,
	//     CodeSettingsOff.
	//  3. SettingsChangeRequest.FieldErrors is empty: else HTTP 400,
	//     CodeInvalidRequest with SettingsErrorResponse.Fields.
	//  4. BaseVersion is the current version: else HTTP 409,
	//     CodeSettingsConflict with SettingsErrorResponse.Settings (the
	//     current settings with the change log).
	//  5. The sim part is present when the stored set has one, and absent
	//     when it has none: else HTTP 400, CodeInvalidRequest with a
	//     FieldError for FieldSim.
	//  6. The set differs from the current set: else HTTP 200,
	//     SettingsChangeResponse with ResultAlreadyDone. Nothing is logged
	//     and the interval does not start.
	//  7. SettingsChangeInterval has passed since the last change by this
	//     call: else HTTP 429, CodeSettingsTooSoon with
	//     SettingsErrorResponse.RetryAfterMS.
	//  8. Applied: the router stores the set with version plus one, logs
	//     the change with Diff of the old and the new set, and answers
	//     HTTP 200, SettingsChangeResponse with ResultDone.
	//
	// CodeUnavailable (503) and CodeInternal (500) can come at any step.
	// Every error body is a SettingsErrorResponse. A repeat of a request
	// that was applied gets CodeSettingsConflict (its base version is no
	// longer current); the Settings of that answer show whether the set
	// is the one the caller wanted.
	PatternPutSettings = "PUT /v1/settings"

	// PatternPutSimDefaults reports the values of the simulator
	// environment. The simulator calls it at each of its starts, after it
	// read PatternSettings. Body: SimDefaultsRequest. Answer:
	// SimDefaultsResponse (HTTP 200).
	//
	// The router stores the values as Defaults.Sim. When the stored set
	// has no sim part yet, it also stores them as Current.Sim: that is a
	// change, with version plus one and a log entry by BySimulator. When
	// it has one, Current does not change. The call is safe to repeat:
	// the same values give ResultAlreadyDone. It is not subject to
	// SettingsChangeInterval and does not start it.
	//
	// Errors, each a SettingsErrorResponse: HTTP 400 CodeInvalidRequest
	// (with Fields when the values break the bounds; nothing is stored),
	// HTTP 501 CodeSettingsOff, and CodeUnavailable or CodeInternal.
	PatternPutSimDefaults = "PUT /v1/settings/sim-defaults"
)

// Patterns lists every route pattern that every router serves.
var Patterns = []string{
	PatternPutOperator, PatternOnline, PatternOffline, PatternAccept, PatternRefuse,
	PatternReply, PatternResolve, PatternSettings,
}

// SettingsPatterns lists the route patterns of the stand settings
// (decision 0014). A router that knows decision 0014 serves them next to
// Patterns, also when it does not offer stand settings (it answers
// CodeSettingsOff then). They are not in Patterns, so a router can be
// built from this package before it has their handlers.
var SettingsPatterns = []string{PatternPutSettings, PatternPutSimDefaults}

// QueryChanges is the query parameter of PatternSettings that asks for the
// change log.
const QueryChanges = "changes"

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

// SettingsPath returns the path of PatternSettings and of
// PatternPutSettings.
func SettingsPath() string { return "/v1/settings" }

// SettingsWithChangesPath returns the path and the query of PatternSettings
// with the change log.
func SettingsWithChangesPath() string { return SettingsPath() + "?" + QueryChanges + "=1" }

// SimDefaultsPath returns the path of PatternPutSimDefaults.
func SimDefaultsPath() string { return SettingsPath() + "/sim-defaults" }

func offerPath(operatorID, offerID string) string {
	return OperatorPath(operatorID) + "/offers/" + url.PathEscape(offerID)
}

// MaxBodyBytes is the largest request or response body, except the answers
// that MaxSettingsBodyBytes names.
const MaxBodyBytes = 4096

// MaxSettingsBodyBytes is the largest answer of SettingsWithChangesPath and
// of PatternPutSettings. A full change log of MaxSettingsChanges entries
// that each change every field, with labels of MaxByLen bytes, is below
// half of it; a test checks this.
const MaxSettingsBodyBytes = 32 << 10

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
	// MaxPriority is the highest priority of one operator. The lowest is 0.
	MaxPriority = 9
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
	// Priority breaks a tie between operators with the same lowest load:
	// the higher one gets the offer (decision 0009, "An operator
	// priority"). From 0 to MaxPriority. 0 or absent is the lowest. Like
	// the languages and the capacity, a request replaces it, so a request
	// without it sets it back to 0.
	Priority int `json:"priority,omitempty"`
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
	if r.Priority < 0 || r.Priority > MaxPriority {
		errs = append(errs, fmt.Errorf("priority: %d is not between 0 and %d", r.Priority, MaxPriority))
	}
	return errors.Join(errs...)
}

// Operator is an operator as the router stores it.
type Operator struct {
	ID string `json:"id"`
	// Languages are sorted.
	Languages []string `json:"languages"`
	// Capacity is the effective capacity, after the default was applied.
	Capacity int `json:"capacity"`
	// Priority is the stored priority, 0 when none was sent.
	Priority int      `json:"priority"`
	Presence Presence `json:"presence"`
	// TeamCompanyID is the company of the dedicated team the operator is
	// in, or empty in the shared pool.
	TeamCompanyID string `json:"team_company_id,omitempty"`
}

// OperatorResponse is the answer of PatternPutOperator. Result is
// ResultDone when the operator was created or changed, and
// ResultAlreadyDone when it already had these languages, capacity and
// priority.
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
	// AssignmentTargetUS and FirstResponseTargetUS are the two SLA
	// targets of decision 0009 in model time, in whole microseconds. The
	// gateway passes them to the console (decision 0011). They are
	// optional: a router from before stage 4 does not send them, and 0
	// means "not known". The simulator does not use them.
	AssignmentTargetUS    int64 `json:"assignment_target_us,omitempty"`
	FirstResponseTargetUS int64 `json:"first_response_target_us,omitempty"`
	// Stand is the stand settings block of decision 0014. It is optional:
	// a router from before decision 0014, or one that does not offer stand
	// settings, does not send it. Absent means "stand settings are off".
	Stand *StandState `json:"stand,omitempty"`
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
	if s.AssignmentTargetUS < 0 {
		errs = append(errs, fmt.Errorf("assignment_target_us: %d is negative", s.AssignmentTargetUS))
	}
	if s.FirstResponseTargetUS < 0 {
		errs = append(errs, fmt.Errorf("first_response_target_us: %d is negative", s.FirstResponseTargetUS))
	}
	if s.Stand != nil {
		if err := s.Stand.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("stand: %w", err))
		}
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
	// CodeSettingsConflict: the base version of a stand settings change is
	// not the current version; someone changed the set first. 409.
	CodeSettingsConflict ErrorCode = "settings_conflict"
	// CodeSettingsTooSoon: the last stand settings change was less than
	// SettingsChangeInterval ago. Retry after RetryAfterMS. 429.
	CodeSettingsTooSoon ErrorCode = "settings_too_soon"
	// CodeSettingsOff: this router does not offer stand settings, for
	// example because its environment values are outside the bounds.
	// Retrying does not help. 501.
	CodeSettingsOff ErrorCode = "settings_off"
)

// Codes lists every error code.
var Codes = []ErrorCode{
	CodeInvalidRequest, CodeOperatorNotFound, CodeOfferNotFound, CodeOfferExpired,
	CodeOfferRefused, CodeOfferAccepted, CodeOfferEnded, CodeOfferNotAccepted,
	CodeAssignmentEnded, CodeUnavailable, CodeInternal,
	CodeSettingsConflict, CodeSettingsTooSoon, CodeSettingsOff,
}

func (c ErrorCode) valid() bool {
	for _, k := range Codes {
		if c == k {
			return true
		}
	}
	return false
}

// Status returns the HTTP status of the code. An unknown code gives 500.
func (c ErrorCode) Status() int {
	switch c {
	case CodeInvalidRequest:
		return http.StatusBadRequest
	case CodeOperatorNotFound, CodeOfferNotFound:
		return http.StatusNotFound
	case CodeOfferExpired, CodeOfferRefused, CodeOfferAccepted, CodeOfferEnded,
		CodeOfferNotAccepted, CodeAssignmentEnded, CodeSettingsConflict:
		return http.StatusConflict
	case CodeSettingsTooSoon:
		return http.StatusTooManyRequests
	case CodeUnavailable:
		return http.StatusServiceUnavailable
	case CodeSettingsOff:
		return http.StatusNotImplemented
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
