package gwapi

import "github.com/ZAUakaAlexey/threadroute/internal/opapi"

// CloseCode is the status code of a WebSocket close frame that the server
// sends. Codes below 4000 are the standard ones of RFC 6455; codes from
// 4000 are this protocol's own. The close reason is CloseCode.Name.
type CloseCode int

// Close codes.
const (
	// CloseNormal: the client or the server ended the connection on
	// purpose. Do not reconnect.
	CloseNormal CloseCode = 1000
	// CloseGoingAway: the gateway shuts down or restarts. Reconnect with
	// backoff.
	CloseGoingAway CloseCode = 1001
	// CloseMessageTooBig: a client message was larger than
	// MaxClientMessageBytes. A bug in the client: do not reconnect at
	// once.
	CloseMessageTooBig CloseCode = 1009
	// CloseInternal: an unexpected error in the gateway. Reconnect with
	// backoff.
	CloseInternal CloseCode = 1011
	// CloseTryAgainLater: the gateway cannot take the connection now (for
	// example it is overloaded). Reconnect with backoff.
	CloseTryAgainLater CloseCode = 1013

	// CloseBadMessage: a message was not valid JSON, had an unknown type,
	// had invalid fields, or the first message was not a Hello. A bug in
	// the client: do not reconnect at once.
	CloseBadMessage CloseCode = 4400
	// CloseUnauthorized: the site rejected the token of the Hello. The
	// client drops the token and may connect again without one, at
	// LevelDemo.
	CloseUnauthorized CloseCode = 4401
	// CloseHelloTimeout: no valid Hello within HelloTimeout.
	CloseHelloTimeout CloseCode = 4408
	// CloseReplaced: a newer connection took over the operator of this
	// connection (the same person in another tab, or after a reload). Do
	// not reconnect automatically: the newer connection is the one in use.
	CloseReplaced CloseCode = 4409
	// CloseSlowClient: the client did not read its messages fast enough
	// (decision 0011, point 4). Reconnect: the new connection starts with
	// a snapshot.
	CloseSlowClient CloseCode = 4410
	// CloseUnsupportedVersion: the Hello named a protocol version other
	// than ProtocolVersion. The reason is the name, like for every code.
	// The client must be reloaded, not reconnected.
	CloseUnsupportedVersion CloseCode = 4426
	// CloseTooManyConnections: this address holds too many connections.
	// Reconnect later.
	CloseTooManyConnections CloseCode = 4429
	// CloseNoOperatorSeat: the operator view at LevelDemo is full: the
	// limit of demo operators at the same time is reached (decision 0012,
	// point 3). Try later, or open the supervisor view.
	CloseNoOperatorSeat CloseCode = 4503
	// CloseAuthUnavailable: the token could not be checked because the
	// site API did not answer. Try later, or connect without a token.
	CloseAuthUnavailable CloseCode = 4504
)

// CloseCodes lists every close code of the protocol.
var CloseCodes = []CloseCode{
	CloseNormal, CloseGoingAway, CloseMessageTooBig, CloseInternal, CloseTryAgainLater,
	CloseBadMessage, CloseUnauthorized, CloseHelloTimeout, CloseReplaced, CloseSlowClient,
	CloseUnsupportedVersion, CloseTooManyConnections, CloseNoOperatorSeat, CloseAuthUnavailable,
}

// Name returns the short name of the code, which the server also sends as
// the close reason. An unknown code gives "unknown".
func (c CloseCode) Name() string {
	switch c {
	case CloseNormal:
		return "normal"
	case CloseGoingAway:
		return "going_away"
	case CloseMessageTooBig:
		return "message_too_big"
	case CloseInternal:
		return "internal"
	case CloseTryAgainLater:
		return "try_again_later"
	case CloseBadMessage:
		return "bad_message"
	case CloseUnauthorized:
		return "unauthorized"
	case CloseHelloTimeout:
		return "hello_timeout"
	case CloseReplaced:
		return "replaced"
	case CloseSlowClient:
		return "slow_client"
	case CloseUnsupportedVersion:
		return "unsupported_version"
	case CloseTooManyConnections:
		return "too_many_connections"
	case CloseNoOperatorSeat:
		return "no_operator_seat"
	case CloseAuthUnavailable:
		return "auth_unavailable"
	}
	return "unknown"
}

// Reconnect reports whether a client should reconnect by itself, with
// backoff, after the server closed with this code.
func (c CloseCode) Reconnect() bool {
	switch c {
	case CloseGoingAway, CloseInternal, CloseTryAgainLater, CloseSlowClient,
		CloseTooManyConnections, CloseNoOperatorSeat, CloseAuthUnavailable:
		return true
	}
	return false
}

// ErrorCode is the code of a rejected request in a Result. It has every
// code of the operator API (opapi.Codes), which the gateway passes on as it
// gets them from the router, and the gateway's own codes.
type ErrorCode string

// Error codes of the operator API, passed on from the router.
const (
	CodeInvalidRequest   = ErrorCode(opapi.CodeInvalidRequest)
	CodeOperatorNotFound = ErrorCode(opapi.CodeOperatorNotFound)
	CodeOfferNotFound    = ErrorCode(opapi.CodeOfferNotFound)
	CodeOfferExpired     = ErrorCode(opapi.CodeOfferExpired)
	CodeOfferRefused     = ErrorCode(opapi.CodeOfferRefused)
	CodeOfferAccepted    = ErrorCode(opapi.CodeOfferAccepted)
	CodeOfferEnded       = ErrorCode(opapi.CodeOfferEnded)
	CodeOfferNotAccepted = ErrorCode(opapi.CodeOfferNotAccepted)
	CodeAssignmentEnded  = ErrorCode(opapi.CodeAssignmentEnded)
	CodeUnavailable      = ErrorCode(opapi.CodeUnavailable)
	CodeInternal         = ErrorCode(opapi.CodeInternal)
)

// Error codes of the gateway.
const (
	// CodeNotAllowed: the request is not allowed for the view or level of
	// the connection, for example an operator action on a supervisor
	// connection.
	CodeNotAllowed ErrorCode = "not_allowed"
	// CodeRateLimited: the connection sent more requests than
	// ActionsPerSecond allows. Retry later.
	CodeRateLimited ErrorCode = "rate_limited"
	// CodeUnknownOffer: the offer is not a live offer or assignment of the
	// operator of this connection in the gateway's view. The gateway does
	// not call the router.
	CodeUnknownOffer ErrorCode = "unknown_offer"
	// CodeStaleConnection: the connection no longer owns its operator (a
	// newer connection took it over, or the grace time ended), or the
	// gateway's data is stale so that it cannot check the request. The
	// client reconnects.
	CodeStaleConnection ErrorCode = "stale_connection"
	// CodeUnknownConversation: Watch named a conversation that is not in
	// the view of this connection, or that it may not watch. An Unwatch of
	// a conversation that is not watched gets OutcomeAlreadyDone instead.
	CodeUnknownConversation ErrorCode = "unknown_conversation"
	// CodeTooManyWatched: Watch when the connection already watches
	// MaxWatched conversations.
	CodeTooManyWatched ErrorCode = "too_many_watched"
)

// Codes lists every error code.
var Codes = []ErrorCode{
	CodeInvalidRequest, CodeOperatorNotFound, CodeOfferNotFound, CodeOfferExpired,
	CodeOfferRefused, CodeOfferAccepted, CodeOfferEnded, CodeOfferNotAccepted,
	CodeAssignmentEnded, CodeUnavailable, CodeInternal,
	CodeNotAllowed, CodeRateLimited, CodeUnknownOffer, CodeStaleConnection,
	CodeUnknownConversation, CodeTooManyWatched,
}

// Valid reports whether c is a known code.
func (c ErrorCode) Valid() bool {
	for _, k := range Codes {
		if c == k {
			return true
		}
	}
	return false
}

// Retry reports whether the same request may succeed later without a
// change: the router or the gateway was busy, or the rate limit applied.
func (c ErrorCode) Retry() bool {
	return c == CodeUnavailable || c == CodeRateLimited
}

// FromOpapi returns the gwapi code of an operator API code.
func FromOpapi(c opapi.ErrorCode) ErrorCode {
	return ErrorCode(c)
}
