package router

import (
	"fmt"

	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// ActionError is the answer to an operator action that was not applied: an
// unknown operator or offer, or a rejection because the state moved on
// (decision 0010, "Three outcomes"). The HTTP layer reads Code with
// errors.As. Every other error of an action is a database error.
type ActionError struct {
	Code    opapi.ErrorCode
	Message string
}

func (e *ActionError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func actionErr(code opapi.ErrorCode, format string, args ...any) *ActionError {
	return &ActionError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Offer states, as in the offers table.
const (
	offerOffered         = "offered"
	offerAccepted        = "accepted"
	offerExpired         = "expired"
	offerRefused         = "refused"
	offerEnded           = "ended"
	offerResolved        = "resolved"
	offerAssignmentEnded = "assignment_ended"
)

// Offer end reasons, as in the offers table.
const (
	endTimeout         = "timeout"
	endRefused         = "refused"
	endOperatorOffline = "operator_offline"
	endClosed          = "closed"
)

// action is an operator action on an offer.
type action int

const (
	actAccept action = iota
	actRefuse
	actReply
	actResolve
)

func (a action) String() string {
	switch a {
	case actAccept:
		return "accept"
	case actRefuse:
		return "refuse"
	case actReply:
		return "reply"
	case actResolve:
		return "resolve"
	}
	return "unknown"
}

// outcome decides what an action on an offer does, from the state of the
// offer alone:
//
//   - pastDeadline: the accept timeout passed, but the timer has not
//     expired the offer yet. The offer counts as expired.
//   - replySeen: for a reply, the reply ID was recorded before.
//
// It returns opapi.ResultDone when the action must be applied now,
// opapi.ResultAlreadyDone for a repeat, or an *ActionError with the
// rejection code. The codes are the ones listed in opapi for each pattern.
func outcome(a action, state string, pastDeadline, replySeen bool) (opapi.Result, error) {
	if a == actReply && replySeen {
		return opapi.ResultAlreadyDone, nil
	}
	if state == offerOffered && pastDeadline {
		state = offerExpired
	}
	reject := func(code opapi.ErrorCode) (opapi.Result, error) {
		return "", actionErr(code, "%s: the offer is %s", a, state)
	}
	switch state {
	case offerExpired:
		return reject(opapi.CodeOfferExpired)
	case offerEnded:
		return reject(opapi.CodeOfferEnded)
	case offerRefused:
		if a == actRefuse {
			return opapi.ResultAlreadyDone, nil
		}
		return reject(opapi.CodeOfferRefused)
	}

	switch a {
	case actAccept:
		switch state {
		case offerOffered:
			return opapi.ResultDone, nil
		case offerAccepted, offerResolved, offerAssignmentEnded:
			return opapi.ResultAlreadyDone, nil
		}
	case actRefuse:
		switch state {
		case offerOffered:
			return opapi.ResultDone, nil
		case offerAccepted, offerResolved, offerAssignmentEnded:
			return reject(opapi.CodeOfferAccepted)
		}
	case actReply:
		switch state {
		case offerOffered:
			return reject(opapi.CodeOfferNotAccepted)
		case offerAccepted:
			return opapi.ResultDone, nil
		case offerResolved, offerAssignmentEnded:
			return reject(opapi.CodeAssignmentEnded)
		}
	case actResolve:
		switch state {
		case offerOffered:
			return reject(opapi.CodeOfferNotAccepted)
		case offerAccepted:
			return opapi.ResultDone, nil
		case offerResolved:
			return opapi.ResultAlreadyDone, nil
		case offerAssignmentEnded:
			// Decision 0010: resolving a conversation that conv closed is
			// a rejection, because the operator did not end it.
			return reject(opapi.CodeAssignmentEnded)
		}
	}
	return "", fmt.Errorf("%s: unknown offer state %q", a, state)
}
