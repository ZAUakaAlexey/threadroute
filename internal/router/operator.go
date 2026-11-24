package router

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgdb"
)

// The operator actions of decision 0010. Each returns opapi.ResultDone or
// opapi.ResultAlreadyDone, or an *ActionError with the code the HTTP layer
// answers. Any other error is a database error. A late or repeated call
// changes nothing.

// PutOperator creates an operator or sets its languages and capacity
// (opapi.PatternPutOperator). A new operator is offline. A capacity of 0
// takes Config.Capacity. Lowering the capacity below the current load ends
// nothing: the operator gets no new slot until the load is below the new
// capacity. A real change writes an operator_updated event; a call that
// changes nothing writes none.
//
// Locks: the operator row first when it exists, so the upsert compares
// with the state that it then overwrites.
func (s *Service) PutOperator(ctx context.Context, id string, req opapi.OperatorRequest) (opapi.OperatorResponse, error) {
	if !opapi.ValidID(id) {
		return opapi.OperatorResponse{}, actionErr(opapi.CodeInvalidRequest, "operator ID %q is not valid", id)
	}
	if err := req.Validate(); err != nil {
		return opapi.OperatorResponse{}, actionErr(opapi.CodeInvalidRequest, "%v", err)
	}
	capacity := req.Capacity
	if capacity == 0 {
		capacity = s.cfg.Capacity
	}
	var resp opapi.OperatorResponse
	err := s.inTx(ctx, func(q *pgdb.Queries) error {
		if _, err := q.LockOperator(ctx, id); err != nil && !isNoRows(err) {
			return fmt.Errorf("lock operator: %w", err)
		}
		r, err := q.UpsertOperator(ctx, pgdb.UpsertOperatorParams{
			ID: id, Capacity: int32(capacity), Languages: req.Languages,
		})
		if err != nil {
			return fmt.Errorf("upsert: %w", err)
		}
		op, err := q.GetOperator(ctx, id)
		if err != nil {
			return fmt.Errorf("read back: %w", err)
		}
		resp.Result = opapi.ResultAlreadyDone
		resp.Operator = operatorView(op)
		if !r.Created && !r.CapacityChanged && r.LanguagesAdded == 0 && r.LanguagesRemoved == 0 {
			return nil
		}
		resp.Result = opapi.ResultDone
		now := s.now()
		var ob outbox
		if err := addOperatorEvents(ctx, q, &ob, []string{id}, event.TypeOperatorUpdated, now); err != nil {
			return err
		}
		return ob.flush(ctx, q, now)
	})
	if err != nil {
		return opapi.OperatorResponse{}, fmt.Errorf("put operator %s: %w", id, err)
	}
	return resp, nil
}

// GetOperator returns an operator, or an *ActionError with
// opapi.CodeOperatorNotFound.
func (s *Service) GetOperator(ctx context.Context, id string) (opapi.Operator, error) {
	op, err := pgdb.New(s.db).GetOperator(ctx, id)
	if isNoRows(err) {
		return opapi.Operator{}, actionErr(opapi.CodeOperatorNotFound, "operator %s", id)
	}
	if err != nil {
		return opapi.Operator{}, fmt.Errorf("get operator %s: %w", id, err)
	}
	return operatorView(op), nil
}

func operatorView(op pgdb.GetOperatorRow) opapi.Operator {
	langs := slices.Clone(op.Languages)
	slices.Sort(langs)
	return opapi.Operator{
		ID:            op.ID,
		Languages:     langs,
		Capacity:      int(op.Capacity),
		Presence:      opapi.Presence(op.Presence),
		TeamCompanyID: deref(op.DedicatedCompanyID),
	}
}

// Online puts an operator online (opapi.PatternOnline) and writes an
// operator_online event. A repeat writes nothing.
//
// Locks: the operator row, taken by the update of its presence.
func (s *Service) Online(ctx context.Context, id string) (opapi.Result, error) {
	var res opapi.Result
	err := s.inTx(ctx, func(q *pgdb.Queries) error {
		now := s.now()
		n, err := q.SetPresence(ctx, pgdb.SetPresenceParams{Presence: presenceOnline, ChangedAt: now, ID: id})
		if err != nil {
			return fmt.Errorf("set presence: %w", err)
		}
		if n == 1 {
			res = opapi.ResultDone
			var ob outbox
			if err := addOperatorEvents(ctx, q, &ob, []string{id}, event.TypeOperatorOnline, now); err != nil {
				return err
			}
			return ob.flush(ctx, q, now)
		}
		found, err := q.OperatorExists(ctx, id)
		if err != nil {
			return fmt.Errorf("operator exists: %w", err)
		}
		if !found {
			return actionErr(opapi.CodeOperatorNotFound, "operator %s", id)
		}
		res = opapi.ResultAlreadyDone
		return nil
	})
	return res, wrapAction("online", id, err)
}

// Offline puts an operator offline (opapi.PatternOffline). Its offers end
// and the conversations it holds go back to their queues with their
// waiting time kept. The operator is not skipped for them. An
// operator_offline event comes first in the outbox, then the events of the
// conversations. A repeat writes nothing.
//
// Locks: the operator row first, so no new offer can reach the operator,
// then each conversation of its live offers in key order, then the offer.
func (s *Service) Offline(ctx context.Context, id string) (opapi.Result, error) {
	var res opapi.Result
	err := s.inTx(ctx, func(q *pgdb.Queries) error {
		op, err := q.LockOperator(ctx, id)
		if isNoRows(err) {
			return actionErr(opapi.CodeOperatorNotFound, "operator %s", id)
		}
		if err != nil {
			return fmt.Errorf("lock operator: %w", err)
		}
		if op.Presence == presenceOffline {
			// An offline operator gets no offers, so it holds nothing.
			res = opapi.ResultAlreadyDone
			return nil
		}
		now := s.now()
		if _, err := q.SetPresence(ctx, pgdb.SetPresenceParams{Presence: presenceOffline, ChangedAt: now, ID: id}); err != nil {
			return fmt.Errorf("set presence: %w", err)
		}
		var ob outbox
		if err := addOperatorEvents(ctx, q, &ob, []string{id}, event.TypeOperatorOffline, now); err != nil {
			return err
		}
		live, err := q.LiveOffersOfOperator(ctx, id)
		if err != nil {
			return fmt.Errorf("live offers: %w", err)
		}
		for _, l := range live {
			if err := s.returnHeld(ctx, q, &ob, l, id); err != nil {
				return err
			}
		}
		res = opapi.ResultDone
		return ob.flush(ctx, q, s.now())
	})
	return res, wrapAction("offline", id, err)
}

// returnHeld puts one conversation of an operator who went offline back
// into its queue.
func (s *Service) returnHeld(ctx context.Context, q *pgdb.Queries, ob *outbox, l pgdb.LiveOffersOfOperatorRow, opID string) error {
	c, err := q.LockConversation(ctx, pgdb.LockConversationParams{CompanyID: l.CompanyID, CustomerID: l.CustomerID})
	if err != nil {
		return fmt.Errorf("lock conversation: %w", err)
	}
	o, err := q.LockOffer(ctx, l.Num)
	if err != nil {
		return fmt.Errorf("lock offer: %w", err)
	}
	if o.OperatorID != opID || (o.State != offerOffered && o.State != offerAccepted) {
		return nil // ended while this transaction waited for the lock
	}
	now := s.now(c.QueuedAt)
	if o.State == offerOffered {
		err = returnOffer(ctx, q, &c, &o, ob, now, offerEnded, endOperatorOffline,
			event.OfferOperatorOffline, event.QueueOperatorOffline, false)
	} else {
		o.State = offerAssignmentEnded
		o.EndedAt = ptr(now)
		o.EndReason = ptr(endOperatorOffline)
		if err = saveOffer(ctx, q, &o); err == nil {
			err = enqueue(ctx, q, &c, ob, now, event.QueueOperatorOffline, previousOf(&o), true, false)
		}
	}
	if err != nil {
		return err
	}
	return saveConv(ctx, q, &c, now)
}

// Accept accepts an offer (opapi.PatternAccept). The conversation is
// assigned and the SLA target "time to assignment" is met.
func (s *Service) Accept(ctx context.Context, operatorID, offerID string) (opapi.Result, error) {
	return s.offerAction(ctx, actAccept, operatorID, offerID, "")
}

// Refuse refuses an offer (opapi.PatternRefuse). The slot is freed, the
// operator is skipped for the session and the conversation goes back to
// its queue with its waiting time kept.
func (s *Service) Refuse(ctx context.Context, operatorID, offerID string) (opapi.Result, error) {
	return s.offerAction(ctx, actRefuse, operatorID, offerID, "")
}

// Reply records a reply marker of an assignment (opapi.PatternReply). The
// first reply of the session meets the SLA target "time to first
// response".
func (s *Service) Reply(ctx context.Context, operatorID, offerID, replyID string) (opapi.Result, error) {
	if err := (opapi.ReplyRequest{ReplyID: replyID}).Validate(); err != nil {
		return "", actionErr(opapi.CodeInvalidRequest, "%v", err)
	}
	return s.offerAction(ctx, actReply, operatorID, offerID, replyID)
}

// Resolve resolves an assignment (opapi.PatternResolve), which frees the
// slot. The conversation stays open in conv; a later customer message of
// the session puts it back into the queue with the same operator
// preferred.
func (s *Service) Resolve(ctx context.Context, operatorID, offerID string) (opapi.Result, error) {
	return s.offerAction(ctx, actResolve, operatorID, offerID, "")
}

// offerAction runs one action on an offer. The offer is read first without
// a lock to find its conversation. Then the conversation row is locked,
// the offer is locked and read again, and the outcome is decided from the
// state under the lock.
func (s *Service) offerAction(ctx context.Context, a action, operatorID, offerID, replyID string) (opapi.Result, error) {
	if !opapi.ValidID(operatorID) || !opapi.ValidID(offerID) {
		return "", actionErr(opapi.CodeInvalidRequest, "operator ID %q or offer ID %q is not valid", operatorID, offerID)
	}
	o0, err := pgdb.New(s.db).GetOfferByID(ctx, offerID)
	if err != nil && !isNoRows(err) {
		return "", wrapAction(a.String(), operatorID, err)
	}
	if isNoRows(err) || o0.OperatorID != operatorID {
		found, err := pgdb.New(s.db).OperatorExists(ctx, operatorID)
		if err != nil {
			return "", wrapAction(a.String(), operatorID, err)
		}
		if !found {
			return "", actionErr(opapi.CodeOperatorNotFound, "operator %s", operatorID)
		}
		return "", actionErr(opapi.CodeOfferNotFound, "operator %s has no offer %s", operatorID, offerID)
	}

	var res opapi.Result
	err = s.inTx(ctx, func(q *pgdb.Queries) error {
		c, err := q.LockConversation(ctx, pgdb.LockConversationParams{CompanyID: o0.CompanyID, CustomerID: o0.CustomerID})
		if err != nil {
			return fmt.Errorf("lock conversation: %w", err)
		}
		o, err := q.LockOffer(ctx, o0.Num)
		if err != nil {
			return fmt.Errorf("lock offer: %w", err)
		}
		now := s.now(c.QueuedAt, &o.OfferedAt)
		seen := false
		if a == actReply {
			if seen, err = q.ReplyExists(ctx, pgdb.ReplyExistsParams{OfferNum: o.Num, ReplyID: replyID}); err != nil {
				return fmt.Errorf("reply exists: %w", err)
			}
		}
		res, err = outcome(a, o.State, !now.Before(o.AcceptBy), seen)
		if err != nil || res != opapi.ResultDone {
			return err
		}
		want := stateOffered
		if o.State == offerAccepted {
			want = stateAssigned
		}
		if c.State != want {
			return fmt.Errorf("offer %s is %s but its conversation is %s", o.ID, o.State, c.State)
		}
		var ob outbox
		switch a {
		case actAccept:
			err = s.accept(ctx, q, &c, &o, &ob, now)
		case actRefuse:
			err = returnOffer(ctx, q, &c, &o, &ob, now, offerRefused, endRefused,
				event.OfferRefused, event.QueueOfferRefused, true)
		case actReply:
			err = s.reply(ctx, q, &c, &o, &ob, now, replyID)
		case actResolve:
			err = resolve(ctx, q, &c, &o, &ob, now)
		}
		if err != nil {
			return err
		}
		if err := saveConv(ctx, q, &c, now); err != nil {
			return err
		}
		return ob.flush(ctx, q, now)
	})
	if err != nil {
		return "", wrapAction(a.String(), operatorID, err)
	}
	return res, nil
}

func (s *Service) accept(ctx context.Context, q *pgdb.Queries, c *pgdb.Conversation, o *pgdb.Offer, ob *outbox, now time.Time) error {
	o.State = offerAccepted
	o.AcceptedAt = ptr(now)
	if err := saveOffer(ctx, q, o); err != nil {
		return err
	}
	c.State = stateAssigned
	c.LastOperatorID = ptr(o.OperatorID)
	c.LastOfferID = ptr(o.ID)
	c.PreferredOperatorID = nil
	if c.FirstAcceptedAt == nil {
		c.FirstAcceptedAt = ptr(now)
	}
	ev := convEvent(event.TypeAccepted, now)
	ev.QueuedAt = deref(c.QueuedAt)
	ev.OperatorID = o.OperatorID
	ev.OfferID = o.ID
	ev.Wait = s.cfg.model(now.Sub(deref(c.QueuedAt)))
	return ob.addConv(c, ev)
}

func (s *Service) reply(ctx context.Context, q *pgdb.Queries, c *pgdb.Conversation, o *pgdb.Offer, ob *outbox,
	now time.Time, replyID string,
) error {
	if _, err := q.InsertReply(ctx, pgdb.InsertReplyParams{OfferNum: o.Num, ReplyID: replyID, RepliedAt: now}); err != nil {
		return fmt.Errorf("insert reply: %w", err)
	}
	ev := convEvent(event.TypeReplied, now)
	ev.OperatorID = o.OperatorID
	ev.OfferID = o.ID
	if c.FirstRepliedAt == nil {
		start := c.SlaStartedAt
		if start == nil {
			start = c.QueuedAt
		}
		rt := s.cfg.model(now.Sub(deref(start)))
		c.FirstRepliedAt = ptr(now)
		c.SimResponseUs = ptr(rt.Microseconds())
		ev.FirstReply = true
		ev.ResponseTime = rt
	}
	return ob.addConv(c, ev)
}

func resolve(ctx context.Context, q *pgdb.Queries, c *pgdb.Conversation, o *pgdb.Offer, ob *outbox, now time.Time) error {
	o.State = offerResolved
	o.EndedAt = ptr(now)
	if err := saveOffer(ctx, q, o); err != nil {
		return err
	}
	c.State = stateResolved
	ev := convEvent(event.TypeResolved, now)
	ev.OperatorID = o.OperatorID
	ev.OfferID = o.ID
	return ob.addConv(c, ev)
}

// wrapAction adds the action and the operator to a database error. An
// *ActionError is returned as it is.
func wrapAction(action, operatorID string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*ActionError](err); ok {
		return err
	}
	return fmt.Errorf("%s of operator %s: %w", action, operatorID, err)
}
