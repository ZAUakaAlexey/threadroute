package router

import (
	"context"
	"fmt"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgdb"
)

// stored is the part of a conversation row that planApply reads.
type stored struct {
	Session, Seq int64
	State        string
}

// applyPlan is what one conversation event does to the routing state.
type applyPlan struct {
	// skip: the event was applied before (decision 0009, "Events and
	// state"). Nothing changes.
	skip bool
	// reset: conv started the numbering of the conversation again
	// (decision 0009, "Sequence numbers from conv can restart").
	reset bool
	// endSession ends the session the router holds, with endReason. A live
	// offer or assignment ends and its slot is freed.
	endSession bool
	endReason  event.CloseReason
	// startSession starts the session of the event.
	startSession bool
	// closed: the event closes the session of the event.
	closed bool
	// queue puts the conversation into its queue with this reason.
	queue event.QueueReason
	// preferPrevious makes the last operator the preferred one.
	preferPrevious bool
	// realResponse stores the real response time of the thread author.
	realResponse bool
}

// planApply decides what ev does. cur is nil for a conversation the router
// has never seen. It reads nothing else, so it is tested without a
// database.
func planApply(cur *stored, ev event.Conversation) applyPlan {
	var p applyPlan
	session, seq := int64(ev.Session), int64(ev.Seq)
	answered := ev.Type == event.TypeAnswered

	switch {
	case cur == nil:
		p.startSession = true
		if ev.Type == event.TypeClosed {
			p.closed = true
			return p
		}
		p.queue = event.QueueNew
		if ev.Reopened() {
			// The router never saw the earlier session, so it has no
			// operator to prefer.
			p.queue = event.QueueReopened
		}
		p.realResponse = answered
		return p

	case ev.Type == event.TypeOpened && session == 1 && seq == 1 &&
		cur.State == stateClosed && (cur.Session != 1 || cur.Seq != 1):
		p.reset = true
		p.startSession = true
		p.queue = event.QueueNew
		return p

	case session < cur.Session || (session == cur.Session && seq <= cur.Seq):
		p.skip = true
		return p

	case session > cur.Session:
		if cur.State != stateClosed {
			// The closed event of the earlier session never arrived.
			p.endSession = true
			p.endReason = event.CloseInactivity
		}
		p.startSession = true
		if ev.Type == event.TypeClosed {
			p.closed = true
			return p
		}
		p.queue = event.QueueReopened
		p.preferPrevious = true
		p.realResponse = answered
		return p
	}

	// A later event of the same session.
	switch ev.Type {
	case event.TypeClosed:
		if cur.State != stateClosed {
			p.endSession = true
			p.endReason = ev.CloseReason
		}
	case event.TypeMessageAdded:
		if ev.Direction == event.FromCustomer && cur.State == stateResolved {
			p.queue = event.QueueCustomerMessage
			p.preferPrevious = true
		}
	case event.TypeAnswered:
		p.realResponse = true
	}
	return p
}

// ApplyResult says what ApplyConversation did.
type ApplyResult struct {
	// Skipped: the event was applied before and changed nothing.
	Skipped bool
	// Reset: conv restarted the numbers of the conversation.
	Reset bool
	// Queued: the conversation entered its queue. Pending: it would have,
	// but the router is catching up.
	Queued, Pending bool
	// Ended: a session the router routed ended.
	Ended bool
	// Events is the number of routing events written.
	Events int
}

// ApplyConversation applies one conversation event from conv in one
// transaction. It is idempotent by session and sequence number. While
// catchUp is true nothing is queued for offers: a conversation that needs an
// operator becomes pending, and EndCatchUp queues it (decision 0009,
// "Catching up"). Closing a conversation that an operator holds still ends
// the offer or assignment and writes its "ended" event.
//
// Events of one conversation must be applied in order of their sequence
// numbers; an older event that comes after a newer one is skipped.
func (s *Service) ApplyConversation(ctx context.Context, ev event.Conversation, catchUp bool) (ApplyResult, error) {
	if err := ev.Validate(); err != nil {
		return ApplyResult{}, fmt.Errorf("apply conversation event: %w", err)
	}
	var res ApplyResult
	err := s.inTx(ctx, func(q *pgdb.Queries) error {
		res = ApplyResult{}
		c, isNew, err := lockOrInsertConversation(ctx, q, ev, s.now())
		if err != nil {
			return err
		}
		var cur *stored
		if !isNew {
			cur = &stored{Session: c.ConvSession, Seq: c.ConvSeq, State: c.State}
		}
		p := planApply(cur, ev)
		if p.skip {
			res.Skipped = true
			return nil
		}
		now := s.now(c.QueuedAt)
		var ob outbox
		if err := s.applyPlan(ctx, q, &c, &ob, p, ev, isNew, catchUp, now, &res); err != nil {
			return err
		}
		if err := saveConv(ctx, q, &c, now); err != nil {
			return err
		}
		res.Events = ob.len()
		return ob.flush(ctx, q, now)
	})
	if err != nil {
		return ApplyResult{}, fmt.Errorf("apply conversation event: %w", err)
	}
	return res, nil
}

// lockOrInsertConversation locks the row of the conversation of ev, or
// inserts it. isNew reports an inserted row, which holds the numbers of ev
// and a placeholder state until the plan sets them.
func lockOrInsertConversation(ctx context.Context, q *pgdb.Queries, ev event.Conversation, now time.Time) (pgdb.Conversation, bool, error) {
	key := pgdb.LockConversationParams{CompanyID: ev.CompanyID, CustomerID: ev.CustomerID}
	c, err := q.LockConversation(ctx, key)
	if err == nil {
		return c, false, nil
	}
	if !isNoRows(err) {
		return c, false, fmt.Errorf("lock conversation: %w", err)
	}
	n, err := q.InsertConversation(ctx, pgdb.InsertConversationParams{
		CompanyID: ev.CompanyID, CustomerID: ev.CustomerID,
		ConvSession: int64(ev.Session), ConvSeq: int64(ev.Seq), Lang: ev.Lang, UpdatedAt: now,
	})
	if err != nil {
		return c, false, fmt.Errorf("insert conversation: %w", err)
	}
	c, err = q.LockConversation(ctx, key)
	if err != nil {
		return c, false, fmt.Errorf("lock conversation: %w", err)
	}
	// n is 0 when another transaction inserted the row first. The row is
	// then a real one and the plan must see it.
	return c, n == 1, nil
}

func (s *Service) applyPlan(ctx context.Context, q *pgdb.Queries, c *pgdb.Conversation, ob *outbox,
	p applyPlan, ev event.Conversation, isNew, catchUp bool, now time.Time, res *ApplyResult,
) error {
	res.Reset = p.reset
	if p.endSession {
		ended, err := endSession(ctx, q, c, ob, now, p.endReason)
		if err != nil {
			return err
		}
		res.Ended = ended
	}
	if p.startSession {
		if !isNew {
			_, err := q.ClearSkips(ctx, pgdb.ClearSkipsParams{
				CompanyID: c.CompanyID, CustomerID: c.CustomerID, AllSessions: true,
			})
			if err != nil {
				return fmt.Errorf("clear skips: %w", err)
			}
		}
		startSession(c, ev)
		if p.reset {
			c.LastOperatorID = nil
		}
	}
	if p.closed {
		c.State = stateClosed
		c.CloseReason = ptr(string(ev.CloseReason))
	}
	if p.realResponse && c.RealResponseUs == nil {
		c.RealResponseUs = ptr(ev.ResponseTime.Microseconds())
	}
	if p.queue != "" {
		if p.preferPrevious {
			c.PreferredOperatorID = c.LastOperatorID
		}
		if err := enqueue(ctx, q, c, ob, now, p.queue, false, catchUp); err != nil {
			return err
		}
		res.Queued = !catchUp
		res.Pending = catchUp
	}
	c.ConvSession = int64(ev.Session)
	c.ConvSeq = int64(ev.Seq)
	return nil
}

// startSession resets the per-session columns for the session of ev.
func startSession(c *pgdb.Conversation, ev event.Conversation) {
	c.ConvSession = int64(ev.Session)
	c.Lang = ev.Lang
	c.State = stateClosed
	c.TeamQueue = false
	c.Priority = int32(event.PriorityNormal)
	c.QueuedAt = nil
	c.PreferredOperatorID = nil
	c.SlaStartedAt = nil
	c.FirstAcceptedAt = nil
	c.FirstRepliedAt = nil
	c.SlaAssignmentMissed = false
	c.SlaResponseMissed = false
	c.SimResponseUs = nil
	c.RealResponseUs = nil
	c.CloseReason = nil
}

// endSession closes the session the router holds. A live offer or
// assignment ends and its slot is freed (decision 0009, step 6). An
// "ended" event is written when the session was routed, that is when it
// had entered a queue; a session that was only pending (opened and closed
// during the catch-up) never reached an operator and gets no event. It
// reports whether an event was written.
func endSession(ctx context.Context, q *pgdb.Queries, c *pgdb.Conversation, ob *outbox,
	now time.Time, reason event.CloseReason,
) (bool, error) {
	routed := c.State != statePending && c.State != stateClosed
	ev := convEvent(event.TypeEnded, now)
	ev.EndReason = reason
	if c.State == stateOffered || c.State == stateAssigned {
		o, err := q.LiveOffer(ctx, pgdb.LiveOfferParams{CompanyID: c.CompanyID, CustomerID: c.CustomerID})
		if err != nil {
			return false, fmt.Errorf("live offer of a %s conversation: %w", c.State, err)
		}
		if o.State == offerAccepted {
			o.State = offerAssignmentEnded
		} else {
			o.State = offerEnded
		}
		o.EndedAt = ptr(now)
		o.EndReason = ptr(endClosed)
		if err := saveOffer(ctx, q, &o); err != nil {
			return false, err
		}
		ev.OperatorID = o.OperatorID
		ev.OfferID = o.ID
	}
	c.State = stateClosed
	c.CloseReason = ptr(string(reason))
	c.PreferredOperatorID = nil
	if !routed {
		return false, nil
	}
	return true, ob.addConv(c, ev)
}

// EndCatchUp ends the catch-up: every pending conversation enters its
// queue with the reason catch_up, and its waiting time and SLA targets
// start now (decision 0009, "Catching up"). It works in transactions of up
// to batch conversations and returns how many it queued. Call it after the
// last catch-up event was applied.
func (s *Service) EndCatchUp(ctx context.Context, batch int) (int, error) {
	if batch <= 0 {
		batch = 500
	}
	total := 0
	for {
		n := 0
		err := s.inTx(ctx, func(q *pgdb.Queries) error {
			n = 0
			rows, err := q.PendingConversations(ctx, int32(batch))
			if err != nil {
				return fmt.Errorf("pending conversations: %w", err)
			}
			now := s.now()
			var ob outbox
			for i := range rows {
				c := &rows[i]
				if err := enqueue(ctx, q, c, &ob, now, event.QueueCatchUp, false, false); err != nil {
					return err
				}
				if err := saveConv(ctx, q, c, now); err != nil {
					return err
				}
			}
			n = len(rows)
			return ob.flush(ctx, q, now)
		})
		if err != nil {
			return total, fmt.Errorf("end catch-up: %w", err)
		}
		total += n
		if n < batch {
			return total, nil
		}
	}
}
