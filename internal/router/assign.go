package router

import (
	"context"
	"fmt"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgdb"
)

// assignPage is how many waiting conversations one query lists.
const assignPage = 100

// pickAttempts bounds how often one assignment picks an operator that turns
// out to be full, offline or in another group once its lock is held.
const pickAttempts = 3

// AssignResult says what Assign did.
type AssignResult struct {
	// Scanned is the number of waiting conversations tried.
	Scanned int
	// Offered is the number of offers made.
	Offered int
	// GeneralQueue is the number of those offers that went to an operator
	// of the general queue (und) who does not have the language of the
	// conversation, because no online operator of the shared pool had it
	// (decision 0009, "A language that no online operator has").
	GeneralQueue int
	// Overflow is the number of offers of a team-queue conversation that
	// went to an operator of the shared pool, because no member of the team
	// could take it (decision 0009, "Overflow from a team to the shared
	// pool"). An offer can be both: it then counts here and in
	// GeneralQueue.
	Overflow int
}

// Assign offers waiting conversations to operators, in queue order
// (priority, then waiting time), trying at most limit conversations. Each
// offer is one transaction that locks the conversation row and the
// operator row with FOR UPDATE SKIP LOCKED, counts the operator's live
// slots again under the lock, reserves a slot and writes the "offered"
// event (decision 0009, "Assignment"). It is safe to run from several
// workers and several routers at once: a conversation or an operator that
// another worker holds is skipped, not waited for.
//
// A conversation that no operator can serve now stays waiting and does not
// block the ones behind it.
func (s *Service) Assign(ctx context.Context, limit int) (AssignResult, error) {
	var res AssignResult
	arg := pgdb.WaitingConversationsParams{First: true}
	for res.Scanned < limit {
		arg.MaxRows = int32(min(assignPage, limit-res.Scanned))
		rows, err := pgdb.New(s.db).WaitingConversations(ctx, arg)
		if err != nil {
			return res, fmt.Errorf("assign: waiting conversations: %w", err)
		}
		for _, r := range rows {
			res.Scanned++
			pick, err := s.assignOne(ctx, r.CompanyID, r.CustomerID)
			if err != nil {
				return res, fmt.Errorf("assign: %w", err)
			}
			if pick.id != "" {
				res.Offered++
			}
			if pick.generalQueue {
				res.GeneralQueue++
			}
			if pick.overflow {
				res.Overflow++
			}
		}
		if len(rows) < int(arg.MaxRows) {
			break
		}
		last := rows[len(rows)-1]
		arg = pgdb.WaitingConversationsParams{
			AfterPriority: last.Priority, AfterQueuedAt: last.QueuedAt,
			AfterCompanyID: last.CompanyID, AfterCustomerID: last.CustomerID,
		}
	}
	return res, nil
}

// assignOne tries to offer one conversation. It returns the operator of
// the offer that was made, or an empty pick when none was made.
func (s *Service) assignOne(ctx context.Context, companyID, customerID string) (picked, error) {
	var done picked
	err := s.inTx(ctx, func(q *pgdb.Queries) error {
		done = picked{}
		c, err := q.TryLockWaitingConversation(ctx, pgdb.TryLockWaitingConversationParams{
			CompanyID: companyID, CustomerID: customerID,
		})
		if isNoRows(err) {
			return nil // taken by another worker, or no longer waiting
		}
		if err != nil {
			return fmt.Errorf("lock conversation: %w", err)
		}
		pick, err := s.pickOperator(ctx, q, &c)
		if err != nil || pick.id == "" {
			return err
		}
		op := pick.id
		now := s.now(c.QueuedAt)
		cfg := s.config()
		acceptBy := now.Add(cfg.machine(cfg.AcceptTimeout))
		o, err := q.InsertOffer(ctx, pgdb.InsertOfferParams{
			CompanyID: c.CompanyID, CustomerID: c.CustomerID, Session: c.ConvSession,
			OperatorID: op, Capacity: pick.capacity, QueuedAt: *c.QueuedAt, OfferedAt: now, AcceptBy: acceptBy,
		})
		if err != nil {
			return fmt.Errorf("insert offer: %w", err)
		}
		c.State = stateOffered
		c.PreferredOperatorID = nil
		var ob outbox
		ev := convEvent(event.TypeOffered, now)
		ev.QueuedAt = *c.QueuedAt
		ev.OperatorID = op
		ev.OfferID = o.ID
		ev.AcceptBy = acceptBy
		if err := ob.addConv(&c, ev); err != nil {
			return err
		}
		if err := saveConv(ctx, q, &c, now); err != nil {
			return err
		}
		if err := ob.flush(ctx, q, now); err != nil {
			return err
		}
		done = pick
		return nil
	})
	if err != nil {
		return picked{}, err
	}
	return done, nil
}

// picked is the operator that pickOperator locked.
type picked struct {
	id       string
	capacity int32
	// generalQueue: the operator does not have the language and serves
	// the conversation through the general queue rule.
	generalQueue bool
	// overflow: the conversation is in a team queue and the operator is
	// in the shared pool.
	overflow bool
}

// pickOperator picks and locks an operator for the locked conversation c
// and returns it with its capacity, or an empty ID when no operator can
// take it now. The pick locks the operator row with SKIP LOCKED. Its load was read
// from the snapshot of the pick, so it is counted again here, in a new
// statement, while the lock is held: every transaction that adds a slot to
// this operator holds the same lock, so this count is exact.
func (s *Service) pickOperator(ctx context.Context, q *pgdb.Queries, c *pgdb.Conversation) (picked, error) {
	var team *string
	if c.TeamQueue {
		team = &c.CompanyID
	}
	arg := pgdb.PickOperatorParams{
		TeamCompanyID: team, Lang: c.Lang, UseSkips: true,
		CompanyID: c.CompanyID, CustomerID: c.CustomerID, Session: c.ConvSession,
		Exclude: []string{}, PreferredOperatorID: deref(c.PreferredOperatorID),
	}
	skipsChecked := false
	for range pickAttempts {
		row, err := q.PickOperator(ctx, arg)
		if isNoRows(err) {
			if skipsChecked {
				return picked{}, nil
			}
			skipsChecked = true
			cleared, err := clearSkipsIfStuck(ctx, q, c, team)
			if err != nil || !cleared {
				return picked{}, err
			}
			continue
		}
		if err != nil {
			return picked{}, fmt.Errorf("pick operator: %w", err)
		}
		slots, err := q.OperatorSlots(ctx, row.ID)
		if err != nil {
			return picked{}, fmt.Errorf("operator slots: %w", err)
		}
		if ok, overflow := fitsQueue(slots, team, row.GeneralQueue); ok {
			return picked{id: row.ID, capacity: slots.Capacity, generalQueue: row.GeneralQueue, overflow: overflow}, nil
		}
		arg.Exclude = append(arg.Exclude, row.ID)
	}
	return picked{}, nil
}

// fitsQueue checks again, on the operator row read under its lock, the
// part of "who can serve" (internal/pg/queries/routing.sql) that a
// concurrent change can break: the operator is online, has a free slot and
// is in a group that serves the queue. team is the company of a team
// queue, or nil for a shared queue. generalQueue is the flag of the pick:
// the operator does not have the language.
//
// An operator of the shared pool fits every queue; for a team queue that
// is the overflow (decision 0009, "Overflow from a team to the shared
// pool"). A member fits only the team queue of its company, and only with
// the language itself, because a member never serves through the general
// queue. A member of another team fits nothing, so an operator who joined
// another team between the pick and the lock is rejected.
func fitsQueue(slots pgdb.OperatorSlotsRow, team *string, generalQueue bool) (ok, overflow bool) {
	if slots.Presence != presenceOnline || slots.Live >= slots.Capacity {
		return false, false
	}
	switch group := deref(slots.DedicatedCompanyID); {
	case group == "":
		return true, team != nil
	case team != nil && group == *team:
		return !generalQueue, false
	}
	return false, false
}

// clearSkipsIfStuck clears the skipped operators of the session when every
// online operator who could serve the conversation was skipped, so a small
// queue does not keep it waiting until the session ends. "Could serve"
// does not look at the load, and it includes the operators of the general
// queue when the general queue rule applies (UnskippedOperatorExists uses
// the same rule as PickOperator). For a team queue it is every online
// member with the language and every online pool operator with the serving
// language, because the pool takes the overflow. So the skips are cleared
// only when the members and the pool have all been skipped, and a team
// whose members refuse does not keep the conversation going round between
// them while the pool could take it. It reports whether it cleared
// anything.
func clearSkipsIfStuck(ctx context.Context, q *pgdb.Queries, c *pgdb.Conversation, team *string) (bool, error) {
	key := pgdb.HasSkipsParams{CompanyID: c.CompanyID, CustomerID: c.CustomerID, Session: c.ConvSession}
	has, err := q.HasSkips(ctx, key)
	if err != nil || !has {
		return false, err
	}
	found, err := q.UnskippedOperatorExists(ctx, pgdb.UnskippedOperatorExistsParams{
		Lang: c.Lang, TeamCompanyID: team, CompanyID: c.CompanyID, CustomerID: c.CustomerID, Session: c.ConvSession,
	})
	if err != nil || found {
		return false, err
	}
	n, err := q.ClearSkips(ctx, pgdb.ClearSkipsParams{CompanyID: c.CompanyID, CustomerID: c.CustomerID, Session: c.ConvSession})
	if err != nil {
		return false, fmt.Errorf("clear skips: %w", err)
	}
	return n > 0, nil
}

// ExpireOffers ends offers whose accept timeout passed, at most limit of
// them, each in its own transaction. The slot is freed, the operator is
// skipped for the session, and the conversation goes back to its queue
// with its waiting time kept (decision 0009, step 4). An offer whose
// conversation another transaction holds is left for the next call. It
// returns how many offers it expired.
func (s *Service) ExpireOffers(ctx context.Context, limit int) (int, error) {
	rows, err := pgdb.New(s.db).DueOffers(ctx, pgdb.DueOffersParams{Now: s.now(), MaxRows: int32(limit)})
	if err != nil {
		return 0, fmt.Errorf("expire offers: %w", err)
	}
	n := 0
	for _, r := range rows {
		done := false
		err := s.inTx(ctx, func(q *pgdb.Queries) error {
			done = false
			c, err := q.TryLockConversation(ctx, pgdb.TryLockConversationParams{CompanyID: r.CompanyID, CustomerID: r.CustomerID})
			if isNoRows(err) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("lock conversation: %w", err)
			}
			o, err := q.LockOffer(ctx, r.Num)
			if err != nil {
				return fmt.Errorf("lock offer: %w", err)
			}
			now := s.now(c.QueuedAt)
			if o.State != offerOffered || now.Before(o.AcceptBy) {
				return nil
			}
			var ob outbox
			err = returnOffer(ctx, q, &c, &o, &ob, now, offerExpired, endTimeout,
				event.OfferTimeout, event.QueueOfferExpired, true)
			if err != nil {
				return err
			}
			if err := saveConv(ctx, q, &c, now); err != nil {
				return err
			}
			done = true
			return ob.flush(ctx, q, now)
		})
		if err != nil {
			return n, fmt.Errorf("expire offers: %w", err)
		}
		if done {
			n++
		}
	}
	return n, nil
}

// CheckSLA marks missed SLA targets of at most limit conversations (decision
// 0009, "SLA"). For each missed target the priority rises by one and one
// "sla_missed" event is written, once per target and session. A
// conversation that another transaction holds is left for the next call.
// It returns the number of missed targets it marked.
func (s *Service) CheckSLA(ctx context.Context, limit int) (int, error) {
	now := s.now()
	cfg := s.config()
	rows, err := pgdb.New(s.db).SLADue(ctx, pgdb.SLADueParams{
		AssignmentCutoff: now.Add(-cfg.machine(cfg.AssignmentTarget)),
		ResponseCutoff:   now.Add(-cfg.machine(cfg.FirstResponseTarget)),
		MaxRows:          int32(limit),
	})
	if err != nil {
		return 0, fmt.Errorf("check sla: %w", err)
	}
	total := 0
	for _, r := range rows {
		n := 0
		err := s.inTx(ctx, func(q *pgdb.Queries) error {
			n = 0
			c, err := q.TryLockConversation(ctx, pgdb.TryLockConversationParams(r))
			if isNoRows(err) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("lock conversation: %w", err)
			}
			now := s.now(c.QueuedAt, c.SlaStartedAt)
			var ob outbox
			for _, m := range s.slaMisses(&c, now) {
				c.Priority++
				if m.target == event.SLAAssignment {
					c.SlaAssignmentMissed = true
				} else {
					c.SlaResponseMissed = true
				}
				ev := convEvent(event.TypeSLAMissed, now)
				ev.QueuedAt = deref(c.QueuedAt)
				ev.SLATarget = m.target
				ev.Measured = m.measured
				ev.Target = m.limit
				if err := ob.addConv(&c, ev); err != nil {
					return err
				}
			}
			if ob.len() == 0 {
				return nil
			}
			if err := saveConv(ctx, q, &c, now); err != nil {
				return err
			}
			n = ob.len()
			return ob.flush(ctx, q, now)
		})
		if err != nil {
			return total, fmt.Errorf("check sla: %w", err)
		}
		total += n
	}
	return total, nil
}

type slaMiss struct {
	target   event.SLATarget
	measured time.Duration
	limit    time.Duration
}

// slaMisses returns the targets of c that are missed at now and not marked
// yet.
func (s *Service) slaMisses(c *pgdb.Conversation, now time.Time) []slaMiss {
	if c.SlaStartedAt == nil {
		return nil
	}
	cfg := s.config()
	elapsed := cfg.model(now.Sub(*c.SlaStartedAt))
	var out []slaMiss
	add := func(target event.SLATarget, limit time.Duration) {
		if elapsed >= limit {
			out = append(out, slaMiss{target: target, measured: max(elapsed, limit), limit: limit})
		}
	}
	waitingOrOffered := c.State == stateWaiting || c.State == stateOffered
	if waitingOrOffered && c.FirstAcceptedAt == nil && !c.SlaAssignmentMissed {
		add(event.SLAAssignment, cfg.AssignmentTarget)
	}
	if (waitingOrOffered || c.State == stateAssigned) && c.FirstRepliedAt == nil && !c.SlaResponseMissed {
		add(event.SLAFirstResponse, cfg.FirstResponseTarget)
	}
	return out
}
