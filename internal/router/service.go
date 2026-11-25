// Package router holds the routing state of decision 0009 in PostgreSQL and
// the operations on it: applying conversation events from conv, assigning
// waiting conversations to operators, the operator actions of decision 0010,
// the accept and SLA timers, dedicated teams, and the outbox of routing
// events. It has no Kafka consumer, no HTTP handler and no loop of its own:
// the router binary calls these methods.
//
// # Transactions
//
// Every operation that changes state runs in one database transaction (or,
// for the operations that work through a list, one transaction per item).
// The transaction changes the state and appends the routing events to the
// outbox, each with the next sequence number of its record key. The
// sequence counter lives on the conversation row (the company row for team
// events, the operator row for operator events), which the transaction
// holds locked, so the numbers of one key have no gap and no repeat, and
// the outbox order of one key is its commit order.
//
// Transactions run at READ COMMITTED. Correctness rests on row locks, and
// on re-reading rows in a new statement after a lock is held.
//
// # Lock order
//
// See migrations/00002_routing.sql and 00003_operator_events.sql in
// internal/pg. In short: assignment, the timer sweeps and the refresh of
// operator snapshots take conversation and operator rows with SKIP LOCKED
// and never wait for a row lock. Every other operation takes locks in one
// order: the team advisory lock, companies, operators, conversations by
// key, offers. Operator actions read the offer without a lock to find its
// conversation, lock the conversation, and then lock and read the offer
// again. No operation retries on a deadlock or a serialization failure.
// The lock order is meant to make both impossible, and the concurrency test
// counts them.
//
// The row of stand_settings (00005_stand_settings.sql) has its place right
// after the team advisory lock (00006_team_trim.sql). A change of the
// stand settings and the seed lock only that row. The trim after a lowered
// share (trim.go) takes the team lock, then that row, then companies, then
// operators, and no conversation. Nothing holds the settings row while it
// waits for the team lock.
//
// Operator events (decision 0011) take their sequence number at the
// "operators" step: PutOperator, Online and Offline lock the one operator
// row first, a team change locks its members after the company row, and
// the refresh locks its batch with SKIP LOCKED. The number is given in a
// statement after the lock is held, so the snapshot in the event is the
// state that commits.
//
// # Clock
//
// Every time comes from the injected Clock, read after the row locks of
// the operation are held. Durations in routing events and the timer limits
// are model time: machine time multiplied by Config.SpeedFactor. The
// database clock is never used for a routing decision.
//
// # Rules where the decision records are silent
//
//   - An operator is skipped for the rest of the session after a timeout
//     or a refusal. When every online operator who could serve the
//     conversation has been skipped, the skips of the session are cleared,
//     so the conversation does not wait forever in a small queue. For a
//     team queue that is every member with the language and every
//     operator of the shared pool that could take the overflow.
//   - The preferred operator must pass every rule (online, language, group,
//     free slot, not skipped). Among the operators that pass, it wins over
//     a lower load. The preference ends with the next offer.
//   - The general queue (und) is the language "und": an operator serves it
//     when it lists "und", as the operator API says.
//   - SLA targets are measured once per session, from the first queue
//     entry of the session. A customer message after resolve puts the
//     conversation back into the queue with a new waiting time, but the
//     SLA targets of the session are not started again. Targets are only
//     checked while an operator is still needed (waiting, offered, and for
//     the first response also assigned).
//   - An event of a later session while the router still holds an earlier
//     session open (the "closed" event was lost) ends the earlier session
//     as if conv had closed it for inactivity.
//   - A team is formed only from online operators of the shared pool. When
//     no such operator has a language of the company, no team is formed
//     and the check tries again next time. The release time of a team is
//     model time like the other timers.
//   - A conversation that moves between the team queue and the shared
//     queue because a team was resized gets a queued event with the reason
//     team_formed (into the team queue) or team_released (out of it).
package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgdb"
)

// DB is what the Service needs from a connection pool. *pgxpool.Pool has
// it.
type DB interface {
	pgdb.DBTX
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Conversation states, as in the conversations table.
const (
	statePending  = "pending"
	stateWaiting  = "waiting"
	stateOffered  = "offered"
	stateAssigned = "assigned"
	stateResolved = "resolved"
	stateClosed   = "closed"
)

// Presence values, as in the operators table.
const (
	presenceOnline  = "online"
	presenceOffline = "offline"
)

// Outbox families.
const (
	familyConversation = "conversation"
	familyTeam         = "team"
	familyOperator     = "operator"
)

// teamLockKey is the key of the advisory lock that serializes team
// operations. Any fixed number works, it only has to be the same in every
// router.
const teamLockKey int64 = 0x7472_7465_616d // "trteam"

// Service runs the routing operations.
type Service struct {
	db     DB
	clock  Clock
	logger *slog.Logger

	// rules is the snapshot of the routing numbers in use (settings.go).
	// Read it with config, once per operation that needs the team numbers.
	rules atomic.Pointer[rules]
	// standOn reports whether the Service offers stand settings. Only
	// LoadSettings sets it.
	standOn atomic.Bool
	// consoleChanges and defaultsChanges count the stand settings changes
	// this Service applied.
	consoleChanges, defaultsChanges atomic.Int64
}

// New builds a Service. The configuration must be valid.
func New(db DB, cfg Config, clock Clock, logger *slog.Logger) (*Service, error) {
	if db == nil || clock == nil {
		return nil, errors.New("new router: db and clock are required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("new router: %w", err)
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	s := &Service{db: db, clock: clock, logger: logger}
	s.rules.Store(&rules{cfg: cfg, current: opapi.StandSettings{Teams: teamsOf(cfg)}})
	return s, nil
}

// Config returns the configuration in use: the configuration given to New,
// with the team part of the current stand settings.
func (s *Service) Config() Config { return s.config() }

// inTx runs fn in one transaction. It commits when fn returns nil and
// rolls back otherwise. It never retries.
func (s *Service) inTx(ctx context.Context, fn func(q *pgdb.Queries) error) error {
	return pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		return fn(pgdb.New(tx))
	})
}

// now reads the clock, in whole microseconds like the database, and never
// before any of the given times. Pass the stored times an event of the
// operation is compared with: the clocks of two routers can differ a
// little, and an event must never be earlier than its QueuedAt.
func (s *Service) now(after ...*time.Time) time.Time {
	t := s.clock.Now().UTC().Truncate(time.Microsecond)
	for _, a := range after {
		if a != nil && a.After(t) {
			t = *a
		}
	}
	return t
}

// outbox collects the routing events of one transaction.
type outbox struct {
	families []string
	keys     [][]byte
	seqs     []int64
	types    []string
	payloads [][]byte
}

// addConv gives ev the next sequence number of the conversation, fills the
// fields every conversation event has, and adds it. The caller saves c
// afterwards, which stores the new sequence number.
func (o *outbox) addConv(c *pgdb.Conversation, ev event.Routing) error {
	c.RouteSeq++
	ev.CompanyID = c.CompanyID
	ev.CustomerID = c.CustomerID
	ev.Session = uint64(c.ConvSession)
	ev.Lang = c.Lang
	ev.TeamQueue = c.TeamQueue
	ev.Priority = uint32(c.Priority)
	ev.Seq = uint64(c.RouteSeq)
	return o.add(familyConversation, ev)
}

// addTeam gives ev the next team sequence number of the company and adds
// it. The caller saves co afterwards.
func (o *outbox) addTeam(co *pgdb.Company, ev event.Routing) error {
	co.TeamSeq++
	ev.CompanyID = co.CompanyID
	ev.Seq = uint64(co.TeamSeq)
	return o.add(familyTeam, ev)
}

func (o *outbox) add(family string, ev event.Routing) error {
	b, err := event.MarshalRouting(ev)
	if err != nil {
		return err
	}
	o.families = append(o.families, family)
	o.keys = append(o.keys, ev.Key())
	o.seqs = append(o.seqs, int64(ev.Seq))
	o.types = append(o.types, string(ev.Type))
	o.payloads = append(o.payloads, b)
	return nil
}

func (o *outbox) len() int { return len(o.seqs) }

// flush writes the collected events and empties the outbox.
func (o *outbox) flush(ctx context.Context, q *pgdb.Queries, now time.Time) error {
	if o.len() == 0 {
		return nil
	}
	err := q.InsertOutbox(ctx, pgdb.InsertOutboxParams{
		CreatedAt:  now,
		Families:   o.families,
		RecordKeys: o.keys,
		Seqs:       o.seqs,
		EventTypes: o.types,
		Payloads:   o.payloads,
	})
	if err != nil {
		return fmt.Errorf("write outbox: %w", err)
	}
	*o = outbox{}
	return nil
}

// convEvent returns an event of the conversation family with its type and
// time. addConv fills the rest of the shared fields.
func convEvent(typ event.RoutingType, at time.Time) event.Routing {
	return event.Routing{Type: typ, At: at}
}

// saveConv writes every changeable column of a locked conversation row.
func saveConv(ctx context.Context, q *pgdb.Queries, c *pgdb.Conversation, now time.Time) error {
	c.UpdatedAt = now
	err := q.SaveConversation(ctx, pgdb.SaveConversationParams{
		ConvSession:         c.ConvSession,
		ConvSeq:             c.ConvSeq,
		Lang:                c.Lang,
		State:               c.State,
		TeamQueue:           c.TeamQueue,
		Priority:            c.Priority,
		QueuedAt:            c.QueuedAt,
		PreferredOperatorID: c.PreferredOperatorID,
		LastOperatorID:      c.LastOperatorID,
		LastOfferID:         c.LastOfferID,
		RouteSeq:            c.RouteSeq,
		SlaStartedAt:        c.SlaStartedAt,
		FirstAcceptedAt:     c.FirstAcceptedAt,
		FirstRepliedAt:      c.FirstRepliedAt,
		SlaAssignmentMissed: c.SlaAssignmentMissed,
		SlaResponseMissed:   c.SlaResponseMissed,
		SimResponseUs:       c.SimResponseUs,
		RealResponseUs:      c.RealResponseUs,
		CloseReason:         c.CloseReason,
		UpdatedAt:           c.UpdatedAt,
		CompanyID:           c.CompanyID,
		CustomerID:          c.CustomerID,
	})
	if err != nil {
		return fmt.Errorf("save conversation: %w", err)
	}
	return nil
}

// saveOffer writes the changeable columns of a locked offer row.
func saveOffer(ctx context.Context, q *pgdb.Queries, o *pgdb.Offer) error {
	err := q.SaveOffer(ctx, pgdb.SaveOfferParams{
		State:      o.State,
		AcceptedAt: o.AcceptedAt,
		EndedAt:    o.EndedAt,
		EndReason:  o.EndReason,
		Num:        o.Num,
	})
	if err != nil {
		return fmt.Errorf("save offer: %w", err)
	}
	return nil
}

func ptr[T any](v T) *T { return &v }

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}

// previous names the offer or assignment that ended just before a
// conversation goes back to its queue (decision 0011, "Contract changes").
type previous struct {
	operatorID, offerID string
}

// previousOf returns the previous pair of an offer.
func previousOf(o *pgdb.Offer) previous {
	return previous{operatorID: o.OperatorID, offerID: o.ID}
}

// enqueue puts a conversation into its queue and adds a queued event. In
// catch-up it only marks the conversation pending, without an event
// (decision 0009, "Catching up").
//
// prev must be set exactly for a reason with
// event.QueueReason.HasPreviousOperator, so no queued event of such a reason
// is ever written without the pair.
//
// keepWait keeps the queue time when the conversation goes back to the
// queue after an offer or an assignment (decision 0009, step 4). Both SLA
// targets start at the first queue entry of the session.
//
// The queue (team or shared) is chosen from the teams as they are now,
// without a lock on the company. A team that forms or is released at the
// same moment can leave the conversation in the other queue; the next team
// check moves it.
func enqueue(ctx context.Context, q *pgdb.Queries, c *pgdb.Conversation, ob *outbox, now time.Time,
	reason event.QueueReason, prev previous, keepWait, catchUp bool,
) error {
	if has := prev.operatorID != "" && prev.offerID != ""; has != reason.HasPreviousOperator() ||
		(!has && (prev.operatorID != "" || prev.offerID != "")) {
		return fmt.Errorf("queue reason %s with previous operator %q and offer %q", reason, prev.operatorID, prev.offerID)
	}
	if catchUp {
		c.State = statePending
		return nil
	}
	serves, err := q.TeamServesLang(ctx, pgdb.TeamServesLangParams{Lang: c.Lang, CompanyID: c.CompanyID})
	if err != nil {
		return fmt.Errorf("team queue: %w", err)
	}
	c.TeamQueue = serves
	c.State = stateWaiting
	if !keepWait || c.QueuedAt == nil {
		c.QueuedAt = ptr(now)
	}
	if c.SlaStartedAt == nil {
		c.SlaStartedAt = ptr(now)
	}
	ev := convEvent(event.TypeQueued, now)
	ev.QueuedAt = *c.QueuedAt
	ev.QueueReason = reason
	ev.PreviousOperatorID = prev.operatorID
	ev.PreviousOfferID = prev.offerID
	return ob.addConv(c, ev)
}

// returnOffer ends an offer that was not accepted and puts its
// conversation back into the queue with its waiting time kept. skip adds
// the operator to the skipped operators of the session.
func returnOffer(ctx context.Context, q *pgdb.Queries, c *pgdb.Conversation, o *pgdb.Offer, ob *outbox,
	now time.Time, state, endReason string, evReason event.OfferEndReason, queueReason event.QueueReason, skip bool,
) error {
	o.State = state
	o.EndedAt = ptr(now)
	o.EndReason = ptr(endReason)
	if err := saveOffer(ctx, q, o); err != nil {
		return err
	}
	if skip {
		err := q.InsertSkip(ctx, pgdb.InsertSkipParams{
			CompanyID: c.CompanyID, CustomerID: c.CustomerID, Session: o.Session,
			OperatorID: o.OperatorID, OfferNum: o.Num,
		})
		if err != nil {
			return fmt.Errorf("skip operator: %w", err)
		}
	}
	ev := convEvent(event.TypeOfferExpired, now)
	ev.QueuedAt = deref(c.QueuedAt)
	ev.OperatorID = o.OperatorID
	ev.OfferID = o.ID
	ev.OfferEndReason = evReason
	if err := ob.addConv(c, ev); err != nil {
		return err
	}
	return enqueue(ctx, q, c, ob, now, queueReason, previousOf(o), true, false)
}

// isNoRows reports whether err is pgx.ErrNoRows.
func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
