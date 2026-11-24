package router

import (
	"context"
	"fmt"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgdb"
)

// OutboxRecord is one routing event of the outbox, ready for Kafka.
type OutboxRecord struct {
	// ID is the outbox row. Pass it to MarkSent.
	ID int64
	// Key is the record key: event.Routing.Key of the event.
	Key []byte
	// Value is the event as event.MarshalRouting wrote it.
	Value []byte
	// Type and Seq are copies of the fields in Value, for logs and
	// metrics.
	Type event.RoutingType
	Seq  uint64
	// Team reports an event of the team family, and Operator one of the
	// operator family.
	Team     bool
	Operator bool
	// CreatedAt is the machine time of the transaction that wrote it.
	CreatedAt time.Time
}

// NextOutbox returns up to limit unsent routing events in outbox order. The
// events of one record key are in the order of their sequence numbers. The
// publisher (one at a time, decision 0009) sends them in this order and
// then calls MarkSent. A crash before MarkSent sends them again; readers
// skip sequence numbers they have seen.
func (s *Service) NextOutbox(ctx context.Context, limit int) ([]OutboxRecord, error) {
	rows, err := pgdb.New(s.db).NextOutbox(ctx, int32(limit))
	if err != nil {
		return nil, fmt.Errorf("next outbox: %w", err)
	}
	out := make([]OutboxRecord, len(rows))
	for i, r := range rows {
		out[i] = OutboxRecord{
			ID: r.ID, Key: r.RecordKey, Value: r.Payload, Type: event.RoutingType(r.EventType),
			Seq: uint64(r.Seq), Team: r.Family == familyTeam, Operator: r.Family == familyOperator,
			CreatedAt: r.CreatedAt,
		}
	}
	return out, nil
}

// MarkSent marks outbox rows as sent and returns how many were unsent.
func (s *Service) MarkSent(ctx context.Context, ids []int64) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	n, err := pgdb.New(s.db).MarkOutboxSent(ctx, pgdb.MarkOutboxSentParams{SentAt: s.now(), Ids: ids})
	if err != nil {
		return 0, fmt.Errorf("mark outbox sent: %w", err)
	}
	return n, nil
}

// PruneOutbox deletes up to limit rows that were sent before sentBefore and
// returns how many it deleted.
func (s *Service) PruneOutbox(ctx context.Context, sentBefore time.Time, limit int) (int64, error) {
	n, err := pgdb.New(s.db).PruneOutbox(ctx, pgdb.PruneOutboxParams{SentBefore: sentBefore, MaxRows: int32(limit)})
	if err != nil {
		return 0, fmt.Errorf("prune outbox: %w", err)
	}
	return n, nil
}

// Settings returns the settings of the operator API (opapi.PatternSettings).
func (s *Service) Settings() opapi.Settings { return s.cfg.Settings() }

// QueueDepth is the number of waiting conversations of one queue.
type QueueDepth struct {
	Lang string
	// TeamCompanyID is the company of a team queue, or empty for the
	// shared queue of Lang.
	TeamCompanyID string
	Waiting       int64
	// Oldest is the earliest queue time in the queue.
	Oldest time.Time
}

// OperatorCount counts operators by presence and group.
type OperatorCount struct {
	Presence opapi.Presence
	InTeam   bool
	N        int64
	Capacity int64
}

// Counts is a snapshot of the routing state for metrics and the public
// page. Every number is aggregated; there are no IDs except the companies
// of team queues.
type Counts struct {
	// Conversations counts conversations by state (pending, waiting,
	// offered, assigned, resolved, closed).
	Conversations map[string]int64
	Queues        []QueueDepth
	Operators     []OperatorCount
	// LiveSlots counts offers and assignments that use a slot.
	LiveSlots   int64
	ActiveTeams int64
	// TeamOnline counts the online operators that are dedicated to a team,
	// and TeamCap is the cap of decision 0009 for the online operators
	// (Config.TeamCap). Both come from the Operators rows.
	TeamOnline int64
	TeamCap    int64
	// UnsentOutbox counts routing events not yet published, and
	// OldestUnsent is the time of the oldest one, or zero.
	UnsentOutbox int64
	OldestUnsent time.Time
}

// Counts reads a snapshot of the routing state. Each number comes from its
// own query, so they are not from one instant.
func (s *Service) Counts(ctx context.Context) (Counts, error) {
	q := pgdb.New(s.db)
	c := Counts{Conversations: map[string]int64{}}
	states, err := q.CountConversations(ctx)
	if err != nil {
		return c, fmt.Errorf("counts: %w", err)
	}
	for _, r := range states {
		c.Conversations[r.State] = r.N
	}
	queues, err := q.QueueDepths(ctx)
	if err != nil {
		return c, fmt.Errorf("counts: %w", err)
	}
	for _, r := range queues {
		c.Queues = append(c.Queues, QueueDepth{Lang: r.Lang, TeamCompanyID: r.TeamCompanyID, Waiting: r.N, Oldest: r.Oldest})
	}
	ops, err := q.CountOperators(ctx)
	if err != nil {
		return c, fmt.Errorf("counts: %w", err)
	}
	var online int64
	for _, r := range ops {
		c.Operators = append(c.Operators, OperatorCount{
			Presence: opapi.Presence(r.Presence), InTeam: r.InTeam, N: r.N, Capacity: r.Capacity,
		})
		if opapi.Presence(r.Presence) == opapi.PresenceOnline {
			online += r.N
			if r.InTeam {
				c.TeamOnline += r.N
			}
		}
	}
	c.TeamCap = s.cfg.TeamCap(online)
	if c.LiveSlots, err = q.CountLiveSlots(ctx); err != nil {
		return c, fmt.Errorf("counts: %w", err)
	}
	if c.ActiveTeams, err = q.CountActiveTeams(ctx); err != nil {
		return c, fmt.Errorf("counts: %w", err)
	}
	if c.UnsentOutbox, err = q.CountUnsentOutbox(ctx); err != nil {
		return c, fmt.Errorf("counts: %w", err)
	}
	if c.UnsentOutbox > 0 {
		oldest, err := q.OldestUnsentOutbox(ctx)
		if err != nil && !isNoRows(err) {
			return c, fmt.Errorf("counts: %w", err)
		}
		c.OldestUnsent = oldest
	}
	return c, nil
}
