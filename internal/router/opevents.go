package router

import (
	"context"
	"fmt"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgdb"
)

// Operator events (decision 0011, option 1A, and the operator family in
// internal/event). Every operator event is a full snapshot of the operator,
// written to the outbox in the transaction of the change, with the next
// sequence number of the operator. The number and the snapshot come from
// one statement on the operator row, which the caller has locked already.

// addOperatorEvents gives each operator of ids the next operator sequence
// number and adds a snapshot event of type typ for it. The caller holds the
// row lock of every operator in ids. The first event of an operator (Seq 1)
// is always event.TypeOperatorUpdated, as the contract says, also when the
// change is a presence change of an operator created before operator
// events existed.
func addOperatorEvents(ctx context.Context, q *pgdb.Queries, ob *outbox, ids []string,
	typ event.RoutingType, now time.Time,
) error {
	if len(ids) == 0 {
		return nil
	}
	rows, err := q.NextOperatorEvents(ctx, pgdb.NextOperatorEventsParams{EventAt: now, Ids: ids})
	if err != nil {
		return fmt.Errorf("next operator events: %w", err)
	}
	if len(rows) != len(ids) {
		return fmt.Errorf("operator events: %d of %d operators found", len(rows), len(ids))
	}
	for _, r := range rows {
		t := typ
		if r.OpSeq == 1 {
			t = event.TypeOperatorUpdated
		}
		ev := event.Routing{
			Type:               t,
			Seq:                uint64(r.OpSeq),
			At:                 now,
			OperatorID:         r.ID,
			Languages:          r.Languages,
			Capacity:           int(r.Capacity),
			Presence:           event.Presence(r.Presence),
			DedicatedCompanyID: deref(r.DedicatedCompanyID),
		}
		if err := ob.add(familyOperator, ev); err != nil {
			return err
		}
	}
	return nil
}

// RefreshOperators publishes the snapshot of up to batch operators again,
// as event.TypeOperatorUpdated with the next sequence number: the operators
// whose last operator event is older than before, or who have none yet
// (decision 0011, "Operators stay in the view"). It is one transaction. An
// operator that another transaction holds is skipped and stays due, so two
// routers can run it at once without waiting for each other or publishing
// one operator twice. It returns how many operators it refreshed.
func (s *Service) RefreshOperators(ctx context.Context, before time.Time, batch int) (int, error) {
	if batch <= 0 {
		batch = 100
	}
	n := 0
	err := s.inTx(ctx, func(q *pgdb.Queries) error {
		n = 0
		ids, err := q.LockOperatorsDueForRefresh(ctx, pgdb.LockOperatorsDueForRefreshParams{
			Before: before, MaxRows: int32(batch),
		})
		if err != nil {
			return fmt.Errorf("operators due: %w", err)
		}
		if len(ids) == 0 {
			return nil
		}
		now := s.now()
		var ob outbox
		if err := addOperatorEvents(ctx, q, &ob, ids, event.TypeOperatorUpdated, now); err != nil {
			return err
		}
		n = len(ids)
		return ob.flush(ctx, q, now)
	})
	if err != nil {
		return 0, fmt.Errorf("refresh operators: %w", err)
	}
	return n, nil
}
