//go:build integration

package router

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
	"github.com/ZAUakaAlexey/threadroute/internal/pg"
	"github.com/ZAUakaAlexey/threadroute/internal/pg/pgtest"
)

// startDB starts PostgreSQL, opens a pool of maxConns connections and
// applies the migrations. pgtest.Open caps the pool at 8, which is too
// small for the concurrency test.
func startDB(t *testing.T, maxConns int32) *pgxpool.Pool {
	t.Helper()
	dsn := pgtest.Start(t)
	pool, err := pg.Open(t.Context(), pg.Config{DSN: dsn, MaxConns: maxConns, ConnectWait: 30 * time.Second}, nil)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pg.Migrate(t.Context(), pool, nil); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

// resetDB empties every table of the router.
func resetDB(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `TRUNCATE outbox, replies, offer_skips, offers, conversations,
		companies, operator_languages, operators RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
}

type env struct {
	t     *testing.T
	pool  *pgxpool.Pool
	clock *fakeClock
	svc   *Service
}

func newEnv(t *testing.T, pool *pgxpool.Pool, cfg Config) *env {
	t.Helper()
	resetDB(t, pool)
	clock := newFakeClock()
	svc, err := New(pool, cfg, clock, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &env{t: t, pool: pool, clock: clock, svc: svc}
}

// Synthetic, clearly fake test data (decision 0004).
func company(i int) string  { return fmt.Sprintf("did:plc:testroutercompany%010d", i) }
func customer(i int) string { return fmt.Sprintf("did:plc:testroutercustomer%09d", i) }
func operator(i int) string { return fmt.Sprintf("sim-op-%04d", i) }

// convStream produces the conversation events of one conversation the way
// conv numbers them: the sequence number grows over every session.
type convStream struct {
	company, customer, lang string
	session, seq            uint64
	open                    bool
	src                     time.Time
	msg                     int
}

func newStream(company, customer, lang string) *convStream {
	return &convStream{company: company, customer: customer, lang: lang,
		src: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
}

func (s *convStream) next(typ event.ConversationType) event.Conversation {
	s.seq++
	s.msg++
	s.src = s.src.Add(time.Second)
	ev := event.Conversation{
		Type: typ, Channel: event.ChannelBluesky, CompanyID: s.company, CustomerID: s.customer,
		Session: s.session, Seq: s.seq, SourceTime: s.src, OpenedAt: s.src, Lang: s.lang,
		MessageID: fmt.Sprintf("test-msg-%s-%d", s.customer, s.msg), Direction: event.FromCustomer,
	}
	return ev
}

// opened starts the next session.
func (s *convStream) opened() event.Conversation {
	s.session++
	s.open = true
	return s.next(event.TypeOpened)
}

func (s *convStream) message(dir event.Direction) event.Conversation {
	ev := s.next(event.TypeMessageAdded)
	ev.Direction = dir
	return ev
}

func (s *convStream) answered(rt time.Duration) event.Conversation {
	ev := s.next(event.TypeAnswered)
	ev.Direction = event.FromCompany
	ev.ResponseTime = rt
	return ev
}

func (s *convStream) closed() event.Conversation {
	s.open = false
	ev := s.next(event.TypeClosed)
	ev.MessageID, ev.Direction, ev.CloseReason = "", "", event.CloseInactivity
	return ev
}

// removed returns a "message removed" event of the current session. The
// counters of conv do not change for it, and it is no message of its own.
func (s *convStream) removed() event.Conversation {
	ev := s.next(event.TypeMessageRemoved)
	ev.RemoveReason = event.RemoveDeleted
	return ev
}

// forget restarts the numbering as conv does 24 hours after a close.
func (s *convStream) forget() { s.session, s.seq = 0, 0 }

func (s *convStream) key() []byte { return event.ConversationKey(s.company, s.customer) }

func (e *env) apply(ev event.Conversation, catchUp bool) ApplyResult {
	e.t.Helper()
	r, err := e.svc.ApplyConversation(e.t.Context(), ev, catchUp)
	if err != nil {
		e.t.Fatalf("ApplyConversation(%s %d/%d) error = %v", ev.Type, ev.Session, ev.Seq, err)
	}
	return r
}

func (e *env) putOperator(id string, capacity int, langs ...string) {
	e.t.Helper()
	if _, err := e.svc.PutOperator(e.t.Context(), id, opapi.OperatorRequest{Languages: langs, Capacity: capacity}); err != nil {
		e.t.Fatalf("PutOperator(%s) error = %v", id, err)
	}
}

func (e *env) online(id string) {
	e.t.Helper()
	if _, err := e.svc.Online(e.t.Context(), id); err != nil {
		e.t.Fatalf("Online(%s) error = %v", id, err)
	}
}

func (e *env) assign() AssignResult {
	e.t.Helper()
	r, err := e.svc.Assign(e.t.Context(), 1000)
	if err != nil {
		e.t.Fatalf("Assign() error = %v", err)
	}
	return r
}

// liveOffer returns the live offer of a conversation, or zero values.
func (e *env) liveOffer(s *convStream) (offerID, operatorID, state string) {
	e.t.Helper()
	err := e.pool.QueryRow(e.t.Context(),
		`SELECT id, operator_id, state FROM offers
		 WHERE company_id = $1 AND customer_id = $2 AND state IN ('offered', 'accepted')`,
		s.company, s.customer).Scan(&offerID, &operatorID, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", ""
	}
	if err != nil {
		e.t.Fatal(err)
	}
	return offerID, operatorID, state
}

func (e *env) convState(s *convStream) string {
	e.t.Helper()
	var st string
	err := e.pool.QueryRow(e.t.Context(),
		`SELECT state FROM conversations WHERE company_id = $1 AND customer_id = $2`, s.company, s.customer).Scan(&st)
	if err != nil {
		e.t.Fatal(err)
	}
	return st
}

// events returns the routing events of one record key in outbox order,
// decoded and validated.
func (e *env) events(key []byte) []event.Routing {
	e.t.Helper()
	rows, err := e.pool.Query(e.t.Context(), `SELECT payload FROM outbox WHERE record_key = $1 ORDER BY id`, key)
	if err != nil {
		e.t.Fatal(err)
	}
	payloads, err := pgx.CollectRows(rows, pgx.RowTo[[]byte])
	if err != nil {
		e.t.Fatal(err)
	}
	out := make([]event.Routing, len(payloads))
	for i, p := range payloads {
		if out[i], err = event.UnmarshalRouting(p); err != nil {
			e.t.Fatalf("outbox payload %s: %v", p, err)
		}
	}
	return out
}

// types lists the types of events, for short failure messages.
func types(evs []event.Routing) string {
	var b strings.Builder
	for i, ev := range evs {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(string(ev.Type))
		if ev.QueueReason != "" {
			b.WriteString("(" + string(ev.QueueReason) + ")")
		}
	}
	return b.String()
}

// invariantChecks are single SQL statements, each run on one snapshot,
// that return a row for every violation. They hold at every commit.
var invariantChecks = []struct{ name, sql string }{
	{
		"at most one live offer or assignment per conversation",
		`SELECT company_id || ' ' || customer_id FROM offers
		 WHERE state IN ('offered', 'accepted') GROUP BY company_id, customer_id HAVING count(*) > 1`,
	},
	{
		"conversation state matches its live offer (no conversation both waiting and held)",
		`SELECT cv.customer_id || ' ' || cv.state || ' ' || COALESCE(f.state, '-')
		 FROM conversations AS cv
		 LEFT JOIN offers AS f ON f.company_id = cv.company_id AND f.customer_id = cv.customer_id
		      AND f.state IN ('offered', 'accepted')
		 WHERE NOT ((cv.state = 'offered' AND f.state = 'offered')
		         OR (cv.state = 'assigned' AND f.state = 'accepted')
		         OR (cv.state NOT IN ('offered', 'assigned') AND f.num IS NULL))`,
	},
	{
		// A capacity cut does not end slots taken before it. Each slot
		// records the capacity in force when it was reserved, and the
		// reservation checked "live < capacity" under the operator lock.
		// So the live slots of an operator are at most the capacity of its
		// newest live slot.
		"live slots of an operator within capacity",
		`SELECT operator_id || ' live ' || count(*) || ' cap ' || (array_agg(capacity ORDER BY num DESC))[1]
		 FROM offers WHERE state IN ('offered', 'accepted')
		 GROUP BY operator_id
		 HAVING count(*) > (array_agg(capacity ORDER BY num DESC))[1]`,
	},
	{
		"an offline operator holds no slot",
		`SELECT DISTINCT o.id FROM operators AS o
		 JOIN offers AS f ON f.operator_id = o.id AND f.state IN ('offered', 'accepted')
		 WHERE o.presence = 'offline'`,
	},
	{
		"no offer to a skipped operator after the skip",
		`SELECT s.customer_id || ' ' || s.operator_id FROM offer_skips AS s
		 JOIN offers AS f ON f.company_id = s.company_id AND f.customer_id = s.customer_id
		      AND f.session = s.session AND f.operator_id = s.operator_id AND f.num > s.offer_num`,
	},
	{
		"outbox sequence numbers per key start at 1, no gap, no repeat, in outbox order",
		`SELECT family || ' ' || encode(record_key, 'escape') || ' seq ' || seq || ' at ' || rn
		 FROM (SELECT family, record_key, seq,
		              row_number() OVER (PARTITION BY family, record_key ORDER BY id) AS rn
		       FROM outbox) AS x
		 WHERE seq <> rn`,
	},
	{
		"route_seq of a conversation equals its number of routing events",
		`SELECT cv.customer_id || ' ' || cv.route_seq || ' ' || COALESCE(o.n, 0)
		 FROM conversations AS cv
		 LEFT JOIN (SELECT record_key, count(*) AS n FROM outbox WHERE family = 'conversation'
		            GROUP BY record_key) AS o
		      ON o.record_key = convert_to(cv.company_id || '|' || cv.customer_id, 'UTF8')
		 WHERE cv.route_seq <> COALESCE(o.n, 0)`,
	},
	{
		"team_seq of a company equals its number of team events",
		`SELECT c.company_id || ' ' || c.team_seq || ' ' || COALESCE(o.n, 0)
		 FROM companies AS c
		 LEFT JOIN (SELECT record_key, count(*) AS n FROM outbox WHERE family = 'team'
		            GROUP BY record_key) AS o
		      ON o.record_key = convert_to(c.company_id, 'UTF8')
		 WHERE c.team_seq <> COALESCE(o.n, 0)`,
	},
	{
		"op_seq of an operator equals its number of operator events",
		`SELECT o.id || ' ' || o.op_seq || ' ' || COALESCE(x.n, 0)
		 FROM operators AS o
		 LEFT JOIN (SELECT record_key, count(*) AS n FROM outbox WHERE family = 'operator'
		            GROUP BY record_key) AS x
		      ON x.record_key = convert_to(o.id, 'UTF8')
		 WHERE o.op_seq <> COALESCE(x.n, 0)`,
	},
	{
		"the first event of an operator is operator_updated",
		`SELECT encode(record_key, 'escape') || ' ' || event_type FROM outbox
		 WHERE family = 'operator' AND seq = 1 AND event_type <> 'operator_updated'`,
	},
	{
		"the newest operator event is the snapshot of the operator row",
		`SELECT o.id || ' ' || x.j::text
		 FROM operators AS o
		 JOIN LATERAL (SELECT convert_from(payload, 'UTF8')::jsonb AS j FROM outbox
		               WHERE family = 'operator' AND record_key = convert_to(o.id, 'UTF8')
		               ORDER BY seq DESC LIMIT 1) AS x ON true
		 WHERE (x.j->>'capacity')::integer <> o.capacity
		    OR x.j->>'presence' <> o.presence
		    OR (x.j->>'dedicated_company_id') IS DISTINCT FROM o.dedicated_company_id
		    OR ARRAY(SELECT jsonb_array_elements_text(x.j->'languages'))
		       <> COALESCE((SELECT array_agg(ol.lang ORDER BY ol.lang) FROM operator_languages AS ol
		                    WHERE ol.operator_id = o.id), '{}')`,
	},
	{
		"a queued event carries the previous pair exactly for the reasons that have one",
		`SELECT encode(record_key, 'escape') || ' seq ' || seq || ' ' || convert_from(payload, 'UTF8')
		 FROM outbox
		 WHERE event_type = 'queued'
		   AND (convert_from(payload, 'UTF8')::jsonb->>'queue_reason'
		            IN ('offer_expired', 'offer_refused', 'operator_offline', 'customer_message_after_resolve'))
		       <> (convert_from(payload, 'UTF8')::jsonb ? 'previous_operator_id'
		           AND convert_from(payload, 'UTF8')::jsonb ? 'previous_offer_id')`,
	},
	{
		"the previous offer of a queued event is an offer of that operator and conversation",
		`SELECT x.j::text
		 FROM (SELECT convert_from(payload, 'UTF8')::jsonb AS j FROM outbox WHERE event_type = 'queued') AS x
		 LEFT JOIN offers AS f ON f.id = x.j->>'previous_offer_id'
		 WHERE x.j ? 'previous_offer_id'
		   AND (f.num IS NULL OR f.operator_id <> x.j->>'previous_operator_id'
		        OR f.company_id <> x.j->>'company_id' OR f.customer_id <> x.j->>'customer_id')`,
	},
	{
		"team members belong to an active team",
		`SELECT o.id FROM operators AS o
		 LEFT JOIN companies AS c ON c.company_id = o.dedicated_company_id
		 WHERE o.dedicated_company_id IS NOT NULL AND NOT COALESCE(c.team_active, false)`,
	},
}

// checkInvariants runs every check and returns one line per violation.
func checkInvariants(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	var out []string
	for _, c := range invariantChecks {
		rows, err := pool.Query(ctx, c.sql)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", c.name, err)
		}
		bad, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", c.name, err)
		}
		for _, b := range bad {
			out = append(out, c.name+": "+b)
		}
	}
	return out, nil
}

// requireInvariants fails the test on any violation, and checks that every
// outbox payload decodes and has the sequence number of its row.
func requireInvariants(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	bad, err := checkInvariants(t.Context(), pool)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range bad {
		t.Error(b)
	}
	rows, err := pool.Query(t.Context(), `SELECT record_key, seq, event_type, payload FROM outbox ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var key, payload []byte
		var seq int64
		var typ string
		if err := rows.Scan(&key, &seq, &typ, &payload); err != nil {
			t.Fatal(err)
		}
		ev, err := event.UnmarshalRouting(payload)
		if err != nil {
			t.Errorf("payload %s: %v", payload, err)
			continue
		}
		if string(ev.Key()) != string(key) || ev.Seq != uint64(seq) || string(ev.Type) != typ {
			t.Errorf("row %s seq %d %s does not match payload %s", key, seq, typ, payload)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
