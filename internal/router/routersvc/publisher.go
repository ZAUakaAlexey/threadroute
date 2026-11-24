package routersvc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/router"
)

// PublisherLockKey is the key of the PostgreSQL session advisory lock that
// the outbox publisher holds. It differs from the team lock of the router
// core and from the default lock ID of goose, which share the same key
// space.
const PublisherLockKey int64 = 0x7472_6f75_7462 // "troutb"

// Produce limits of the publisher.
const (
	// deliveryTimeout bounds how long Kafka may take to acknowledge a
	// record. A record that times out fails the round, and the rows stay
	// unsent.
	deliveryTimeout = 30 * time.Second
	// produceTimeout bounds one produce call. It is a little longer than
	// deliveryTimeout, so the client reports the timeout itself.
	produceTimeout = deliveryTimeout + 5*time.Second
	// unlockTimeout bounds the release of the lock at the end.
	unlockTimeout = 5 * time.Second
)

// outboxStore is the part of *router.Service that the publisher uses.
type outboxStore interface {
	NextOutbox(ctx context.Context, limit int) ([]router.OutboxRecord, error)
	MarkSent(ctx context.Context, ids []int64) (int64, error)
}

// producer sends records and waits for every acknowledgement. The results
// are in the order of the records. *kgo.Client has it.
type producer interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

// locker is a lock that one process at a time can hold.
type locker interface {
	// TryLock takes the lock if it is free and reports whether this
	// process holds it now. It never waits for another holder.
	TryLock(ctx context.Context) (bool, error)
	// Check returns an error when the lock may be lost.
	Check(ctx context.Context) error
	// Unlock gives the lock up. It is safe to call when the lock is not
	// held.
	Unlock(ctx context.Context)
}

// Publisher sends the outbox to the routing topic (decision 0009, "Events
// and state"). One publisher at a time across all processes: it publishes
// only while it holds the lock, and a process that does not get the lock
// tries again at the next interval.
//
// A round reads the oldest unsent rows (router.Service.NextOutbox), produces
// them in that order with their record key and stored payload, waits for
// every acknowledgement and marks the rows sent. The order of the rows is
// kept per partition by the idempotent producer, so the events of one key
// reach the topic in sequence order. When a record fails, the rows before
// the first failed row are marked sent and the round ends with an error;
// the next round starts again from the first unsent row. Rows after the
// failed one that Kafka did acknowledge are sent again: a consumer skips a
// sequence number it has seen (decision 0009, "Order of routing events").
// A crash between the produce and the mark has the same effect.
//
// The publisher never keeps a cursor of the last sent row ID: rows of
// different keys can commit out of ID order, so it always asks for the
// unsent rows.
type Publisher struct {
	store    outboxStore
	producer producer
	lock     locker
	topic    string
	batch    int
	interval time.Duration
	runner   *runner
	m        *metrics
	logger   *slog.Logger

	held atomic.Bool
	sent atomic.Int64
}

// Holding reports whether the publisher holds the lock now. It is safe for
// concurrent use.
func (p *Publisher) Holding() bool { return p.held.Load() }

// Sent returns the number of records this publisher produced and marked
// sent. It is safe for concurrent use.
func (p *Publisher) Sent() int64 { return p.sent.Load() }

// Run publishes until ctx ends, then gives up the lock. It returns an error
// when the publisher failed for longer than the outage limit.
func (p *Publisher) Run(ctx context.Context) error {
	defer func() {
		// Stop counting as the holder before the lock is free, so two
		// publishers never both report Holding.
		wasHeld := p.held.Swap(false)
		p.m.lockHeld.Set(0)
		uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout)
		defer cancel()
		p.lock.Unlock(uctx)
		if wasHeld {
			p.logger.Info("publisher lock released")
		}
	}()
	return p.runner.loop(ctx, loopSpec{name: "publish", interval: p.interval, step: p.step})
}

// step is one round. more reports a full batch, so the next round starts
// at once.
func (p *Publisher) step(ctx context.Context) (bool, error) {
	if !p.held.Load() {
		ok, err := p.lock.TryLock(ctx)
		if err != nil {
			return false, fmt.Errorf("publisher lock: %w", err)
		}
		if !ok {
			return false, nil
		}
		p.held.Store(true)
		p.m.lockHeld.Set(1)
		p.logger.Info("publisher lock taken, publishing the outbox", "topic", p.topic)
	} else if err := p.lock.Check(ctx); err != nil {
		// The session may be gone, and with it the lock: another process
		// may hold it already. Take it again before the next send.
		p.held.Store(false)
		p.m.lockHeld.Set(0)
		return false, fmt.Errorf("publisher lock lost: %w", err)
	}

	rows, err := p.store.NextOutbox(ctx, p.batch)
	if err != nil {
		return false, err
	}
	if len(rows) == 0 {
		return false, nil
	}
	recs := make([]*kgo.Record, len(rows))
	for i, r := range rows {
		recs[i] = &kgo.Record{Topic: p.topic, Key: r.Key, Value: r.Value}
	}
	pctx, cancel := context.WithTimeout(ctx, produceTimeout)
	results := p.producer.ProduceSync(pctx, recs...)
	cancel()

	// ok is the number of rows before the first failed one.
	ok := len(rows)
	var produceErr error
	for i, res := range results {
		if res.Err != nil {
			ok, produceErr = i, res.Err
			break
		}
	}
	if len(results) != len(rows) && produceErr == nil {
		ok, produceErr = 0, fmt.Errorf("produce returned %d results for %d records", len(results), len(rows))
	}
	if ok > 0 {
		ids := make([]int64, ok)
		for i := range ok {
			ids[i] = rows[i].ID
		}
		if _, err := p.store.MarkSent(ctx, ids); err != nil {
			return false, err
		}
		p.sent.Add(int64(ok))
		for _, r := range rows[:ok] {
			p.observe(r)
		}
	}
	if produceErr != nil {
		p.m.publishErrors.Inc()
		return false, fmt.Errorf("produce routing events: %w", produceErr)
	}
	return len(rows) == p.batch, nil
}

// observe counts one published row by type, and an SLA miss by target.
func (p *Publisher) observe(r router.OutboxRecord) {
	p.m.published.WithLabelValues(string(r.Type)).Inc()
	if r.Type != event.TypeSLAMissed {
		return
	}
	ev, err := event.UnmarshalRouting(r.Value)
	if err != nil {
		p.logger.Warn("published sla_missed event does not decode", "id", r.ID, "error", err)
		return
	}
	p.m.slaMissed.WithLabelValues(string(ev.SLATarget)).Inc()
}

// pgLocker is a PostgreSQL session advisory lock on a connection of its
// own. The connection is taken out of the pool (hijacked), so it does not
// lower the pool size, and the lock lives as long as the session: when the
// connection breaks, the server releases the lock. Check runs a query on
// the same connection, so a successful Check means the session, and with it
// the lock, is still there.
//
// It is used by one goroutine at a time.
type pgLocker struct {
	pool *pgxpool.Pool
	key  int64
	conn *pgx.Conn
}

func newPGLocker(pool *pgxpool.Pool, key int64) *pgLocker {
	return &pgLocker{pool: pool, key: key}
}

// TryLock implements locker.
func (l *pgLocker) TryLock(ctx context.Context) (bool, error) {
	if l.conn == nil {
		c, err := l.pool.Acquire(ctx)
		if err != nil {
			return false, fmt.Errorf("acquire connection: %w", err)
		}
		l.conn = c.Hijack()
	}
	var ok bool
	if err := l.conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", l.key).Scan(&ok); err != nil {
		l.drop(ctx)
		return false, fmt.Errorf("try advisory lock: %w", err)
	}
	return ok, nil
}

// Check implements locker.
func (l *pgLocker) Check(ctx context.Context) error {
	if l.conn == nil {
		return errors.New("no lock connection")
	}
	if err := l.conn.Ping(ctx); err != nil {
		l.drop(ctx)
		return fmt.Errorf("lock connection: %w", err)
	}
	return nil
}

// Unlock implements locker. Closing the connection ends the session, which
// releases a session lock in any case; the explicit unlock only makes the
// release visible at once to a server that has not noticed the close yet.
func (l *pgLocker) Unlock(ctx context.Context) {
	if l.conn == nil {
		return
	}
	_, _ = l.conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", l.key)
	l.drop(ctx)
}

func (l *pgLocker) drop(ctx context.Context) {
	_ = l.conn.Close(ctx)
	l.conn = nil
}
