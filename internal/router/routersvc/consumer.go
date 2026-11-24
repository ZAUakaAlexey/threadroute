package routersvc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/router"
)

const (
	// maxPollRecords bounds one input batch.
	maxPollRecords = 1000
	// catchUpBatch is the transaction size of EndCatchUp.
	catchUpBatch = 500
	// shutdownGrace is how long the batch in progress may still run after
	// a stop, so its events are applied and its offsets committed.
	shutdownGrace = 10 * time.Second
)

// applier is the part of *router.Service that the consumer uses.
type applier interface {
	ApplyConversation(ctx context.Context, ev event.Conversation, catchUp bool) (router.ApplyResult, error)
	EndCatchUp(ctx context.Context, batch int) (int, error)
}

// offsetAdmin is the part of *kadm.Client that the catch-up uses.
type offsetAdmin interface {
	ListStartOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	ListEndOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	FetchOffsets(ctx context.Context, group string) (kadm.OffsetResponses, error)
}

// consumer reads the conversations topic in a consumer group and applies
// every record to the routing state. Offsets are committed only after the
// records of a batch are applied, so delivery is at least once; the core
// skips an event it has applied before.
type consumer struct {
	client  *kgo.Client
	admin   offsetAdmin
	apply   applier
	topic   string
	group   string
	runner  *runner
	m       *metrics
	logger  *slog.Logger
	timer   timer
	applied func(queued bool)
}

// catchUp applies the backlog of the topic with catchUp=true (decision
// 0009, "Catching up"). The backlog ends at the end offsets read before
// the first poll. A partition is done when the group position reaches its
// end offset. The start position of a partition is the committed offset of
// the group, or the start of the log for a new group, so a partition that
// is empty or already consumed is done at once.
//
// Records polled while some partition still has backlog are all applied
// with catchUp=true, also records past the end offset of their partition:
// nothing is offered before EndCatchUp, so this only moves them into the
// catch-up.
func (c *consumer) catchUp(ctx context.Context) error {
	// The offset requests fail for a while on a broker that has just
	// started (the group coordinator is not available yet), so they are
	// retried like a loop step.
	var pos, end map[int32]int64
	err := c.runner.retry(ctx, "catchup_offsets", func(ctx context.Context) error {
		var err error
		pos, end, err = c.backlog(ctx)
		return err
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	remaining := remainingPartitions(pos, end)
	c.logger.Info("catch-up started", "topic", c.topic, "partitions_behind", len(remaining),
		"records_behind", behind(pos, end))
	for len(remaining) > 0 {
		recs, err := c.poll(ctx)
		if err != nil || ctx.Err() != nil {
			return err
		}
		if err := c.process(ctx, recs, true); err != nil {
			return err
		}
		for _, r := range recs {
			pos[r.Partition] = max(pos[r.Partition], r.Offset+1)
		}
		remaining = remainingPartitions(pos, end)
		c.client.AllowRebalance()
	}
	return nil
}

// backlog reads the start position of the group and the end offset of
// every partition.
func (c *consumer) backlog(ctx context.Context) (pos, end map[int32]int64, err error) {
	ends, err := c.admin.ListEndOffsets(ctx, c.topic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("list end offsets: %w", err)
	}
	starts, err := c.admin.ListStartOffsets(ctx, c.topic)
	if err == nil {
		err = starts.Error()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("list start offsets: %w", err)
	}
	committed, err := c.admin.FetchOffsets(ctx, c.group)
	if err == nil {
		err = committed.Error()
	}
	if err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
		return nil, nil, fmt.Errorf("fetch committed offsets: %w", err)
	}
	pos, end = map[int32]int64{}, map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Topic == c.topic {
			end[o.Partition] = o.Offset
		}
	})
	starts.Each(func(o kadm.ListedOffset) {
		if o.Topic == c.topic {
			pos[o.Partition] = o.Offset
		}
	})
	for p := range end {
		if r, ok := committed.Lookup(c.topic, p); ok && r.Err == nil && r.At >= 0 {
			pos[p] = max(pos[p], r.At)
		}
	}
	return pos, end, nil
}

// remainingPartitions returns the partitions whose position is before
// their end offset, sorted.
func remainingPartitions(pos, end map[int32]int64) []int32 {
	var out []int32
	for p, e := range end {
		if pos[p] < e {
			out = append(out, p)
		}
	}
	slices.Sort(out)
	return out
}

// behind returns the number of records between the positions and the end
// offsets.
func behind(pos, end map[int32]int64) int64 {
	var n int64
	for p, e := range end {
		n += max(e-pos[p], 0)
	}
	return n
}

// endCatchUp queues the pending conversations. It is retried like a loop
// step: EndCatchUp is safe to repeat.
func (c *consumer) endCatchUp(ctx context.Context) (int, error) {
	total := 0
	err := c.runner.retry(ctx, "end_catchup", func(ctx context.Context) error {
		n, err := c.apply.EndCatchUp(ctx, catchUpBatch)
		total += n
		return err
	})
	return total, err
}

// live applies records until ctx ends.
func (c *consumer) live(ctx context.Context) error {
	for {
		recs, err := c.poll(ctx)
		if err != nil {
			return err
		}
		if err := c.process(ctx, recs, false); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
		c.client.AllowRebalance()
	}
}

// poll returns the next records. A run of fetch errors longer than the
// outage limit is an error. A fatal fetch error is returned at once.
func (c *consumer) poll(ctx context.Context) ([]*kgo.Record, error) {
	var since time.Time
	for {
		fetches := c.client.PollRecords(ctx, maxPollRecords)
		var fatal, last error
		fetches.EachError(func(topic string, partition int32, err error) {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			c.m.fetchErrors.Inc()
			c.logger.Warn("fetch error", "topic", topic, "partition", partition, "error", err)
			last = err
			if fatal == nil && isFatalFetchError(err) {
				fatal = fmt.Errorf("fetch %s partition %d: %w", topic, partition, err)
			}
		})
		if fatal != nil {
			return nil, fatal
		}
		recs := fetches.Records()
		if last == nil || len(recs) > 0 || ctx.Err() != nil {
			return recs, nil
		}
		now := c.timer.Now()
		if since.IsZero() {
			since = now
		}
		if now.Sub(since) >= c.runner.outageLimit {
			return nil, fmt.Errorf("consume %s: fetch failing for %s: %w", c.topic, now.Sub(since).Round(time.Millisecond), last)
		}
		select {
		case <-ctx.Done():
			return nil, nil
		case <-c.timer.After(minBackoff):
		}
	}
}

// process applies one batch and commits its offsets. The partitions of the
// batch are applied in parallel, each in record order, so the events of
// one conversation (one key, one partition) are applied in order.
//
// After a stop the batch still gets shutdownGrace, so it is applied and
// committed. A batch that fails is not committed: the process stops, and
// a restart applies the records again, which changes nothing that was
// applied.
func (c *consumer) process(ctx context.Context, recs []*kgo.Record, catchUp bool) error {
	if len(recs) == 0 {
		return nil
	}
	bctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	stop := context.AfterFunc(ctx, func() {
		t := time.AfterFunc(shutdownGrace, cancel)
		context.AfterFunc(bctx, func() { t.Stop() })
	})
	defer stop()

	byPartition := map[int32][]*kgo.Record{}
	for _, r := range recs {
		byPartition[r.Partition] = append(byPartition[r.Partition], r)
	}
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for _, p := range slices.Sorted(maps.Keys(byPartition)) {
		wg.Go(func() {
			if err := c.applyPartition(bctx, byPartition[p], catchUp); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
	}
	if err := c.client.CommitUncommittedOffsets(bctx); err != nil {
		// Not fatal: the next commit covers these offsets, and a
		// redelivered record changes nothing.
		c.logger.Warn("offset commit failed", "error", err)
	}
	return nil
}

func (c *consumer) applyPartition(ctx context.Context, recs []*kgo.Record, catchUp bool) error {
	for _, rec := range recs {
		ev, err := event.UnmarshalConversation(rec.Value)
		if err != nil {
			// The error names fields only. Conversation events carry no
			// text anyway.
			c.m.applied.WithLabelValues(typeUnknown, resultInvalid).Inc()
			c.logger.Warn("invalid conversation event skipped", "partition", rec.Partition, "offset", rec.Offset, "error", err)
			continue
		}
		var res router.ApplyResult
		rejected := false
		err = c.runner.retry(ctx, "apply", func(ctx context.Context) error {
			r, err := c.apply.ApplyConversation(ctx, ev, catchUp)
			if err != nil && isDataError(err) {
				rejected = true
				c.logger.Error("conversation event rejected by the database, skipped",
					"partition", rec.Partition, "offset", rec.Offset, "type", ev.Type, "error", err)
				return nil
			}
			res = r
			return err
		})
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("apply %s partition %d offset %d: stopped: %w", c.topic, rec.Partition, rec.Offset, err)
			}
			return fmt.Errorf("apply %s partition %d offset %d: %w", c.topic, rec.Partition, rec.Offset, err)
		}
		if rejected {
			c.m.applied.WithLabelValues(string(ev.Type), resultRejected).Inc()
			continue
		}
		c.m.applied.WithLabelValues(string(ev.Type), applyResult(res)).Inc()
		if c.applied != nil {
			c.applied(res.Queued)
		}
	}
	return nil
}

// isDataError reports a PostgreSQL error that the same event would get
// again: a data exception (class 22), an integrity constraint violation
// (class 23) or a program limit, such as a key too large for an index
// (class 54). Such an event is counted and skipped. Every other error is
// treated as an outage and retried.
func isDataError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || len(pgErr.Code) < 2 {
		return false
	}
	switch pgErr.Code[:2] {
	case "22", "23", "54":
		return true
	}
	return false
}

// isFatalFetchError reports an error that the client does not retry: a
// closed client, or an authorization or authentication error.
func isFatalFetchError(err error) bool {
	if errors.Is(err, kgo.ErrClientClosed) {
		return true
	}
	for _, e := range []error{
		kerr.TopicAuthorizationFailed,
		kerr.GroupAuthorizationFailed,
		kerr.ClusterAuthorizationFailed,
		kerr.SaslAuthenticationFailed,
	} {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}
