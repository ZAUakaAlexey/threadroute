package record

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
)

// shutdownTimeout bounds the final offset commit after the context of Run is
// canceled.
const shutdownTimeout = 10 * time.Second

// maxPollRecords bounds one poll, so a rebalance is not blocked for long.
const maxPollRecords = 10_000

// defaultCommitInterval is the longest a poll waits for records. After each
// poll, also an empty one, the Recorder commits the offsets of segments that
// were completed in the meantime, for example by the age check of the
// Writer while no records arrive.
const defaultCommitInterval = time.Second

// EntryWriter is what the Recorder needs from a Writer.
type EntryWriter interface {
	// Append adds an entry to the open segment and rotates it when a limit
	// is reached.
	Append(Entry) error
	// Rotate completes the open segment now.
	Rotate() error
	// CompletePositions returns, per partition, the highest offset that is
	// stored in a complete segment. It also works after Close.
	CompletePositions() map[int32]int64
	// Close completes the open segment.
	Close() error
}

// Recorder consumes TopicRaw with a consumer group and appends every record
// to an EntryWriter.
//
// Offsets are committed only for records that are in complete segments,
// never for records in the open (temporary) segment. A segment becomes
// complete on rotation by size or age, on a partition revoke and on
// shutdown. After each poll the Recorder asks the writer for the highest
// offset per partition in complete segments and commits the next offset.
// Auto commit is off, and a flush is never a reason to commit.
//
// What a crash causes: the open segment stays under its temporary name and
// the next OpenWriter removes it. None of its records were committed, so the
// next run reads them again from Kafka. At most one segment of records is
// read again, nothing is lost, and no record appears twice in complete
// segments.
//
// The cost: the consumer group lag includes the open segment, so the lag
// can show up to one segment age (1 hour by default) of delay even when the
// recorder keeps up.
//
// Edge cases that can still repeat a record in complete segments: a failed
// sync of the directory after a rename (the writer then does not report the
// segment as complete), and partitions lost without a clean revoke (another
// member reads records that are also in this member's open segment).
type Recorder struct {
	client         *kgo.Client
	w              EntryWriter
	logger         *slog.Logger
	errors         *prometheus.CounterVec
	commitInterval time.Duration

	// mu guards committed. The poll loop and the revoke callback both
	// commit, gated by BlockRebalanceOnPoll, but the lock keeps it simple.
	mu        sync.Mutex
	committed map[int32]int64 // next offset per partition, as committed
}

// NewRecorder creates the Kafka client. It does not connect until Run.
// extra options are applied last, so tests can point the client at a fake
// cluster.
func NewRecorder(brokers []string, groupID string, start StartPosition, w EntryWriter,
	logger *slog.Logger, reg prometheus.Registerer, extra ...kgo.Opt,
) (*Recorder, error) {
	if len(brokers) == 0 {
		return nil, errors.New("new recorder: no brokers")
	}
	if groupID == "" {
		return nil, errors.New("new recorder: group ID is empty")
	}
	reset := kgo.NewOffset().AtEnd()
	switch start {
	case StartEnd, "":
	case StartBeginning:
		reset = kgo.NewOffset().AtStart()
	default:
		return nil, fmt.Errorf("new recorder: unknown start position %q", start)
	}
	errs, err := registerErrors(reg)
	if err != nil {
		return nil, fmt.Errorf("new recorder: %w", err)
	}

	r := &Recorder{
		w:              w,
		logger:         logger,
		errors:         errs,
		commitInterval: defaultCommitInterval,
		committed:      map[int32]int64{},
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(bus.TopicRaw),
		kgo.ConsumeResetOffset(reset),
		kgo.DisableAutoCommit(),
		// Rebalance callbacks only run between polls, never while records
		// are being appended.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsRevoked(r.onRevoked),
		kgo.OnPartitionsLost(r.onLost),
	}
	opts = append(opts, extra...)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("new recorder: create kafka client: %w", err)
	}
	r.client = client
	return r, nil
}

// Run consumes until ctx is canceled or a fatal error happens. On
// cancellation it closes the writer, which completes the open segment, then
// commits the offsets of everything in complete segments, closes the client
// and returns nil.
//
// Run owns the writer and the client: both are closed when Run returns.
func (r *Recorder) Run(ctx context.Context) error {
	runErr := r.loop(ctx)

	// Shutdown. ctx is already canceled, so the commit uses its own
	// deadline. The writer is closed first, so the open segment is complete
	// before its offsets are committed. After a fatal error the commit
	// still runs, but only covers segments that the writer completed.
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if err := r.w.Close(); err != nil && runErr == nil {
		runErr = fmt.Errorf("close writer: %w", err)
	}
	if err := r.commitComplete(sctx); err != nil && runErr == nil {
		runErr = fmt.Errorf("final offset commit: %w", err)
	}
	r.client.AllowRebalance()
	// Close leaves the group, so another member gets the partitions at once.
	r.client.Close()
	if runErr != nil {
		return runErr
	}
	r.logger.Info("recorder stopped")
	return nil
}

func (r *Recorder) loop(ctx context.Context) error {
	r.logger.Info("recorder started", "topic", bus.TopicRaw)
	for {
		pctx, cancel := context.WithTimeout(ctx, r.commitInterval)
		fetches := r.client.PollRecords(pctx, maxPollRecords)
		cancel()
		// Every returned record is appended, even during shutdown: the
		// records are then part of the segment that Run completes.
		if err := r.appendAll(fetches); err != nil {
			return err
		}
		if ctx.Err() != nil {
			// Rebalances stay blocked until Run has committed.
			return nil
		}
		if err := r.commitComplete(ctx); err != nil && ctx.Err() == nil {
			// Not fatal: the next commit includes these offsets. At worst
			// a restart reads a complete segment again.
			r.errors.WithLabelValues(opCommit).Inc()
			r.logger.Warn("offset commit failed", "error", err)
		}
		r.client.AllowRebalance()
	}
}

// appendAll appends the records of one poll. The writer rotates by itself.
func (r *Recorder) appendAll(fetches kgo.Fetches) error {
	if err := r.checkFetchErrors(fetches); err != nil {
		return err
	}
	var appendErr error
	fetches.EachRecord(func(rec *kgo.Record) {
		if appendErr != nil {
			return
		}
		e, badWitnessed := entryFromRecord(rec)
		if badWitnessed {
			// The entry is kept without a witnessed time. The header value
			// is not logged.
			r.errors.WithLabelValues(opHeader).Inc()
			r.logger.Warn("invalid witnessed time header, treated as absent",
				"header", bus.HeaderEventWitnessed, "partition", rec.Partition, "offset", rec.Offset)
		}
		err := r.w.Append(e)
		switch {
		case err == nil:
		case errors.Is(err, ErrInvalidEntry):
			// The value is not JSON or the key is not UTF-8. It cannot be
			// replayed in this format, so it is skipped. The error does not
			// hold the key or the value.
			r.errors.WithLabelValues(opInvalid).Inc()
			r.logger.Warn("record skipped", "partition", rec.Partition, "offset", rec.Offset, "error", err)
		default:
			appendErr = err
		}
	})
	if appendErr != nil {
		return fmt.Errorf("append: %w", appendErr)
	}
	return nil
}

// commitComplete commits, for each partition, the offset after the highest
// record stored in a complete segment, if it moved since the last commit.
func (r *Recorder) commitComplete(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	offsets := map[int32]kgo.EpochOffset{}
	for p, last := range r.w.CompletePositions() {
		next := last + 1
		if done, ok := r.committed[p]; ok && done >= next {
			continue
		}
		offsets[p] = kgo.EpochOffset{Epoch: -1, Offset: next}
	}
	if len(offsets) == 0 {
		return nil
	}

	var commitErr error
	r.client.CommitOffsetsSync(ctx, map[string]map[int32]kgo.EpochOffset{bus.TopicRaw: offsets},
		func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				commitErr = err
				return
			}
			for _, t := range resp.Topics {
				for _, p := range t.Partitions {
					if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
						commitErr = errors.Join(commitErr, fmt.Errorf("partition %d: %w", p.Partition, err))
						continue
					}
					if t.Topic == bus.TopicRaw {
						r.committed[p.Partition] = offsets[p.Partition].Offset
					}
				}
			}
		})
	if commitErr != nil {
		r.errors.WithLabelValues(opCommit).Inc()
		return commitErr
	}
	r.logger.Debug("offsets committed", "partitions", len(offsets))
	return nil
}

// onRevoked completes the open segment and commits before the partitions go
// to another member. Otherwise the new owner would read records that are
// also in this member's open segment, and both would end in complete
// segments.
func (r *Recorder) onRevoked(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
	if err := r.w.Rotate(); err != nil {
		if !errors.Is(err, ErrClosed) {
			r.logger.Error("rotate on revoke failed", "error", err)
		}
	}
	if err := r.commitComplete(ctx); err != nil {
		r.logger.Error("commit on revoke failed", "error", err)
	}
	r.logger.Info("partitions revoked", "partitions", revoked[bus.TopicRaw])
}

// onLost only logs. A commit is not possible for lost partitions.
func (r *Recorder) onLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	r.errors.WithLabelValues(opFetch).Inc()
	r.logger.Warn("partitions lost", "partitions", lost[bus.TopicRaw])
}

// checkFetchErrors logs and counts fetch errors and returns the first fatal
// one.
//
// Fatal errors:
//   - the client was closed, because no more records can arrive;
//   - authorization and authentication errors, because they only go away
//     with a configuration change, so a restart with new settings is the
//     fix and retrying forever only hides the problem.
//
// Everything else is logged and counted, and consuming goes on: the client
// retries broker and group errors itself, data loss errors are informational
// (the client resets the position itself), and a lost group session is
// joined again.
func (r *Recorder) checkFetchErrors(fetches kgo.Fetches) error {
	var fatal error
	fetches.EachError(func(topic string, partition int32, err error) {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		r.errors.WithLabelValues(opFetch).Inc()
		r.logger.Error("fetch error", "topic", topic, "partition", partition, "error", err)
		if fatal == nil && isFatalFetchError(err) {
			fatal = fmt.Errorf("fetch %s partition %d: %w", topic, partition, err)
		}
	})
	return fatal
}

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

// entryFromRecord copies the fields a replay needs. badWitnessed is true
// when the witnessed time header is present but not a positive integer; the
// entry then has no witnessed time.
func entryFromRecord(rec *kgo.Record) (e Entry, badWitnessed bool) {
	e = Entry{
		Partition: rec.Partition,
		Offset:    rec.Offset,
		Timestamp: rec.Timestamp,
		Key:       string(rec.Key),
		Value:     rec.Value,
	}
	for _, h := range rec.Headers {
		switch h.Key {
		case bus.HeaderEventID:
			e.EventID = string(h.Value)
		case bus.HeaderEventKind:
			e.EventKind = string(h.Value)
		case bus.HeaderEventWitnessed:
			t, ok := parseWitnessed(h.Value)
			e.Witnessed, badWitnessed = t, !ok
		}
	}
	return e, badWitnessed
}
