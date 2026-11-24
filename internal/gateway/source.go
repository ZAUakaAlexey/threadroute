package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
)

const (
	// pollRecords bounds one batch of records.
	pollRecords = 2000
	// monitorEvery is how often the end offsets are read.
	monitorEvery = time.Second
	// listTimeout bounds one read of the offsets.
	listTimeout = 3 * time.Second
	// viewSweepEvery is how often the view forgets old rows on the
	// machine clock, for a quiet topic.
	viewSweepEvery = time.Minute
	// clientCloseTimeout bounds the close of the Kafka client.
	clientCloseTimeout = 10 * time.Second
)

type topicPartition struct {
	topic     string
	partition int32
}

type lagMark struct {
	end  int64
	seen time.Time
}

// Source reads every partition of the conversations and routing topics
// from the beginning, without a consumer group, and applies them to the
// view of the gateway. It does not stop on an outage: it keeps trying,
// and the status says that the data is stale (decision 0011, "Outages").
//
// # Ready
//
// At start it reads the end offsets of both topics. It is caught up when
// it has read every partition up to them; an empty partition, or one
// whose records expired, counts as read. Then once a second it reads the
// end offsets again with a short timeout, an active probe: when that has
// failed for SourceTimeout the source is unavailable, and when a
// partition has stayed behind an end it saw for LagLimit the source is
// lagging. A quiet topic is never lagging, because its end does not move.
type Source struct {
	g       *Gateway
	brokers []string
	topics  [2]string
	logger  *slog.Logger
	opts    []kgo.Opt

	mu        sync.Mutex
	pos       map[topicPartition]int64
	startEnds map[topicPartition]int64
	marks     map[topicPartition]lagMark
	lastOK    time.Time

	caughtUp atomic.Bool
	fresh    atomic.Bool
	// onCaughtUp runs once, when the source first caught up.
	onCaughtUp func()
}

// SourceOptions configure a Source.
type SourceOptions struct {
	Brokers []string
	// ConversationsTopic and RoutingTopic are the bus names when empty.
	ConversationsTopic string
	RoutingTopic       string
	// ConsumerOpts are applied last to the Kafka client, for tests.
	ConsumerOpts []kgo.Opt
	// OnCaughtUp runs once when the source first caught up.
	OnCaughtUp func()
}

// NewSource returns the source of g.
func (g *Gateway) NewSource(o SourceOptions) *Source {
	if o.ConversationsTopic == "" {
		o.ConversationsTopic = bus.TopicConversations
	}
	if o.RoutingTopic == "" {
		o.RoutingTopic = bus.TopicRouting
	}
	return &Source{
		g: g, brokers: o.Brokers, topics: [2]string{o.ConversationsTopic, o.RoutingTopic},
		logger: g.logger, opts: o.ConsumerOpts, onCaughtUp: o.OnCaughtUp,
		pos: map[topicPartition]int64{}, marks: map[topicPartition]lagMark{},
	}
}

// Ready reports whether the source has caught up and the data is fresh.
func (s *Source) Ready() bool { return s.caughtUp.Load() && s.fresh.Load() }

// CaughtUp reports whether the source has read up to the start ends.
func (s *Source) CaughtUp() bool { return s.caughtUp.Load() }

// Run reads until ctx ends. It returns nil then.
func (s *Source) Run(ctx context.Context) error {
	opts := []kgo.Opt{
		kgo.SeedBrokers(s.brokers...),
		kgo.ConsumeTopics(s.topics[0], s.topics[1]),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(500 * time.Millisecond),
	}
	cl, err := kgo.NewClient(append(opts, s.opts...)...)
	if err != nil {
		return fmt.Errorf("gateway: kafka client: %w", err)
	}
	defer closeKafka(cl, s.logger)
	adm := kadm.NewClient(cl)

	var wg sync.WaitGroup
	wg.Go(func() { s.monitor(ctx, adm) })
	s.consume(ctx, cl)
	wg.Wait()
	return nil
}

func (s *Source) consume(ctx context.Context, cl *kgo.Client) {
	m := s.g.m
	for {
		fetches := cl.PollRecords(ctx, pollRecords)
		if ctx.Err() != nil {
			return
		}
		fetches.EachError(func(topic string, p int32, err error) {
			if errors.Is(err, context.Canceled) {
				return
			}
			m.fetchErrors.Inc()
			s.logger.Debug("fetch error", "topic", topic, "partition", p, "error", err)
		})
		recs := fetches.Records()
		if len(recs) == 0 {
			continue
		}
		items := make([]Item, 0, len(recs))
		topics := make([]string, 0, len(recs))
		for _, rec := range recs {
			it := Item{Time: rec.Timestamp}
			switch rec.Topic {
			case s.topics[0]:
				ev, err := event.UnmarshalConversation(rec.Value)
				if err != nil {
					s.invalid(rec, err)
					continue
				}
				it.Conv = &ev
			default:
				ev, err := event.UnmarshalRouting(rec.Value)
				if err != nil {
					s.invalid(rec, err)
					continue
				}
				it.Routing = &ev
			}
			items = append(items, it)
			topics = append(topics, rec.Topic)
		}
		for i, res := range s.g.view.ApplyBatch(items) {
			m.events.WithLabelValues(topics[i], string(res)).Inc()
		}
		s.mu.Lock()
		for _, rec := range recs {
			tp := topicPartition{rec.Topic, rec.Partition}
			if rec.Offset+1 > s.pos[tp] {
				s.pos[tp] = rec.Offset + 1
			}
		}
		s.mu.Unlock()
		s.g.Publish()
	}
}

func (s *Source) invalid(rec *kgo.Record, err error) {
	s.g.m.events.WithLabelValues(rec.Topic, "invalid").Inc()
	// The error names fields only, never values with text.
	s.logger.Warn("invalid record skipped", "topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset,
		"unknown_kind", errors.Is(err, event.ErrUnknownKind))
}

// monitor reads the offsets every second and sets the status.
func (s *Source) monitor(ctx context.Context, adm *kadm.Client) {
	t := time.NewTicker(monitorEvery)
	defer t.Stop()
	lastSweep := s.g.clock.Now()
	for {
		s.check(ctx, adm)
		if now := s.g.clock.Now(); now.Sub(lastSweep) >= viewSweepEvery {
			lastSweep = now
			s.g.view.Sweep(now)
			s.g.Publish()
		}
		for kind, n := range s.g.view.Sizes() {
			s.g.m.viewRows.WithLabelValues(kind).Set(float64(n))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *Source) check(ctx context.Context, adm *kadm.Client) {
	lctx, cancel := context.WithTimeout(ctx, listTimeout)
	starts, serr := adm.ListStartOffsets(lctx, s.topics[0], s.topics[1])
	ends, eerr := adm.ListEndOffsets(lctx, s.topics[0], s.topics[1])
	cancel()
	if ctx.Err() != nil {
		return
	}
	now := s.g.clock.Now()
	err := errors.Join(serr, eerr)
	if err == nil {
		err = errors.Join(starts.Error(), ends.Error())
	}
	if err == nil && (len(ends) < 2) {
		err = errors.New("a topic is missing")
	}
	s.apply(now, starts, ends, err)
}

// apply updates the state from one reading of the offsets and sets the
// status.
func (s *Source) apply(now time.Time, starts, ends kadm.ListedOffsets, listErr error) {
	cfg := s.g.cfg
	s.mu.Lock()
	if listErr == nil {
		s.lastOK = now
		endMap := map[topicPartition]int64{}
		ends.Each(func(o kadm.ListedOffset) {
			endMap[topicPartition{o.Topic, o.Partition}] = o.Offset
		})
		starts.Each(func(o kadm.ListedOffset) {
			tp := topicPartition{o.Topic, o.Partition}
			// A partition not read yet, or whose records expired, is at
			// its start offset.
			if s.pos[tp] < o.Offset {
				s.pos[tp] = o.Offset
			}
		})
		if s.startEnds == nil {
			s.startEnds = endMap
		}
		for tp, end := range endMap {
			pos := s.pos[tp]
			if pos >= end {
				delete(s.marks, tp)
				continue
			}
			if mk, ok := s.marks[tp]; !ok || pos >= mk.end {
				s.marks[tp] = lagMark{end: end, seen: now}
			}
		}
	}
	caught := s.startEnds != nil
	for tp, end := range s.startEnds {
		if s.pos[tp] < end {
			caught = false
		}
	}
	var lag time.Duration
	for _, mk := range s.marks {
		lag = max(lag, now.Sub(mk.seen))
	}
	unavailable := s.lastOK.IsZero() || now.Sub(s.lastOK) >= cfg.SourceTimeout
	sinceOK := time.Duration(0)
	if !s.lastOK.IsZero() {
		sinceOK = now.Sub(s.lastOK)
	}
	s.mu.Unlock()

	first := caught && !s.caughtUp.Swap(caught)
	if !caught {
		// The start ends are fixed; once caught up, the source stays so.
		s.caughtUp.Store(false)
	}
	s.g.m.lag.Set(lag.Seconds())

	var st gwapi.Status
	switch {
	case unavailable:
		st = gwapi.Status{Stale: true, Reason: gwapi.StaleSourceUnavailable, LagUS: sinceOK.Microseconds()}
	case !caught:
		st = gwapi.Status{Stale: true, Reason: gwapi.StaleCatchingUp}
	case lag >= cfg.LagLimit:
		st = gwapi.Status{Stale: true, Reason: gwapi.StaleLagging, LagUS: lag.Round(time.Second).Microseconds()}
	default:
		st = gwapi.Status{}
	}
	s.fresh.Store(!st.Stale)
	s.g.SetStatus(st)
	if first {
		s.logger.Info("caught up with the topics")
		if s.onCaughtUp != nil {
			s.onCaughtUp()
		}
	}
}

// closeKafka closes a Kafka client, bounded.
func closeKafka(cl *kgo.Client, logger *slog.Logger) {
	done := make(chan struct{})
	go func() {
		cl.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(clientCloseTimeout):
		logger.Warn("kafka client did not close in time")
	}
}
