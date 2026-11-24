package routersvc

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
	"github.com/ZAUakaAlexey/threadroute/internal/router"
)

const (
	metricsNamespace = "threadroute"
	metricsSubsystem = "router"
)

// Results of an applied conversation event.
const (
	resultQueued    = "queued"
	resultPending   = "pending"
	resultEnded     = "ended"
	resultApplied   = "applied"
	resultDuplicate = "duplicate"
	resultReset     = "reset"
	// resultInvalid: the record could not be decoded or validated.
	resultInvalid = "invalid"
	// resultRejected: PostgreSQL refused the event with a data or
	// constraint error. Retrying would give the same error.
	resultRejected = "rejected"
	typeUnknown    = "unknown"
)

// Queue groups of the gauges.
const (
	groupTeam   = "team"
	groupShared = "shared"
)

type metrics struct {
	applied        *prometheus.CounterVec
	fetchErrors    prometheus.Counter
	catchUpActive  prometheus.Gauge
	catchUpSeconds prometheus.Gauge
	catchUpQueued  prometheus.Gauge

	offers     prometheus.Counter
	expired    prometheus.Counter
	slaMarked  prometheus.Counter
	teamChange *prometheus.CounterVec

	published     *prometheus.CounterVec
	slaMissed     *prometheus.CounterVec
	publishErrors prometheus.Counter
	pruned        prometheus.Counter
	lockHeld      prometheus.Gauge
	refreshed     prometheus.Counter

	loopErrors *prometheus.CounterVec

	conversations *prometheus.GaugeVec
	queueWaiting  *prometheus.GaugeVec
	queueOldest   *prometheus.GaugeVec
	operators     *prometheus.GaugeVec
	capacity      *prometheus.GaugeVec
	liveSlots     prometheus.Gauge
	activeTeams   prometheus.Gauge
	outboxUnsent  prometheus.Gauge
	outboxOldest  prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) (*metrics, error) {
	opts := func(name, help string) prometheus.Opts {
		return prometheus.Opts{Namespace: metricsNamespace, Subsystem: metricsSubsystem, Name: name, Help: help}
	}
	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts(opts(name, help)))
	}
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts(opts(name, help)))
	}
	m := &metrics{
		applied: prometheus.NewCounterVec(prometheus.CounterOpts(opts("events_applied_total",
			"Conversation events by type and result (queued, pending, ended, applied, duplicate, reset, invalid, rejected).")),
			[]string{"type", "result"}),
		fetchErrors: counter("fetch_errors_total", "Fetch errors of the conversations consumer."),
		catchUpActive: gauge("catchup_active",
			"1 while the router catches up on the conversations topic at start, 0 after."),
		catchUpSeconds: gauge("catchup_seconds",
			"Duration of the last catch-up, including the end of the catch-up."),
		catchUpQueued: gauge("catchup_queued",
			"Conversations that entered their queue at the end of the last catch-up."),

		offers:    counter("offers_total", "Offers made by the assignment workers of this process."),
		expired:   counter("offers_expired_total", "Offers that the expiry loop of this process ended after the accept timeout."),
		slaMarked: counter("sla_marked_total", "Missed SLA targets that the SLA loop of this process marked."),
		teamChange: prometheus.NewCounterVec(prometheus.CounterOpts(opts("team_changes_total",
			"Team changes made by the team loop of this process (formed, resized, released, moved).")),
			[]string{"change"}),

		published: prometheus.NewCounterVec(prometheus.CounterOpts(opts("routing_published_total",
			"Routing events acknowledged by Kafka, by type. A row sent again after a crash counts again.")),
			[]string{"type"}),
		slaMissed: prometheus.NewCounterVec(prometheus.CounterOpts(opts("sla_missed_total",
			"Published sla_missed routing events, by target.")),
			[]string{"target"}),
		publishErrors: counter("publish_errors_total", "Publish rounds that ended with a produce error."),
		pruned:        counter("outbox_pruned_total", "Sent outbox rows deleted."),
		lockHeld:      gauge("publisher_lock_held", "1 while this process holds the publisher lock and publishes the outbox."),
		refreshed: counter("operators_refreshed_total",
			"Operator snapshots that the refresh loop of this process wrote to the outbox again (decision 0011)."),

		loopErrors: prometheus.NewCounterVec(prometheus.CounterOpts(opts("loop_errors_total",
			"Failed steps of the background loops and the consumer, by loop.")),
			[]string{"loop"}),

		conversations: prometheus.NewGaugeVec(prometheus.GaugeOpts(opts("conversations",
			"Conversations by state, sampled from PostgreSQL.")), []string{"state"}),
		queueWaiting: prometheus.NewGaugeVec(prometheus.GaugeOpts(opts("queue_waiting",
			"Waiting conversations by language and queue group (team or shared), sampled.")), []string{"lang", "group"}),
		queueOldest: prometheus.NewGaugeVec(prometheus.GaugeOpts(opts("queue_oldest_wait_seconds",
			"Machine time since the oldest queue time, by language and queue group, sampled.")), []string{"lang", "group"}),
		operators: prometheus.NewGaugeVec(prometheus.GaugeOpts(opts("operators",
			"Operators by presence and group (team or pool), sampled.")), []string{"presence", "group"}),
		capacity: prometheus.NewGaugeVec(prometheus.GaugeOpts(opts("operator_capacity",
			"Sum of operator capacity by presence and group, sampled.")), []string{"presence", "group"}),
		liveSlots:    gauge("live_slots", "Offers and assignments that use a slot, sampled."),
		activeTeams:  gauge("active_teams", "Companies with a dedicated team, sampled."),
		outboxUnsent: gauge("outbox_unsent", "Routing events not yet published, sampled."),
		outboxOldest: gauge("outbox_oldest_unsent_seconds", "Machine time since the oldest unsent routing event was written, or 0, sampled."),
	}
	for _, c := range []prometheus.Collector{
		m.applied, m.fetchErrors, m.catchUpActive, m.catchUpSeconds, m.catchUpQueued,
		m.offers, m.expired, m.slaMarked, m.teamChange,
		m.published, m.slaMissed, m.publishErrors, m.pruned, m.lockHeld, m.refreshed, m.loopErrors,
		m.conversations, m.queueWaiting, m.queueOldest, m.operators, m.capacity,
		m.liveSlots, m.activeTeams, m.outboxUnsent, m.outboxOldest,
	} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("register router metrics: %w", err)
		}
	}
	for _, t := range []event.ConversationType{
		event.TypeOpened, event.TypeMessageAdded, event.TypeAnswered, event.TypeClosed, event.TypeMessageRemoved,
	} {
		for _, r := range []string{resultQueued, resultPending, resultEnded, resultApplied, resultDuplicate, resultReset, resultRejected} {
			m.applied.WithLabelValues(string(t), r)
		}
	}
	m.applied.WithLabelValues(typeUnknown, resultInvalid)
	for _, c := range []string{"formed", "resized", "released", "moved"} {
		m.teamChange.WithLabelValues(c)
	}
	for _, t := range []event.RoutingType{
		event.TypeQueued, event.TypeOffered, event.TypeAccepted, event.TypeOfferExpired, event.TypeReplied,
		event.TypeResolved, event.TypeSLAMissed, event.TypeEnded,
		event.TypeTeamFormed, event.TypeTeamResized, event.TypeTeamReleased,
		event.TypeOperatorUpdated, event.TypeOperatorOnline, event.TypeOperatorOffline,
	} {
		m.published.WithLabelValues(string(t))
	}
	for _, t := range []event.SLATarget{event.SLAAssignment, event.SLAFirstResponse} {
		m.slaMissed.WithLabelValues(string(t))
	}
	return m, nil
}

// applyResult names the result of one applied event. When one event does
// several things (a lost close followed by a new session), the queue result
// wins.
func applyResult(r router.ApplyResult) string {
	switch {
	case r.Skipped:
		return resultDuplicate
	case r.Queued:
		return resultQueued
	case r.Pending:
		return resultPending
	case r.Ended:
		return resultEnded
	case r.Reset:
		return resultReset
	}
	return resultApplied
}

// observeTeams counts the changes of one team check.
func (m *metrics) observeTeams(r router.TeamResult) {
	m.teamChange.WithLabelValues("formed").Add(float64(r.Formed))
	m.teamChange.WithLabelValues("resized").Add(float64(r.Resized))
	m.teamChange.WithLabelValues("released").Add(float64(r.Released))
	m.teamChange.WithLabelValues("moved").Add(float64(r.Moved))
}

// observeCounts sets the gauges from one snapshot. The vectors are reset
// first, so a queue or a state that is gone does not keep its last value.
func (m *metrics) observeCounts(c router.Counts, now time.Time) {
	m.conversations.Reset()
	for state, n := range c.Conversations {
		m.conversations.WithLabelValues(state).Set(float64(n))
	}

	type queueKey struct{ lang, group string }
	waiting := map[queueKey]int64{}
	oldest := map[queueKey]time.Time{}
	for _, q := range c.Queues {
		k := queueKey{lang: q.Lang, group: groupShared}
		if q.TeamCompanyID != "" {
			// Team queues are summed per language: a company ID is not a
			// label.
			k.group = groupTeam
		}
		waiting[k] += q.Waiting
		if o, ok := oldest[k]; !ok || q.Oldest.Before(o) {
			oldest[k] = q.Oldest
		}
	}
	m.queueWaiting.Reset()
	m.queueOldest.Reset()
	for k, n := range waiting {
		m.queueWaiting.WithLabelValues(k.lang, k.group).Set(float64(n))
		m.queueOldest.WithLabelValues(k.lang, k.group).Set(max(now.Sub(oldest[k]), 0).Seconds())
	}

	m.operators.Reset()
	m.capacity.Reset()
	for _, o := range c.Operators {
		group := "pool"
		if o.InTeam {
			group = groupTeam
		}
		m.operators.WithLabelValues(string(o.Presence), group).Add(float64(o.N))
		m.capacity.WithLabelValues(string(o.Presence), group).Add(float64(o.Capacity))
	}
	m.liveSlots.Set(float64(c.LiveSlots))
	m.activeTeams.Set(float64(c.ActiveTeams))
	m.outboxUnsent.Set(float64(c.UnsentOutbox))
	if c.OldestUnsent.IsZero() {
		m.outboxOldest.Set(0)
	} else {
		m.outboxOldest.Set(max(now.Sub(c.OldestUnsent), 0).Seconds())
	}
}
