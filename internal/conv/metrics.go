package conv

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

const (
	metricsNamespace = "threadroute"
	metricsSubsystem = "conv"
)

// Label values that are not outcomes or event types.
const (
	directionUnknown = "unknown"
	resultInvalid    = "invalid"
	kindUnknown      = "unknown"
)

type metrics struct {
	messages            *prometheus.CounterVec
	control             *prometheus.CounterVec
	companiesAdmitted   prometheus.Gauge
	admissions          prometheus.Counter
	admissionRefused    prometheus.Counter
	admissionsEnded     prometheus.Counter
	conversationsOpen   prometheus.Gauge
	conversationsOpened prometheus.Counter
	conversationsClosed *prometheus.CounterVec
	sessionsReopened    prometheus.Counter
	responseTime        prometheus.Histogram
	published           *prometheus.CounterVec
	outboxPending       prometheus.Gauge
	redisErrors         prometheus.Counter
	watermarkLag        *prometheus.GaugeVec
	process             prometheus.Histogram
	textPurged          prometheus.Counter
	outboxInvalid       prometheus.Counter
	messagesRemoved     *prometheus.CounterVec
	removedNoEvent      prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) (*metrics, error) {
	opts := func(name, help string) prometheus.Opts {
		return prometheus.Opts{Namespace: metricsNamespace, Subsystem: metricsSubsystem, Name: name, Help: help}
	}
	m := &metrics{
		messages: prometheus.NewCounterVec(prometheus.CounterOpts(opts("messages_total",
			"Message events by direction and result (stored, not_admitted, no_open_conversation, duplicate, edited, edit_dropped, invalid).")),
			[]string{"direction", "result"}),
		control: prometheus.NewCounterVec(prometheus.CounterOpts(opts("control_total",
			"Control events by kind and result (removed, pending, duplicate, invalid).")),
			[]string{"kind", "result"}),
		companiesAdmitted: prometheus.NewGauge(prometheus.GaugeOpts(opts("companies_admitted",
			"Admitted companies, read from Redis: last customer message within the idle period of the newest watermark."))),
		admissions: prometheus.NewCounter(prometheus.CounterOpts(opts("admissions_total",
			"Companies admitted."))),
		admissionRefused: prometheus.NewCounter(prometheus.CounterOpts(opts("admission_refused_total",
			"Admissions refused because the company limit was reached."))),
		admissionsEnded: prometheus.NewCounter(prometheus.CounterOpts(opts("admissions_ended_total",
			"Admissions that ended, after the idle period or by a company removal."))),
		conversationsOpen: prometheus.NewGauge(prometheus.GaugeOpts(opts("conversations_open",
			"Open sessions, read from Redis."))),
		conversationsOpened: prometheus.NewCounter(prometheus.CounterOpts(opts("conversations_opened_total",
			"Sessions opened, including reopened ones."))),
		conversationsClosed: prometheus.NewCounterVec(prometheus.CounterOpts(opts("conversations_closed_total",
			"Sessions closed, by reason.")), []string{"reason"}),
		sessionsReopened: prometheus.NewCounter(prometheus.CounterOpts(opts("sessions_reopened_total",
			"Sessions opened after an earlier session of the same conversation."))),
		responseTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "response_time_seconds",
			Help: "Real response times: from the first unanswered customer message to the company answer, in source time.",
			// 1 second to about 4.5 hours.
			Buckets: prometheus.ExponentialBuckets(1, 2, 15),
		}),
		published: prometheus.NewCounterVec(prometheus.CounterOpts(opts("published_total",
			"Conversation events confirmed by Kafka, by type.")), []string{"type"}),
		outboxPending: prometheus.NewGauge(prometheus.GaugeOpts(opts("outbox_pending",
			"Conversation events in the outbox, read from Redis."))),
		redisErrors: prometheus.NewCounter(prometheus.CounterOpts(opts("redis_errors_total",
			"Failed Redis operations."))),
		watermarkLag: prometheus.NewGaugeVec(prometheus.GaugeOpts(opts("watermark_lag_seconds",
			"Wall-clock time minus the watermark, per owned partition of the events topic.")), []string{"partition"}),
		process: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "process_seconds",
			Help: "Time to process one input batch: scripts, publish and commit.",
			// 0.5 ms to about 8 s.
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 15),
		}),
		textPurged: prometheus.NewCounter(prometheus.CounterOpts(opts("text_purged_total",
			"Messages whose text was removed after their conversation closed."))),
		outboxInvalid: prometheus.NewCounter(prometheus.CounterOpts(opts("outbox_invalid_total",
			"Outbox entries that were not valid conversation events and were dropped."))),
		messagesRemoved: prometheus.NewCounterVec(prometheus.CounterOpts(opts("messages_removed_total",
			"Message removed events appended to the outbox, by reason (deleted, author_removed): one per stored text that a deletion or an author removal dropped.")),
			[]string{"reason"}),
		removedNoEvent: prometheus.NewCounter(prometheus.CounterOpts(opts("removed_without_event_total",
			"Texts that a deletion or an author removal dropped without a message removed event, because the conversation record was gone."))),
	}
	for _, c := range []prometheus.Collector{
		m.messages, m.control, m.companiesAdmitted, m.admissions, m.admissionRefused, m.admissionsEnded,
		m.conversationsOpen, m.conversationsOpened, m.conversationsClosed, m.sessionsReopened, m.responseTime,
		m.published, m.outboxPending, m.redisErrors, m.watermarkLag, m.process, m.textPurged, m.outboxInvalid,
		m.messagesRemoved, m.removedNoEvent,
	} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("register conv metrics: %w", err)
		}
	}
	for _, d := range []event.Direction{event.FromCustomer, event.FromCompany} {
		for _, r := range []string{OutcomeStored, OutcomeNotAdmitted, OutcomeNoOpenConversation, OutcomeDuplicate, OutcomeEdited, OutcomeEditDropped} {
			m.messages.WithLabelValues(string(d), r)
		}
	}
	for _, k := range []event.ControlKind{event.KindDeletion, event.KindAuthorRemoval} {
		for _, r := range []string{OutcomeRemoved, OutcomePending, OutcomeDuplicate} {
			m.control.WithLabelValues(string(k), r)
		}
	}
	for _, r := range []event.CloseReason{event.CloseInactivity, event.CloseCustomerRemoved, event.CloseCompanyRemoved} {
		m.conversationsClosed.WithLabelValues(string(r))
	}
	for _, t := range []event.ConversationType{event.TypeOpened, event.TypeMessageAdded, event.TypeAnswered, event.TypeClosed, event.TypeMessageRemoved} {
		m.published.WithLabelValues(string(t))
	}
	for _, r := range []event.RemoveReason{event.RemoveDeleted, event.RemoveAuthorRemoved} {
		m.messagesRemoved.WithLabelValues(string(r))
	}
	return m, nil
}

// observe counts the state changes of one script result.
func (m *metrics) observe(r Result) {
	m.admissions.Add(float64(r.Admitted))
	m.admissionRefused.Add(float64(r.Refused))
	m.admissionsEnded.Add(float64(r.Ended))
	m.conversationsOpened.Add(float64(r.Opened))
	m.sessionsReopened.Add(float64(r.Reopened))
	for reason, n := range r.Closed {
		m.conversationsClosed.WithLabelValues(string(reason)).Add(float64(n))
	}
	if r.Answered {
		m.responseTime.Observe(r.ResponseTime.Seconds())
	}
	for reason, n := range r.Removed {
		m.messagesRemoved.WithLabelValues(string(reason)).Add(float64(n))
	}
	m.removedNoEvent.Add(float64(r.RemovedWithoutEvent))
}
