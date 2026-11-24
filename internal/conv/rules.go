// Package conv builds conversations from message and control events
// (decisions 0006, 0007 and 0008). It decides which companies are served,
// keeps conversation state in Redis and publishes conversation events to the
// conversations topic.
//
// # One input batch
//
// The Service polls records from the events topic (read committed, because
// normalize writes in Kafka transactions) and from events.control. Each
// record is applied by one Lua script in Redis. The script checks and sets a
// marker for the event ID, changes the state, moves the watermark of the
// partition and appends the conversation events to an outbox in the same
// atomic step, with their sequence numbers. After the batch the Service
// publishes every outbox that got new entries, removes the entries Kafka
// confirmed, and only then commits the consumer offsets. A crash at any
// point repeats work but loses nothing: the outbox is published again and a
// redelivered record finds its event ID marker and changes nothing.
//
// # Ordering of the events of one conversation
//
// There is one outbox per partition of the events topic. A conversation
// stores the partition of its messages, so the message script, the control
// scripts and the sweeper all append its events to the same outbox. Redis
// runs scripts one at a time, so the order in the outbox is the sequence
// order. An outbox is always published from its head, in order, by one
// producer per process to one partition of the conversations topic (same
// key), and an entry is removed only after Kafka confirmed it. If two
// processes publish the same outbox at the same time (a control consumer and
// the owner of the partition, or the old and new owner during a rebalance),
// both read from the head, so every event first appears in sequence order and
// the rest are repeats. Consumers skip a sequence number they have already
// seen (decision 0007). The franz-go producer fails all buffered records of
// a partition together, so a later event is never confirmed while an
// earlier one failed.
//
// # Admitted companies
//
// Decision 0008 ends an admission lazily, when the next customer message
// comes more than AdmissionIdle after the previous one, and has no sweeper.
// So conv keeps one sorted set of admitted companies, scored by the source
// time of their last customer message, and never stores a count. A company
// counts as admitted at source time t when its score is at or after
// t - AdmissionIdle. The company limit check counts at the time of the
// message being decided, and the companies_admitted gauge counts at the
// newest watermark. The same rule decides, per company, whether its
// admission ended, so the count and the per-company check agree. Entries
// more than AdmissionIdle + KeyMargin old are removed for memory only.
// Partitions run at different source times, so near the limit a lagging
// partition can see a company as admitted that a faster one already counts
// as ended; the count can then be over the limit for a short time.
//
// # Edits
//
// A message event with Edited set (decision 0006) replaces the stored text
// and language tags of its message and changes nothing else. An edit of a
// message that is not stored is dropped as edit_dropped and never opens a
// conversation, and a pending early deletion or removal still wins. A
// message event without Edited whose message ID is already stored is a
// duplicate. An applied edit sets the field e of the text hash, which the
// TextReader reports as StoredMessage.Edited.
//
// # Removed messages
//
// A deletion or an author removal that drops a stored text appends one
// message_removed event (decision 0011) to the outbox of the message's
// conversation, in the same script, with the next sequence number of that
// conversation. The event carries the current session and counters of the
// conversation, also when that session is closed and its text is still
// kept, so it can follow the closed event. An author removal writes the
// removed events of a conversation before its closed event, in source time
// order of the messages and then by ID. No event is written for a deletion
// that is remembered because the text is not stored (early, repeated, or
// after the purge), for texts dropped by the MessagesKept limit or by the
// purge after the close, or for a text whose conversation record is gone
// (counted as removed_without_event).
//
// # Rules where the decision records are silent
//
//   - A removal that arrives before an older message of its author is
//     remembered for EarlyDeletionKept: the message is counted without its
//     text and its session closes at once with the removal time. One case
//     differs from the other order: with the removal first, a company reply
//     that follows in that session finds it closed and is dropped as
//     no_open_conversation, so counters and sequence numbers differ.
//   - The early deletion window is measured against the watermark of the
//     events partition of the message when the message arrives.
//   - When the session record is forgotten 24 hours after a close, the next
//     session starts again at session 1 and sequence number 1, so consumers
//     that skip seen sequence numbers must forget the conversation too.
//   - A company message answers the customer when it is not older than the
//     first unanswered customer message; parent_id is not used.
//   - The customer message that brings a company to the threshold is itself
//     stored and opens a conversation.
//   - A late customer message whose source time is before the previous close
//     starts a new session.
//   - The admission window is trimmed relative to the source time of each
//     customer message, as decision 0008 says, also for a late message.
//   - A partition with no new records keeps its watermark, so its
//     conversations stay open until the stream continues (see sweepLoop).
//
// # Redis keys
//
// All keys start with DefaultKeyPrefix. Lifetimes are wall-clock safety nets
// (window + KeyMargin), renewed on every write; the source-time rules are in
// the scripts.
//
//	wm               hash   partition -> watermark (us)         no lifetime
//	adm              zset   company -> last customer msg (us)    no lifetime, pruned
//	cand:<company>   zset   customer -> time, at most N members  window + margin
//	c:<conv key>     hash   session record and counters          timeout + 24h + margin
//	m:<conv key>     zset   message ID -> source time            timeout + text kept + margin
//	x:<message ID>   hash   text, author, conversation, edit     timeout + text kept + margin
//	au:<author>      set    message IDs of the author            timeout + text kept + margin
//	cc:, ck:<author> set    open conversations as customer, as company
//	dl:<partition>   zset   conv key -> close deadline (us)      no lifetime
//	pg:<partition>   zset   "close time conv key" -> purge time  no lifetime
//	ob:<partition>   stream outbox of conversation events        no lifetime
//	ap:<event ID>    string applied marker                       AppliedLifetime
//	pd:<message ID>  string early deletion, source time          early kept + margin
//	pr:<author>      hash   early author removal                 early kept + margin
package conv

import (
	"errors"
	"fmt"
	"time"

	"github.com/ZAUakaAlexey/threadroute/internal/defaults"
)

// Values that decision 0007 fixes but internal/defaults does not hold.
const (
	// DefaultSessionRecordKept is how long the small record with the session
	// number and the sequence number outlives a close, in source time
	// (decision 0007, "Sessions").
	DefaultSessionRecordKept = 24 * time.Hour
	// DefaultEarlyDeletionKept is how long a deletion or an author removal
	// that arrived before its message is remembered (decision 0007, "Early
	// deletion").
	DefaultEarlyDeletionKept = 10 * time.Minute
	// DefaultKeyMargin is added to every source-time window to get the
	// wall-clock lifetime of the Redis key that holds it. conv can be
	// stopped for as long as the events topic keeps records (6 hours, plus
	// up to 1 hour until a segment is deleted) and then resume where it
	// stopped in source time. A key must survive that pause, so the margin
	// is 7 hours. The lifetime is only a safety net for cleaning up and
	// never the window itself (decision 0007, "Which clock the windows
	// use").
	DefaultKeyMargin = 7 * time.Hour
	// DefaultAppliedLifetime is how long the marker of an applied event ID
	// lives, in wall-clock time. 7 hours covers every copy of a record that
	// the events topic can still deliver. Decision 0003 asks services after
	// normalize to be idempotent on the event ID but gives no lifetime.
	//
	// Memory: at about 20 message events per second (decision 0006) plus
	// the control events, 7 hours is roughly 500,000 markers, at about 100
	// bytes each about 50 MB in Redis. A shorter value saves memory in
	// proportion, but a record delivered again after its marker expired (a
	// restart from old offsets, or a duplicate that passed normalize after a
	// long source outage) is applied a second time.
	DefaultAppliedLifetime = 7 * time.Hour
)

// Rules are the numbers of the conversation lifecycle and of admission.
// Every duration except AppliedLifetime and KeyMargin is in source time.
type Rules struct {
	// AdmissionThreshold is N: the number of different customers within
	// AdmissionWindow that admits a company (decision 0008).
	AdmissionThreshold int
	// AdmissionWindow is the sliding window of the count.
	AdmissionWindow time.Duration
	// AdmissionIdle ends an admission after this long without a customer
	// message.
	AdmissionIdle time.Duration
	// CompanyLimit is the most admitted companies, or 0 for no limit.
	CompanyLimit int
	// InactivityTimeout closes a session with no new message.
	InactivityTimeout time.Duration
	// TextKeptAfterClose is how long text stays after a close.
	TextKeptAfterClose time.Duration
	// MessagesKept is how many of the newest messages keep their text.
	MessagesKept int
	// SessionRecordKept is how long the session record outlives a close.
	SessionRecordKept time.Duration
	// EarlyDeletionKept is how long a control event that came before its
	// message is remembered.
	EarlyDeletionKept time.Duration
	// AppliedLifetime is the wall-clock lifetime of an event ID marker.
	AppliedLifetime time.Duration
	// KeyMargin is added to source windows to get wall-clock key lifetimes.
	KeyMargin time.Duration
}

// DefaultRules returns the values of decisions 0007 and 0008.
func DefaultRules() Rules {
	return Rules{
		AdmissionThreshold: defaults.AdmissionThreshold,
		AdmissionWindow:    defaults.AdmissionWindow,
		AdmissionIdle:      defaults.AdmissionIdle,
		CompanyLimit:       defaults.CompanyLimit,
		InactivityTimeout:  defaults.InactivityTimeout,
		TextKeptAfterClose: defaults.TextKeptAfterClose,
		MessagesKept:       defaults.MessagesKept,
		SessionRecordKept:  DefaultSessionRecordKept,
		EarlyDeletionKept:  DefaultEarlyDeletionKept,
		AppliedLifetime:    DefaultAppliedLifetime,
		KeyMargin:          DefaultKeyMargin,
	}
}

// Validate reports every invalid field.
func (r Rules) Validate() error {
	var errs []error
	positive := func(name string, d time.Duration) {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("%s %s is not positive", name, d))
		}
	}
	if r.AdmissionThreshold < 1 {
		errs = append(errs, fmt.Errorf("admission threshold %d is less than 1", r.AdmissionThreshold))
	}
	if r.CompanyLimit < 0 {
		errs = append(errs, fmt.Errorf("company limit %d is negative", r.CompanyLimit))
	}
	if r.MessagesKept < 1 {
		errs = append(errs, fmt.Errorf("messages kept %d is less than 1", r.MessagesKept))
	}
	positive("admission window", r.AdmissionWindow)
	positive("admission idle period", r.AdmissionIdle)
	positive("inactivity timeout", r.InactivityTimeout)
	positive("text kept after close", r.TextKeptAfterClose)
	positive("session record kept", r.SessionRecordKept)
	positive("early deletion kept", r.EarlyDeletionKept)
	// These two are Redis lifetimes in whole milliseconds.
	for _, w := range []struct {
		name string
		d    time.Duration
	}{{"applied lifetime", r.AppliedLifetime}, {"key margin", r.KeyMargin}} {
		if w.d < time.Millisecond {
			errs = append(errs, fmt.Errorf("%s %s is shorter than 1ms", w.name, w.d))
		}
	}
	return errors.Join(errs...)
}

// args returns the shared script arguments in the order of lua/prelude.lua.
// Durations in source time are microseconds, wall-clock lifetimes are
// milliseconds.
func (r Rules) args(prefix string) []any {
	return []any{
		prefix,
		r.AdmissionThreshold,
		r.AdmissionWindow.Microseconds(),
		r.AdmissionIdle.Microseconds(),
		r.CompanyLimit,
		r.InactivityTimeout.Microseconds(),
		r.TextKeptAfterClose.Microseconds(),
		r.MessagesKept,
		r.SessionRecordKept.Microseconds(),
		r.EarlyDeletionKept.Microseconds(),
		r.AppliedLifetime.Milliseconds(),
		r.KeyMargin.Milliseconds(),
	}
}
