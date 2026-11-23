package normalize

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZAUakaAlexey/threadroute/internal/bus"
	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

// Kind is what Map made of one raw record. The values are the "kind" label of
// the emitted_total metric.
type Kind string

// Output kinds (decision 0006).
const (
	// KindDropped means the record gives no output. Result.DropReason says
	// why.
	KindDropped Kind = ""
	// KindCustomerMessage is a reply by someone other than the thread
	// author. It goes to bus.TopicEvents.
	KindCustomerMessage Kind = "customer_message"
	// KindCompanyMessage is a reply by the thread author to another person.
	// It goes to bus.TopicEvents.
	KindCompanyMessage Kind = "company_message"
	// KindDeletion is a deleted post. It goes to bus.TopicControl.
	KindDeletion Kind = "deletion"
	// KindAuthorRemoval is an account that is not active, or a sync event.
	// It goes to bus.TopicControl.
	KindAuthorRemoval Kind = "author_removal"
)

// emitKinds lists every output kind, so the metric starts at zero for each.
var emitKinds = []Kind{KindCustomerMessage, KindCompanyMessage, KindDeletion, KindAuthorRemoval}

// Drop reasons. They are the values of the "reason" label of dropped_total.
const (
	// DropNotReply is a post that is not a reply (decision 0006).
	DropNotReply = "not_reply"
	// DropSelfReply is a reply by the thread author to their own post: the
	// author continues their own thread (decision 0006).
	DropSelfReply = "self_reply"
	// DropAccountActive is an account event for an active account. Nothing
	// has to be removed.
	DropAccountActive = "account_active"
	// DropOtherCollection is a commit for a collection other than posts.
	DropOtherCollection = "other_collection"
	// DropUnknownKind is an event kind normalize does not handle, for
	// example "identity".
	DropUnknownKind = "unknown_kind"
	// DropDecodeFailed is a record that is not valid JSON or misses a
	// required field of its kind.
	DropDecodeFailed = "decode_failed"
	// DropBadReplyRef is a reply whose root or parent URI is missing or
	// malformed, so the company and customer IDs are unknown.
	DropBadReplyRef = "bad_reply_ref"
	// DropMissingEventID is a record without the event ID header. See Map.
	DropMissingEventID = "missing_event_id"
	// DropMissingTime is a record with no witnessed time in the header and
	// neither witnessed_at_us nor time_us in the payload. Every event after
	// normalize needs a source time.
	DropMissingTime = "missing_time"
	// DropInvalidEvent is a record whose normalised event does not pass
	// validation, for example a reply whose parent is the post itself.
	DropInvalidEvent = "invalid_event"
)

// dropReasons lists every drop reason, so the metric starts at zero for each.
var dropReasons = []string{
	DropNotReply, DropSelfReply, DropAccountActive, DropOtherCollection, DropUnknownKind,
	DropDecodeFailed, DropBadReplyRef, DropMissingEventID, DropMissingTime, DropInvalidEvent,
}

// collectionPost is the only collection that gives messages and deletions.
const collectionPost = "app.bsky.feed.post"

// Source event kinds and commit operations, as in the Jetstream wire format.
const (
	kindCommit  = "commit"
	kindAccount = "account"
	kindSync    = "sync"

	opCreate = "create"
	opUpdate = "update"
	opDelete = "delete"
)

// Result is the outcome of Map for one raw record: one record to produce, or
// a drop with a reason. One source event gives at most one normalised event
// (package event), so there is never more than one output.
type Result struct {
	// Kind is the output kind, or KindDropped.
	Kind Kind
	// DropReason is set when Kind is KindDropped.
	DropReason string
	// EventID is the event ID of the source event. The output keeps it. It
	// is empty only for DropMissingEventID.
	EventID string
	// SourceTime is the source time of the event, see Map for the order of
	// the fields it comes from. It is zero when it is unknown.
	SourceTime time.Time
	// DisplayTime is true when SourceTime came from time_us, the last
	// fallback, because no witnessed time was present.
	DisplayTime bool
	// Topic, Key and Value are the record to produce. They are set when
	// Kind is not KindDropped.
	Topic string
	Key   []byte
	Value []byte
}

// Dropped reports whether the record gives no output.
func (r Result) Dropped() bool { return r.Kind == KindDropped }

// OutputRecord returns the record to produce, with the event ID header
// (bus.HeaderEventID), or nil for a drop.
func (r Result) OutputRecord() *kgo.Record {
	if r.Dropped() {
		return nil
	}
	return &kgo.Record{
		Topic:   r.Topic,
		Key:     r.Key,
		Value:   r.Value,
		Headers: []kgo.RecordHeader{{Key: bus.HeaderEventID, Value: []byte(r.EventID)}},
	}
}

// Wire form of a raw record value: the JSON of the SDK event without the
// CBOR copy (decision 0001). Only the fields normalize needs are decoded.
// The struct tags mirror the SDK, which this package does not import
// (decision 0002).
type rawEvent struct {
	DID           string      `json:"did"`
	WitnessedAtUS int64       `json:"witnessed_at_us"`
	TimeUS        int64       `json:"time_us"`
	Kind          string      `json:"kind"`
	Commit        *rawCommit  `json:"commit"`
	Account       *rawAccount `json:"account"`
	Sync          *struct{}   `json:"sync"`
}

type rawCommit struct {
	Operation  string   `json:"operation"`
	Collection string   `json:"collection"`
	Rkey       string   `json:"rkey"`
	Record     *rawPost `json:"record"`
}

// rawPost is an app.bsky.feed.post record.
type rawPost struct {
	Text  string    `json:"text"`
	Langs []string  `json:"langs"`
	Reply *rawReply `json:"reply"`
}

type rawReply struct {
	Root   *rawRef `json:"root"`
	Parent *rawRef `json:"parent"`
}

type rawRef struct {
	URI string `json:"uri"`
}

type rawAccount struct {
	Active bool   `json:"active"`
	Status string `json:"status"`
}

// Map turns one raw record into its output, as the table in decision 0006
// says. It is a pure function: it does not keep or change rec.
//
// Map never fails. A record it cannot use is a drop with a reason, because
// an error would stop the service on the same record again and again.
//
// The event ID comes from the bus.HeaderEventID header and is kept by the
// output. A record without it is dropped with DropMissingEventID: every
// service after normalize needs the ID, and without it the record cannot be
// deduplicated either. Ingest always sets the header, so such a record
// comes from another writer and is counted and logged by the caller.
//
// The source time is taken from the first of these that is a positive
// number: the bus.HeaderEventWitnessed header, the witnessed_at_us field of
// the payload, then the time_us field of the payload. time_us is the display
// time of the SDK, usually the witnessed time but possibly an imported one,
// so its use is marked in Result.DisplayTime and counted. A record with none
// of the three is dropped with DropMissingTime.
//
// A commit with operation "update" (an edited post) is handled like
// "create": the edited post is classified by its content and gives a new
// message event with a new event ID and the same message ID. Decision 0006
// does not name edits. conv treats a known message ID that arrives with a
// new event ID as an edit.
func Map(rec *kgo.Record) Result {
	eventID := headerValue(rec.Headers, bus.HeaderEventID)
	if eventID == "" {
		return drop(DropMissingEventID, "", time.Time{})
	}
	var ev rawEvent
	if err := json.Unmarshal(rec.Value, &ev); err != nil || ev.DID == "" {
		return drop(DropDecodeFailed, eventID, time.Time{})
	}
	at, display := sourceTime(rec.Headers, ev.WitnessedAtUS, ev.TimeUS)
	res := mapEvent(&ev, eventID, at)
	res.DisplayTime = display && !res.SourceTime.IsZero()
	return res
}

// mapEvent maps a decoded event with its event ID and source time.
func mapEvent(ev *rawEvent, eventID string, at time.Time) Result {

	switch ev.Kind {
	case kindCommit:
		return mapCommit(ev, eventID, at)
	case kindAccount:
		if ev.Account == nil {
			return drop(DropDecodeFailed, eventID, at)
		}
		if ev.Account.Active {
			return drop(DropAccountActive, eventID, at)
		}
		return control(event.AuthorRemoval{
			EventID:    eventID,
			Channel:    event.ChannelBluesky,
			SourceTime: at,
			AuthorID:   ev.DID,
			Reason:     event.RemovalInactive,
			Status:     ev.Account.Status,
		}, KindAuthorRemoval, eventID, at)
	case kindSync:
		if ev.Sync == nil {
			return drop(DropDecodeFailed, eventID, at)
		}
		return control(event.AuthorRemoval{
			EventID:    eventID,
			Channel:    event.ChannelBluesky,
			SourceTime: at,
			AuthorID:   ev.DID,
			Reason:     event.RemovalSync,
		}, KindAuthorRemoval, eventID, at)
	case "":
		return drop(DropDecodeFailed, eventID, at)
	default:
		return drop(DropUnknownKind, eventID, at)
	}
}

func mapCommit(ev *rawEvent, eventID string, at time.Time) Result {
	c := ev.Commit
	if c == nil || c.Collection == "" || c.Rkey == "" {
		return drop(DropDecodeFailed, eventID, at)
	}
	// The collection is checked before the operation, so a deletion in
	// another collection is a drop too.
	if c.Collection != collectionPost {
		return drop(DropOtherCollection, eventID, at)
	}
	messageID := recordURI(ev.DID, c.Collection, c.Rkey)

	switch c.Operation {
	case opDelete:
		return control(event.Deletion{
			EventID:    eventID,
			Channel:    event.ChannelBluesky,
			SourceTime: at,
			MessageID:  messageID,
			AuthorID:   ev.DID,
		}, KindDeletion, eventID, at)
	case opCreate, opUpdate:
	default:
		return drop(DropDecodeFailed, eventID, at)
	}

	post := c.Record
	if post == nil {
		return drop(DropDecodeFailed, eventID, at)
	}
	if post.Reply == nil {
		return drop(DropNotReply, eventID, at)
	}
	if post.Reply.Root == nil || post.Reply.Parent == nil {
		return drop(DropBadReplyRef, eventID, at)
	}
	rootAuthor, ok := uriAuthor(post.Reply.Root.URI)
	if !ok {
		return drop(DropBadReplyRef, eventID, at)
	}
	parentAuthor, ok := uriAuthor(post.Reply.Parent.URI)
	if !ok {
		return drop(DropBadReplyRef, eventID, at)
	}

	m := event.Message{
		EventID:    eventID,
		Channel:    event.ChannelBluesky,
		SourceTime: at,
		MessageID:  messageID,
		ParentID:   post.Reply.Parent.URI,
		Text:       post.Text,
		Langs:      post.Langs,
		Lang:       event.NormalizeLang(post.Langs),
	}
	var kind Kind
	switch {
	case ev.DID != rootAuthor:
		// A reply by someone other than the thread author.
		kind = KindCustomerMessage
		m.Direction = event.FromCustomer
		m.CompanyID, m.CustomerID = rootAuthor, ev.DID
	case parentAuthor != rootAuthor:
		// The thread author answers another person.
		kind = KindCompanyMessage
		m.Direction = event.FromCompany
		m.CompanyID, m.CustomerID = rootAuthor, parentAuthor
	default:
		return drop(DropSelfReply, eventID, at)
	}
	if at.IsZero() {
		return drop(DropMissingTime, eventID, at)
	}
	value, err := event.MarshalMessage(m)
	if err != nil {
		return drop(DropInvalidEvent, eventID, at)
	}
	return Result{
		Kind:       kind,
		EventID:    eventID,
		SourceTime: at,
		Topic:      bus.TopicEvents,
		Key:        event.ConversationKey(m.CompanyID, m.CustomerID),
		Value:      value,
	}
}

// control builds the result for a control event.
func control(c event.Control, kind Kind, eventID string, at time.Time) Result {
	if at.IsZero() {
		return drop(DropMissingTime, eventID, at)
	}
	value, err := event.MarshalControl(c)
	if err != nil {
		return drop(DropInvalidEvent, eventID, at)
	}
	return Result{
		Kind:       kind,
		EventID:    eventID,
		SourceTime: at,
		Topic:      bus.TopicControl,
		Key:        event.AuthorKey(c.Author()),
		Value:      value,
	}
}

func drop(reason, eventID string, at time.Time) Result {
	return Result{Kind: KindDropped, DropReason: reason, EventID: eventID, SourceTime: at}
}

// headerValue returns the value of the first header with the given key.
func headerValue(headers []kgo.RecordHeader, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// sourceTime returns the source time in the order documented on Map, and
// whether it came from time_us. It returns the zero time when none of the
// three is a positive number.
func sourceTime(headers []kgo.RecordHeader, witnessedUS, displayUS int64) (time.Time, bool) {
	if v := headerValue(headers, bus.HeaderEventWitnessed); v != "" {
		if us, err := strconv.ParseInt(v, 10, 64); err == nil && us > 0 {
			return time.UnixMicro(us).UTC(), false
		}
	}
	if witnessedUS > 0 {
		return time.UnixMicro(witnessedUS).UTC(), false
	}
	if displayUS > 0 {
		return time.UnixMicro(displayUS).UTC(), true
	}
	return time.Time{}, false
}

// recordURI returns the at:// URI of a record. For a post it is the message
// ID, and a deletion names the same string.
func recordURI(did, collection, rkey string) string {
	return "at://" + did + "/" + collection + "/" + rkey
}

// uriAuthor returns the author DID of a record URI of the form
// at://<did>/<collection>/<rkey>. It reports false when the URI does not
// have this form or its first path part is not a DID. A handle in place of
// the DID is not accepted, because author IDs are DIDs everywhere else.
func uriAuthor(uri string) (string, bool) {
	rest, ok := strings.CutPrefix(uri, "at://")
	if !ok {
		return "", false
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 3 || parts[1] == "" || parts[2] == "" {
		return "", false
	}
	did := parts[0]
	if !strings.HasPrefix(did, "did:") || len(did) <= len("did:") {
		return "", false
	}
	return did, true
}
