package source

import (
	"strconv"
	"strings"
	"time"
)

// Event ID format (decision 0003):
//
//	commit:  c:<did>:<collection>:<rkey>:<rev>:<operation>
//	account: a:<did>:<active>:<status>:<upstream time>
//	sync:    s:<did>:<rev>:<upstream time>
//	sync, when rev and upstream time are both empty:
//	         s:<did>:w<witnessed unix microseconds>
//
// For account events, active is "1" or "0", status is the status string
// (empty when active), and upstream time is the time string of the account
// payload exactly as the source sent it. The seq inside the account payload
// is not used: a live check showed it differs between the two hosts.
//
// For sync events, upstream time is the time string inside the sync payload.
// The fallback with the witnessed time is host-specific, so such an event can
// be delivered twice after a host change. This is harmless, because handling
// a sync event is idempotent.
//
// The separator is ":". A DID always contains ":" and a record key may contain
// it too, so the format needs a rule to stay unambiguous:
//
//   - The DID is written as it is. It is always the first field after the
//     kind prefix.
//   - In every other text field, "%" is written as "%25" and ":" as "%3A".
//
// After this escaping only the DID can contain a raw ":". Splitting an ID from
// the right therefore gives back every field, so two different events never
// get the same ID. For normal values (a record key without ":" or "%") the ID
// is exactly the documented format.
//
// The ID is built from event content only, never from the host or seq, so the
// same event gets the same ID on every host. The only exception is the sync
// fallback above.

const (
	prefixCommit  = "c"
	prefixAccount = "a"
	prefixSync    = "s"
	idSeparator   = ":"
)

var idEscaper = strings.NewReplacer("%", "%25", ":", "%3A")

// CommitEventID returns the event ID of a commit event.
func CommitEventID(did, collection, rkey, rev, operation string) string {
	return joinID(prefixCommit, did, collection, rkey, rev, operation)
}

// AccountEventID returns the event ID of an account event. status is empty
// when the account is active. upstreamTime is the time field of the account
// payload (Account.Time in the SDK), exactly as the source sent it.
func AccountEventID(did string, active bool, status, upstreamTime string) string {
	activeField := "0"
	if active {
		activeField = "1"
	}
	return joinID(prefixAccount, did, activeField, status, upstreamTime)
}

// SyncEventID returns the event ID of a sync event. upstreamTime is the time
// field of the sync payload (Sync.Time in the SDK). When rev and upstreamTime
// are both empty, the ID falls back to the witnessed time, which is
// host-specific (see the format comment above). A zero witnessed time is
// written as 0.
func SyncEventID(did, rev, upstreamTime string, witnessed time.Time) string {
	if rev == "" && upstreamTime == "" {
		var us int64
		if !witnessed.IsZero() {
			us = witnessed.UnixMicro()
		}
		return joinID(prefixSync, did, "w"+strconv.FormatInt(us, 10))
	}
	return joinID(prefixSync, did, rev, upstreamTime)
}

// joinID writes prefix, the DID as it is, and the escaped rest.
func joinID(prefix, did string, rest ...string) string {
	var b strings.Builder
	b.WriteString(prefix)
	b.WriteString(idSeparator)
	b.WriteString(did)
	for _, field := range rest {
		b.WriteString(idSeparator)
		b.WriteString(idEscaper.Replace(field))
	}
	return b.String()
}
