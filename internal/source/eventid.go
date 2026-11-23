package source

import (
	"strconv"
	"strings"
)

// Event ID format (decision 0003):
//
//	commit:  c:<did>:<collection>:<rkey>:<rev>:<operation>
//	account: a:<did>:<account seq>
//	sync:    s:<did>:<rev>
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
// same event gets the same ID on every host.

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

// AccountEventID returns the event ID of an account event. accountSeq is the
// seq field inside the account payload (Account.Seq in the SDK), not the
// source host's event seq.
func AccountEventID(did string, accountSeq int64) string {
	return joinID(prefixAccount, did, strconv.FormatInt(accountSeq, 10))
}

// SyncEventID returns the event ID of a sync event.
func SyncEventID(did, rev string) string {
	return joinID(prefixSync, did, rev)
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
