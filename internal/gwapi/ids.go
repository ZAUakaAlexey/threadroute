package gwapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base32"
	"slices"
	"strings"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

// Pseudonyms (decision 0012, "Pseudonyms"). At LevelDemo every conversation
// key, company ID, customer ID and message ID is replaced by a pseudonym:
// PseudonymPrefix and PseudonymLen characters of the lower case base32
// alphabet (a to z and 2 to 7), made from a keyed hash of the ID. The key
// changes every day and never leaves the gateway, so a pseudonym is stable
// for a day and cannot be turned back into the ID. The same ID gives the
// same pseudonym everywhere on that day, so a company in a team row and in
// a conversation row has the same label.
const (
	PseudonymPrefix = "ps-"
	PseudonymLen    = 12
)

var pseudonymEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// Pseudonym returns the pseudonym of id under the key of the day.
func Pseudonym(dayKey []byte, id string) string {
	mac := hmac.New(sha256.New, dayKey)
	mac.Write([]byte(id))
	sum := pseudonymEncoding.EncodeToString(mac.Sum(nil))
	return PseudonymPrefix + strings.ToLower(sum[:PseudonymLen])
}

// ValidPseudonym reports whether s has the form Pseudonym returns.
func ValidPseudonym(s string) bool {
	rest, ok := strings.CutPrefix(s, PseudonymPrefix)
	if !ok || len(rest) != PseudonymLen {
		return false
	}
	for i := range len(rest) {
		c := rest[i]
		if (c < 'a' || c > 'z') && (c < '2' || c > '7') {
			return false
		}
	}
	return true
}

// MaxIDLen is the longest conversation ID, company or customer label, or
// message ID at LevelFull. IDs are opaque strings for the client.
const MaxIDLen = 512

// checkLabel checks a company, customer or message label: a non-empty ID
// of at most MaxIDLen bytes at LevelFull, and a pseudonym at every other
// level, so an unknown level fails closed.
func checkLabel(errs *fieldErrors, level Level, field, s string) {
	switch {
	case s == "":
		errs.missing(field)
	case level != LevelFull && !ValidPseudonym(s):
		errs.invalid(field, "is not a pseudonym at level %q", level)
	case len(s) > MaxIDLen:
		errs.invalid(field, "is longer than %d bytes", MaxIDLen)
	}
}

// checkConversationID checks the ID of a conversation and its two labels.
// At LevelFull the ID is the conversation key (event.ConversationKey) of
// the company and the customer. At LevelDemo all three are pseudonyms.
func checkConversationID(errs *fieldErrors, level Level, id, company, customer string) {
	checkLabel(errs, level, "id", id)
	checkLabel(errs, level, "company", company)
	checkLabel(errs, level, "customer", customer)
	if level != LevelFull || id == "" {
		return
	}
	co, cu, err := event.ParseConversationKey([]byte(id))
	switch {
	case err != nil:
		errs.invalid("id", "is not a conversation key")
	case co != company || cu != customer:
		errs.invalid("id", "does not match company and customer")
	}
}

// checkConversationRef checks a conversation ID without its labels, as in
// Watch and Messages.
func checkConversationRef(errs *fieldErrors, level Level, field, id string) {
	checkLabel(errs, level, field, id)
	if level == LevelFull && id != "" {
		if _, _, err := event.ParseConversationKey([]byte(id)); err != nil {
			errs.invalid(field, "is not a conversation key")
		}
	}
}

// checkOperatorID checks an operator ID. Operator IDs are synthetic and are
// shown at every level.
func checkOperatorID(errs *fieldErrors, field, s string) {
	switch {
	case s == "":
		errs.missing(field)
	case !event.ValidRoutingID(s):
		errs.invalid(field, "%q is not a valid operator ID", s)
	}
}

// checkLanguages checks a language list: 1 to MaxLanguages values that
// event.NormalizeLang can return, without duplicates. Lists from the server
// are also sorted; a client may send them in any order.
func checkLanguages(errs *fieldErrors, field string, langs []string, sorted bool) {
	switch {
	case len(langs) == 0:
		errs.missing(field)
	case len(langs) > MaxLanguages:
		errs.invalid(field, "%d are more than %d", len(langs), MaxLanguages)
	}
	for i, l := range langs {
		switch {
		case !event.ValidLang(l):
			errs.invalid(field, "%q is not 2 or 3 lower case letters", l)
		case sorted && i > 0 && l <= langs[i-1]:
			errs.invalid(field, "are not sorted without duplicates at %q", l)
		case !sorted && slices.Contains(langs[:i], l):
			errs.invalid(field, "%q is listed twice", l)
		}
	}
}
