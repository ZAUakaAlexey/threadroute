package event

import "strings"

// LangUndetermined is the normalised language when a message has no usable
// language tag. It is the BCP 47 code for "undetermined".
const LangUndetermined = "und"

// NormalizeLang returns the one normalised language of a message (decision
// 0006): the primary subtag of the first tag, in lower case, or
// LangUndetermined when there is no first tag or it is not valid.
//
// Only the first tag is looked at. If it is not valid, the result is
// LangUndetermined even when a later tag is valid, because the record names
// the first tag.
//
// The rule for "valid" is kept simple on purpose. Spaces around the tag are
// removed. The primary subtag is the part before the first "-" or "_" ("_"
// is not BCP 47, but some clients send "en_US"). It is valid when it has 2
// or 3 ASCII letters, the length of ISO 639 codes. The rest of the tag is
// not checked. So "en-US", "EN" and "zh-Hant" give "en", "en" and "zh", and
// "english", "x-private", "i-klingon", "12" and "" give "und".
func NormalizeLang(tags []string) string {
	if len(tags) == 0 {
		return LangUndetermined
	}
	tag := strings.TrimSpace(tags[0])
	primary, _, _ := strings.Cut(tag, "-")
	primary, _, _ = strings.Cut(primary, "_")
	if !isLangCode(primary) {
		return LangUndetermined
	}
	return strings.ToLower(primary)
}

// isLangCode reports whether s has 2 or 3 ASCII letters in any case.
func isLangCode(s string) bool {
	if len(s) < 2 || len(s) > 3 {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
			return false
		}
	}
	return true
}

// validLang reports whether s is a value NormalizeLang can return: 2 or 3
// lower case ASCII letters. LangUndetermined is one of them.
func validLang(s string) bool {
	return isLangCode(s) && s == strings.ToLower(s)
}
