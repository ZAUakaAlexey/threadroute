package sim

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/ZAUakaAlexey/threadroute/internal/event"
)

// LangShare is one entry of a language spread: a language and its count.
type LangShare struct {
	Lang  string
	Count int
}

// Spread is the language spread of the operators, in the order it was
// written. Each operator gets one language of the spread, plus
// event.LangUndetermined for the general queue.
type Spread []LangShare

// ParseSpread reads a spread such as "en:176,ja:6,de:4". Each entry is a
// language (2 or 3 lower case letters, not "und", which every operator has
// anyway) and a positive count. A language may appear once.
func ParseSpread(s string) (Spread, error) {
	var out Spread
	var errs []error
	seen := map[string]bool{}
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		lang, count, ok := strings.Cut(part, ":")
		lang, count = strings.TrimSpace(lang), strings.TrimSpace(count)
		if !ok {
			errs = append(errs, fmt.Errorf("%q is not lang:count", part))
			continue
		}
		n, err := strconv.Atoi(count)
		switch {
		case !event.ValidLang(lang):
			errs = append(errs, fmt.Errorf("%q is not 2 or 3 lower case letters", lang))
		case lang == event.LangUndetermined:
			errs = append(errs, fmt.Errorf("%q is not allowed: every operator serves the general queue already", lang))
		case seen[lang]:
			errs = append(errs, fmt.Errorf("%q is listed twice", lang))
		case err != nil || n < 1:
			errs = append(errs, fmt.Errorf("%s: count %q is not a positive whole number", lang, count))
		default:
			out = append(out, LangShare{Lang: lang, Count: n})
		}
		seen[lang] = true
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, errors.New("no languages")
	}
	return out, nil
}

// String returns the spread in the form ParseSpread reads.
func (s Spread) String() string {
	parts := make([]string, len(s))
	for i, l := range s {
		parts[i] = l.Lang + ":" + strconv.Itoa(l.Count)
	}
	return strings.Join(parts, ",")
}

// total returns the sum of the counts.
func (s Spread) total() int {
	n := 0
	for _, l := range s {
		n += l.Count
	}
	return n
}

// Validate checks that the spread can be scaled to operators operators.
func (s Spread) Validate(operators int) error {
	if len(s) == 0 {
		return errors.New("no languages")
	}
	if len(s) > operators {
		return fmt.Errorf("%d languages need at least %d operators, have %d", len(s), len(s), operators)
	}
	return nil
}

// Scale returns the spread scaled to the operator count. This is the one
// rule for counts that do not add up to the operator count:
//
//   - When the counts add up to operators, they are used as they are.
//   - Otherwise each count is treated as a weight. A language gets its
//     share of operators rounded down, and the operators left over go to
//     the languages with the largest remainders (ties: the order of the
//     spread).
//   - Every language keeps at least one operator. A language that got
//     none takes one from the language that has the most (ties: the
//     earlier one). So the operator count must be at least the number of
//     languages, which Validate checks.
//
// The result has the same languages in the same order, and its counts add
// up to operators.
func (s Spread) Scale(operators int) (Spread, error) {
	if err := s.Validate(operators); err != nil {
		return nil, err
	}
	total := s.total()
	out := slices.Clone(s)
	if total == operators {
		return out, nil
	}
	type rest struct {
		i   int
		rem int
	}
	rests := make([]rest, len(s))
	given := 0
	for i, l := range s {
		// Integer arithmetic, so the result does not depend on float
		// rounding.
		out[i].Count = l.Count * operators / total
		rests[i] = rest{i: i, rem: l.Count * operators % total}
		given += out[i].Count
	}
	slices.SortStableFunc(rests, func(a, b rest) int { return b.rem - a.rem })
	for k := 0; given < operators; k++ {
		out[rests[k].i].Count++
		given++
	}
	for i := range out {
		if out[i].Count > 0 {
			continue
		}
		most := 0
		for j := range out {
			if out[j].Count > out[most].Count {
				most = j
			}
		}
		out[most].Count--
		out[i].Count++
	}
	return out, nil
}

// operatorID returns the ID of the operator with the 1-based index i. IDs
// are synthetic and stable across restarts.
func operatorID(i int) string {
	return fmt.Sprintf("sim-op-%04d", i)
}

// MaxOperators is the largest operator count. IDs have four digits.
const MaxOperators = 9999

// operatorLangs returns the languages of each operator, in index order:
// the languages of a scaled spread in blocks, in spread order, each with
// event.LangUndetermined added.
func operatorLangs(scaled Spread) [][]string {
	var out [][]string
	for _, l := range scaled {
		for range l.Count {
			out = append(out, []string{l.Lang, event.LangUndetermined})
		}
	}
	return out
}
