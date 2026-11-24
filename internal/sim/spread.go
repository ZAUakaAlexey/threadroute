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

// maxSpreadTotal bounds the sum of the counts of a spread, so the integer
// arithmetic of langOrder cannot overflow.
const maxSpreadTotal = 1_000_000

// Validate checks that operators operators can have every language of the
// spread.
func (s Spread) Validate(operators int) error {
	if len(s) == 0 {
		return errors.New("no languages")
	}
	if len(s) > operators {
		return fmt.Errorf("%d languages need at least %d operators, have %d", len(s), len(s), operators)
	}
	if t := s.total(); t > maxSpreadTotal {
		return fmt.Errorf("the counts add up to %d, more than %d", t, maxSpreadTotal)
	}
	return nil
}

// langOrder returns the language of each operator number from 1 to n: the
// language of number i is element i-1. The language of a number depends
// only on the spread and the number, never on n: langOrder(m) is the start
// of langOrder(n) for m < n. So an operator keeps its language through any
// change of the count and any restart (decision 0014, point 3A).
//
// Each count is a weight w, and W is the sum of the weights. For the
// operators 1 to n, the exact share of a language is n*w/W. The numbers
// are given out one at a time:
//
//   - The first numbers go to the languages that have no operator yet,
//     the largest weight first, so every language has an operator once n
//     is at least the number of languages.
//   - After that each number goes to a language whose new count is at most
//     1 - u above its exact share, where u is 1/(2k-2) for k languages. Among those it goes to the language whose count would fall
//     furthest below its share first (the earliest deadline, Tijdeman's
//     rule for the chairman assignment problem). Such a language always
//     exists, because the shares minus the counts add up to 1 before each
//     number, so the largest of them is at least 1/k, which is at least u.
//   - Ties go to the earlier language of the spread.
//
// What this gives for every n and every language (TestLangOrder checks it):
//
//   - The count is less than its share plus 1. So when n equals W, every
//     share is a whole number, no count is above it, and the counts are
//     exactly the counts of the spread.
//   - The count is more than its share minus 1 minus F, where F is the sum
//     over all languages of max(0, 1 - share): the operators that the
//     rule "every language has one" gives to languages whose share is below
//     1. F is 0 once every share is at least 1 (for the default spread from
//     67 operators on). Without that rule the bound is Tijdeman's 1 - u.
//
// The arithmetic is in integers, so the result does not depend on float
// rounding. Validate bounds W, so it cannot overflow.
func (s Spread) langOrder(n int) []string {
	out := make([]string, 0, max(n, 0))
	k := len(s)
	if k == 0 {
		return out
	}
	w := s.total()
	// The values below are multiplied by 2k-2, so u = 1/(2k-2) is a whole
	// number: 1.
	m := 2*k - 2
	counts := make([]int, k)
	missing := k
	for num := 1; num <= n; num++ {
		best := -1
		for j, l := range s {
			switch {
			case missing > 0:
				if counts[j] > 0 {
					continue
				}
				// The largest weight first.
				if best < 0 || l.Count > s[best].Count {
					best = j
				}
				continue
			case k == 1:
				best = 0
			case (num*l.Count-counts[j]*w)*m < w:
				// The new count would be 1 - u or more above the share:
				// num*w_j/W - c_j < u.
				continue
			case best < 0 || deadlineBefore(counts[j], l.Count, counts[best], s[best].Count, m):
				best = j
			}
		}
		if best < 0 {
			// Not reached, see above. The largest deficit is a safe choice.
			best = largestDeficit(s, counts, num, w)
		}
		if counts[best] == 0 {
			missing--
		}
		counts[best]++
		out = append(out, s[best].Lang)
	}
	return out
}

// deadlineBefore reports whether a language with count ca and weight wa
// falls 1 - u below its share before one with count cb and weight wb. The
// time of that is (c + 1 - u) / w in units of W, and u = 1/m.
func deadlineBefore(ca, wa, cb, wb, m int) bool {
	return (ca*m+m-1)*wb < (cb*m+m-1)*wa
}

// largestDeficit returns the language whose count is furthest below its
// share for num operators.
func largestDeficit(s Spread, counts []int, num, w int) int {
	best := 0
	for j, l := range s {
		if num*l.Count-counts[j]*w > num*s[best].Count-counts[best]*w {
			best = j
		}
	}
	return best
}

// layout returns the count of each language among the operators 1 to n of
// langOrder, in the order of the spread.
func (s Spread) layout(n int) Spread {
	at := make(map[string]int, len(s))
	out := slices.Clone(s)
	for i, l := range out {
		at[l.Lang] = i
		out[i].Count = 0
	}
	for _, lang := range s.langOrder(n) {
		out[at[lang]].Count++
	}
	return out
}

// operatorID returns the ID of the operator with the 1-based index i. IDs
// are synthetic and stable across restarts.
func operatorID(i int) string {
	return fmt.Sprintf("sim-op-%04d", i)
}

// MaxOperators is the largest operator count. IDs have four digits.
const MaxOperators = 9999

// operatorLangs returns the languages an operator is sent with: its
// language of the spread and event.LangUndetermined.
func operatorLangs(lang string) []string {
	return []string{lang, event.LangUndetermined}
}
