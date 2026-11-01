package parser

import (
	"regexp"
	"regexp/syntax"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// hintNgram is the n-gram size of the norm_log text index; shorter values
	// have no token for it to look up.
	hintNgram = 3
	// maxHintValues caps the values hinted for one filter, and the index lookups
	// its hint makes: a long list would cost more than the blocks it skips.
	maxHintValues = 16
	// maxHintRuns caps the runs hinted for one value or regex. Dropping a run
	// only weakens a hint; the longest runs are kept as the most selective.
	maxHintRuns = 3
)

// normLogHint returns an indexHint on the norm_log n-gram index for a positive
// filter on a stored JSON field, or on norm_log itself when onNormLog is set, or
// "" when the filter does not qualify. indexHint is true for every row, so the
// rows that match cannot change; it only lets ClickHouse skip blocks whose index
// proves every value absent.
//
// A field filter is hinted by the runs norm_log must contain wherever it matches
// (see hintRuns), every run required.
func normLogHint(cond HavingCondition, mode SourceMode, onNormLog bool) string {
	if cond.ValueField != "" {
		return ""
	}
	if cond.IsRegex {
		if onNormLog || cond.Negate || cond.Operator == "!=" {
			return ""
		}
		return runsHint(longestRuns(regexHintRuns(cond.Value), maxHintRuns), "=~", mode)
	}
	switch cond.Operator {
	case "=":
		// norm_log="x" compares the raw literal, not its JSON-escaped form.
		if onNormLog {
			return ""
		}
	case "=~", "=^", "=$":
		if cond.Negate {
			return ""
		}
	default:
		return ""
	}
	values := conditionValues(cond)
	if len(values) == 0 || len(values) > maxHintValues {
		return ""
	}
	// Every value is an OR branch and its runs an AND, so share the lookup budget.
	perValue := min(maxHintRuns, max(1, maxHintValues/len(values)))
	hints := make([]string, len(values))
	for i, v := range values {
		if onNormLog {
			if match := normLogValueMatch(v, cond.Operator, mode); match != "" {
				hints[i] = "indexHint(" + match + ")"
			}
		} else {
			hints[i] = runsHint(longestRuns(hintRuns(v, false), perValue), cond.Operator, mode)
		}
		// A value with no hint may match in any block, so no block can be skipped.
		if hints[i] == "" {
			return ""
		}
	}
	if len(hints) == 1 {
		return hints[0]
	}
	return "(" + strings.Join(hints, " OR ") + ")"
}

// runsHint ANDs an indexHint for each run, or returns "" when none can be hinted.
func runsHint(runs []string, op string, mode SourceMode) string {
	var hints []string
	for _, r := range runs {
		if match := normLogValueMatch(r, op, mode); match != "" {
			hints = append(hints, "indexHint("+match+")")
		}
	}
	switch len(hints) {
	case 0:
		return ""
	case 1:
		return hints[0]
	}
	return "(" + strings.Join(hints, " AND ") + ")"
}

// hintRuns returns the stretches of s made only of hintRune characters, which
// any rendering of a matching value carries unchanged into norm_log.
func hintRuns(s string, fold bool) []string {
	var runs []string
	start := -1
	end := func(i int) {
		if start >= 0 && hintableRun(s[start:i]) {
			runs = append(runs, s[start:i])
		}
		start = -1
	}
	for i, r := range s {
		if hintRune(r, fold) {
			if start < 0 {
				start = i
			}
		} else {
			end(i)
		}
	}
	end(len(s))
	return runs
}

// longestRuns returns at most n distinct runs, longest first.
func longestRuns(runs []string, n int) []string {
	seen := make(map[string]bool, len(runs))
	var out []string
	for _, r := range runs {
		if !seen[r] {
			seen[r] = true
			out = append(out, r)
		}
	}
	slices.SortStableFunc(out, func(a, b string) int { return len(b) - len(a) })
	return out[:min(n, len(out))]
}

// hintRune reports whether r can sit in a hinted run: a small set of characters
// JSON never escapes. Under fold, as a case-insensitive regex matches, a letter
// whose folding leaves ASCII is excluded, since (?i)k matches the Kelvin sign and
// lower() does not fold it back.
func hintRune(r rune, fold bool) bool {
	letter := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
	if !letter {
		return r >= '0' && r <= '9' || r == '.' || r == '-' || r == '_' || r == ':' || r == '@'
	}
	if fold {
		for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
			if f >= utf8.RuneSelf {
				return false
			}
		}
	}
	return true
}

// hintableRun reports whether a run is long enough to look up and reads the same
// in a field's String cast and in norm_log's JSON: not number-like, since a number
// may be stored typed and serialized differently.
func hintableRun(v string) bool {
	if len(v) < hintNgram {
		return false
	}
	if isDottedQuad(v) {
		return true // an IPv4 address is never read as a number or a date
	}
	numeric := true
	for i := 0; i < len(v); i++ {
		switch c := v[i]; {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
			if c != 'e' && c != 'E' {
				numeric = false
			}
		case c == '_', c == ':', c == '@':
			numeric = false
		}
	}
	// JSON renders inf and nan as null, so reject any piece of those words.
	lower := strings.ToLower(v)
	return !numeric && !strings.Contains("-infinity", lower) && !strings.Contains("nan", lower)
}

// isDottedQuad reports whether v is four dot-separated groups of one to three digits.
func isDottedQuad(v string) bool {
	groups := strings.Split(v, ".")
	if len(groups) != 4 {
		return false
	}
	for _, g := range groups {
		if len(g) == 0 || len(g) > 3 || strings.Trim(g, "0123456789") != "" {
			return false
		}
	}
	return true
}

// regexHintRuns returns the hintable runs of the literals every match of an RE2
// pattern contains, or none when the pattern does not parse.
func regexHintRuns(pattern string) []string {
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return nil
	}
	var runs []string
	for _, lit := range requiredLiterals(re, nil) {
		runs = append(runs, hintRuns(string(lit.Rune), lit.Flags&syntax.FoldCase != 0)...)
	}
	return runs
}

// requiredLiterals collects the literal nodes every match passes through: those
// of a concatenation, a group, or a repetition of at least one. An alternation or
// an optional part contributes nothing.
func requiredLiterals(re *syntax.Regexp, out []*syntax.Regexp) []*syntax.Regexp {
	switch re.Op {
	case syntax.OpLiteral:
		out = append(out, re)
	case syntax.OpConcat:
		for _, sub := range re.Sub {
			out = requiredLiterals(sub, out)
		}
	case syntax.OpCapture, syntax.OpPlus:
		out = requiredLiterals(re.Sub[0], out)
	case syntax.OpRepeat:
		if re.Min >= 1 {
			out = requiredLiterals(re.Sub[0], out)
		}
	}
	return out
}

// normLogValueMatch returns the indexed match a filter value implies: its
// JSON-escaped literal, lowercased as the operator lowers it, or "" when that
// cannot be expressed against lower(norm_log).
func normLogValueMatch(v, op string, mode SourceMode) string {
	if len(v) < hintNgram {
		return ""
	}
	literal := escapeLiteralForNormLog(v, mode)
	if op == "=^" || op == "=$" {
		// startsWith and endsWith look for strings.ToLower of the literal itself.
		literal = strings.ToLower(literal)
	}
	match, _ := indexedNormLogMatch(regexp.QuoteMeta(literal))
	return match
}
