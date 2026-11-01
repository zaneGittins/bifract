package parser

import (
	"regexp"
	"strings"
)

const (
	// hintNgram is the n-gram size of the norm_log text index; shorter values
	// have no token for it to look up.
	hintNgram = 3
	// maxHintValues caps the values hinted for one filter: each is an index
	// lookup, and a long list would cost more than the blocks it skips.
	maxHintValues = 16
)

// normLogHint returns an indexHint on the norm_log n-gram index for a positive
// filter on a stored JSON field, or on norm_log itself when onNormLog is set, or
// "" when the filter does not qualify. indexHint is true for every row, so the
// rows that match cannot change; it only lets ClickHouse skip blocks whose index
// proves every value absent.
func normLogHint(cond HavingCondition, mode SourceMode, onNormLog bool) string {
	if cond.IsRegex || cond.ValueField != "" {
		return ""
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
	hints := make([]string, len(values))
	for i, v := range values {
		if !onNormLog && !hintableValue(v) {
			return ""
		}
		match := normLogValueMatch(v, cond.Operator, mode)
		if match == "" {
			return ""
		}
		hints[i] = "indexHint(" + match + ")"
	}
	if len(hints) == 1 {
		return hints[0]
	}
	return "(" + strings.Join(hints, " OR ") + ")"
}

// hintableValue reports whether v reads the same in a field's String cast and in
// norm_log's JSON: characters JSON never escapes, and not number-like.
func hintableValue(v string) bool {
	if len(v) < hintNgram {
		return false
	}
	numeric := true
	for i := 0; i < len(v); i++ {
		switch c := v[i]; {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
			if c != 'e' && c != 'E' {
				numeric = false
			}
		case c >= '0' && c <= '9', c == '.', c == '-':
		case c == '_', c == ':', c == '@':
			numeric = false
		default:
			return false
		}
	}
	// JSON renders inf and nan as null, so reject any piece of those words.
	lower := strings.ToLower(v)
	return !numeric && !strings.Contains("-infinity", lower) && !strings.Contains("nan", lower)
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
