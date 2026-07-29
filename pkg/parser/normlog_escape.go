package parser

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
)

// escapeLiteralForNormLog renders an analyst-typed literal the way it appears
// inside the norm_log column of the target store, so a substring search finds it.
//
// norm_log holds a serialized JSON object, so characters the serializer escapes
// ('\', '"', control chars) are stored in escaped form and a naive pattern built
// from the raw literal never matches. The two stores do not serialize identically:
//
//   - SourceHot: ClickHouse DEFAULT toString(fields). Escapes '/' as '\/'.
//   - SourceIceberg: the archiver's encoding/json with SetEscapeHTML(false)
//     (pkg/archive/schema.go marshalFields). Leaves '/' raw.
//
// Verified against ClickHouse 26.6 that the two agree on every other case: '\',
// '"', \n \t \r, backslash-u control escapes, U+2028, and raw pass-through of
// DEL, '<' '>' '&', "'", and non-ASCII UTF-8. encoding/json therefore models both,
// with the forward slash applied as the single mode-dependent difference.
func escapeLiteralForNormLog(literal string, mode SourceMode) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(literal); err != nil {
		return literal
	}
	s := string(bytes.TrimRight(buf.Bytes(), "\n"))
	s = strings.TrimSuffix(strings.TrimPrefix(s, `"`), `"`)
	if mode == SourceHot {
		// encoding/json never emits '/' as part of an escape sequence, so every
		// '/' left in s is a literal the analyst typed.
		s = strings.ReplaceAll(s, "/", `\/`)
	}
	return s
}

// normLogLiteralPattern builds the case-insensitive RE2 pattern for a bare-term
// search against norm_log. The literal is serialization-escaped first, then
// QuoteMeta'd: the escaping introduces backslashes that must themselves be
// regex-literal, so the order is load-bearing.
func normLogLiteralPattern(literal string, mode SourceMode) string {
	return caseInsensitiveFlag + regexp.QuoteMeta(escapeLiteralForNormLog(literal, mode))
}

// normLogSearchValues serialization-escapes literal search values when the target
// is the norm_log content column. Unlike normLogLiteralPattern these feed literal
// substring functions (multiSearchAnyCaseInsensitive, startsWith, endsWith), so no
// regex quoting is applied. Values for any other column pass through untouched.
func normLogSearchValues(fieldRef string, values []string, mode SourceMode) []string {
	if fieldRef != normLogColumn {
		return values
	}
	out := make([]string, len(values))
	for i, v := range values {
		out[i] = escapeLiteralForNormLog(v, mode)
	}
	return out
}

// sourceModeOf reads the source mode from a registry that may be nil (legacy
// translation paths construct conditions without one), defaulting to SourceHot.
func sourceModeOf(registry *FieldRegistry) SourceMode {
	if registry == nil {
		return SourceHot
	}
	return registry.sourceMode
}
