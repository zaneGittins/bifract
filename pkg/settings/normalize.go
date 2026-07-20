package settings

import (
	"strings"
	"unicode"
)

// NormalizeFieldName takes a dotted field name and returns the normalized version
// Example: "data.win.eventdata.queryName" -> "query_name"
func NormalizeFieldName(fieldName string) string {
	// Split by dots and take the last part
	parts := strings.Split(fieldName, ".")
	lastPart := parts[len(parts)-1]

	// Convert to snake_case and lowercase
	return ToSnakeCase(lastPart)
}

// isFieldSeparator reports whether r marks a word boundary in a log field name
// while carrying no meaning of its own: HTTP headers ("Accept-Encoding"),
// Windows event fields ("Process Name"), rate-style names ("bytes/sec").
//
// Dots are deliberately excluded. Flatten already rewrites them, and the dedot
// transform exists to handle the un-flattened case, so folding them in here
// would silently duplicate a separately selectable transform. Sigils such as
// '@' are excluded too: "@timestamp" must stay distinct from "timestamp".
func isFieldSeparator(r rune) bool {
	switch r {
	case '-', ' ', '\t', '/':
		return true
	}
	return false
}

// ToSnakeCase converts a field name to snake_case. Word boundaries come from
// case changes and from separator runes, so every spelling of a header
// converges on one name instead of fragmenting into separate ClickHouse
// columns depending on how the shipper capitalised it.
//
// Examples:
//   - "queryName"       -> "query_name"
//   - "EventID"         -> "event_id"
//   - "HTTPStatusCode"  -> "http_status_code"
//   - "Accept-Encoding" -> "accept_encoding"
//   - "accept-encoding" -> "accept_encoding"
//   - "Process Name"    -> "process_name"
func ToSnakeCase(s string) string {
	if s == "" {
		return ""
	}

	runes := []rune(s)
	var result strings.Builder
	result.Grow(len(s) + 4)
	lastUnderscore := false

	for i, r := range runes {
		switch {
		case r == '_':
			// Literal underscores pass through, including a leading one, never doubled.
			if !lastUnderscore {
				result.WriteRune('_')
				lastUnderscore = true
			}

		case isFieldSeparator(r):
			// Separators become underscores but never lead or double.
			if result.Len() > 0 && !lastUnderscore {
				result.WriteRune('_')
				lastUnderscore = true
			}

		case unicode.IsUpper(r):
			// Underscore before an uppercase letter only at a real word boundary:
			// previous char lowercase, or next char lowercase (ends an acronym).
			if result.Len() > 0 && !lastUnderscore {
				prevIsLower := unicode.IsLower(runes[i-1])
				nextIsLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
				if prevIsLower || nextIsLower {
					result.WriteRune('_')
				}
			}
			result.WriteRune(unicode.ToLower(r))
			lastUnderscore = false

		default:
			result.WriteRune(r)
			lastUnderscore = false
		}
	}

	// A trailing separator leaves a dangling underscore. Keep the untrimmed form
	// when trimming would empty the name, so a degenerate input still yields one.
	out := result.String()
	if trimmed := strings.TrimRight(out, "_"); trimmed != "" {
		return trimmed
	}
	return out
}
