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

// ToSnakeCase converts camelCase/PascalCase to snake_case
// Examples:
//   - "queryName" -> "query_name"
//   - "EventID" -> "event_id"
//   - "HTTPStatusCode" -> "http_status_code"
//   - "userId" -> "user_id"
func ToSnakeCase(s string) string {
	if s == "" {
		return ""
	}

	var result strings.Builder
	runes := []rune(s)

	for i, r := range runes {
		if unicode.IsUpper(r) {
			// Add underscore before uppercase letter if:
			// 1. Not at the beginning
			// 2. Previous char is lowercase OR next char is lowercase (for handling acronyms)
			if i > 0 {
				prevIsLower := unicode.IsLower(runes[i-1])
				nextIsLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])

				if prevIsLower || nextIsLower {
					result.WriteRune('_')
				}
			}
			result.WriteRune(unicode.ToLower(r))
		} else {
			result.WriteRune(r)
		}
	}

	return result.String()
}
