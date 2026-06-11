package models

import (
	"fmt"
	"strings"
)

// GenerateQuery generates a BQL alert query string from a ModelDefinition.
// The returned string is suitable for use as an alert's query_string.
func GenerateQuery(name string, def ModelDefinition, mt ModelType) string {
	var lines []string

	// Filter conditions
	for _, fc := range def.Filter {
		lines = append(lines, filterConditionToBQL(fc))
	}

	// Extraction steps
	for _, ext := range def.Extractions {
		from := ext.FromField
		// regex() BQL command
		lines = append(lines, fmt.Sprintf("| regex(field=%s, regex=%s, as=%s)",
			from, escapeBQLString(ext.Pattern), ext.OutputField))
		if ext.MinLength > 0 {
			lines = append(lines, fmt.Sprintf("| len(%s) >= %d", ext.OutputField, ext.MinLength))
		}
		if ext.Lowercase {
			lines = append(lines, fmt.Sprintf("| lowercase(%s)", ext.OutputField))
		}
	}

	// model_lookup
	switch mt {
	case ModelTypeRarity:
		keyParts := []string{def.PartitionKey, def.ValueKey}
		lines = append(lines, fmt.Sprintf("| model_lookup(model=%s, key=[%s])",
			escapeBQLString(name), strings.Join(keyParts, ", ")))
		if def.Alert != nil {
			if def.Alert.ConfidenceThreshold > 0 {
				lines = append(lines, fmt.Sprintf("| confidence > %.2f", def.Alert.ConfidenceThreshold))
			}
			if def.Alert.PercentThreshold > 0 {
				lines = append(lines, fmt.Sprintf("| percent < %.2f", def.Alert.PercentThreshold))
			}
		}
	case ModelTypeFirstSeen:
		keyParts := def.KeyFields
		lines = append(lines, fmt.Sprintf("| model_lookup(model=%s, key=[%s])",
			escapeBQLString(name), strings.Join(keyParts, ", ")))
		if def.Alert != nil && def.Alert.AlertOnNew {
			lines = append(lines, `| is_new = "1"`)
		}
	}

	return strings.Join(lines, "\n")
}

// filterConditionToBQL converts a FilterCondition to a BQL filter line (no leading pipe).
func filterConditionToBQL(fc FilterCondition) string {
	switch fc.Op {
	case "=":
		return fmt.Sprintf("%s = %s", fc.Field, escapeBQLString(fc.Value))
	case "!=":
		return fmt.Sprintf("NOT %s = %s", fc.Field, escapeBQLString(fc.Value))
	case "~":
		return fmt.Sprintf("%s ~ %s", fc.Field, escapeBQLRegex(fc.Value))
	case "!~":
		return fmt.Sprintf("%s !~ %s", fc.Field, escapeBQLRegex(fc.Value))
	default:
		return fmt.Sprintf("%s = %s", fc.Field, escapeBQLString(fc.Value))
	}
}

// escapeBQLString wraps a value in double quotes with internal double-quotes escaped.
func escapeBQLString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return `"` + s + `"`
}

// escapeBQLRegex wraps a regex in backslash-delimited form for BQL.
// Backslashes in the pattern are doubled so BQL can parse them correctly.
func escapeBQLRegex(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return `\` + s + `\`
}
