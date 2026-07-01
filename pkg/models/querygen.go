package models

import (
	"fmt"
	"strings"
)

// GenerateQuery generates a BQL alert query string from a ModelDefinition.
// The returned string is suitable for use as an alert's query_string.
func GenerateQuery(name string, def ModelDefinition, mt ModelType) string {
	lines := filterLines(def.Filter)

	// Extraction steps
	for _, ext := range def.Extractions {
		from := ext.FromField
		// regex() BQL command
		lines = append(lines, fmt.Sprintf("| regex(field=%s, regex=%s, as=%s)",
			from, escapeBQLString(ext.Pattern), ext.OutputField))
		if ext.MinLength > 0 {
			lenName := ext.OutputField + "_len"
			lines = append(lines, fmt.Sprintf("| len(%s, as=%s) | %s >= %d", ext.OutputField, lenName, lenName, ext.MinLength))
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
	case ModelTypeVolumeBaseline:
		lines = append(lines, fmt.Sprintf("| model_lookup(model=%s, key=[%s])",
			escapeBQLString(name), strings.Join(def.KeyFields, ", ")))
		z := 3.5
		if def.Alert != nil && def.Alert.ZThreshold > 0 {
			z = def.Alert.ZThreshold
		}
		lines = append(lines, fmt.Sprintf("| z_score > %.2f", z))
	case ModelTypeBeacon:
		nf := def.Network.WithDefaults()
		lines = append(lines, fmt.Sprintf("| model_lookup(model=%s, key=[%s, %s, %s])",
			escapeBQLString(name), nf.SrcField, nf.DstField, nf.PortField))
		thr := def.Beacon.WithDefaults(int64(def.WindowDays()) * 86400).ScoreThreshold
		lines = append(lines, fmt.Sprintf("| beacon_score > %.2f", thr))
	case ModelTypeLongConnection:
		nf := def.Network.WithDefaults()
		lines = append(lines, fmt.Sprintf("| model_lookup(model=%s, key=[%s, %s, %s])",
			escapeBQLString(name), nf.SrcField, nf.DstField, nf.PortField))
		thr := def.LongConn.WithDefaults().ScoreThreshold
		lines = append(lines, fmt.Sprintf("| longconn_score > %.2f", thr))
	}

	return strings.Join(lines, "\n")
}

// GenerateSourceQuery generates the BQL "source query" for a model: the filter
// and extraction half of the definition only, stopping before model_lookup and
// alert thresholds. This is the canonical authoring form shown in the model
// builder's query editor and the inverse of ParseSourceQuery.
//
// Unlike GenerateQuery (the alert query), this emits only constructs the BQL
// parser accepts: inline filters (=, !=, regex via /.../), cidr() commands,
// regex(field=, regex=, as=) extractions, and per-extraction refinements
// (len(x, as=name) | name >= n for minimum length, lowercase(x)).
func GenerateSourceQuery(def ModelDefinition) string {
	lines := filterLines(def.Filter)

	// Extraction steps, with optional minimum-length and lowercase refinements.
	// Minimum length is expressed as `len(x, as=name) | name >= n` -- len()
	// registers the numeric field, which a following bare comparison filters on.
	for _, ext := range def.Extractions {
		from := ext.FromField
		if from == "" {
			from = "raw_log"
		}
		lines = append(lines, fmt.Sprintf("| regex(field=%s, regex=%s, as=%s)",
			from, escapeBQLString(ext.Pattern), ext.OutputField))
		if ext.MinLength > 0 {
			lenName := ext.OutputField + "_len"
			lines = append(lines, fmt.Sprintf("| len(%s, as=%s) | %s >= %d", ext.OutputField, lenName, lenName, ext.MinLength))
		}
		if ext.Lowercase {
			lines = append(lines, fmt.Sprintf("| lowercase(%s)", ext.OutputField))
		}
	}

	return strings.Join(lines, "\n")
}

// sourceFilterLine renders an inline (non-cidr) filter condition as a BQL line.
func sourceFilterLine(fc FilterCondition) string {
	switch fc.Op {
	case "=":
		return fmt.Sprintf("%s = %s", fc.Field, escapeBQLString(fc.Value))
	case "!=":
		return fmt.Sprintf("%s != %s", fc.Field, escapeBQLString(fc.Value))
	case "~":
		return fmt.Sprintf("%s = %s", fc.Field, bqlRegexLiteral(fc.Value))
	case "!~":
		return fmt.Sprintf("NOT %s = %s", fc.Field, bqlRegexLiteral(fc.Value))
	default:
		return fmt.Sprintf("%s = %s", fc.Field, escapeBQLString(fc.Value))
	}
}

// bqlRegexLiteral wraps a pattern as a /.../ regex literal, escaping only bare
// forward slashes (which would otherwise terminate the literal early). Existing
// backslash escape sequences are copied verbatim so an already-escaped "\/" is
// not double-escaped, and all other backslash sequences round-trip unchanged.
func bqlRegexLiteral(pattern string) string {
	var b strings.Builder
	b.WriteByte('/')
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		if c == '\\' && i+1 < len(pattern) {
			b.WriteByte(c)
			b.WriteByte(pattern[i+1])
			i++
			continue
		}
		if c == '/' {
			b.WriteByte('\\')
		}
		b.WriteByte(c)
	}
	b.WriteByte('/')
	return b.String()
}

// filterLines renders a model's filter conditions as BQL: inline conditions
// first (forming the leading filter expression), then cidr() pipeline commands
// (cidr is not an inline operator in BQL). Shared by GenerateQuery and
// GenerateSourceQuery so both stay consistent and parseable.
func filterLines(filter []FilterCondition) []string {
	var lines []string
	for _, fc := range filter {
		if fc.Op == "cidr" || fc.Op == "!cidr" {
			continue
		}
		lines = append(lines, sourceFilterLine(fc))
	}
	for _, fc := range filter {
		switch fc.Op {
		case "cidr":
			lines = append(lines, fmt.Sprintf("| cidr(%s, %s)", fc.Field, escapeBQLString(fc.Value)))
		case "!cidr":
			lines = append(lines, fmt.Sprintf("| !cidr(%s, %s)", fc.Field, escapeBQLString(fc.Value)))
		}
	}
	return lines
}

// escapeBQLString wraps a value in double quotes with internal double-quotes escaped.
func escapeBQLString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return `"` + s + `"`
}
