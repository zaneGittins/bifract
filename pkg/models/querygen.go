package models

import (
	"fmt"
	"strings"
)

// GenerateQuery generates a BQL alert query string from a ModelDefinition.
// The returned string is suitable for use as an alert's query_string.
func GenerateQuery(name string, def ModelDefinition, mt ModelType) string {
	// The alert must read the same rows the model scored. When the author wrote a
	// source query it is emitted verbatim: re-rendering it from def.Filter would
	// drop every filter the structured form cannot hold, and the alert would then
	// fire on rows outside the model's source.
	lines := sourceLines(def)

	// modelLookup
	switch mt {
	case ModelTypeRarity:
		keyParts := []string{def.PartitionKey, def.ValueKey}
		lines = append(lines, fmt.Sprintf("| modelLookup(model=%s, key=[%s])",
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
		lines = append(lines, fmt.Sprintf("| modelLookup(model=%s, key=[%s])",
			escapeBQLString(name), strings.Join(keyParts, ", ")))
		if def.Alert != nil && def.Alert.AlertOnNew {
			lines = append(lines, `| is_new = "1"`)
		}
	case ModelTypeVolumeBaseline:
		lines = append(lines, fmt.Sprintf("| modelLookup(model=%s, key=[%s])",
			escapeBQLString(name), strings.Join(def.KeyFields, ", ")))
		z := 3.5
		if def.Alert != nil && def.Alert.ZThreshold > 0 {
			z = def.Alert.ZThreshold
		}
		lines = append(lines, fmt.Sprintf("| z_score > %.2f", z))
	case ModelTypeBeacon:
		nf := def.Network.WithDefaults()
		lines = append(lines, fmt.Sprintf("| modelLookup(model=%s, key=[%s, %s, %s])",
			escapeBQLString(name), nf.SrcField, nf.DstField, nf.PortField))
		thr := def.Beacon.WithDefaults(int64(def.WindowDays()) * 86400).ScoreThreshold
		lines = append(lines, fmt.Sprintf("| beacon_score > %.2f", thr))
	case ModelTypeLongConnection:
		nf := def.Network.WithDefaults()
		lines = append(lines, fmt.Sprintf("| modelLookup(model=%s, key=[%s, %s, %s])",
			escapeBQLString(name), nf.SrcField, nf.DstField, nf.PortField))
		thr := def.LongConn.WithDefaults().ScoreThreshold
		lines = append(lines, fmt.Sprintf("| longconn_score > %.2f", thr))
	}

	return strings.Join(lines, "\n")
}

// GenerateSourceQuery returns the BQL source query for a model: what the author
// wrote, or, for a model stored before source queries were kept verbatim, the
// filter and extraction half rendered from the structured definition. It is the
// authoring form shown in the builder's query editor, and it stops before
// modelLookup and the alert thresholds GenerateQuery adds.
func GenerateSourceQuery(def ModelDefinition) string {
	return strings.Join(sourceLines(def), "\n")
}

// sourceLines is the source half of a model's query: the author's own source
// query when there is one, otherwise the filter and extraction commands rendered
// from the structured definition. Minimum length is expressed as
// `len(x, as=name) | name >= n` -- len() registers the numeric field, which a
// following bare comparison filters on.
func sourceLines(def ModelDefinition) []string {
	if bql := strings.TrimSpace(def.SourceBQL); bql != "" {
		return []string{bql}
	}
	lines := filterLines(def.Filter)
	for _, ext := range def.Extractions {
		from := ext.FromField
		if from == "" {
			from = "norm_log"
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
	return lines
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
