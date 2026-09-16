package models

import (
	"fmt"
	"strconv"
	"strings"

	"bifract/pkg/parser"
)

// ParsedSource is the result of lowering a BQL source query into the structured
// filter + extraction half of a ModelDefinition. Validation problems that the
// user can fix are reported in Errors (never as a hard failure) so the UI can
// surface them inline; Warnings are non-fatal advisories.
type ParsedSource struct {
	// SourceBQL is the query as written. It is compiled by the translator at
	// render time, which is every filter BQL has rather than the handful the
	// structured fields below can carry.
	SourceBQL string `json:"source_bql,omitempty"`
	// FilterComplete says the Filter list describes the whole query. When it is
	// false the editor must not present the chips as the model's filter: the
	// query carries conditions the structured form has no shape for.
	FilterComplete  bool              `json:"filter_complete"`
	Filter          []FilterCondition `json:"filter"`
	Extractions     []ExtractionStep  `json:"extractions"`
	CandidateFields []string          `json:"candidate_fields"`
	// ComputedFields names the columns the source query computes rather than reads
	// from the log. They are usable keys, because the model's scan projects them,
	// but their value follows whatever computed it: a dictionary lookup keyed on a
	// list that is later edited yields a different key for rows read after the edit.
	ComputedFields []string `json:"computed_fields"`
	Errors         []string `json:"errors"`
	Warnings       []string `json:"warnings"`
}

// ParseSourceQuery validates a BQL source query and lowers what it can into a
// model's Filter + Extractions. The query itself is what runs (SourceBQL), so a
// construct the structured form cannot hold is kept rather than refused; only a
// query that cannot mean anything as a model source is an error.
//
// Extractions are the exception: they are columns the model renders itself, so a
// regex() the structured form cannot carry warns rather than passing silently.
func ParseSourceQuery(query string) ParsedSource {
	res := ParsedSource{
		Filter:      []FilterCondition{},
		Extractions: []ExtractionStep{},
		Errors:      []string{},
		Warnings:    []string{},
	}

	if strings.TrimSpace(query) == "" {
		// An empty source query is valid: the model consumes all logs in the fractal.
		res.FilterComplete = true
		res.CandidateFields = res.candidateFields(nil, nil)
		return res
	}

	ast, err := parser.ParseQuery(query)
	if err != nil {
		res.Errors = append(res.Errors, fmt.Sprintf("could not parse query: %s", err.Error()))
		return res
	}

	// A source the translator can compile is kept verbatim and compiled at render
	// time, which is every filter BQL has rather than the handful below. The
	// structured fields are still filled in where they can be, so the editor keeps
	// its field-by-field view of what it understands.
	if err := validateSourcePipeline(ast); err != nil {
		res.Errors = append(res.Errors, err.Error())
		return res
	}
	res.SourceBQL = query
	res.FilterComplete = true

	var referenced []string

	// Filter expression. Lowering is best effort from here on: SourceBQL is what
	// runs, so a condition the structured form cannot hold is skipped rather than
	// refused. Its fields still reach CandidateFields so the editor can offer them.
	if ast.Filter != nil {
		flatAnd := func(c parser.ConditionNode) bool {
			return !c.IsCompound && c.GroupID == 0 && !c.GroupNegate && !strings.EqualFold(c.Logic, "OR")
		}
		for _, c := range ast.Filter.Conditions {
			collectConditionFields(c, &referenced)
			if !flatAnd(c) || c.Field == "" {
				res.FilterComplete = false
				continue
			}
			fc, ok := conditionToFilter(c)
			if !ok {
				res.FilterComplete = false
				continue
			}
			res.Filter = append(res.Filter, fc)
		}
	}

	// extIndex maps an extraction output field to its index in res.Extractions so
	// lowercase()/len() refinements can attach to the right step.
	extIndex := map[string]int{}
	// lenOutputs maps each len() command's output field name to the field it
	// measures, so the following comparison (e.g. tld_len >= 4) recovers MinLength.
	lenOutputs := map[string]string{}

	// Pipeline commands, in order. Only these four have a structured shape; any
	// other command runs in the source without appearing in Filter.
	for _, cmd := range ast.Commands {
		switch strings.ToLower(cmd.Name) {
		case "cidr":
			field, value, ok := cidrArgs(cmd)
			if !ok {
				res.FilterComplete = false
				continue
			}
			op := "cidr"
			if cmd.Negate {
				op = "!cidr"
			}
			res.Filter = append(res.Filter, FilterCondition{Field: field, Op: op, Value: value})
		case "regex":
			ext, perr := regexCommandToExtraction(cmd)
			if perr != "" {
				// The command still runs in the source, but its column is not one the
				// model can key on, so say so rather than letting the author pick it.
				res.Warnings = append(res.Warnings, "regex() filters the source but does not give the model a field to key on: "+perr)
				res.FilterComplete = false
				continue
			}
			extIndex[ext.OutputField] = len(res.Extractions)
			res.Extractions = append(res.Extractions, ext)
		case "lowercase":
			field, bound := singleFieldArg(cmd)
			idx, ok := extIndex[field]
			if !bound || !ok {
				res.FilterComplete = false
				continue
			}
			res.Extractions[idx].Lowercase = true
		case "len", "length":
			field, outName, ok := lenCommandArgs(cmd)
			if !ok {
				res.FilterComplete = false
				continue
			}
			lenOutputs[outName] = field
		default:
			res.FilterComplete = false
		}
	}

	// Each comparison must reference a len() output field; map it back to the
	// extraction it measures and set MinLength. Named len outputs (len(x, as=x_len))
	// keep multiple length filters independent, so there is no collision.
	for _, hv := range ast.HavingConditions {
		field, ok := lenOutputs[hv.Field]
		if !ok {
			res.FilterComplete = false
			continue
		}
		idx, ok := extIndex[field]
		if !ok {
			res.FilterComplete = false
			continue
		}
		min, ok := minLengthFromHaving(hv)
		if !ok {
			res.FilterComplete = false
			continue
		}
		res.Extractions[idx].MinLength = min
	}

	res.ComputedFields = computedFields(query, res.Extractions)
	res.CandidateFields = res.candidateFields(referenced, res.ComputedFields)

	return res
}

// collectConditionFields appends every field a condition references, walking
// compound children so a grouped or OR filter still offers its fields.
func collectConditionFields(c parser.ConditionNode, out *[]string) {
	if c.IsCompound {
		for _, child := range c.Children {
			collectConditionFields(child, out)
		}
		return
	}
	if c.Field != "" {
		*out = append(*out, c.Field)
	}
	if c.ValueField != "" {
		*out = append(*out, c.ValueField)
	}
}

// minLengthFromHaving converts a `_len <op> n` comparison to a MinLength value.
// `>= n` maps to n; `> n` maps to n+1. Any other shape is a filter the query
// applies and the structured form does not carry.
func minLengthFromHaving(hv parser.HavingCondition) (int, bool) {
	n, err := strconv.Atoi(strings.TrimSpace(hv.Value))
	if err != nil {
		return 0, false
	}
	switch hv.Operator {
	case ">=":
		return n, true
	case ">":
		return n + 1, true
	}
	return 0, false
}

// lenCommandArgs returns the measured field and the output field name of a
// len() command. The output defaults to _len, or the as= value when given.
func lenCommandArgs(cmd parser.CommandNode) (field, outName string, ok bool) {
	b, err := parser.BindCommand(cmd)
	if err != nil {
		return "", "", false
	}
	field, outName = b.Str("field", ""), b.Str("as", "_len")
	return field, outName, field != ""
}

// singleFieldArg returns the lone field argument of a command like lowercase(x).
// An output rename means the command produces a new column rather than adorning
// an extraction, which the structured form has no shape for.
func singleFieldArg(cmd parser.CommandNode) (string, bool) {
	b, err := parser.BindCommand(cmd)
	if err != nil || b.Has("output") || b.Has("as") {
		return "", false
	}
	field := b.Str("field", "")
	return field, field != ""
}

// conditionToFilter maps a parsed filter condition to a model FilterCondition.
// The second result is false for a condition the structured form cannot hold; the
// query still runs it, so there is nothing to report, only a chip not to draw.
func conditionToFilter(c parser.ConditionNode) (FilterCondition, bool) {
	switch c.Operator {
	case "=", "":
		if c.IsRegex {
			op := "~"
			if c.Negate {
				op = "!~"
			}
			// Undo the forward-slash escaping applied by bqlRegexLiteral.
			return FilterCondition{Field: c.Field, Op: op, Value: strings.ReplaceAll(c.Value, `\/`, "/")}, true
		}
		if c.Negate {
			return FilterCondition{Field: c.Field, Op: "!=", Value: c.Value}, true
		}
		return FilterCondition{Field: c.Field, Op: "=", Value: c.Value}, true
	case "!=":
		if c.Negate {
			return FilterCondition{Field: c.Field, Op: "=", Value: c.Value}, true
		}
		return FilterCondition{Field: c.Field, Op: "!=", Value: c.Value}, true
	case "~", "!~":
		op := "~"
		if c.Operator == "!~" || c.Negate {
			op = "!~"
		}
		return FilterCondition{Field: c.Field, Op: op, Value: strings.ReplaceAll(c.Value, `\/`, "/")}, true
	default:
		return FilterCondition{}, false
	}
}

// cidrArgs extracts the field and range from a cidr() command's arguments,
// tolerating both positional and field=/range= forms.
func cidrArgs(cmd parser.CommandNode) (field, value string, ok bool) {
	b, err := parser.BindCommand(cmd)
	if err != nil {
		return "", "", false
	}
	field, value = b.Str("field", ""), b.Str("range", "")
	return field, value, field != "" && value != ""
}

// regexCommandToExtraction maps a regex() command to an ExtractionStep. The
// output name follows the engine's precedence: a named capture group wins over
// as= (so the parsed OutputField matches the column the live preview produces).
func regexCommandToExtraction(cmd parser.CommandNode) (ExtractionStep, string) {
	b, err := parser.BindCommand(cmd)
	if err != nil {
		return ExtractionStep{}, err.Error()
	}
	from := b.Str("field", "norm_log")
	pattern := b.StrOf("pattern", "regex")
	asName := b.Str("as", "")
	if pattern == "" {
		return ExtractionStep{}, "regex() requires a pattern"
	}
	// raw_log is a 7-day ephemeral troubleshooting column; model state is long-lived,
	// so an extraction sourced from it would silently yield nothing for older data.
	// Extract from norm_log (canonical normalized text) or a specific field instead.
	if from == "raw_log" {
		return ExtractionStep{}, "raw_log cannot be a model extraction source (it is not retained); use norm_log or a specific field"
	}
	// A model extraction has a single output column. The engine creates one column
	// per named group, so reject patterns with more than one named group.
	named := parser.NamedCaptureGroups(pattern)
	if len(named) > 1 {
		return ExtractionStep{}, "a model extraction supports only one named capture group; use a single (?<name>...) group"
	}
	output := asName
	if len(named) == 1 {
		// Named group wins over as=, matching the regex() runtime.
		output = named[0]
	}
	if output == "" {
		return ExtractionStep{}, "regex() needs an output name: add as=<field> or use a (?<name>...) capture group"
	}
	return ExtractionStep{FromField: from, Pattern: pattern, OutputField: output}, ""
}

// candidateFields returns the fields available for shaping a model: every field
// referenced in filters plus every extraction output and every column the source
// computes, de-duplicated in order, always including norm_log (the canonical
// normalized text). The frontend may additionally merge its own list of known log
// fields.
func (p *ParsedSource) candidateFields(referenced, computed []string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(f string) {
		if f == "" || seen[f] {
			return
		}
		seen[f] = true
		out = append(out, f)
	}
	add("norm_log")
	// A column the source computes is a usable key: the model's scan projects it.
	for _, f := range computed {
		add(f)
	}
	for _, fc := range p.Filter {
		add(fc.Field)
	}
	for _, f := range referenced {
		add(f)
	}
	for _, ext := range p.Extractions {
		add(ext.FromField)
	}
	for _, ext := range p.Extractions {
		add(ext.OutputField)
	}
	return out
}
