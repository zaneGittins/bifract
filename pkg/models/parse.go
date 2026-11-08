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

	// Conditions before the first command are the source's filter. Lowering them
	// to chips is best effort, since SourceBQL is what runs: one the structured
	// form cannot hold is skipped, its fields still offered as candidates.
	assigned := map[string]bool{}
	var lower func(c parser.Condition)
	lower = func(c parser.Condition) {
		if andGroup(c) {
			for _, child := range c.Children {
				lower(child)
			}
			return
		}
		if fc := cidrFilter(c); fc != nil {
			res.Filter = append(res.Filter, *fc)
			return
		}
		fc, ok := conditionToFilter(c)
		if !ok || assigned[c.Field] {
			res.FilterComplete = false
			return
		}
		res.Filter = append(res.Filter, fc)
	}
	var later []parser.Condition
	seenCommand := false
	for _, st := range ast.Steps {
		switch {
		case st.Command != nil:
			seenCommand = true
		case st.Assign != nil:
			// A name assigned ahead of a filter holds the computed value, not the log field.
			if !seenCommand {
				assigned[st.Assign.Field] = true
			}
		case seenCommand && cidrFilter(*st.Filter) == nil:
			later = append(later, *st.Filter)
		default:
			collectConditionFields(*st.Filter, &referenced)
			lower(*st.Filter)
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
	for _, cmd := range ast.Commands() {
		switch strings.ToLower(cmd.Name) {
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
	for _, hv := range later {
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
		min, ok := minLengthFromCondition(hv)
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

// collectConditionFields appends every field a condition reads.
func collectConditionFields(c parser.Condition, out *[]string) {
	for _, child := range c.Children {
		collectConditionFields(child, out)
	}
	if c.Field != "" {
		*out = append(*out, c.Field)
	}
	if c.ValueField != "" {
		*out = append(*out, c.ValueField)
	}
}

// andGroup reports whether a condition is a plain AND of conditions, which
// distributes into separate chips.
func andGroup(c parser.Condition) bool {
	return c.Op == parser.CondAnd && !c.Negate
}

// conditionToFilter maps a filter condition to a model FilterCondition. Negation is
// already folded into the operator. The second result is false for a condition the
// structured form cannot hold; the query still runs it.
func conditionToFilter(c parser.Condition) (FilterCondition, bool) {
	if c.IsCompound() || c.Expr != nil || c.Command != nil || c.ValueField != "" || c.Field == "" || len(c.Values) > 1 {
		return FilterCondition{}, false
	}
	value := c.Value
	if c.IsRegex {
		// Undo the forward-slash escaping applied by bqlRegexLiteral.
		value = strings.ReplaceAll(value, `\/`, "/")
	}
	switch {
	case c.IsRegex && (c.Operator == "=" || c.Operator == "~"):
		return FilterCondition{Field: c.Field, Op: "~", Value: value}, true
	case c.IsRegex && c.Operator == "!=":
		return FilterCondition{Field: c.Field, Op: "!~", Value: value}, true
	case c.Operator == "=" || c.Operator == "!=":
		return FilterCondition{Field: c.Field, Op: c.Operator, Value: value}, true
	}
	return FilterCondition{}, false
}

// minLengthFromCondition converts a `_len <op> n` comparison to a MinLength value.
// `>= n` maps to n; `> n` maps to n+1. Any other shape, a negated one included,
// is a filter the query applies and the structured form does not carry.
func minLengthFromCondition(hv parser.Condition) (int, bool) {
	if hv.Negate {
		return 0, false
	}
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

// cidrFilter is the chip a cidr() filter leaf maps to, or nil for any other
// condition. A cidr() keeps its chip wherever it is written.
func cidrFilter(c parser.Condition) *FilterCondition {
	if c.IsCompound() || c.Command == nil || parser.CanonicalCommandName(c.Command.Name) != "cidr" {
		return nil
	}
	field, value, ok := cidrArgs(*c.Command)
	if !ok {
		return nil
	}
	op := "cidr"
	if c.Command.Negate {
		op = "!cidr"
	}
	return &FilterCondition{Field: field, Op: op, Value: value}
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
