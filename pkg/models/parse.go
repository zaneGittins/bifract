package models

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"bifract/pkg/parser"
)

// ParsedSource is the result of lowering a BQL source query into the structured
// filter + extraction half of a ModelDefinition. Validation problems that the
// user can fix are reported in Errors (never as a hard failure) so the UI can
// surface them inline; Warnings are non-fatal advisories.
type ParsedSource struct {
	Filter          []FilterCondition `json:"filter"`
	Extractions     []ExtractionStep  `json:"extractions"`
	CandidateFields []string          `json:"candidate_fields"`
	Errors          []string          `json:"errors"`
	Warnings        []string          `json:"warnings"`
}

var parseNamedGroupRe = regexp.MustCompile(`\(\?<([a-zA-Z_][a-zA-Z0-9_]*)>`)

// ParseSourceQuery lowers a BQL source query into a model's Filter + Extractions.
// It accepts only the model-expressible subset of BQL (flat-AND inline filters,
// cidr() filter commands, and regex(... as=) extractions) and rejects anything
// else with a friendly, specific message. It is the inverse of GenerateSourceQuery.
//
// Per-extraction lowercase and minimum-length are intentionally NOT parsed from
// the query (they are configured as adornments in the builder UI), so a query
// containing lowercase()/len() is rejected with guidance.
func ParseSourceQuery(query string, mt ModelType) ParsedSource {
	res := ParsedSource{
		Filter:      []FilterCondition{},
		Extractions: []ExtractionStep{},
		Errors:      []string{},
		Warnings:    []string{},
	}

	if strings.TrimSpace(query) == "" {
		// An empty source query is valid: the model consumes all logs in the fractal.
		res.CandidateFields = res.candidateFields()
		return res
	}

	ast, err := parser.ParseQuery(query)
	if err != nil {
		res.Errors = append(res.Errors, fmt.Sprintf("could not parse query: %s", err.Error()))
		return res
	}

	// Constructs that have no representation in a model definition.
	if len(ast.Assignments) > 0 {
		res.Errors = append(res.Errors, "field assignments (:=) are not supported in a model source query")
	}

	// Filter expression: flat AND only.
	if ast.Filter != nil {
		for _, c := range ast.Filter.Conditions {
			if c.IsCompound || c.GroupID != 0 || c.GroupNegate {
				res.Errors = append(res.Errors, "grouped/parenthesized filters are not supported; use a flat list of AND conditions")
				continue
			}
			if strings.EqualFold(c.Logic, "OR") {
				res.Errors = append(res.Errors, "OR is not supported in a model source query; use separate AND conditions")
				continue
			}
			if c.Field == "" {
				res.Errors = append(res.Errors, "every filter needs a field, e.g. raw_log = /pattern/")
				continue
			}
			fc, perr := conditionToFilter(c)
			if perr != "" {
				res.Errors = append(res.Errors, perr)
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

	// Pipeline commands, in order.
	for _, cmd := range ast.Commands {
		switch strings.ToLower(cmd.Name) {
		case "cidr":
			field, value, perr := cidrArgs(cmd)
			if perr != "" {
				res.Errors = append(res.Errors, perr)
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
				res.Errors = append(res.Errors, perr)
				continue
			}
			extIndex[ext.OutputField] = len(res.Extractions)
			res.Extractions = append(res.Extractions, ext)
		case "lowercase":
			field, perr := singleFieldArg(cmd, "lowercase")
			if perr != "" {
				res.Errors = append(res.Errors, perr)
				continue
			}
			if idx, ok := extIndex[field]; ok {
				res.Extractions[idx].Lowercase = true
			} else {
				res.Errors = append(res.Errors, fmt.Sprintf("lowercase(%s) must target a field produced by a preceding regex()", field))
			}
		case "len", "length":
			field, outName, perr := lenCommandArgs(cmd)
			if perr != "" {
				res.Errors = append(res.Errors, perr)
				continue
			}
			lenOutputs[outName] = field
		case "uppercase":
			res.Errors = append(res.Errors, "uppercase is not supported in a model source query")
		case "in":
			res.Errors = append(res.Errors, "in() is not supported in a model source query; use separate filters")
		default:
			res.Errors = append(res.Errors, fmt.Sprintf("%s() is not supported in a model source query", cmd.Name))
		}
	}

	// Each comparison must reference a len() output field; map it back to the
	// extraction it measures and set MinLength. Named len outputs (len(x, as=x_len))
	// keep multiple length filters independent, so there is no collision.
	for _, hv := range ast.HavingConditions {
		field, ok := lenOutputs[hv.Field]
		if !ok {
			res.Errors = append(res.Errors, fmt.Sprintf("comparison on %q after extraction is not supported in a model source query", hv.Field))
			continue
		}
		idx, ok := extIndex[field]
		if !ok {
			res.Errors = append(res.Errors, fmt.Sprintf("len(%s) must target a field produced by a preceding regex()", field))
			continue
		}
		min, perr := minLengthFromHaving(hv)
		if perr != "" {
			res.Errors = append(res.Errors, perr)
			continue
		}
		res.Extractions[idx].MinLength = min
	}

	res.CandidateFields = res.candidateFields()
	return res
}

// minLengthFromHaving converts a `_len <op> n` comparison to a MinLength value.
// `>= n` maps to n; `> n` maps to n+1. Other operators are rejected.
func minLengthFromHaving(hv parser.HavingCondition) (int, string) {
	n, err := strconv.Atoi(strings.TrimSpace(hv.Value))
	if err != nil {
		return 0, fmt.Sprintf("len() comparison needs a whole number, got %q", hv.Value)
	}
	switch hv.Operator {
	case ">=":
		return n, ""
	case ">":
		return n + 1, ""
	default:
		return 0, fmt.Sprintf("len() supports only >= or > (got %q); use len(x) | _len >= n", hv.Operator)
	}
}

// lenCommandArgs returns the measured field and the output field name of a
// len() command. The output defaults to _len, or the as= value when given.
func lenCommandArgs(cmd parser.CommandNode) (field, outName, errMsg string) {
	outName = "_len"
	for _, arg := range cmd.Arguments {
		arg = strings.TrimSpace(arg)
		switch {
		case strings.HasPrefix(arg, "as="):
			outName = strings.Trim(strings.TrimPrefix(arg, "as="), `"'`)
		case strings.HasPrefix(arg, "field="):
			field = strings.TrimPrefix(arg, "field=")
		case field == "":
			field = strings.Trim(arg, `"'`)
		}
	}
	if field == "" {
		return "", "", "len() requires a field, e.g. len(tld) | _len >= 4"
	}
	return field, outName, ""
}

// singleFieldArg returns the lone field argument of a command like lowercase(x),
// rejecting an output rename (a second argument).
func singleFieldArg(cmd parser.CommandNode, name string) (string, string) {
	var fields []string
	for _, arg := range cmd.Arguments {
		arg = strings.TrimSpace(arg)
		if strings.HasPrefix(arg, "field=") {
			arg = strings.TrimPrefix(arg, "field=")
		} else if strings.HasPrefix(arg, "as=") {
			return "", fmt.Sprintf("%s() output rename is not supported; apply it to a field in place", name)
		}
		if arg != "" {
			fields = append(fields, strings.Trim(arg, `"'`))
		}
	}
	if len(fields) == 0 {
		return "", fmt.Sprintf("%s() requires a field", name)
	}
	if len(fields) > 1 {
		return "", fmt.Sprintf("%s() output rename is not supported; apply it to a field in place", name)
	}
	return fields[0], ""
}

// conditionToFilter maps a parsed filter condition to a model FilterCondition,
// returning a friendly error string for unsupported operators.
func conditionToFilter(c parser.ConditionNode) (FilterCondition, string) {
	switch c.Operator {
	case "=", "":
		if c.IsRegex {
			op := "~"
			if c.Negate {
				op = "!~"
			}
			// Undo the forward-slash escaping applied by bqlRegexLiteral.
			return FilterCondition{Field: c.Field, Op: op, Value: strings.ReplaceAll(c.Value, `\/`, "/")}, ""
		}
		if c.Negate {
			return FilterCondition{Field: c.Field, Op: "!=", Value: c.Value}, ""
		}
		return FilterCondition{Field: c.Field, Op: "=", Value: c.Value}, ""
	case "!=":
		if c.Negate {
			return FilterCondition{Field: c.Field, Op: "=", Value: c.Value}, ""
		}
		return FilterCondition{Field: c.Field, Op: "!=", Value: c.Value}, ""
	case "~", "!~":
		op := "~"
		if c.Operator == "!~" || c.Negate {
			op = "!~"
		}
		return FilterCondition{Field: c.Field, Op: op, Value: strings.ReplaceAll(c.Value, `\/`, "/")}, ""
	case ">", "<", ">=", "<=":
		return FilterCondition{}, fmt.Sprintf("comparison operator %q is not supported in model filters", c.Operator)
	default:
		return FilterCondition{}, fmt.Sprintf("operator %q is not supported in model filters", c.Operator)
	}
}

// cidrArgs extracts the field and range from a cidr() command's arguments,
// tolerating both positional and field=/range= forms.
func cidrArgs(cmd parser.CommandNode) (field, value, errMsg string) {
	var positional []string
	for _, arg := range cmd.Arguments {
		arg = strings.TrimSpace(arg)
		switch {
		case strings.HasPrefix(arg, "field="):
			field = strings.Trim(strings.TrimPrefix(arg, "field="), `"'`)
		case strings.HasPrefix(arg, "range="), strings.HasPrefix(arg, "cidr="):
			value = strings.Trim(arg[strings.IndexByte(arg, '=')+1:], `"'`)
		default:
			positional = append(positional, strings.Trim(arg, `"'`))
		}
	}
	if field == "" && len(positional) > 0 {
		field = positional[0]
		positional = positional[1:]
	}
	if value == "" && len(positional) > 0 {
		value = positional[0]
	}
	if field == "" || value == "" {
		return "", "", "cidr() requires a field and a CIDR range, e.g. cidr(src_ip, \"10.0.0.0/8\")"
	}
	return field, value, ""
}

// regexCommandToExtraction maps a regex() command to an ExtractionStep. The
// output name follows the engine's precedence: a named capture group wins over
// as= (so the parsed OutputField matches the column the live preview produces).
func regexCommandToExtraction(cmd parser.CommandNode) (ExtractionStep, string) {
	from := "raw_log"
	var pattern, asName string
	patternSet := false
	for _, arg := range cmd.Arguments {
		arg = strings.TrimSpace(arg)
		switch {
		case strings.HasPrefix(arg, "field="):
			from = strings.TrimPrefix(arg, "field=")
		case strings.HasPrefix(arg, "regex="):
			pattern = strings.Trim(strings.TrimPrefix(arg, "regex="), `"'`)
			patternSet = true
		case strings.HasPrefix(arg, "pattern="):
			pattern = strings.Trim(strings.TrimPrefix(arg, "pattern="), `"'`)
			patternSet = true
		case strings.HasPrefix(arg, "as="):
			asName = strings.Trim(strings.TrimPrefix(arg, "as="), `"'`)
		case !patternSet:
			pattern = strings.Trim(arg, `"'`)
			patternSet = true
		}
	}
	if pattern == "" {
		return ExtractionStep{}, "regex() requires a pattern"
	}
	// A model extraction has a single output column. The engine creates one column
	// per named group, so reject patterns with more than one named group.
	named := parseNamedGroupRe.FindAllStringSubmatch(pattern, -1)
	if len(named) > 1 {
		return ExtractionStep{}, "a model extraction supports only one named capture group; use a single (?<name>...) group"
	}
	output := asName
	if len(named) == 1 {
		// Named group wins over as=, matching the regex() runtime.
		output = named[0][1]
	}
	if output == "" {
		return ExtractionStep{}, "regex() needs an output name: add as=<field> or use a (?<name>...) capture group"
	}
	return ExtractionStep{FromField: from, Pattern: pattern, OutputField: output}, ""
}

// candidateFields returns the fields available for shaping a model: every field
// referenced in filters plus every extraction output, de-duplicated in order,
// always including raw_log. The frontend may additionally merge its own list of
// known log fields.
func (p *ParsedSource) candidateFields() []string {
	seen := map[string]bool{}
	var out []string
	add := func(f string) {
		if f == "" || seen[f] {
			return
		}
		seen[f] = true
		out = append(out, f)
	}
	add("raw_log")
	for _, fc := range p.Filter {
		add(fc.Field)
	}
	for _, ext := range p.Extractions {
		add(ext.FromField)
	}
	for _, ext := range p.Extractions {
		add(ext.OutputField)
	}
	return out
}
