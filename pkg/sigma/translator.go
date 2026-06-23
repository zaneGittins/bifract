package sigma

import (
	"fmt"
	"strings"
	"unicode"
)

// TranslatorVersion identifies the Sigma->BQL translation behavior. It is mixed
// into the feed rule content hash so that bumping it forces every existing feed
// alert to be re-translated in place on the next sync (no DB migration needed).
// Bump this whenever Translate's output changes for unchanged Sigma input.
//
//	v2: consolidate OR'd regex values (contains/startswith/endswith/re/keyword)
//	    into a single alternation regex (one match() per field instead of N).
//	v3: contains/startswith/endswith emit native =~/=^/=$ operators instead of
//	    regex, gaining SIMD multi-pattern search and text-index acceleration.
//	    Falls back to regex only when a value contains a literal comma (which
//	    would corrupt the comma-delimited list syntax) or for keyword/re searches.
const TranslatorVersion = "v3"

// Translate converts a parsed Sigma rule's detection logic into a BQL query string.
// fieldMapper optionally transforms Sigma field names to match stored field names.
func Translate(rule *SigmaRule, fieldMapper func(string) string) (string, error) {
	if fieldMapper == nil {
		fieldMapper = func(s string) string { return s }
	}

	// Translate each named selection into a BQL sub-expression
	selectionExprs := make(map[string]string)
	for name, sg := range rule.Detection.Selections {
		expr, err := translateSelection(sg, fieldMapper)
		if err != nil {
			return "", fmt.Errorf("selection '%s': %w", name, err)
		}
		selectionExprs[name] = expr
	}

	// Parse and evaluate the condition string
	result, err := evaluateCondition(rule.Detection.Condition, selectionExprs)
	if err != nil {
		return "", fmt.Errorf("condition: %w", err)
	}

	return result, nil
}

// translateSelection converts a SelectionGroup into a BQL expression.
func translateSelection(sg SelectionGroup, fieldMapper func(string) string) (string, error) {
	if len(sg.Alternatives) > 0 {
		// List of maps - OR the alternatives
		var parts []string
		for _, alt := range sg.Alternatives {
			expr, err := translateFieldConditions(alt, fieldMapper)
			if err != nil {
				return "", err
			}
			parts = append(parts, expr)
		}
		if len(parts) == 1 {
			return parts[0], nil
		}
		return "(" + strings.Join(parts, " OR ") + ")", nil
	}

	if len(sg.FieldConditions) > 0 {
		return translateFieldConditions(sg.FieldConditions, fieldMapper)
	}

	return "*", nil
}

// translateFieldConditions converts a list of field conditions into a BQL expression.
// Multiple conditions within a selection are AND'd.
func translateFieldConditions(conditions []FieldCondition, fieldMapper func(string) string) (string, error) {
	var parts []string

	for _, fc := range conditions {
		expr, err := translateFieldCondition(fc, fieldMapper)
		if err != nil {
			return "", err
		}
		parts = append(parts, expr)
	}

	if len(parts) == 0 {
		return "*", nil
	}
	if len(parts) == 1 {
		return parts[0], nil
	}
	return "(" + strings.Join(parts, " AND ") + ")", nil
}

// translateFieldCondition converts a single FieldCondition into a BQL expression.
func translateFieldCondition(fc FieldCondition, fieldMapper func(string) string) (string, error) {
	field := fc.Field
	if field != "" {
		field = fieldMapper(field)
	}

	// Check for unsupported modifiers
	for _, mod := range fc.Modifiers {
		switch mod {
		case "contains", "startswith", "endswith", "re", "all",
			"base64", "base64offset", "cidr", "windash", "expand",
			"utf16le", "utf16be", "utf16", "wide":
		default:
			// Unknown modifier - pass through but warn
		}
	}

	hasAll := containsModifier(fc.Modifiers, "all")
	hasBase64 := containsModifier(fc.Modifiers, "base64") || containsModifier(fc.Modifiers, "base64offset")
	hasCIDR := containsModifier(fc.Modifiers, "cidr")

	if hasBase64 {
		return "", fmt.Errorf("field '%s': base64/base64offset modifiers are not supported", fc.Field)
	}
	if hasCIDR {
		return "", fmt.Errorf("field '%s': cidr modifier is not supported", fc.Field)
	}

	// Handle null values (field should be empty/absent)
	if fc.Values == nil {
		if field == "" {
			return "*", nil
		}
		return fmt.Sprintf("NOT %s=*", field), nil
	}

	// Determine the match type from modifiers
	matchType := "exact"
	for _, mod := range fc.Modifiers {
		switch mod {
		case "contains":
			matchType = "contains"
		case "startswith":
			matchType = "startswith"
		case "endswith":
			matchType = "endswith"
		case "re":
			matchType = "regex"
		}
	}

	// Generate expressions for each value
	var valueExprs []string
	for _, val := range fc.Values {
		expr, err := buildMatchExpr(field, val, matchType)
		if err != nil {
			return "", err
		}
		valueExprs = append(valueExprs, expr)
	}

	if len(valueExprs) == 0 {
		return "*", nil
	}

	if len(valueExprs) == 1 {
		return valueExprs[0], nil
	}

	// |all modifier: every value must match, so AND the individual expressions.
	if hasAll {
		return "(" + strings.Join(valueExprs, " AND ") + ")", nil
	}

	// OR semantics.
	// Keyword searches (no field) always use regex contains on raw_log.
	if field == "" {
		return buildCombinedRegexExpr("", fc.Values, "contains")
	}
	// Exact matches stay as OR'd equalities, cheaper than regex in ClickHouse.
	if matchType == "exact" {
		return "(" + strings.Join(valueExprs, " OR ") + ")", nil
	}
	// regex type always consolidates into an alternation regex.
	if matchType == "regex" {
		return buildCombinedRegexExpr(field, fc.Values, matchType)
	}
	// contains/startswith/endswith: use native =~/=^/=$ operators when no value
	// contains a comma (which would corrupt the comma-delimited list syntax).
	if !valuesHaveComma(fc.Values) {
		return buildNativeOpExpr(field, fc.Values, matchType)
	}
	return buildCombinedRegexExpr(field, fc.Values, matchType)
}

// buildNativeOpExpr emits a =~/=^/=$ expression for OR-semantics
// contains/startswith/endswith. Values must not contain commas.
func buildNativeOpExpr(field string, values []string, matchType string) (string, error) {
	var op string
	switch matchType {
	case "contains":
		op = "=~"
	case "startswith":
		op = "=^"
	case "endswith":
		op = "=$"
	default:
		return "", fmt.Errorf("unknown match type for native op: %s", matchType)
	}
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = encodeBQLNativeValue(v)
	}
	return fmt.Sprintf("%s%s%s", field, op, strings.Join(parts, ",")), nil
}

// encodeBQLNativeValue encodes a value for use in =~/=^/=$ value lists.
// Values whose characters are all safe for the BQL identifier lexer are
// returned bare; all others are returned as a double-quoted BQL string.
func encodeBQLNativeValue(v string) string {
	safe := len(v) > 0
	for _, ch := range v {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' ||
			ch >= '0' && ch <= '9' || ch == '_' || ch == '-' || ch == '.' || ch == '*') {
			safe = false
			break
		}
	}
	if safe {
		return v
	}
	var b strings.Builder
	b.WriteRune('"')
	for _, ch := range v {
		if ch == '\\' || ch == '"' {
			b.WriteRune('\\')
		}
		b.WriteRune(ch)
	}
	b.WriteRune('"')
	return b.String()
}

// valuesHaveComma reports whether any value contains a literal comma, which
// would corrupt the comma-delimited =~/=^/=$ list syntax.
func valuesHaveComma(values []string) bool {
	for _, v := range values {
		if strings.Contains(v, ",") {
			return true
		}
	}
	return false
}

// buildCombinedRegexExpr consolidates multiple OR'd values of the same regex
// match type into a single BQL regex using alternation. The result is one
// match() call in the generated SQL rather than one per value. The combination
// is exactly equivalent to OR'ing the individual single-value patterns.
func buildCombinedRegexExpr(field string, values []string, matchType string) (string, error) {
	pieces := make([]string, 0, len(values))
	for _, v := range values {
		if matchType == "regex" {
			// Raw user regex: wrap each in a non-capturing group so internal
			// anchors and top-level alternation keep their precedence.
			pieces = append(pieces, "(?:"+v+")")
		} else {
			pieces = append(pieces, escapeRegex(v))
		}
	}
	alt := strings.Join(pieces, "|")

	switch matchType {
	case "regex":
		return fmt.Sprintf("%s=/%s/", field, alt), nil
	case "contains":
		if field == "" {
			return fmt.Sprintf("/.*(?:%s).*/i", alt), nil
		}
		return fmt.Sprintf("%s=/.*(?:%s).*/i", field, alt), nil
	case "startswith":
		return fmt.Sprintf("%s=/^(?:%s).*/i", field, alt), nil
	case "endswith":
		return fmt.Sprintf("%s=/.*(?:%s)$/i", field, alt), nil
	default:
		return "", fmt.Errorf("unknown match type for combination: %s", matchType)
	}
}

// buildMatchExpr creates a single field=value BQL expression.
func buildMatchExpr(field, value, matchType string) (string, error) {
	// If no field, it's a keyword search on raw_log
	if field == "" {
		escaped := escapeRegex(value)
		return fmt.Sprintf("/.*%s.*/i", escaped), nil
	}

	switch matchType {
	case "exact":
		return fmt.Sprintf("%s=\"%s\"", field, escapeBQLString(value)), nil
	case "contains":
		if !strings.Contains(value, ",") {
			return fmt.Sprintf("%s=~%s", field, encodeBQLNativeValue(value)), nil
		}
		escaped := escapeRegex(value)
		return fmt.Sprintf("%s=/.*%s.*/i", field, escaped), nil
	case "startswith":
		if !strings.Contains(value, ",") {
			return fmt.Sprintf("%s=^%s", field, encodeBQLNativeValue(value)), nil
		}
		escaped := escapeRegex(value)
		return fmt.Sprintf("%s=/^%s.*/i", field, escaped), nil
	case "endswith":
		if !strings.Contains(value, ",") {
			return fmt.Sprintf("%s=$%s", field, encodeBQLNativeValue(value)), nil
		}
		escaped := escapeRegex(value)
		return fmt.Sprintf("%s=/.*%s$/i", field, escaped), nil
	case "regex":
		return fmt.Sprintf("%s=/%s/", field, value), nil
	default:
		return "", fmt.Errorf("unknown match type: %s", matchType)
	}
}

// escapeRegex escapes special regex characters in a value for safe embedding in /pattern/.
// Also escapes '/' since it is the regex delimiter in BQL.
func escapeRegex(s string) string {
	special := `\.+*?^${}()|[]/`
	var b strings.Builder
	for _, ch := range s {
		if strings.ContainsRune(special, ch) {
			b.WriteRune('\\')
		}
		b.WriteRune(ch)
	}
	return b.String()
}

// escapeBQLString escapes a value for safe embedding in "quoted string".
func escapeBQLString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return s
}

func containsModifier(mods []string, target string) bool {
	for _, m := range mods {
		if m == target {
			return true
		}
	}
	return false
}

// --- Condition parser ---
// Parses Sigma condition strings like:
//   "selection and not filter"
//   "(selection1 or selection2) and not filter"
//   "1 of selection*"
//   "all of them"

type condParser struct {
	tokens []string
	pos    int
	exprs  map[string]string // selection name -> BQL expression
}

func evaluateCondition(condition string, selectionExprs map[string]string) (string, error) {
	// Check for unsupported aggregation syntax (contains pipe)
	if strings.Contains(condition, "|") {
		return "", fmt.Errorf("Sigma aggregation conditions (with '|') are not supported")
	}

	tokens := tokenizeCondition(condition)
	if len(tokens) == 0 {
		return "", fmt.Errorf("empty condition")
	}

	p := &condParser{
		tokens: tokens,
		pos:    0,
		exprs:  selectionExprs,
	}

	result, err := p.parseOr()
	if err != nil {
		return "", err
	}

	if p.pos < len(p.tokens) {
		return "", fmt.Errorf("unexpected token '%s' at position %d", p.tokens[p.pos], p.pos)
	}

	return result, nil
}

// tokenizeCondition splits a condition string into tokens.
func tokenizeCondition(cond string) []string {
	var tokens []string
	var current strings.Builder

	flush := func() {
		if current.Len() > 0 {
			tokens = append(tokens, current.String())
			current.Reset()
		}
	}

	runes := []rune(cond)
	for i := 0; i < len(runes); i++ {
		ch := runes[i]
		switch {
		case ch == '(' || ch == ')':
			flush()
			tokens = append(tokens, string(ch))
		case unicode.IsSpace(ch):
			flush()
		default:
			current.WriteRune(ch)
		}
	}
	flush()

	return tokens
}

func (p *condParser) peek() string {
	if p.pos >= len(p.tokens) {
		return ""
	}
	return p.tokens[p.pos]
}

func (p *condParser) advance() string {
	t := p.peek()
	p.pos++
	return t
}

// parseOr handles: expr ("or" expr)*
func (p *condParser) parseOr() (string, error) {
	left, err := p.parseAnd()
	if err != nil {
		return "", err
	}

	for strings.ToLower(p.peek()) == "or" {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return "", err
		}
		left = "(" + left + " OR " + right + ")"
	}

	return left, nil
}

// parseAnd handles: factor ("and" factor)*
func (p *condParser) parseAnd() (string, error) {
	left, err := p.parseFactor()
	if err != nil {
		return "", err
	}

	for strings.ToLower(p.peek()) == "and" {
		p.advance()
		right, err := p.parseFactor()
		if err != nil {
			return "", err
		}
		left = "(" + left + " AND " + right + ")"
	}

	return left, nil
}

// parseFactor handles: "not" factor | "(" expr ")" | quantifier | IDENT
func (p *condParser) parseFactor() (string, error) {
	tok := p.peek()

	// NOT
	if strings.ToLower(tok) == "not" {
		p.advance()
		inner, err := p.parseFactor()
		if err != nil {
			return "", err
		}
		return "NOT (" + inner + ")", nil
	}

	// Parenthesized expression
	if tok == "(" {
		p.advance()
		expr, err := p.parseOr()
		if err != nil {
			return "", err
		}
		if p.peek() != ")" {
			return "", fmt.Errorf("expected ')' but got '%s'", p.peek())
		}
		p.advance()
		return "(" + expr + ")", nil
	}

	// Quantifiers: "1 of ...", "all of ..."
	tokLower := strings.ToLower(tok)
	if tokLower == "1" || tokLower == "all" {
		return p.parseQuantifier()
	}

	// Selection reference
	p.advance()
	expr, ok := p.exprs[tok]
	if !ok {
		return "", fmt.Errorf("unknown selection '%s'", tok)
	}
	return expr, nil
}

// parseQuantifier handles: ("1" | "all") "of" (IDENT_GLOB | "them")
func (p *condParser) parseQuantifier() (string, error) {
	quantifier := strings.ToLower(p.advance()) // "1" or "all"

	if strings.ToLower(p.peek()) != "of" {
		return "", fmt.Errorf("expected 'of' after '%s'", quantifier)
	}
	p.advance() // consume "of"

	target := p.advance()
	if target == "" {
		return "", fmt.Errorf("expected target after 'of'")
	}

	var matchedExprs []string

	if strings.ToLower(target) == "them" {
		// All selections
		for _, expr := range p.exprs {
			matchedExprs = append(matchedExprs, expr)
		}
	} else if strings.HasSuffix(target, "*") {
		// Glob match on selection names
		prefix := strings.TrimSuffix(target, "*")
		for name, expr := range p.exprs {
			if strings.HasPrefix(name, prefix) {
				matchedExprs = append(matchedExprs, expr)
			}
		}
	} else {
		// Exact match
		expr, ok := p.exprs[target]
		if !ok {
			return "", fmt.Errorf("unknown selection '%s'", target)
		}
		matchedExprs = []string{expr}
	}

	if len(matchedExprs) == 0 {
		return "", fmt.Errorf("no selections matched '%s'", target)
	}

	if len(matchedExprs) == 1 {
		return matchedExprs[0], nil
	}

	var joiner string
	if quantifier == "all" {
		joiner = " AND "
	} else {
		joiner = " OR "
	}

	return "(" + strings.Join(matchedExprs, joiner) + ")", nil
}
