package sigma

import (
	"fmt"
	"strings"
	"unicode"
)

// Translate converts a parsed Sigma rule's detection logic into a Quandrix query string.
// fieldMapper optionally transforms Sigma field names to match stored field names.
func Translate(rule *SigmaRule, fieldMapper func(string) string) (string, error) {
	if fieldMapper == nil {
		fieldMapper = func(s string) string { return s }
	}

	// Translate each named selection into a Quandrix sub-expression
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

// translateSelection converts a SelectionGroup into a Quandrix expression.
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

// translateFieldConditions converts a list of field conditions into a Quandrix expression.
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

// translateFieldCondition converts a single FieldCondition into a Quandrix expression.
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

	// If |all modifier: AND the values; otherwise OR them
	if hasAll || len(valueExprs) == 1 {
		if len(valueExprs) == 1 {
			return valueExprs[0], nil
		}
		return "(" + strings.Join(valueExprs, " AND ") + ")", nil
	}

	return "(" + strings.Join(valueExprs, " OR ") + ")", nil
}

// buildMatchExpr creates a single field=value Quandrix expression.
func buildMatchExpr(field, value, matchType string) (string, error) {
	// If no field, it's a keyword search on raw_log
	if field == "" {
		escaped := escapeRegex(value)
		return fmt.Sprintf("/.*%s.*/i", escaped), nil
	}

	switch matchType {
	case "exact":
		return fmt.Sprintf("%s=\"%s\"", field, escapeQuandrixString(value)), nil
	case "contains":
		escaped := escapeRegex(value)
		return fmt.Sprintf("%s=/.*%s.*/i", field, escaped), nil
	case "startswith":
		escaped := escapeRegex(value)
		return fmt.Sprintf("%s=/^%s.*/i", field, escaped), nil
	case "endswith":
		escaped := escapeRegex(value)
		return fmt.Sprintf("%s=/.*%s$/i", field, escaped), nil
	case "regex":
		return fmt.Sprintf("%s=/%s/", field, value), nil
	default:
		return "", fmt.Errorf("unknown match type: %s", matchType)
	}
}

// escapeRegex escapes special regex characters in a value for safe embedding in /pattern/.
// Also escapes '/' since it is the regex delimiter in Quandrix.
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

// escapeQuandrixString escapes a value for safe embedding in "quoted string".
func escapeQuandrixString(s string) string {
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
	exprs  map[string]string // selection name -> Quandrix expression
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
