package sigma

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
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
//	v4: scope rules by logsource.category via a bifract_category prefilter. Without
//	    it a rule's detection ran against every event type, so for example a
//	    create_stream_hash rule matching on Hashes fired on every process-creation
//	    event, which also carries an IMPHASH.
//	v5: collapse Sigma's four registry categories onto registry_event.
//	v6: honor '*' and '?' wildcards and match every non-regex value
//	    case-insensitively, as Sigma specifies; refuse unsupported modifiers
//	    instead of dropping them; support |re|i|m|s; escape '/' in |re values;
//	    leave underscore selections out of "them"; emit fields and quantified
//	    selections in sorted order.
//	v7: compare cased exact values as lower(field) IN (...) instead of a regex.
const TranslatorVersion = "v7"

// sigmaCategoryAliases maps Sigma logsource categories onto Bifract's canonical
// bifract_category vocabulary where the two names differ. Categories not listed pass
// through unchanged, so Sigma's taxonomy is the vocabulary a normalizer should target.
var sigmaCategoryAliases = map[string]string{
	"network_connection":   "network_connect",
	"dns_query_unfiltered": "dns_query",
	"file_event":           "file_write",
	"file_create":          "file_write",
	"create_remote_thread": "remote_thread",

	// Sigma splits registry activity four ways (Sysmon 12/13/14). Collapsing them onto one
	// category spares normalizers from reproducing that split, and costs almost no precision
	// because registry rules carry the discriminator (EventType: SetValue, DeleteValue, ...)
	// inside their own detection block.
	"registry_add":    "registry_event",
	"registry_set":    "registry_event",
	"registry_delete": "registry_event",
	"registry_rename": "registry_event",
}

// CategoryFilter returns the bifract_category prefilter for a rule's logsource, or ""
// when the rule is not category-scoped. Product/service-only rules (Windows Security log
// rules, for instance) return "" deliberately: they scope themselves via EventID inside
// their own detection block, and forcing a category on them would match nothing.
func CategoryFilter(ls LogSource) string {
	cat := strings.ToLower(strings.TrimSpace(ls.Category))
	if cat == "" {
		return ""
	}
	if alias, ok := sigmaCategoryAliases[cat]; ok {
		cat = alias
	}
	return "bifract_category=" + encodeBQLNativeValue(cat)
}

// Translate converts a parsed Sigma rule's detection logic into a BQL query string.
// fieldMapper optionally transforms Sigma field names to match stored field names.
func Translate(rule *SigmaRule, fieldMapper func(string) string) (string, error) {
	if fieldMapper == nil {
		fieldMapper = func(s string) string { return s }
	}

	// Translate each named selection into a BQL sub-expression
	selectionExprs := make(map[string]string)
	for _, name := range sortedKeys(rule.Detection.Selections) {
		sg := rule.Detection.Selections[name]
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

	// Scope the detection to the rule's logsource category. This goes first so the
	// low-cardinality equality prunes before the detection predicates run.
	if cf := CategoryFilter(rule.LogSource); cf != "" && result != "" {
		result = cf + " AND (" + result + ")"
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

	if err := checkModifiers(fc); err != nil {
		return "", err
	}
	hasAll := containsModifier(fc.Modifiers, "all")

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

	if len(fc.Values) == 0 {
		return "*", nil
	}

	// re: the author's own pattern, matched case-sensitively as Sigma specifies
	// unless |i says otherwise. No wildcard or case handling applies.
	if matchType == "regex" {
		inline, suffix := regexFlags(fc.Modifiers)
		if hasAll || len(fc.Values) == 1 {
			parts := make([]string, len(fc.Values))
			for i, v := range fc.Values {
				parts[i] = fmt.Sprintf("%s=/%s%s/%s", field, inline, escapeRegexDelimiter(v), suffix)
			}
			return joinExprs(parts, " AND "), nil
		}
		return buildCombinedRegexExpr(field, fc.Values, inline, suffix), nil
	}

	// Keywords (no field): a case-insensitive substring search over the whole event.
	if field == "" {
		return buildKeywordExpr(fc.Values), nil
	}

	matches := make([]valueMatch, len(fc.Values))
	for i, v := range fc.Values {
		matches[i] = classifyValue(v, matchType)
	}

	// |all: every value must match, so AND one predicate per value.
	if hasAll {
		parts := make([]string, len(matches))
		for i, m := range matches {
			parts[i] = renderMatches(field, []valueMatch{m})
		}
		return joinExprs(parts, " AND "), nil
	}
	return renderMatches(field, matches), nil
}

// matchModifiers are the modifiers that choose how a value is compared. At most one
// may appear: Sigma gives no meaning to combining them.
var matchModifiers = map[string]bool{"contains": true, "startswith": true, "endswith": true, "re": true}

// regexFlagModifiers are the |re sub-modifiers: case-insensitive, multi-line (^ and $
// match at line breaks) and single-line (. matches a line break).
var regexFlagModifiers = map[string]bool{"i": true, "m": true, "s": true}

// regexFlags renders |re|i|m|s as BQL: i as the /i flag, m and s as an inline RE2
// flag group, which applies to the whole pattern including every alternative.
func regexFlags(mods []string) (inline, suffix string) {
	var group string
	for _, f := range []string{"m", "s"} {
		if containsModifier(mods, f) {
			group += f
		}
	}
	if group != "" {
		inline = "(?" + group + ")"
	}
	if containsModifier(mods, "i") {
		suffix = "i"
	}
	return inline, suffix
}

// checkModifiers refuses any modifier the translator does not implement. A modifier
// silently dropped would deploy a different rule than the one written (windash, a
// value transform, or gt/lt, a different comparison, would match something else
// entirely), so the rule fails instead, naming what it could not express.
func checkModifiers(fc FieldCondition) error {
	var unsupported, match []string
	isRegex := containsModifier(fc.Modifiers, "re")
	for _, mod := range fc.Modifiers {
		switch {
		case matchModifiers[mod]:
			match = append(match, mod)
		case mod == "all", isRegex && regexFlagModifiers[mod]:
		default:
			unsupported = append(unsupported, "'"+mod+"'")
		}
	}
	if len(unsupported) > 0 {
		return fmt.Errorf("field '%s': unsupported modifier %s", fc.Field, strings.Join(unsupported, ", "))
	}
	if len(match) > 1 {
		return fmt.Errorf("field '%s': modifiers '%s' cannot be combined", fc.Field, strings.Join(match, "', '"))
	}
	return nil
}

// Sigma string values are glob patterns matched case-insensitively against the whole
// field value: '*' is any run of characters, '?' is exactly one, and '\' escapes
// either (or itself) to make it literal. A backslash before anything else is a
// plain backslash, so Windows paths need no escaping. contains, startswith and
// endswith are the same glob with a '*' added on the open side(s).

// globToken is one piece of a parsed Sigma value: a literal run, or a wildcard.
type globToken struct {
	lit  string
	wild byte // 0 for a literal run, '*' or '?'
}

// parseSigmaValue splits a Sigma value into literal runs and wildcards, resolving
// escapes. Consecutive '*' collapse to one.
func parseSigmaValue(v string) []globToken {
	var toks []globToken
	var lit strings.Builder
	flush := func() {
		if lit.Len() > 0 {
			toks = append(toks, globToken{lit: lit.String()})
			lit.Reset()
		}
	}
	for i := 0; i < len(v); i++ {
		c := v[i]
		switch {
		case c == '\\' && i+1 < len(v) && (v[i+1] == '*' || v[i+1] == '?' || v[i+1] == '\\'):
			i++
			lit.WriteByte(v[i])
		case c == '*' || c == '?':
			flush()
			if c == '*' && len(toks) > 0 && toks[len(toks)-1].wild == '*' {
				continue
			}
			toks = append(toks, globToken{wild: c})
		default:
			lit.WriteByte(c)
		}
	}
	flush()
	return toks
}

// trimStars drops leading and trailing '*' wildcards, reporting which ends had one.
// A '*' at an end means that end is unanchored, which a substring, prefix or suffix
// match already expresses without the wildcard.
func trimStars(toks []globToken) (rest []globToken, lead, trail bool) {
	for len(toks) > 0 && toks[0].wild == '*' {
		toks, lead = toks[1:], true
	}
	for len(toks) > 0 && toks[len(toks)-1].wild == '*' {
		toks, trail = toks[:len(toks)-1], true
	}
	return toks, lead, trail
}

// globRegex renders glob tokens as an RE2 fragment, reporting whether it holds a
// wildcard (whose '.' must be allowed to cross newlines).
func globRegex(toks []globToken) (pattern string, wild bool) {
	var b strings.Builder
	for _, t := range toks {
		switch t.wild {
		case '*':
			b.WriteString(".*")
			wild = true
		case '?':
			b.WriteString(".")
			wild = true
		default:
			b.WriteString(escapeRegex(t.lit))
		}
	}
	return b.String(), wild
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// hasCaseVariants reports whether some other string equals s case-insensitively.
// When it does not (digits, punctuation), a case-sensitive equality is exact.
func hasCaseVariants(s string) bool {
	for _, r := range s {
		if unicode.SimpleFold(r) != r {
			return true
		}
	}
	return false
}

type matchKind int

const (
	matchAny    matchKind = iota // the value matches every present value
	matchEqual                   // equality; only for values with no case variants
	matchLower                   // lower(field) equality; ASCII values only
	matchNative                  // =~ / =^ / =$, case-insensitive literal operators
	matchRegex                   // case-insensitive RE2
)

// valueMatch is how one Sigma value is expressed in BQL.
type valueMatch struct {
	kind    matchKind
	op      string // matchNative: "=~", "=^" or "=$"
	value   string // matchEqual, matchNative: the literal; matchLower: lowercased
	pattern string // matchRegex: anchored RE2
	wild    bool   // matchRegex: pattern holds a wildcard
}

// classifyValue picks the cheapest BQL form that matches a Sigma value exactly as
// Sigma does: wildcards honored, case ignored.
//
//   - A value without case variants under an exact match is an equality. It is the
//     only form the field's skip index can prune with, and is exact for digits and
//     punctuation (EventID: 4688).
//   - A literal under a substring, prefix or suffix match uses =~, =^ or =$, which
//     are case-insensitive. A '*' at one end of an exact value makes it one of
//     these, so the common '*\cmd.exe' is a suffix match, not a regex.
//   - A cased ASCII value under an exact match compares lower(field), about four
//     times cheaper than an anchored regex. It cannot use the skip index, which is
//     over the value as stored. ClickHouse lower() folds ASCII only, so a value
//     with other letters stays a regex.
//   - Everything else is a case-insensitive regex: a wildcard in the middle, '?',
//     or a literal comma (which would split the native list).
func classifyValue(v string, matchType string) valueMatch {
	anchorStart := matchType == "exact" || matchType == "startswith"
	anchorEnd := matchType == "exact" || matchType == "endswith"
	toks, lead, trail := trimStars(parseSigmaValue(v))
	if lead {
		anchorStart = false
	}
	if trail {
		anchorEnd = false
	}
	if len(toks) == 0 && (lead || trail) {
		return valueMatch{kind: matchAny}
	}

	if len(toks) <= 1 && (len(toks) == 0 || toks[0].wild == 0) {
		lit := ""
		if len(toks) == 1 {
			lit = toks[0].lit
		}
		switch {
		case anchorStart && anchorEnd:
			if !hasCaseVariants(lit) {
				return valueMatch{kind: matchEqual, value: lit}
			}
			if isASCII(lit) {
				return valueMatch{kind: matchLower, value: strings.ToLower(lit)}
			}
		case !strings.Contains(lit, ","):
			op := "=~"
			if anchorStart {
				op = "=^"
			} else if anchorEnd {
				op = "=$"
			}
			return valueMatch{kind: matchNative, op: op, value: lit}
		}
	}

	pattern, wild := globRegex(toks)
	if anchorStart {
		pattern = "^" + pattern
	}
	if anchorEnd {
		pattern += "$"
	}
	return valueMatch{kind: matchRegex, pattern: pattern, wild: wild}
}

// renderMatches ORs the values of one field. Each form is consolidated: equalities
// stay separate (each can use the skip index), lowercase equalities share one set,
// native values share one operator list per operator, and regexes share one
// alternation, so the SQL runs one IN, multiSearch or match() per field instead of
// one per value. The set is written as in() because lower(f)="v" does not parse
// before a =~ leaf.
func renderMatches(field string, matches []valueMatch) string {
	var parts, lowers, patterns []string
	native := map[string][]string{}
	wild := false
	for _, m := range matches {
		switch m.kind {
		case matchAny:
			return field + "=*"
		case matchEqual:
			parts = append(parts, fmt.Sprintf(`%s="%s"`, field, escapeBQLString(m.value)))
		case matchLower:
			lowers = append(lowers, `"`+escapeBQLString(m.value)+`"`)
		case matchNative:
			native[m.op] = append(native[m.op], m.value)
		case matchRegex:
			patterns = append(patterns, m.pattern)
			wild = wild || m.wild
		}
	}
	if len(lowers) > 0 {
		parts = append(parts, fmt.Sprintf("in(lower(%s), values=[%s])", field, strings.Join(lowers, ",")))
	}
	for _, op := range []string{"=~", "=^", "=$"} {
		if vals := native[op]; len(vals) > 0 {
			parts = append(parts, buildNativeOpExpr(field, op, vals))
		}
	}
	if len(patterns) > 0 {
		flags := ""
		if wild {
			flags = "(?s)"
		}
		parts = append(parts, fmt.Sprintf("%s=/%s%s/i", field, flags, strings.Join(patterns, "|")))
	}
	return joinExprs(parts, " OR ")
}

// buildNativeOpExpr emits a =~/=^/=$ expression. Values must not contain commas.
func buildNativeOpExpr(field, op string, values []string) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = encodeBQLNativeValue(v)
	}
	return field + op + strings.Join(parts, ",")
}

// buildKeywordExpr ORs keyword values (no field) as one case-insensitive regex over
// the whole event. Keywords are substring matches, so their wildcards apply inside.
func buildKeywordExpr(values []string) string {
	pieces := make([]string, 0, len(values))
	for _, v := range values {
		toks, lead, trail := trimStars(parseSigmaValue(v))
		if len(toks) == 0 && (lead || trail) {
			return "*"
		}
		p, _ := globRegex(toks)
		pieces = append(pieces, p)
	}
	if len(pieces) == 1 {
		return "/.*" + pieces[0] + ".*/i"
	}
	return "/.*(?:" + strings.Join(pieces, "|") + ").*/i"
}

func joinExprs(parts []string, op string) string {
	if len(parts) == 1 {
		return parts[0]
	}
	return "(" + strings.Join(parts, op) + ")"
}

// encodeBQLNativeValue encodes a value for use in =~/=^/=$ value lists.
// Values the BQL lexer reads back as one bare word are returned bare; all others
// are returned as a double-quoted BQL string. A bare word must start with a
// letter, digit or underscore ("-enc" would lex as a minus sign and fail the
// whole rule) and must not be a boolean keyword.
func encodeBQLNativeValue(v string) string {
	safe := len(v) > 0 && isBareWordStart(v[0]) && !isBQLKeyword(v)
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

func isBareWordStart(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_'
}

func isBQLKeyword(v string) bool {
	return strings.EqualFold(v, "and") || strings.EqualFold(v, "or") || strings.EqualFold(v, "not")
}

// buildCombinedRegexExpr consolidates OR'd |re values into one alternation, each
// wrapped in a non-capturing group so its own anchors and alternation keep their
// precedence. One match() call instead of one per value, with identical results.
func buildCombinedRegexExpr(field string, values []string, inline, suffix string) string {
	pieces := make([]string, len(values))
	for i, v := range values {
		pieces[i] = "(?:" + escapeRegexDelimiter(v) + ")"
	}
	return fmt.Sprintf("%s=/%s%s/%s", field, inline, strings.Join(pieces, "|"), suffix)
}

// escapeRegexDelimiter escapes each unescaped '/' in an author's regex, which would
// otherwise end the BQL /pattern/ early and fail the rule. RE2 reads \/ as '/', in a
// character class too, so the pattern means what it did. Existing escapes, \/
// included, are copied untouched.
func escapeRegexDelimiter(v string) string {
	if !strings.Contains(v, "/") {
		return v
	}
	var b strings.Builder
	for i := 0; i < len(v); i++ {
		switch c := v[i]; {
		case c == '\\' && i+1 < len(v):
			b.WriteByte(c)
			i++
			b.WriteByte(v[i])
		case c == '/':
			b.WriteString(`\/`)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
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
		// All selections, except those named with a leading underscore: Sigma reserves
		// those for helpers a condition references by name or explicit pattern only.
		for _, name := range sortedKeys(p.exprs) {
			if !strings.HasPrefix(name, "_") {
				matchedExprs = append(matchedExprs, p.exprs[name])
			}
		}
	} else if strings.HasSuffix(target, "*") {
		// Glob match on selection names
		prefix := strings.TrimSuffix(target, "*")
		for _, name := range sortedKeys(p.exprs) {
			if strings.HasPrefix(name, prefix) {
				matchedExprs = append(matchedExprs, p.exprs[name])
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
