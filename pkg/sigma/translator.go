package sigma

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"bifract/pkg/parser"
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
//	v8: match an escaped lone '*' as a literal, not as field presence.
//	v9: support |windash.
//	v10: search keywords in norm_log as JSON serialized them ('/', '\' and '"'
//	     escaped), so keywords holding those characters match.
//	v11: support cidr, exists, fieldref, gt/gte/lt/lte, cased, base64, base64offset
//	     and the UTF-16 encodings. Keep a value with a comma in the native list;
//	     match non-ASCII substrings and keywords with a Unicode-aware regex; make
//	     '?' in a keyword one serialized character; read an empty non-exact value
//	     as field presence; refuse field names that are not plain identifiers.
const TranslatorVersion = "v11"

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
		if !validFieldName(field) {
			return "", fmt.Errorf("field '%s': %q is not a valid field name", fc.Field, field)
		}
	}

	if err := checkModifiers(fc); err != nil {
		return "", err
	}
	if expr, ok, err := translateStandalone(fc, field, fieldMapper); ok {
		if err != nil {
			return "", fmt.Errorf("field '%s': %w", fc.Field, err)
		}
		return expr, nil
	}
	hasAll := containsModifier(fc.Modifiers, "all")
	cased := containsModifier(fc.Modifiers, "cased")

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

	// Each value with its |windash variants or base64 encodings, any of which
	// matches for it.
	windash := containsModifier(fc.Modifiers, "windash")
	encoded := containsModifier(fc.Modifiers, "base64") || containsModifier(fc.Modifiers, "base64offset")
	groups := make([][]string, len(fc.Values))
	var all []string
	for i, v := range fc.Values {
		groups[i] = []string{v}
		switch {
		case windash:
			variants, err := expandWindash(v)
			if err != nil {
				return "", fmt.Errorf("field '%s': %w", fc.Field, err)
			}
			groups[i] = variants
		case encoded:
			variants, err := encodeValue(v, fc.Modifiers)
			if err != nil {
				return "", fmt.Errorf("field '%s': %w", fc.Field, err)
			}
			// Encoded text is case-significant.
			groups[i], cased = variants, true
		}
		all = append(all, groups[i]...)
	}

	// Keywords (no field): a substring search over the whole event.
	if field == "" {
		return buildKeywordExpr(all, cased), nil
	}

	classify := func(values []string) []valueMatch {
		matches := make([]valueMatch, len(values))
		for i, v := range values {
			matches[i] = classifyValue(v, matchType, cased)
		}
		return matches
	}

	// |all: every value must match, so AND one predicate per value.
	if hasAll {
		parts := make([]string, len(groups))
		for i, g := range groups {
			parts[i] = renderMatches(field, classify(g))
		}
		return joinExprs(parts, " AND "), nil
	}
	return renderMatches(field, classify(all)), nil
}

// windashChars are the dashes Windows tools accept before a flag: hyphen, slash, en
// dash, em dash and horizontal bar.
var windashChars = []rune{'-', '/', '\u2013', '\u2014', '\u2015'}

// maxWindashDashes is pySigma's limit, which keeps a value to 125 variants.
const maxWindashDashes = 3

// expandWindash returns every variant of a Sigma value with each flag dash, a - or /
// that starts a word, replaced by each of windashChars. Dashes inside a word, as in
// "a-b", are left alone, matching pySigma's \B[-/]\b.
func expandWindash(v string) ([]string, error) {
	runes := []rune(v)
	var at []int
	for i, r := range runes {
		if (r == '-' || r == '/') && (i == 0 || !isWordRune(runes[i-1])) && i+1 < len(runes) && isWordRune(runes[i+1]) {
			at = append(at, i)
		}
	}
	if len(at) > maxWindashDashes {
		return nil, fmt.Errorf("windash expands at most %d dashes in one value, %q has %d", maxWindashDashes, v, len(at))
	}
	variants := [][]rune{runes}
	for _, i := range at {
		next := make([][]rune, 0, len(variants)*len(windashChars))
		for _, base := range variants {
			for _, d := range windashChars {
				r := slices.Clone(base)
				r[i] = d
				next = append(next, r)
			}
		}
		variants = next
	}
	out := make([]string, len(variants))
	for i, r := range variants {
		out[i] = string(r)
	}
	return out, nil
}

func isWordRune(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
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

// standaloneModifiers each replace the value comparison outright, so they take no
// other modifier.
var standaloneModifiers = map[string]bool{"exists": true, "fieldref": true, "cidr": true, "gt": true, "gte": true, "lt": true, "lte": true}

// utf16Modifiers encode a value as UTF-16 bytes for base64 or base64offset to encode.
var utf16Modifiers = map[string]bool{"wide": true, "utf16le": true, "utf16be": true, "utf16": true}

// checkModifiers refuses any modifier the translator does not implement, and any
// combination Sigma gives no meaning. A modifier silently dropped would deploy a
// different rule than the one written, so the rule fails instead, naming what it
// could not express.
func checkModifiers(fc FieldCondition) error {
	var unsupported, match []string
	mods := fc.Modifiers
	isRegex := containsModifier(mods, "re")
	for _, mod := range mods {
		switch {
		case matchModifiers[mod]:
			match = append(match, mod)
		case mod == "all", mod == "cased", mod == "base64", mod == "base64offset", standaloneModifiers[mod], utf16Modifiers[mod]:
		case mod == "windash" && !isRegex, isRegex && regexFlagModifiers[mod]:
		default:
			unsupported = append(unsupported, "'"+mod+"'")
		}
	}
	fail := func(format string, args ...any) error {
		return fmt.Errorf("field '%s': "+format, append([]any{fc.Field}, args...)...)
	}
	if len(unsupported) > 0 {
		return fail("unsupported modifier %s", strings.Join(unsupported, ", "))
	}
	if len(match) > 1 {
		return fail("modifiers '%s' cannot be combined", strings.Join(match, "', '"))
	}
	for _, mod := range mods {
		if standaloneModifiers[mod] && len(mods) > 1 {
			return fail("'%s' cannot be combined with other modifiers", mod)
		}
	}
	if isRegex && containsModifier(mods, "cased") {
		return fail("'cased' cannot be combined with 're', which is already case-sensitive")
	}
	return checkEncoding(fc, fail)
}

// checkEncoding enforces how Sigma chains value encodings: at most one UTF-16
// encoding, then one of base64 and base64offset, matched as a literal.
func checkEncoding(fc FieldCondition, fail func(string, ...any) error) error {
	enc, b64 := -1, -1
	for i, mod := range fc.Modifiers {
		switch {
		case utf16Modifiers[mod]:
			if enc >= 0 {
				return fail("use one UTF-16 encoding, not '%s' and '%s'", fc.Modifiers[enc], mod)
			}
			enc = i
		case mod == "base64" || mod == "base64offset":
			if b64 >= 0 {
				return fail("use one of 'base64' and 'base64offset'")
			}
			b64 = i
		}
	}
	if enc >= 0 && (b64 < 0 || enc > b64) {
		return fail("'%s' needs 'base64' or 'base64offset' after it", fc.Modifiers[enc])
	}
	if b64 < 0 {
		return nil
	}
	b64mod := fc.Modifiers[b64]
	for _, other := range []string{"re", "windash"} {
		if containsModifier(fc.Modifiers, other) {
			return fail("'%s' cannot be combined with '%s'", b64mod, other)
		}
	}
	contains := containsModifier(fc.Modifiers, "contains")
	if b64mod == "base64offset" && !contains {
		return fail("'base64offset' needs 'contains': it yields fragments of the encoding")
	}
	if containsModifier(fc.Modifiers, "startswith") || containsModifier(fc.Modifiers, "endswith") {
		return fail("'base64' matches whole values or with 'contains'")
	}
	return nil
}

// numericPattern accepts plain decimals only: BQL also reads inf and nan as
// numbers, which a Sigma comparison must not match. It checks both a
// comparison's field, in the guard, and its Sigma value.
const numericPattern = `^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$`

var numericRe = regexp.MustCompile(numericPattern)

var comparisonOps = map[string]string{"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}

// translateStandalone translates the modifiers that replace the value comparison:
// exists, fieldref, cidr and the numeric comparisons. ok is false for any other.
func translateStandalone(fc FieldCondition, field string, fieldMapper func(string) string) (expr string, ok bool, err error) {
	if len(fc.Modifiers) != 1 || !standaloneModifiers[fc.Modifiers[0]] {
		return "", false, nil
	}
	mod := fc.Modifiers[0]
	if field == "" {
		return "", true, fmt.Errorf("'%s' needs a field", mod)
	}
	if len(fc.Values) == 0 {
		return "", true, fmt.Errorf("'%s' needs a value", mod)
	}
	var parts []string
	switch mod {
	case "exists":
		if len(fc.Values) == 1 {
			// pySigma reads rules as YAML 1.1, where yes and no are booleans too.
			switch strings.ToLower(fc.Values[0]) {
			case "true", "yes":
				return field + "=*", true, nil
			case "false", "no":
				return "NOT " + field + "=*", true, nil
			}
		}
		return "", true, fmt.Errorf("'exists' takes true or false")
	case "cidr":
		for _, v := range fc.Values {
			if _, _, err := net.ParseCIDR(v); err != nil {
				return "", true, fmt.Errorf("%q is not a CIDR range", v)
			}
			parts = append(parts, fmt.Sprintf(`cidr(%s, "%s")`, field, escapeBQLString(v)))
		}
		return joinExprs(parts, " OR "), true, nil
	case "fieldref":
		for _, v := range fc.Values {
			ref := fieldMapper(v)
			if !validFieldName(ref) {
				return "", true, fmt.Errorf("%q is not a valid field name", ref)
			}
			parts = append(parts, fmt.Sprintf("%s=field(%s)", field, ref))
		}
		// A missing field reads as '', which would equal another missing field.
		return fmt.Sprintf("(%s=* AND %s)", field, joinExprs(parts, " OR ")), true, nil
	}
	for _, v := range fc.Values {
		if !numericRe.MatchString(v) {
			return "", true, fmt.Errorf("%q is not a number", v)
		}
		n, err := strconv.ParseFloat(v, 64)
		mantissa, _, _ := strings.Cut(strings.ToLower(v), "e")
		if err != nil || (n == 0 && strings.ContainsAny(mantissa, "123456789")) {
			return "", true, fmt.Errorf("%q is out of range", v)
		}
		// BQL reads 1e300 but not 1e+300.
		parts = append(parts, field+comparisonOps[mod]+strings.Replace(strconv.FormatFloat(n, 'g', -1, 64), "e+", "e", 1))
	}
	// The guard keeps |lt 5 from matching inf, nan or other text BQL might read.
	return fmt.Sprintf("(%s=/%s/ AND %s)", field, numericPattern, joinExprs(parts, " OR ")), true, nil
}

// encodeValue returns a value's base64 encoding, or under base64offset its three
// encodings at each byte offset, after any UTF-16 encoding. It is pySigma's
// algorithm, so a rule matches the same text in every backend.
func encodeValue(v string, mods []string) ([]string, error) {
	lit, ok := soleLiteral(parseSigmaValue(v))
	if !ok {
		return nil, fmt.Errorf("a wildcard cannot be base64-encoded: %q", v)
	}
	if lit == "" {
		return nil, fmt.Errorf("an empty value has nothing to base64-encode")
	}
	b := []byte(lit)
	for _, mod := range mods {
		switch mod {
		case "wide", "utf16le":
			b = utf16Bytes(lit, binary.LittleEndian, false)
		case "utf16be":
			b = utf16Bytes(lit, binary.BigEndian, false)
		case "utf16":
			b = utf16Bytes(lit, binary.LittleEndian, true)
		}
	}
	if !containsModifier(mods, "base64offset") {
		return []string{base64.StdEncoding.EncodeToString(b)}, nil
	}
	starts, ends := [3]int{0, 2, 3}, [3]int{0, 3, 2}
	out := make([]string, 3)
	for i := range 3 {
		enc := base64.StdEncoding.EncodeToString(append(bytes.Repeat([]byte(" "), i), b...))
		// An empty fragment would match every value.
		if starts[i] >= len(enc)-ends[(len(b)+i)%3] {
			return nil, fmt.Errorf("%q is too short for base64offset: an offset encodes no whole character", v)
		}
		out[i] = enc[starts[i] : len(enc)-ends[(len(b)+i)%3]]
	}
	return out, nil
}

// soleLiteral returns the text of a value without wildcards: no tokens, or one literal.
func soleLiteral(toks []globToken) (string, bool) {
	switch {
	case len(toks) == 0:
		return "", true
	case len(toks) == 1 && toks[0].wild == 0:
		return toks[0].lit, true
	}
	return "", false
}

// validFieldName reports whether name can be written into BQL unquoted. Field names
// come from rule files and fieldref values, so anything else could rewrite the query.
func validFieldName(name string) bool {
	return parser.IsPlainFieldName(name) && !isBQLKeyword(name)
}

// utf16Bytes encodes s as UTF-16 in the given byte order, with a byte-order mark if bom.
func utf16Bytes(s string, order binary.ByteOrder, bom bool) []byte {
	units := utf16.Encode([]rune(s))
	if bom {
		units = append([]uint16{0xFEFF}, units...)
	}
	b := make([]byte, 2*len(units))
	for i, u := range units {
		order.PutUint16(b[2*i:], u)
	}
	return b
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

// lowerFolds reports whether ClickHouse lower(), which folds ASCII only, folds every
// letter of s that has a case. Caseless characters such as dashes need no folding.
func lowerFolds(s string) bool {
	for _, r := range s {
		if r >= utf8.RuneSelf && unicode.SimpleFold(r) != r {
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
	matchEqual                   // equality: a value without case variants, or any literal under cased
	matchLower                   // lower(field) equality; ASCII values only
	matchNative                  // =~ / =^ / =$, case-insensitive literal operators
	matchRegex                   // RE2, case-insensitive unless cased
)

// valueMatch is how one Sigma value is expressed in BQL.
type valueMatch struct {
	kind    matchKind
	op      string // matchNative: "=~", "=^" or "=$"
	value   string // matchEqual, matchNative: the literal; matchLower: lowercased
	pattern string // matchRegex: anchored RE2
	wild    bool   // matchRegex: pattern holds a wildcard
	cased   bool   // matchRegex: case-sensitive
}

// classifyValue picks the cheapest BQL form that matches a Sigma value exactly as
// Sigma does: wildcards honored, case ignored unless cased.
//
//   - A value without case variants under an exact match is an equality. It is the
//     only form the field's skip index can prune with, and is exact for digits and
//     punctuation (EventID: 4688).
//   - A literal under a substring, prefix or suffix match uses =~, =^ or =$, which
//     are case-insensitive. A '*' at one end of an exact value makes it one of
//     these, so the common '*\cmd.exe' is a suffix match, not a regex.
//   - An exact value whose cased letters are all ASCII compares lower(field), about
//     four times cheaper than an anchored regex. It cannot use the skip index, which
//     is over the value as stored. ClickHouse lower() folds ASCII only, so a value
//     with other cased letters stays a regex, under any match type.
//   - Everything else is a case-insensitive regex: a wildcard in the middle or '?'.
//
// Under cased, an exact literal is an equality and everything else a regex without /i.
// An empty value that is not exact is in every value, so it matches field presence.
func classifyValue(v string, matchType string, cased bool) valueMatch {
	anchorStart := matchType == "exact" || matchType == "startswith"
	anchorEnd := matchType == "exact" || matchType == "endswith"
	toks, lead, trail := trimStars(parseSigmaValue(v))
	if lead {
		anchorStart = false
	}
	if trail {
		anchorEnd = false
	}
	if len(toks) == 0 && !(anchorStart && anchorEnd) {
		return valueMatch{kind: matchAny}
	}

	lit, literal := soleLiteral(toks)
	switch {
	case cased:
		// BQL reads ="*" as presence, so a literal * takes the regex.
		if literal && anchorStart && anchorEnd && lit != "*" {
			return valueMatch{kind: matchEqual, value: lit}
		}
	case literal && anchorStart && anchorEnd:
		// BQL reads ="*" as presence, so a literal * takes the in() form.
		if !hasCaseVariants(lit) && lit != "*" {
			return valueMatch{kind: matchEqual, value: lit}
		}
		if lowerFolds(lit) {
			return valueMatch{kind: matchLower, value: strings.ToLower(lit)}
		}
	case literal && lowerFolds(lit):
		op := "=~"
		if anchorStart {
			op = "=^"
		} else if anchorEnd {
			op = "=$"
		}
		return valueMatch{kind: matchNative, op: op, value: lit}
	}

	pattern, wild := globRegex(toks)
	if anchorStart {
		pattern = "^" + pattern
	}
	if anchorEnd {
		pattern += "$"
	}
	return valueMatch{kind: matchRegex, pattern: pattern, wild: wild, cased: cased}
}

// renderMatches ORs the values of one field. Each form is consolidated: equalities
// stay separate (each can use the skip index), lowercase equalities share one set,
// native values share one operator list per operator, and regexes share one
// alternation, so the SQL runs one IN, multiSearch or match() per field instead of
// one per value. The set is written as in() because lower(f)="v" does not parse
// before a =~ leaf.
func renderMatches(field string, matches []valueMatch) string {
	var parts, lowers []string
	var patterns, casedPatterns []string
	native := map[string][]string{}
	wild, casedWild := false, false
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
			if m.cased {
				casedPatterns = append(casedPatterns, m.pattern)
				casedWild = casedWild || m.wild
			} else {
				patterns = append(patterns, m.pattern)
				wild = wild || m.wild
			}
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
	regex := func(pats []string, wild bool, suffix string) {
		if len(pats) == 0 {
			return
		}
		flags := ""
		if wild {
			flags = "(?s)"
		}
		parts = append(parts, fmt.Sprintf("%s=/%s%s/%s", field, flags, strings.Join(pats, "|"), suffix))
	}
	regex(patterns, wild, "i")
	regex(casedPatterns, casedWild, "")
	return joinExprs(parts, " OR ")
}

// buildNativeOpExpr emits a =~/=^/=$ expression. encodeBQLNativeValue quotes any
// value holding a comma, which keeps it one value.
func buildNativeOpExpr(field, op string, values []string) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = encodeBQLNativeValue(v)
	}
	return field + op + strings.Join(parts, ",")
}

// buildKeywordExpr ORs keyword values (no field) as a substring search over
// norm_log, the event serialized as JSON. A literal takes norm_log's =~, which BQL
// escapes the way each store serialized the event and matches case-insensitively;
// anything =~ cannot carry (a wildcard, a cased non-ASCII letter) is a regex over
// that serialization. Under cased every value is that regex, without /i.
func buildKeywordExpr(values []string, cased bool) string {
	var lits, pieces []string
	for _, v := range values {
		toks, _, _ := trimStars(parseSigmaValue(v))
		if len(toks) == 0 {
			return "*"
		}
		if lit, ok := soleLiteral(toks); ok && !cased && lowerFolds(lit) {
			lits = append(lits, lit)
			continue
		}
		pieces = append(pieces, serializedGlobRegex(toks))
	}
	var parts []string
	if len(lits) > 0 {
		parts = append(parts, buildNativeOpExpr(normLogField, "=~", lits))
	}
	if len(pieces) > 0 {
		suffix := "i"
		if cased {
			suffix = ""
		}
		parts = append(parts, normLogField+"=/"+strings.Join(pieces, "|")+"/"+suffix)
	}
	return joinExprs(parts, " OR ")
}

// normLogField is the column holding each event serialized as JSON.
const normLogField = "norm_log"

// serializedGlobRegex renders glob tokens as an RE2 fragment matching their text as
// norm_log stores it: JSON-escaped, with '/' escaped in the hot store but not the
// archive, so either spelling matches. '?' takes one serialized character: a \uXXXX
// or two-character escape, or anything but the quote that ends the value. Its group
// captures because lowerRegexForLowercasedColumn refuses "(?", which would cost the
// n-gram index.
func serializedGlobRegex(toks []globToken) string {
	var b strings.Builder
	for _, t := range toks {
		switch t.wild {
		case '*':
			b.WriteString(".*")
		case '?':
			b.WriteString(`(\\u[0-9a-fA-F]{4}|\\.|[^"\\])`)
		default:
			parts := strings.Split(parser.NormLogJSON(t.lit), "/")
			for i, p := range parts {
				if i > 0 {
					b.WriteString(`\\?\/`)
				}
				b.WriteString(escapeRegex(p))
			}
		}
	}
	return b.String()
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
