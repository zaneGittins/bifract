// Package bqlvars provides the single, authoritative implementation of BQL
// query variable substitution shared by search, dashboards and notebooks.
//
// A variable is written as @name in a query (name matches the identifier
// grammar [A-Za-z_][A-Za-z0-9_]*). Before a query is parsed, each @name is
// replaced by its value (an empty value becomes "*", a match-all wildcard).
//
// Substitution is deliberately conservative so it never corrupts a real query:
//   - Quote-aware: tokens inside single- or double-quoted strings are left
//     untouched, so quoted text such as "user@example.com" is never rewritten.
//   - Boundary-aware: an '@' immediately preceded by a word character is not a
//     variable start, so an unquoted user@host is left alone.
//   - Exact-name: the identifier grammar bounds each name, so @host and
//     @hostname resolve independently (no prefix collision).
//
// A referenced name that is NOT in the provided set is left intact, so genuine
// typos still surface as a parse error rather than being silently wildcarded.
//
// Trust boundary: a value is substituted as a raw BQL fragment, not as an
// escaped value literal, so a value may legitimately carry operators or pipe
// stages (matching LogScale-style variables). The substituted query is still
// parsed and fractal-scoped server-side afterward, so a value can only express
// BQL the substituting user could already run in their own scope; it cannot
// widen access. Callers persisting shared defaults (e.g. saved queries) should
// be aware the stored value is author-controlled BQL executed by viewers.
package bqlvars

import (
	"encoding/json"
	"strings"
)

// Variable is one query variable as persisted in the variables JSONB column.
type Variable struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ParseVariables unmarshals the JSON variable list. Malformed input yields nil
// (substitution then becomes a no-op) rather than an error, matching the
// best-effort contract callers rely on.
func ParseVariables(raw json.RawMessage) []Variable {
	if len(raw) == 0 {
		return nil
	}
	var vars []Variable
	if err := json.Unmarshal(raw, &vars); err != nil {
		return nil
	}
	return vars
}

// ToValueMap builds a name->value map from the variable list. Variables with an
// empty name are skipped. Values are stored verbatim; the empty-to-"*" default
// is applied at substitution time so the map faithfully reflects what was set.
func ToValueMap(vars []Variable) map[string]string {
	if len(vars) == 0 {
		return nil
	}
	m := make(map[string]string, len(vars))
	for _, v := range vars {
		if v.Name == "" {
			continue
		}
		m[v.Name] = v.Value
	}
	return m
}

// Substitute replaces @name tokens in q using the variables encoded in raw.
func Substitute(q string, raw json.RawMessage) string {
	return SubstituteMap(q, ToValueMap(ParseVariables(raw)))
}

// SubstituteMap replaces @name tokens in q using a prepared name->value map.
// An empty value (or a value that is itself empty) substitutes as "*".
func SubstituteMap(q string, values map[string]string) string {
	if len(values) == 0 || !strings.ContainsRune(q, '@') {
		return q
	}
	runes := []rune(q)
	var b strings.Builder
	b.Grow(len(q))
	var quote rune // the active quote char, or 0 when outside a string

	for i := 0; i < len(runes); {
		c := runes[i]
		if quote != 0 {
			b.WriteRune(c)
			// A backslash escapes the next char (covers escaped quotes); copy it
			// through so the quote state is not toggled by an escaped delimiter.
			if c == '\\' && i+1 < len(runes) {
				b.WriteRune(runes[i+1])
				i += 2
				continue
			}
			if c == quote {
				quote = 0
			}
			i++
			continue
		}
		if c == '\'' || c == '"' {
			quote = c
			b.WriteRune(c)
			i++
			continue
		}
		if c == '@' && !(i > 0 && isWordRune(runes[i-1])) {
			if j := i + 1; j < len(runes) && isNameStart(runes[j]) {
				k := j + 1
				for k < len(runes) && isNamePart(runes[k]) {
					k++
				}
				if val, ok := values[string(runes[j:k])]; ok {
					if val == "" {
						val = "*"
					}
					b.WriteString(val)
					i = k
					continue
				}
			}
		}
		b.WriteRune(c)
		i++
	}
	return b.String()
}

// Detect returns the distinct variable names referenced in q, in first-seen
// order, using the same scanning rules as Substitute. It is the backend
// counterpart to the frontend detector and is handy for tests/validation.
func Detect(q string) []string {
	if !strings.ContainsRune(q, '@') {
		return nil
	}
	runes := []rune(q)
	var names []string
	seen := map[string]bool{}
	var quote rune

	for i := 0; i < len(runes); {
		c := runes[i]
		if quote != 0 {
			if c == '\\' && i+1 < len(runes) {
				i += 2
				continue
			}
			if c == quote {
				quote = 0
			}
			i++
			continue
		}
		if c == '\'' || c == '"' {
			quote = c
			i++
			continue
		}
		if c == '@' && !(i > 0 && isWordRune(runes[i-1])) {
			if j := i + 1; j < len(runes) && isNameStart(runes[j]) {
				k := j + 1
				for k < len(runes) && isNamePart(runes[k]) {
					k++
				}
				name := string(runes[j:k])
				if !seen[name] {
					seen[name] = true
					names = append(names, name)
				}
				i = k
				continue
			}
		}
		i++
	}
	return names
}

func isWordRune(r rune) bool  { return isNamePart(r) }
func isNameStart(r rune) bool { return r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') }
func isNamePart(r rune) bool  { return isNameStart(r) || (r >= '0' && r <= '9') }
