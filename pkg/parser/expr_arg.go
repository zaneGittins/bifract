package parser

import (
	"strings"
)

// Expressions in command arguments: groupby(lower(user)), sort(len(commandline)),
// table(user, round(bytes, 2)), sum(bytes * 8).
//
// parseCommand already captures a nested call as one argument string, so what is
// missing is resolution: a field position must compile `lower(user)` rather than
// look for a log field of that name. resolveFieldRef is the single place every
// field position goes through, so the check lives there.
//
// It is deliberately narrow. An argument is an expression only when it parses as
// one AND its root is a call to a registered expression function. Anything else
// resolves exactly as before, which keeps aggregate specs (`count()`, `avg(x)`)
// and every other argument shape on their existing path.

// exprArgSQL compiles a command argument that is an expression. ok is false when
// the argument is not one, in which case the caller keeps its existing behaviour.
func exprArgSQL(arg string, registry *FieldRegistry) (sql string, ok bool) {
	expr, isExpr := parseExprArg(arg)
	if !isExpr {
		return "", false
	}
	compiled, _, err := compileExpr(expr, registry, "")
	if err != nil {
		// Resolution cannot report an error from here. Fall back so the argument
		// takes its old meaning rather than silently becoming empty SQL; a genuinely
		// malformed expression still fails in the command that consumes it.
		return "", false
	}
	return compiled, true
}

// parseExprArg parses an argument as an expression, reporting false unless it is
// a call to a known expression function that consumes the whole argument.
func parseExprArg(arg string) (*ExprNode, bool) {
	arg = strings.TrimSpace(arg)
	if !strings.Contains(arg, "(") {
		return nil, false
	}
	runes := []rune(arg)
	expr, end, err := ParseExpressionAt(runes, 0)
	if err != nil || end != len(runes) {
		return nil, false
	}
	if expr.Kind != ExprCall {
		return nil, false
	}
	if _, known := exprFuncs[expr.Value]; !known {
		return nil, false
	}
	return expr, true
}

// exprArgAlias derives the output column name for an expression argument, so
// `groupby(lower(user))` produces a column called lower_user. Callers that accept
// as= should prefer what the author wrote.
func exprArgAlias(arg string) string {
	var b strings.Builder
	prevUnderscore := false
	for _, r := range arg {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
			prevUnderscore = false
		default:
			if !prevUnderscore && b.Len() > 0 {
				b.WriteRune('_')
				prevUnderscore = true
			}
		}
	}
	return strings.Trim(b.String(), "_")
}

// isExprArg reports whether an argument should be treated as an expression. Used
// by handlers that need the derived alias as well as the SQL.
func isExprArg(arg string) bool {
	_, ok := parseExprArg(arg)
	return ok
}

// outputAlias is the column name a field-position argument projects under. A
// plain field keeps its own name; an expression gets a derived one, since
// `lower(user)` is not a usable SQL identifier.
func outputAlias(arg string) (string, error) {
	if isExprArg(arg) {
		return sanitizeIdentifier(exprArgAlias(arg))
	}
	return sanitizeIdentifier(arg)
}

// listArg normalises a command argument that carries a list.
//
// A bracket list reaches a handler as ONE comma-joined argument (parseCommand
// joins the elements), so every handler taking a list must split it. hash() did
// not, and hashed a field literally named "a,b" that existed on no row. The
// split is top-level only, so a nested call such as substr(image, 1, 10)
// survives it intact, which matters now that a field position accepts an
// expression.
func listArg(raw string) []string {
	var out []string
	for _, part := range splitTopLevelArgs(strings.Trim(strings.TrimSpace(raw), "[]")) {
		part = strings.Trim(strings.TrimSpace(part), `"'`)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

// namedListArg returns the list held by a name=[...] or name=a,b argument, and
// whether the argument carried that name at all.
func namedListArg(arg, name string) ([]string, bool) {
	prefix := name + "="
	if !strings.HasPrefix(arg, prefix) {
		return nil, false
	}
	return listArg(strings.TrimPrefix(arg, prefix)), true
}
