package parser

import (
	"fmt"
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
	for _, part := range splitNestedArgs(strings.Trim(strings.TrimSpace(raw), "[]")) {
		part = strings.Trim(strings.TrimSpace(part), `"'`)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

// splitNestedArgs splits on commas outside parentheses and brackets. Unlike
// splitTopLevelArgs it does not track quotes: parseCommand has already stripped
// them, so a value containing an apostrophe (O'Brien) would otherwise open a
// string that never closes and swallow the rest of the list.
func splitNestedArgs(s string) []string {
	var parts []string
	depth, start := 0, 0
	for i, ch := range s {
		switch ch {
		case '(', '[':
			depth++
		case ')', ']':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	if depth != 0 {
		// The brackets do not balance, so they were never grouping: a value simply
		// contains one. Splitting on depth here would swallow the rest of the list.
		return strings.Split(s, ",")
	}
	return append(parts, s[start:])
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

// validateExprArgs rejects a command argument that is written as an expression
// but cannot be compiled.
//
// Resolution happens in resolveFieldRef, which returns only a string and so
// cannot report an error: a broken expression there falls back to a field
// reference, and a field nothing produced matches nothing. That is the silent
// failure this pass exists to prevent, so it runs before Execute, where an
// error can still be returned.
func validateExprArgs(pipeline *PipelineNode, registry *FieldRegistry) error {
	var err error
	ForEachCommand(pipeline, func(cmd CommandNode) {
		if err != nil {
			return
		}
		for i, arg := range cmd.Arguments {
			// A quoted literal is data, not code. A regex pattern is routinely
			// call-shaped, so reading one as a call rejects valid queries.
			if cmd.IsQuotedArg(i) {
				continue
			}
			// An aggregate spec (the value of function=) is the handler's to
			// validate: it reports an unknown name with aggregate-specific advice.
			// Its arguments are still checked, since nothing else checks them.
			inAggregate := i > 0 && strings.TrimSpace(cmd.Arguments[i-1]) == "function="
			if spec, ok := strings.CutPrefix(arg, "function="); ok {
				arg, inAggregate = spec, true
			}
			if e := validateExprArg(arg, registry, inAggregate); e != nil {
				err = fmt.Errorf("%s(): %w", cmd.Name, e)
				return
			}
		}
	})
	return err
}

// statsSubFunctions are the aggregate spec names processStatsFn dispatches, which
// appear in a command argument (function=, multi()) without being scalar
// functions. Listed so a call-shaped argument that is neither can be reported as
// a typo rather than resolving to a field nothing produces.
var statsSubFunctions = map[string]bool{
	"count": true, "avg": true, "sum": true, "max": true, "min": true,
	"percentile": true, "stddev": true, "skewness": true, "kurtosis": true,
	"iqr": true, "selectfirst": true, "selectlast": true, "collect": true,
	"top": true, "median": true, "mad": true, "multi": true,
}

// validateExprArg checks one argument. deferUnknown leaves an unrecognised call
// name to the handler that owns the argument, while still checking what is
// nested inside it.
func validateExprArg(arg string, registry *FieldRegistry, deferUnknown bool) error {
	arg = strings.TrimSpace(arg)
	open := strings.IndexByte(arg, '(')
	if open < 0 {
		return nil
	}
	// Strip a leading name=, which handlers do before resolving: field=lower(x).
	if eq := strings.IndexByte(arg, '='); eq >= 0 && eq < open {
		arg = strings.TrimSpace(arg[eq+1:])
		open = strings.IndexByte(arg, '(')
		if open < 0 {
			return nil
		}
	}

	if !isCallShaped(arg[:open]) || !strings.HasSuffix(arg, ")") {
		// Not a call. It may still be a list whose elements are, since a bracket
		// list arrives as one comma-joined argument.
		if elements := listArg(arg); len(elements) > 1 {
			for _, e := range elements {
				if err := validateExprArg(e, registry, deferUnknown); err != nil {
					return err
				}
			}
		}
		return nil
	}
	name := strings.ToLower(arg[:open])
	_, isExprFunc := exprFuncs[name]

	// An aggregate spec (sum(len(x)), multi(count(), avg(len(x)))) is not itself a
	// scalar function, but its arguments may be.
	if !isExprFunc && (statsSubFunctions[name] || deferUnknown) {
		// multi() holds further aggregate specs, so an unrecognised name inside it
		// is still the handler's to report. Anywhere else the arguments are field
		// positions, where an unrecognised name is a typo.
		nested := name == "multi"
		for _, inner := range listArg(arg[open+1 : len(arg)-1]) {
			if e := validateExprArg(inner, registry, nested); e != nil {
				return e
			}
		}
		return nil
	}
	if !isExprFunc {
		return unknownFunctionError(arg[:open])
	}

	expr, ok := parseExprArg(arg)
	if !ok {
		// Written as a call to a known function but unparseable: say so rather
		// than letting it resolve as a field name nothing produces.
		if _, _, perr := ParseExpressionAt([]rune(arg), 0); perr != nil {
			return perr
		}
		return fmt.Errorf("%s is not a valid expression", arg)
	}
	_, _, cerr := compileExpr(expr, registry, "")
	return cerr
}

// isCallShaped reports whether the text before "(" is a plain identifier, which
// is what distinguishes a function call from an argument that merely contains a
// parenthesis.
func isCallShaped(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case i > 0 && r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}
