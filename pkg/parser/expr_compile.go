package parser

import (
	"fmt"
	"strings"
)

// compileExpr lowers an expression to SQL and reports its static type.
//
// Typing is scoped deliberately: a bare field reference is TypeAny and coerces
// to whichever side of an operator needs it, so `score := bytes * 2` keeps
// working on a field with no type hint. Only an expression that is *known* to be
// a string (a literal, or a function declared to return one) is rejected in a
// numeric position.
//
// selfField names the assignment being defined, if any. An identifier matching
// it bypasses the registry so `x := x * 100` reads the log field rather than the
// alias it is about to create.
func compileExpr(e *ExprNode, registry *FieldRegistry, selfField string) (string, ExprType, error) {
	return compileIn(e, exprCtx{registry: registry, selfField: selfField})
}

// exprCtx carries what compilation needs beyond the expression itself.
type exprCtx struct {
	registry  *FieldRegistry
	selfField string
	// scope, when set, means the expression is being built above the source scan.
	// A leaf that only the scan can compute is exported through it under a hidden
	// alias, because a raw JSON sub-column does not resolve at that layer
	// (ClickHouse code 47). See deferredScope.
	scope *deferredScope
	// deferredFields names the columns that the deferred layer produces, which are
	// therefore already bare columns there and must not be exported.
	deferredFields map[string]bool
}

func compileIn(e *ExprNode, ctx exprCtx) (string, ExprType, error) {
	switch e.Kind {
	case ExprString:
		return "'" + escapeString(e.Value) + "'", TypeString, nil
	case ExprNumber:
		return e.Value, TypeNumber, nil
	case ExprBoolean:
		if strings.EqualFold(e.Value, "true") {
			return "1", TypeBool, nil
		}
		return "0", TypeBool, nil
	case ExprField:
		return compileFieldRef(e.Value, ctx)
	case ExprUnary:
		return compileUnary(e, ctx)
	case ExprBinary:
		return compileBinary(e, ctx)
	case ExprCall:
		return compileCall(e, ctx)
	}
	return "", TypeAny, fmt.Errorf("unsupported expression")
}

// compileFieldRef resolves an identifier to a column reference. A registered
// field resolves to its alias (or is folded in when the registry marks it
// inline); anything else is a JSON sub-column of the log.
func compileFieldRef(name string, ctx exprCtx) (string, ExprType, error) {
	registry, selfField := ctx.registry, ctx.selfField
	if registry == nil {
		// No registry, but the base columns are base columns regardless: resolving
		// norm_log as a JSON path makes this clause disagree with the one the same
		// query builds with a registry.
		if isBaseColumn(name) {
			return name, TypeAny, nil
		}
		return ctx.export(groupableCast(jsonFieldRef(name)), name), TypeAny, nil
	}
	if ctx.deferredFields[name] {
		// Produced by the deferred layer: already a bare column there.
		return name, TypeAny, nil
	}
	if name != selfField && registry.Has(name) {
		if registry.IsInline(name) {
			// An inline field is a pre-aggregation assignment folded in at each
			// reference. Its expression is already numeric.
			return "(" + registry.Resolve(name) + ")", TypeNumber, nil
		}
		// The bare alias, not Resolve(): an aggregate output registers with
		// Expr == its own name, which Resolve() reads as a placeholder and turns
		// back into a fields.`name` JSON path that does not exist in this stage.
		return name, TypeAny, nil
	}
	return ctx.export(groupableCast(registry.fieldRef(name)), name), TypeAny, nil
}

// isBaseColumn reports whether a name is a column of the logs table itself
// rather than a JSON field inside it.
func isBaseColumn(name string) bool {
	switch name {
	case "timestamp", normLogColumn, "log_id", "fractal_id", "ingest_timestamp", "normalizer":
		return true
	}
	return false
}

// export routes a source-scope reference through the deferred scope when the
// expression is being built above the scan, and is a no-op otherwise.
func (c exprCtx) export(sql, label string) string {
	if c.scope == nil {
		return sql
	}
	return c.scope.ref(sql, label)
}

func compileUnary(e *ExprNode, ctx exprCtx) (string, ExprType, error) {
	sql, typ, err := compileIn(e.Arg, ctx)
	if err != nil {
		return "", TypeAny, err
	}
	// A unary operator binds tighter than every infix one, so a compound operand
	// must be bracketed or the operator rebinds to its left half: -(a + b) would
	// emit (-a) + b.
	if e.Arg.Kind == ExprBinary {
		sql = "(" + sql + ")"
	}
	switch e.Value {
	case "-":
		n, err := asNumber(sql, typ, e.Arg)
		if err != nil {
			return "", TypeAny, err
		}
		return "-" + n, TypeNumber, nil
	case "!", "NOT", "not":
		if typ != TypeBool {
			return "", TypeAny, fmt.Errorf("NOT expects a condition, got %s (%s)", typ, e.Arg.String())
		}
		return "NOT " + sql, TypeBool, nil
	}
	return "", TypeAny, fmt.Errorf("unsupported operator %q", e.Value)
}

func compileBinary(e *ExprNode, ctx exprCtx) (string, ExprType, error) {
	leftSQL, leftType, err := compileIn(e.Left, ctx)
	if err != nil {
		return "", TypeAny, err
	}
	rightSQL, rightType, err := compileIn(e.Right, ctx)
	if err != nil {
		return "", TypeAny, err
	}

	join := func(l, r string, op string) string {
		return parenthesise(l, e.Left, e.Value, false) + " " + op + " " + parenthesise(r, e.Right, e.Value, true)
	}

	switch e.Value {
	case "+", "-", "*", "/":
		// Joining text with + is the most likely reason to land here, so name the
		// replacement rather than only reporting the type mismatch.
		if e.Value == "+" && (leftType == TypeString || rightType == TypeString) {
			return "", TypeAny, fmt.Errorf("+ expects numbers, not text; use concat(%s, %s) to join strings",
				e.Left.String(), e.Right.String())
		}
		l, err := asNumber(leftSQL, leftType, e.Left)
		if err != nil {
			return "", TypeAny, arithmeticError(e.Value, err)
		}
		r, err := asNumber(rightSQL, rightType, e.Right)
		if err != nil {
			return "", TypeAny, arithmeticError(e.Value, err)
		}
		return join(l, r, e.Value), TypeNumber, nil

	case ">", "<", ">=", "<=":
		l, err := asNumber(leftSQL, leftType, e.Left)
		if err != nil {
			return "", TypeAny, err
		}
		r, err := asNumber(rightSQL, rightType, e.Right)
		if err != nil {
			return "", TypeAny, err
		}
		return join(l, r, e.Value), TypeBool, nil

	case "=", "!=":
		// Compare numerically only when a side is known to be a number; two
		// untyped fields compare as the strings they are stored as.
		if leftType == TypeNumber || rightType == TypeNumber {
			l, err := asNumber(leftSQL, leftType, e.Left)
			if err != nil {
				return "", TypeAny, err
			}
			r, err := asNumber(rightSQL, rightType, e.Right)
			if err != nil {
				return "", TypeAny, err
			}
			return join(l, r, e.Value), TypeBool, nil
		}
		return join(asString(leftSQL, leftType), asString(rightSQL, rightType), e.Value), TypeBool, nil

	case "AND", "and", "OR", "or":
		if leftType != TypeBool || rightType != TypeBool {
			return "", TypeAny, fmt.Errorf("%s expects conditions on both sides, got %s and %s",
				strings.ToUpper(e.Value), leftType, rightType)
		}
		return join(leftSQL, rightSQL, strings.ToUpper(e.Value)), TypeBool, nil
	}
	return "", TypeAny, fmt.Errorf("unsupported operator %q", e.Value)
}

// operatorPrecedence mirrors binaryPrecedence for an operator's text form.
func operatorPrecedence(op string) int {
	switch strings.ToUpper(op) {
	case "OR":
		return 1
	case "AND":
		return 2
	case "=", "!=", ">", "<", ">=", "<=":
		return 3
	case "+", "-":
		return 4
	case "*", "/":
		return 5
	}
	return 0
}

// parenthesise brackets a compiled operand only where the SQL would otherwise
// regroup it. A right operand of equal precedence keeps its parentheses because
// the parser only produces one when the query wrote it: a - (b - c).
func parenthesise(sql string, child *ExprNode, parentOp string, isRight bool) string {
	if child.Kind != ExprBinary {
		return sql
	}
	childPrec, parentPrec := operatorPrecedence(child.Value), operatorPrecedence(parentOp)
	if childPrec < parentPrec || (isRight && childPrec == parentPrec) {
		return "(" + sql + ")"
	}
	return sql
}

func arithmeticError(op string, err error) error {
	return fmt.Errorf("%s: %w", op, err)
}

// asNumber coerces an expression into numeric SQL, rejecting anything known to
// be a string. toFloat64OrNull requires String input, so a column alias is
// stringified first while a JSON sub-column (already ::String) is not.
func asNumber(sql string, typ ExprType, node *ExprNode) (string, error) {
	switch typ {
	case TypeNumber:
		return sql, nil
	case TypeString:
		return "", fmt.Errorf("expected a number, got the string %s", node.String())
	case TypeBool:
		return "", fmt.Errorf("expected a number, got the condition %s", node.String())
	}
	if strings.HasSuffix(sql, "::String") {
		return "toFloat64OrNull(" + sql + ")", nil
	}
	return "toFloat64OrNull(toString(" + sql + "))", nil
}

// asString renders an expression as string SQL.
func asString(sql string, typ ExprType) string {
	if typ == TypeNumber {
		return "toString(" + sql + ")"
	}
	return sql
}

func compileCall(e *ExprNode, ctx exprCtx) (string, ExprType, error) {
	fn, ok := exprFuncs[e.Value]
	if !ok {
		return "", TypeAny, unknownFunctionError(e.Value)
	}
	bound, err := fn.bindArgs(e)
	if err != nil {
		return "", TypeAny, err
	}

	args := make([]string, len(bound))
	types := make([]ExprType, len(bound))
	for i, arg := range bound {
		if arg == nil {
			continue // omitted optional; Render sees an empty string
		}
		sql, typ, err := compileIn(arg, ctx)
		if err != nil {
			return "", TypeAny, err
		}
		want := fn.param(i).Type
		sql, err = conform(sql, typ, want, arg)
		if err != nil {
			return "", TypeAny, fmt.Errorf("%s(): %s: %w", fn.Name, fn.param(i).Name, err)
		}
		args[i], types[i] = sql, typ
	}

	if fn.Name == "if" {
		return compileIf(fn, args, types)
	}
	return fn.Render(args), fn.Returns, nil
}

// compileIf types the conditional by its branches: both must agree, and the
// result takes their type rather than the signature's TypeAny.
func compileIf(fn *exprFunc, args []string, types []ExprType) (string, ExprType, error) {
	then, els := types[1], types[2]
	switch {
	case then == els:
		return fn.Render(args), then, nil
	case then == TypeAny || els == TypeAny:
		known := then
		if known == TypeAny {
			known = els
		}
		return fn.Render(args), known, nil
	}
	return "", TypeAny, fmt.Errorf("if(): branches must have the same type, got %s and %s", then, els)
}

// conform adapts an argument to the parameter type, or explains why it cannot.
func conform(sql string, have, want ExprType, node *ExprNode) (string, error) {
	if want == TypeAny || have == want {
		return sql, nil
	}
	switch want {
	case TypeNumber:
		return asNumber(sql, have, node)
	case TypeString:
		if have == TypeBool {
			return "", fmt.Errorf("expected text, got the condition %s", node.String())
		}
		return asString(sql, have), nil
	case TypeBool:
		return "", fmt.Errorf("expected a condition, got %s (%s)", have, node.String())
	}
	return sql, nil
}

// unknownFunctionError names the nearest registered function when the name looks
// like a typo, and otherwise says plainly that the function does not exist.
// An unknown name must never fall through to a field reference: that is how a
// typo becomes a filter on a column that does not exist and silently matches
// nothing.
func unknownFunctionError(name string) error {
	if best, ok := closestExprFunc(name); ok {
		return fmt.Errorf("unknown function %s(); did you mean %s()?", name, best)
	}
	return fmt.Errorf("unknown function %s()", name)
}

func closestExprFunc(name string) (string, bool) {
	best, bestDist := "", 3 // only suggest for near misses
	for candidate := range exprFuncs {
		if d := editDistanceWithin(name, candidate, bestDist); d < bestDist {
			best, bestDist = candidate, d
		}
	}
	return best, best != ""
}

// editDistanceWithin is Levenshtein distance, abandoned once it exceeds limit.
func editDistanceWithin(a, b string, limit int) int {
	if abs(len(a)-len(b)) >= limit {
		return limit
	}
	prev := make([]int, len(b)+1)
	curr := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		curr[0] = i
		rowMin := curr[0]
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			curr[j] = min(prev[j]+1, min(curr[j-1]+1, prev[j-1]+cost))
			rowMin = min(rowMin, curr[j])
		}
		if rowMin >= limit {
			return limit
		}
		prev, curr = curr, prev
	}
	return prev[len(b)]
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// isComputedExpr reports whether an expression does work, as opposed to naming a
// field or a literal. Computed assignments bind to a pipeline stage, because
// where they are evaluated decides what they can see.
func isComputedExpr(e *ExprNode) bool {
	if e == nil {
		return false
	}
	switch e.Kind {
	case ExprBinary, ExprUnary, ExprCall:
		return true
	}
	return false
}

func assignmentError(field string, err error) error {
	return fmt.Errorf("assignment to %s: %w", field, err)
}

// compileExpressionText parses and compiles an expression given as raw text,
// for callers that hold a string rather than a parsed pipeline (eval()).
func compileExpressionText(text string, registry *FieldRegistry, selfField string) (string, error) {
	runes := []rune(text)
	expr, end, err := ParseExpressionAt(runes, 0)
	if err != nil {
		return "", err
	}
	// Anything after the first complete expression was silently discarded, so the
	// column held whatever the leading fragment resolved to.
	if trailing := strings.TrimSpace(string(runes[end:])); trailing != "" {
		return "", fmt.Errorf("unexpected %q after the expression", trailing)
	}
	sql, _, err := compileExpr(expr, registry, selfField)
	return sql, err
}
