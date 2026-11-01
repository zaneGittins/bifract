package parser

import (
	"fmt"
	"slices"
	"strings"
)

// Expression filters: a scalar expression used as a condition, e.g.
//   | lower(image) = "cmd.exe"
//   | len(commandline) > 500
//
// A call at the head of a pipeline stage is ambiguous, since a name can be both
// a command and an expression function. It is read as an expression only when
// that cannot change what an existing query means:
//
//   - the name is an expression function the command registry does not have, or
//   - the call is immediately followed by a comparison or arithmetic operator.
//
// A bracket followed by one of those operators is an expression too, so
// `(len(a) + 1) > 2` is a leaf rather than a condition group.
//
// AND/OR deliberately do not qualify. `cidr(a) OR cidr(b)` is a chain of boolean
// command operands today, and rerouting it through the expression compiler would
// change how every such query is built.

// atExprFilter reports whether the call at the current position should be parsed
// as an expression rather than dispatched as a command.
func (p *Parser) atExprFilter() bool {
	tok := p.current()
	if tok.Type != TokenFunction {
		return false
	}
	name := strings.ToLower(tok.Value)
	if _, known := exprFuncs[name]; !known {
		// Not an expression function. Leave it to the command path, which reports
		// an unknown name better than the expression compiler would.
		return false
	}
	if getCommandHandler(name) == nil {
		return true
	}
	return p.continuesExpr(p.groupEndIndex(p.pos + 1))
}

// atBracketExpr reports whether the bracket at the current position opens an
// expression, `(len(a) + 1) > 2`, rather than a condition group.
func (p *Parser) atBracketExpr() bool {
	return p.current().Type == TokenLParen && p.continuesExpr(p.groupEndIndex(p.pos))
}

// continuesExpr reports whether the token at index i is a comparison, or an
// arithmetic operator with an operand after it, which only the expression
// grammar can continue with.
func (p *Parser) continuesExpr(i int) bool {
	if i < 0 || i+1 >= len(p.tokens) {
		return false
	}
	t := p.tokens[i].Type
	if isScalarComparison(t) {
		return true
	}
	if !isArithmeticOperator(t) {
		return false
	}
	switch p.tokens[i+1].Type {
	case TokenField, TokenValue, TokenFunction, TokenString, TokenBinding, TokenLParen, TokenMinus:
		return true
	}
	return false
}

// arithmeticLeafAt reports whether the operand at index i starts unbracketed
// arithmetic that a comparison closes, `n + 1 > 2`, as opposed to bare words
// such as `cmd /c tasklist`.
func (p *Parser) arithmeticLeafAt(i int) bool {
	j := i + 1
	for j+1 < len(p.tokens) && isArithmeticOperator(p.tokens[j].Type) {
		switch p.tokens[j+1].Type {
		case TokenField, TokenValue:
			j += 2
		case TokenFunction:
			if j = p.groupEndIndex(j + 2); j < 0 {
				return false
			}
		default:
			return false
		}
	}
	return j > i+1 && j < len(p.tokens) && isScalarComparison(p.tokens[j].Type)
}

// isScalarComparison reports whether t compares two values, as opposed to the
// multi-value match operators.
func isScalarComparison(t TokenType) bool {
	switch t {
	case TokenEqual, TokenNotEqual, TokenGreater, TokenLess, TokenGreaterEqual, TokenLessEqual:
		return true
	}
	return false
}

func isArithmeticOperator(t TokenType) bool {
	switch t {
	case TokenPlus, TokenMinus, TokenMultiply, TokenDivide:
		return true
	}
	return false
}

// groupEndIndex returns the token index just past the bracket opened at index
// i, or -1 when there is no bracket there or it is never closed.
func (p *Parser) groupEndIndex(i int) int {
	if i >= len(p.tokens) || p.tokens[i].Type != TokenLParen {
		return -1
	}
	depth := 0
	for ; i < len(p.tokens); i++ {
		switch p.tokens[i].Type {
		case TokenLParen:
			depth++
		case TokenRParen:
			depth--
			if depth == 0 {
				return i + 1
			}
		case TokenEOF:
			return -1
		}
	}
	return -1
}

// parseExprFilterPrec parses an expression filter bounded to operators binding
// more tightly than minPrec, so a caller can stop before an AND/OR chain.
func (p *Parser) parseExprFilterPrec(minPrec int) (*ExprNode, error) {
	if p.input == nil {
		return nil, newPosError(p.current(), "expressions are not supported here")
	}
	expr, end, err := ParseExpressionAtPrec(p.input, p.current().Pos, minPrec)
	if err != nil {
		return nil, err
	}
	p.syncTo(end)
	return expr, nil
}

// exprConditionSQL compiles an expression used as a filter. The expression must
// type as a condition: `| lower(image)` on its own is a value, not a test, and
// letting it through would filter on whatever ClickHouse made of a string.
func exprConditionSQL(expr *ExprNode, registry *FieldRegistry, negate bool) (string, error) {
	return exprConditionSQLIn(expr, exprCtx{registry: registry}, negate)
}

// exprConditionSQLDeferred compiles a filter that sits above the source scan.
// Leaves only the scan can compute are exported through the scope; columns the
// deferred layer itself produces are read directly.
func exprConditionSQLDeferred(expr *ExprNode, registry *FieldRegistry, scope *deferredScope, negate bool) (string, error) {
	return exprConditionSQLIn(expr, exprCtx{
		registry:       registry,
		scope:          scope,
		deferredFields: deferredFieldSet(expr, registry),
	}, negate)
}

// hasWindowField reports whether the expression reads a window output.
func hasWindowField(expr *ExprNode, registry *FieldRegistry) bool {
	return slices.ContainsFunc(exprFieldNames(expr), func(f string) bool {
		entry := registry.Get(f)
		return entry != nil && entry.ClassifyKind() == FieldKindWindow
	})
}

// deferredFieldSet names the expression's fields that the deferred layer
// produces: window outputs and join outputs exist only above the scan.
func deferredFieldSet(expr *ExprNode, registry *FieldRegistry) map[string]bool {
	out := map[string]bool{}
	for _, f := range exprFieldNames(expr) {
		if entry := registry.Get(f); entry != nil {
			switch entry.ClassifyKind() {
			case FieldKindWindow, FieldKindJoined:
				out[f] = true
			}
		}
	}
	return out
}

func exprConditionSQLIn(expr *ExprNode, ctx exprCtx, negate bool) (string, error) {
	sql, typ, err := compileIn(expr, ctx)
	if err != nil {
		return "", err
	}
	if typ != TypeBool {
		return "", fmt.Errorf("%s is a %s, not a condition; compare it to something (for example %s = \"value\")",
			expr.String(), typ, expr.String())
	}
	// Always bracketed: the predicate is spliced into an AND chain that carries the
	// fractal and time-range guards, and a top-level OR inside it would re-associate
	// with that chain. `a OR b` would become `(guards AND a) OR b`, and the right
	// disjunct would read every fractal at any time.
	if negate {
		return "NOT (" + sql + ")", nil
	}
	return "(" + sql + ")", nil
}

// exprFieldNames lists the log fields and computed columns an expression reads,
// so a filter on it can be routed to the same pipeline stage a plain condition
// on those fields would be.
func exprFieldNames(e *ExprNode) []string {
	var out []string
	var walk func(*ExprNode)
	walk = func(n *ExprNode) {
		if n == nil {
			return
		}
		switch n.Kind {
		case ExprField:
			out = append(out, n.Value)
		case ExprUnary:
			walk(n.Arg)
		case ExprBinary:
			walk(n.Left)
			walk(n.Right)
		case ExprCall:
			for _, a := range n.Args {
				walk(a)
			}
			for _, a := range n.Named {
				walk(a)
			}
		}
	}
	walk(e)
	return out
}

// exprPriority is fieldPriority over every field an expression reads: the
// expression binds to the latest stage any of its inputs is available in, so a
// filter mixing a joined score with a log field defers to the post-join layer.
func exprPriority(e *ExprNode, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) int {
	best := 0
	for _, f := range exprFieldNames(e) {
		if p := fieldPriority(f, registry, plan, willHaveAggregation); p > best {
			best = p
		}
	}
	return best
}

// exprMatchSQL compiles a multi-value match whose left operand is an expression:
// `lower(image) =~ "cmd","ps"`. The operators =~, =^ and =$ belong to the filter
// grammar rather than the expression grammar, so the expression is compiled to a
// reference and handed to the same builders a field condition uses.
func exprMatchSQL(cond HavingCondition, registry *FieldRegistry) (string, error) {
	ref, _, err := compileExpr(cond.Expr, registry)
	if err != nil {
		return "", err
	}
	values := cond.Values
	if len(values) == 0 && cond.Value != "" {
		values = []string{cond.Value}
	}
	mode := sourceModeOf(registry)
	switch cond.Operator {
	case "=~":
		return buildContainsAnySQL(ref, values, cond.Negate, mode), nil
	case "=^":
		return buildStartsWithAnySQL(ref, values, cond.Negate, mode), nil
	case "=$":
		return buildEndsWithAnySQL(ref, values, cond.Negate, mode), nil
	}
	return "", fmt.Errorf("operator %s is not supported on an expression", cond.Operator)
}
