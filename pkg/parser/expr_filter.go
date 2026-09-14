package parser

import (
	"fmt"
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
	end := p.callEndIndex()
	if end < 0 || end >= len(p.tokens) {
		return false
	}
	switch p.tokens[end].Type {
	case TokenEqual, TokenNotEqual, TokenGreater, TokenLess, TokenGreaterEqual, TokenLessEqual,
		TokenPlus, TokenMinus, TokenMultiply, TokenDivide:
		return true
	}
	return false
}

// callEndIndex returns the token index just past the call starting at the
// current position, or -1 when the call is malformed.
func (p *Parser) callEndIndex() int {
	i := p.pos + 1
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

// parseExprFilter parses the expression at the current position and advances the
// main token stream past it.
func (p *Parser) parseExprFilter() (*ExprNode, error) {
	expr, end, err := p.parseExprHere()
	if err != nil {
		return nil, err
	}
	p.syncTo(end)
	return expr, nil
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

// deferredFieldSet names the expression's fields that the deferred layer
// produces: window outputs and join outputs exist only after the outer wrap.
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
