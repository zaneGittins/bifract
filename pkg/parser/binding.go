package parser

import (
	"fmt"
	"sort"
	"strings"
)

// A let binding names an expression so a query can state it once and use it in
// several places:
//
//	let &lolbin = lower(image) =~ "rundll32.exe","mshta.exe";
//	* | &lolbin AND parent_image = "winword.exe"
//
// The & sigil is mandatory and is part of the name. BQL resolves an unknown
// identifier to an empty JSON field rather than erroring, so a binding sharing a
// namespace with log fields would turn a typo into a query that silently matches
// nothing. The sigil keeps the two apart, and an unresolved reference is a hard
// error (see compileFieldRef).
//
// Bindings are substituted into the AST as they are parsed, so everything
// downstream sees the query the author would have written by hand. A binding may
// reference one declared before it; referencing itself or a later one cannot
// resolve, which is what stops a cycle.
type BindingKind int

const (
	// BindingValue is an expression: a number, a string, or anything the
	// expression grammar accepts, comparisons and AND/OR included.
	BindingValue BindingKind = iota
	// BindingCondition is an expression followed by one of the filter-only match
	// operators (=~, =^, =$), which the expression grammar has no place for.
	BindingCondition
)

type BindingNode struct {
	Name string // with the sigil, e.g. "&lolbin"
	Kind BindingKind
	Expr *ExprNode        // BindingValue
	Cond *HavingCondition // BindingCondition
	Pos  int
}

// atLetStatement reports whether a `let &name =` statement starts here. The
// binding token is required, so a query whose first field happens to be called
// "let" still parses as it always did.
func (p *Parser) atLetStatement() bool {
	tok := p.current()
	if tok.Type != TokenField && tok.Type != TokenValue && tok.Type != TokenFunction {
		return false
	}
	return strings.EqualFold(tok.Value, "let") && p.peek().Type == TokenBinding
}

// parseLetStatements reads the binding statements that precede the pipeline.
func (p *Parser) parseLetStatements() error {
	for p.atLetStatement() {
		if err := p.parseLetStatement(); err != nil {
			return err
		}
	}
	return nil
}

func (p *Parser) parseLetStatement() error {
	p.advance() // let
	nameTok := p.current()
	name := nameTok.Value
	if _, taken := p.bindings[name]; taken {
		return newPosError(nameTok, "%s is already declared", name)
	}
	p.advance()

	if p.current().Type != TokenEqual {
		return newPosError(p.current(), "let %s: expected '=' after the name", name)
	}
	p.advance()
	if p.current().Type == TokenSemicolon || p.current().Type == TokenEOF {
		return newPosError(nameTok, "let %s: has no value", name)
	}

	expr, err := p.parseExprFilterPrec(0)
	if err != nil {
		return fmt.Errorf("let %s: %w", name, err)
	}
	if err := p.substituteExpr(expr); err != nil {
		return fmt.Errorf("let %s: %w", name, err)
	}

	binding := &BindingNode{Name: name, Kind: BindingValue, Expr: expr, Pos: nameTok.Pos}
	if p.atExprMatchOperator() {
		cond := HavingCondition{Expr: expr}
		if err := p.readExprMatchOperator(&cond); err != nil {
			return fmt.Errorf("let %s: %w", name, err)
		}
		binding.Kind, binding.Cond = BindingCondition, &cond
	}

	switch p.current().Type {
	case TokenSemicolon:
		p.advance()
	case TokenPipe:
		// Phase 2. Saying so beats "expected ';'", which reads as a typo.
		return newPosError(p.current(), "let %s: a binding cannot be a pipeline yet; it holds an expression or a filter", name)
	default:
		return newPosError(p.current(), "let %s: expected ';' after the value", name)
	}

	if p.bindings == nil {
		p.bindings = map[string]*BindingNode{}
	}
	p.bindings[name] = binding
	p.bindingOrder = append(p.bindingOrder, binding)
	return nil
}

// lookupBinding resolves a reference, reporting what is declared when it misses.
func (p *Parser) lookupBinding(tok Token) (*BindingNode, error) {
	if b, ok := p.bindings[tok.Value]; ok {
		return b, nil
	}
	return nil, newPosError(tok, "unknown binding %s%s", tok.Value, p.declaredBindings())
}

func (p *Parser) declaredBindings() string {
	if len(p.bindings) == 0 {
		return "; no bindings are declared (write `let " + "&name = ...;` before the query)"
	}
	names := make([]string, 0, len(p.bindings))
	for n := range p.bindings {
		names = append(names, n)
	}
	sort.Strings(names)
	return " (declared: " + strings.Join(names, ", ") + ")"
}

// parseBindingCondition reads a binding used as a filter leaf.
func (p *Parser) parseBindingCondition(negate bool) (*HavingCondition, error) {
	b, err := p.lookupBinding(p.current())
	if err != nil {
		return nil, err
	}
	p.advance()
	var cond HavingCondition
	if b.Kind == BindingCondition {
		cond = *cloneCondition(b.Cond)
	} else {
		cond = HavingCondition{Expr: cloneExpr(b.Expr)}
		// `&susp =~ "a","b"`: the match operators are filter syntax, so they
		// follow the substituted expression exactly as they follow a written one.
		if p.atExprMatchOperator() {
			if err := p.readExprMatchOperator(&cond); err != nil {
				return nil, err
			}
		}
	}
	if negate {
		cond.Negate = !cond.Negate
	}
	return &cond, nil
}

// substituteExpr replaces every binding reference in an expression with a copy of
// what it names.
func (p *Parser) substituteExpr(e *ExprNode) error {
	if e == nil {
		return nil
	}
	switch e.Kind {
	case ExprField:
		if !strings.HasPrefix(e.Value, "&") {
			return nil
		}
		b, err := p.lookupBinding(Token{Value: e.Value, Pos: e.Pos})
		if err != nil {
			return err
		}
		if b.Kind != BindingValue {
			return fmt.Errorf("%s is a filter, not a value: use it on its own (`| %s`), not inside an expression", b.Name, b.Name)
		}
		*e = *cloneExpr(b.Expr)
		return nil
	case ExprUnary:
		return p.substituteExpr(e.Arg)
	case ExprBinary:
		if err := p.substituteExpr(e.Left); err != nil {
			return err
		}
		return p.substituteExpr(e.Right)
	case ExprCall:
		for _, a := range e.Args {
			if err := p.substituteExpr(a); err != nil {
				return err
			}
		}
		for _, a := range e.Named {
			if err := p.substituteExpr(a); err != nil {
				return err
			}
		}
	}
	return nil
}

// cloneExpr deep-copies an expression so one binding used twice never shares a
// node with itself.
func cloneExpr(e *ExprNode) *ExprNode {
	if e == nil {
		return nil
	}
	out := *e
	out.Left = cloneExpr(e.Left)
	out.Right = cloneExpr(e.Right)
	out.Arg = cloneExpr(e.Arg)
	if e.Args != nil {
		out.Args = make([]*ExprNode, len(e.Args))
		for i, a := range e.Args {
			out.Args[i] = cloneExpr(a)
		}
	}
	if e.Named != nil {
		out.Named = make(map[string]*ExprNode, len(e.Named))
		for k, v := range e.Named {
			out.Named[k] = cloneExpr(v)
		}
	}
	return &out
}

func cloneCondition(c *HavingCondition) *HavingCondition {
	if c == nil {
		return nil
	}
	out := *c
	out.Expr = cloneExpr(c.Expr)
	if c.Values != nil {
		out.Values = append([]string(nil), c.Values...)
	}
	if c.Children != nil {
		out.Children = make([]HavingCondition, len(c.Children))
		for i := range c.Children {
			out.Children[i] = *cloneCondition(&c.Children[i])
		}
	}
	return &out
}

// substitutePipeline resolves every binding reference left in the tree after the
// pipeline is parsed. Filter leaves and expression stage heads substitute as they
// are read; this covers the rest, chiefly command arguments and := assignments.
// Anything it misses is caught at compile time, where an unresolved & is an
// error rather than a JSON field that matches nothing.
func (p *Parser) substitutePipeline(pipeline *PipelineNode) error {
	if len(p.bindings) == 0 || pipeline == nil {
		return nil
	}
	if pipeline.Filter != nil {
		for i := range pipeline.Filter.Conditions {
			if err := p.substituteFilterCondition(&pipeline.Filter.Conditions[i]); err != nil {
				return err
			}
		}
	}
	for i := range pipeline.Assignments {
		if err := p.substituteExpr(pipeline.Assignments[i].Expr); err != nil {
			return err
		}
	}
	for i := range pipeline.HavingConditions {
		if err := p.substituteHavingCondition(&pipeline.HavingConditions[i]); err != nil {
			return err
		}
	}
	for i := range pipeline.Commands {
		if err := p.substituteCommand(&pipeline.Commands[i]); err != nil {
			return err
		}
	}
	return nil
}

func (p *Parser) substituteCommand(cmd *CommandNode) error {
	for i := range cmd.Args {
		if err := p.substituteArg(&cmd.Args[i]); err != nil {
			return err
		}
	}
	return nil
}

func (p *Parser) substituteArg(a *Argument) error {
	if err := p.substituteExpr(a.Expr); err != nil {
		return err
	}
	for i := range a.List {
		if err := p.substituteArg(&a.List[i]); err != nil {
			return err
		}
	}
	if a.Agg != nil {
		for i := range a.Agg.Args {
			if err := p.substituteArg(&a.Agg.Args[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Parser) substituteHavingCondition(c *HavingCondition) error {
	if err := p.substituteExpr(c.Expr); err != nil {
		return err
	}
	for i := range c.Children {
		if err := p.substituteHavingCondition(&c.Children[i]); err != nil {
			return err
		}
	}
	if c.Command != nil {
		return p.substituteCommand(c.Command)
	}
	return nil
}

func (p *Parser) substituteFilterCondition(c *ConditionNode) error {
	if err := p.substituteExpr(c.Expr); err != nil {
		return err
	}
	for i := range c.Children {
		if err := p.substituteFilterCondition(&c.Children[i]); err != nil {
			return err
		}
	}
	if c.Command != nil {
		return p.substituteCommand(c.Command)
	}
	return nil
}

// atBindingChain reports whether the binding at the current position is one leaf
// of a boolean chain rather than a whole stage on its own.
func (p *Parser) atBindingChain() bool {
	switch p.peek().Type {
	case TokenAnd, TokenOr:
		return true
	}
	return false
}

// atBindingExpr reports whether the binding at the current position continues
// into an expression, as `&cmdlen > 500` does, rather than standing alone as a
// filter. The operator set mirrors atExprFilter's.
func (p *Parser) atBindingExpr() bool {
	if p.current().Type != TokenBinding {
		return false
	}
	switch p.peek().Type {
	case TokenEqual, TokenNotEqual, TokenGreater, TokenLess, TokenGreaterEqual, TokenLessEqual,
		TokenPlus, TokenMinus, TokenMultiply, TokenDivide:
		return true
	}
	return false
}
