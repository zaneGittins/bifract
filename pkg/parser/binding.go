package parser

import (
	"fmt"
	"sort"
	"strings"
)

// A let binding names an expression, a filter, or a whole pipeline so a query can
// state it once and use it in several places:
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
// This file reads the statements. binding_subst.go puts what they name into the
// tree; binding_sql.go renders the ones that name a set of rows.

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
	// BindingPipeline is a whole query. It names a set of rows, usable where a
	// subquery goes: in() and a join() block.
	BindingPipeline
)

// describeKind names a binding's kind in the voice of an error message.
func (k BindingKind) describe() string {
	switch k {
	case BindingCondition:
		return "a filter"
	case BindingPipeline:
		return "a result set"
	}
	return "a value"
}

type BindingNode struct {
	Name string // with the sigil, e.g. "&lolbin"
	Kind BindingKind
	Expr *ExprNode        // BindingValue
	Cond *HavingCondition // BindingCondition
	Body string           // BindingPipeline: the source, as written
	Pipe *PipelineNode    // BindingPipeline
	// SetRefs counts the places that read this binding as a set. More than one
	// means the query materialises it once rather than pasting the subquery in
	// at each use.
	SetRefs int
	Pos     int
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

	end, err := p.findStatementEnd()
	if err != nil {
		return fmt.Errorf("let %s: %w", name, err)
	}

	binding, err := p.parseBindingValue(name, nameTok, end)
	if err != nil {
		return err
	}
	p.pos = end + 1

	if p.bindings == nil {
		p.bindings = map[string]*BindingNode{}
	}
	p.bindings[name] = binding
	p.bindingOrder = append(p.bindingOrder, binding)
	return nil
}

// parseBindingValue reads the right-hand side of one let statement. A top-level
// pipe means it names a set of rows; anything else is an expression, optionally
// closed by a match operator the expression grammar does not have.
func (p *Parser) parseBindingValue(name string, nameTok Token, end int) (*BindingNode, error) {
	if p.spanHasTopLevelPipe(p.pos, end) {
		body := strings.TrimSpace(string(p.input[p.tokens[p.pos].Pos:p.tokens[end].Pos]))
		sub, err := p.parseSubPipeline(body)
		if err != nil {
			return nil, fmt.Errorf("let %s: %w", name, err)
		}
		return &BindingNode{Name: name, Kind: BindingPipeline, Body: body, Pipe: sub, Pos: nameTok.Pos}, nil
	}

	expr, err := p.parseExprFilterPrec(0)
	if err != nil {
		return nil, fmt.Errorf("let %s: %w", name, err)
	}
	if err := p.substituteExpr(expr); err != nil {
		return nil, fmt.Errorf("let %s: %w", name, err)
	}
	binding := &BindingNode{Name: name, Kind: BindingValue, Expr: expr, Pos: nameTok.Pos}
	if p.atExprMatchOperator() {
		cond := HavingCondition{Expr: expr}
		if err := p.readExprMatchOperator(&cond); err != nil {
			return nil, fmt.Errorf("let %s: %w", name, err)
		}
		binding.Kind, binding.Cond = BindingCondition, &cond
	}
	if p.pos != end {
		return nil, newPosError(p.current(), "let %s: unexpected %q", name, p.current().Value)
	}
	return binding, nil
}

// findStatementEnd returns the index of the ';' closing the statement that starts
// at the current position. Depth-aware, because chain() and case() bodies carry
// their own ';' inside braces.
func (p *Parser) findStatementEnd() (int, error) {
	depth := 0
	for i := p.pos; i < len(p.tokens); i++ {
		switch p.tokens[i].Type {
		case TokenLParen, TokenLBracket, TokenLBrace:
			depth++
		case TokenRParen, TokenRBracket, TokenRBrace:
			depth--
		case TokenSemicolon:
			if depth == 0 {
				return i, nil
			}
		case TokenEOF:
			return 0, newPosError(p.tokens[i], "expected ';' to end the binding")
		}
	}
	return 0, newPosError(p.current(), "expected ';' to end the binding")
}

// spanHasTopLevelPipe reports whether the token range carries a pipe outside any
// bracket, which is what separates a pipeline binding from an expression one.
func (p *Parser) spanHasTopLevelPipe(start, end int) bool {
	depth := 0
	for i := start; i < end && i < len(p.tokens); i++ {
		switch p.tokens[i].Type {
		case TokenLParen, TokenLBracket, TokenLBrace:
			depth++
		case TokenRParen, TokenRBracket, TokenRBrace:
			depth--
		case TokenPipe:
			if depth == 0 {
				return true
			}
		}
	}
	return false
}

// parseSubPipeline parses a binding's pipeline body. The bindings declared so far
// are in scope, so one binding can build on another.
func (p *Parser) parseSubPipeline(body string) (*PipelineNode, error) {
	if strings.TrimSpace(body) == "" {
		return nil, fmt.Errorf("the pipeline is empty")
	}
	lexer := NewLexer(body)
	tokens, err := lexer.Tokenize()
	if err != nil {
		return nil, err
	}
	sub := NewParser(tokens)
	sub.input = []rune(body)
	// A copy, not the map itself: a body cannot declare a binding today, since
	// the statement ends at the first ';', but sharing the map would make it leak
	// into the outer query the day that changes.
	sub.bindings = make(map[string]*BindingNode, len(p.bindings))
	for k, v := range p.bindings {
		sub.bindings[k] = v
	}
	sub.bindingBudget = p.budget()
	return sub.Parse()
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

// parseBindingCondition reads a binding used as a filter leaf.
func (p *Parser) parseBindingCondition(negate bool) (*HavingCondition, error) {
	b, err := p.lookupBinding(p.current())
	if err != nil {
		return nil, err
	}
	if b.Kind == BindingPipeline {
		return nil, newPosError(p.current(), "%s is a result set, not a filter: use it in in(field, %s) or a join() block", b.Name, b.Name)
	}
	p.advance()
	var cond HavingCondition
	if b.Kind == BindingCondition {
		clone, err := cloneCondition(b.Cond, p.budget())
		if err != nil {
			return nil, err
		}
		cond = *clone
	} else {
		clone, err := cloneExpr(b.Expr, p.budget())
		if err != nil {
			return nil, err
		}
		cond = HavingCondition{Expr: clone}
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

// resolveBlockBinding expands a join block written as nothing but a binding
// reference. The block becomes that binding's own source, which already parsed as
// a pipeline, so the handler reads exactly what the author wrote under the name.
func (p *Parser) resolveBlockBinding(body string) (string, error) {
	name := strings.TrimSpace(body)
	if !strings.HasPrefix(name, "&") {
		return body, nil
	}
	b, err := p.lookupBinding(Token{Value: name})
	if err != nil {
		return "", err
	}
	if b.Kind != BindingPipeline {
		return "", fmt.Errorf("%s is %s, not a result set: a join() block needs a pipeline", b.Name, b.Kind.describe())
	}
	return b.Body, nil
}
