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
	// BindingPipeline is a whole query. It names a set of rows, usable where a
	// subquery goes: in() and a join() block.
	BindingPipeline
)

// describeKind names a binding's kind in the voice of an error message.
func (k BindingKind) describeKind() string {
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
	if b.Kind == BindingPipeline {
		return nil, newPosError(p.current(), "%s is a result set, not a filter: use it in in(field, %s) or a join() block", b.Name, b.Name)
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
			return fmt.Errorf("%s is %s, not a value: it cannot be used inside an expression", b.Name, b.Kind.describeKind())
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
		if err := p.substituteArg(&cmd.Args[i], cmd.Name, true); err != nil {
			return err
		}
	}
	return nil
}

// substituteArg resolves the bindings one argument names. whole says the argument
// is the command's own, not a member of a list: a result set is a whole argument
// or nothing, since a list of them has no meaning.
func (p *Parser) substituteArg(a *Argument, cmdName string, whole bool) error {
	if name, ok := bareBindingRef(a); ok {
		b, err := p.lookupBinding(Token{Value: name, Pos: a.Pos})
		if err != nil {
			return err
		}
		if b.Kind == BindingPipeline {
			switch {
			case !whole:
				return fmt.Errorf("%s(): %s is a result set and cannot be a member of a list", cmdName, b.Name)
			case !strings.EqualFold(cmdName, "in"):
				return fmt.Errorf("%s(): %s is a result set; only in() and a join() block take one", cmdName, b.Name)
			}
			a.Kind, a.Binding, a.Expr = ArgBinding, b, nil
			return nil
		}
	}
	if err := p.substituteExpr(a.Expr); err != nil {
		return err
	}
	for i := range a.List {
		if err := p.substituteArg(&a.List[i], cmdName, false); err != nil {
			return err
		}
	}
	if a.Agg != nil {
		for i := range a.Agg.Args {
			if err := p.substituteArg(&a.Agg.Args[i], cmdName, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// bareBindingRef reports the binding an argument names when the argument is
// nothing but that reference, as in in(user, &admins).
func bareBindingRef(a *Argument) (string, bool) {
	if a.Kind != ArgExpr || a.Expr == nil || a.Expr.Kind != ExprField {
		return "", false
	}
	if !strings.HasPrefix(a.Expr.Value, "&") {
		return "", false
	}
	return a.Expr.Value, true
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
	sub.bindings = p.bindings
	return sub.Parse()
}

// bindingSetMaxRows caps the rows a result-set binding contributes to an IN
// list. IN materialises the whole set in memory, so it is bounded like join().
const bindingSetMaxRows = 50000

// bindingSubquerySQL renders a result-set binding as a single-column subquery for
// an IN test. The column is the one named after the field being tested, matching
// how a join() block names its key; a binding that returns exactly one column
// needs no name at all.
func bindingSubquerySQL(b *BindingNode, ctx *CommandContext, field string) (string, error) {
	if b == nil || b.Pipe == nil {
		return "", fmt.Errorf("%s is not a result set", bindingName(b))
	}
	result, err := TranslateToSQLWithOrder(b.Pipe, subqueryOptions(ctx, bindingSetMaxRows))
	if err != nil {
		return "", fmt.Errorf("%s: %w", b.Name, err)
	}
	column, err := bindingSetColumn(b, result.FieldOrder, field)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("SELECT %s FROM (%s)", column, result.SQL), nil
}

func bindingSetColumn(b *BindingNode, outputs []string, field string) (string, error) {
	if field != "" && contains(outputs, field) {
		return field, nil
	}
	if len(outputs) == 1 {
		return outputs[0], nil
	}
	if len(outputs) == 0 {
		return "", fmt.Errorf("%s returns no columns", b.Name)
	}
	return "", fmt.Errorf("%s returns [%s]; name the column to test by making the binding return only it, or test a field one of them is named after",
		b.Name, strings.Join(outputs, ", "))
}

func bindingName(b *BindingNode) string {
	if b == nil {
		return "the binding"
	}
	return b.Name
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
		return "", fmt.Errorf("%s is %s, not a result set: a join() block needs a pipeline", b.Name, b.Kind.describeKind())
	}
	return b.Body, nil
}
