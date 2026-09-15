package parser

import (
	"strings"
)

// Command arguments are parsed into typed nodes, guided by the command's schema.
// The schema says what each position is, so nothing has to be guessed from text:
// a ParamField position parses as an expression, a ParamList as a real list, a
// ParamAggSpec as an aggregate specification, and a literal keeps its quoting.
// This is the only place a command's arguments are read, so quoting, whitespace
// and structure reach the handler intact.

// argParser walks a command's argument tokens.
type argParser struct {
	tokens []Token
	pos    int
	source []rune
	spec   *CommandSpec
	// cursor picks the parameter each bare argument fills, so a value written
	// after a named one is read in the shape the schema declares for it.
	cursor *positionalCursor
}

// parseArguments builds the typed arguments for a command from the tokens
// between its parentheses.
func parseArguments(tokens []Token, source []rune, spec *CommandSpec) ([]Argument, error) {
	p := &argParser{tokens: tokens, source: source, spec: spec, cursor: newPositionalCursor(spec)}
	var out []Argument
	for !p.done() {
		before := p.pos
		arg, err := p.parseOne()
		if err != nil {
			return nil, err
		}
		out = append(out, arg)
		switch p.current().Type {
		case TokenComma:
			p.advance()
		case TokenEOF:
		default:
			// An argument that ends before the next comma left text behind, which
			// used to be dropped silently: eval(x = a b) assigned only a.
			return nil, newPosError(p.current(), "%s(): unexpected %q after %s",
				spec.Name, p.current().Value, arg)
		}
		if p.pos == before {
			// Nothing was consumed, so looping again would spin forever.
			return nil, newPosError(p.current(), "%s(): unexpected %s in arguments", spec.Name, p.current().Type)
		}
	}
	return out, nil
}

func (p *argParser) done() bool {
	return p.pos >= len(p.tokens) || p.current().Type == TokenEOF
}

func (p *argParser) current() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *argParser) peek() Token {
	if p.pos+1 >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos+1]
}

// startsNamedArgument reports whether the tokens at i begin a "name=" argument.
func (p *argParser) startsNamedArgument(i int) bool {
	return i+1 < len(p.tokens) && p.tokens[i].Type == TokenField && p.tokens[i+1].Type == TokenEqual
}

func (p *argParser) advance() Token {
	t := p.current()
	p.pos++
	return t
}

// parseOne reads a single argument, named or positional, and parses its value
// according to the parameter it binds to.
func (p *argParser) parseOne() (Argument, error) {
	start := p.current()

	// name=value, where name is a declared parameter. An identifier followed by
	// '=' that is not a parameter is a value in its own right.
	if start.Type == TokenField && p.peek().Type == TokenEqual {
		param, ok := p.spec.lookup(start.Value)
		if !ok && !p.spec.FreeForm {
			// A typo'd parameter used to bind positionally and run at the default,
			// which is a silently different query.
			return Argument{}, newPosError(start, "%s(): unknown parameter %s (accepts %s)",
				p.spec.Name, start.Value, p.spec.paramList())
		}
		p.advance()
		p.advance()
		// A free-form command's name=value is an assignment it defines itself,
		// such as eval(total = bytes * 2), whose value is an expression. A
		// declared parameter is read in the shape its schema gives it, and is
		// marked filled so a later bare value moves on to the next parameter.
		name, read := start.Value, p.parseExprValue
		if ok {
			p.cursor.fill(param.Name)
			name = strings.ToLower(param.Name)
			read = func() (Argument, error) { return p.parseValue(param.Kind) }
		}
		if p.valueMissing() {
			// name= with nothing after it. Parsing on would read the next argument,
			// or the query text beyond this command, as the value; defaulting would
			// run a different query than the one written.
			return Argument{}, newPosError(start, "%s(): %s= has no value", p.spec.Name, strings.ToLower(name))
		}
		value, err := read()
		if err != nil {
			return Argument{}, err
		}
		value.Name = name
		value.Pos = start.Pos
		return value, nil
	}

	kind := ParamAny
	if param, ok := p.cursor.take(); ok {
		kind = param.Kind
	}
	value, err := p.parseValue(kind)
	if err != nil {
		return Argument{}, err
	}
	value.Pos = start.Pos
	return value, nil
}

// valueMissing reports whether a name= is followed by no value at all.
func (p *argParser) valueMissing() bool {
	switch p.current().Type {
	case TokenEOF, TokenComma, TokenRParen, TokenRBracket:
		return true
	}
	return p.pos >= len(p.tokens)
}

// parseValue reads one value in the shape the schema declares.
func (p *argParser) parseValue(kind ParamKind) (Argument, error) {
	// A binding reads the same in every parameter position. Letting a list or a
	// literal parameter take it as text made in(user, &admins) compile to
	// IN ('&admins'), a filter that matches nothing and says nothing.
	if p.current().Type == TokenBinding {
		return p.parseBindingRef(), nil
	}
	if p.current().Type == TokenLBracket {
		return p.parseList(elementKind(kind))
	}
	switch kind {
	case ParamAggSpec:
		return p.parseAggSpec()
	case ParamField:
		return p.parseExpr()
	case ParamList:
		return p.parseList(ParamAny)
	case ParamLiteral:
		return p.parseLiteral(), nil
	}
	// ParamAny and ParamBlock: a call is an expression, anything else a literal.
	if p.current().Type == TokenFunction {
		if _, known := exprFuncs[strings.ToLower(p.current().Value)]; known {
			return p.parseExpr()
		}
		return p.parseAggSpec()
	}
	return p.parseLiteral(), nil
}

// elementKind is what the members of a list are. A list parameter holds values,
// not further lists; every other kind describes its own members.
func elementKind(k ParamKind) ParamKind {
	if k == ParamList {
		return ParamAny
	}
	return k
}

// parseLiteral reads a single token as text, recording whether it was quoted. A
// leading minus belongs to the number that follows it: split(path, "/", -1)
// indexes from the end.
func (p *argParser) parseLiteral() Argument {
	if p.current().Type == TokenMinus && p.peek().Type == TokenValue && isNumericLiteral(p.peek().Value) {
		minus := p.advance()
		num := p.advance()
		return Argument{Kind: ArgLiteral, Text: "-" + num.Value, Pos: minus.Pos}
	}
	tok := p.advance()
	kind := ArgLiteral
	if tok.Type == TokenRegex {
		kind = ArgRegex
	}
	return Argument{Kind: kind, Text: tok.Value, Quoted: tok.Type == TokenString, Pos: tok.Pos}
}

// parseExpr parses a field position as an expression. A quoted value there names
// a field, so it stays a literal; parseExprValue is the form that reads it as a
// string expression.
func (p *argParser) parseExpr() (Argument, error) {
	if t := p.current().Type; t == TokenString || t == TokenRegex {
		return p.parseLiteral(), nil
	}
	return p.parseExprValue()
}

// parseExprValue parses a value position as an expression, using the original
// source so nothing is reconstructed from tokens.
func (p *argParser) parseExprValue() (Argument, error) {
	tok := p.current()
	if tok.Type == TokenRegex {
		return p.parseLiteral(), nil
	}
	if p.source == nil {
		return p.parseLiteral(), nil
	}
	expr, end, err := ParseExpressionAtPrec(p.source, tok.Pos, booleanChainPrecedence)
	if err != nil {
		return Argument{}, err
	}
	p.skipPast(end)
	// Keep the source text: it is what a derived alias and an error message use,
	// and the parsed node cannot reproduce the author's spelling.
	return Argument{Kind: ArgExpr, Expr: expr, Text: string(p.source[tok.Pos:end]), Pos: tok.Pos}, nil
}

// parseList reads a bracket list or a bare comma-separated run. Elements keep
// their own kind, so a list of expressions and a list of literals both work.
func (p *argParser) parseList(elem ParamKind) (Argument, error) {
	start := p.current()
	bracketed := start.Type == TokenLBracket
	if bracketed {
		p.advance()
	}
	list := Argument{Kind: ArgList, Pos: start.Pos}
	for !p.done() && p.current().Type != TokenRBracket {
		before := p.pos
		element, err := p.parseValue(elem)
		if err != nil {
			return Argument{}, err
		}
		list.List = append(list.List, element)
		if p.pos == before || p.current().Type != TokenComma {
			break
		}
		// An unbracketed list ends where the next named argument begins, and the
		// comma stays for it: hash=a,b, threshold=50.
		if p.startsNamedArgument(p.pos + 1) {
			break
		}
		p.advance()
	}
	if bracketed && p.current().Type == TokenRBracket {
		p.advance()
	}
	return list, nil
}

// parseAggSpec reads count(), sum(bytes) or multi(...), recursively.
func (p *argParser) parseAggSpec() (Argument, error) {
	tok := p.current()
	if tok.Type == TokenString {
		// bucket("1h", "count()") quotes the spec. The only thing that may appear
		// in this position is a call, so reading it as one is unambiguous.
		if spec, ok := quotedAggSpec(tok.Value); ok {
			p.advance()
			return Argument{Kind: ArgAggSpec, Agg: spec, Pos: tok.Pos}, nil
		}
	}
	if tok.Type != TokenFunction {
		return p.parseLiteral(), nil
	}
	p.advance()
	spec := &AggSpec{Name: strings.ToLower(tok.Value), Pos: tok.Pos}
	if p.current().Type != TokenLParen {
		return Argument{Kind: ArgAggSpec, Agg: spec, Pos: tok.Pos}, nil
	}
	p.advance()

	nested := ParamAny
	if spec.Name == "multi" {
		nested = ParamAggSpec
	}
	for !p.done() && p.current().Type != TokenRParen {
		before := p.pos
		arg, err := p.parseAggArg(nested)
		if err != nil {
			return Argument{}, err
		}
		spec.Args = append(spec.Args, arg)
		if p.current().Type == TokenComma {
			p.advance()
		}
		if p.pos == before {
			break
		}
	}
	if p.current().Type == TokenRParen {
		p.advance()
	}
	return Argument{Kind: ArgAggSpec, Agg: spec, Pos: tok.Pos}, nil
}

// parseAggArg reads one argument of an aggregate specification. These carry
// their own name=value options (field=, as=, unique=).
func (p *argParser) parseAggArg(elem ParamKind) (Argument, error) {
	if p.current().Type == TokenField && p.peek().Type == TokenEqual {
		name := strings.ToLower(p.advance().Value)
		p.advance()
		kind := ParamAny
		if name == "field" {
			kind = ParamField
		}
		value, err := p.parseValue(kind)
		if err != nil {
			return Argument{}, err
		}
		value.Name = name
		return value, nil
	}
	if p.current().Type == TokenLBracket {
		return p.parseList(elem)
	}
	if elem == ParamAggSpec {
		return p.parseAggSpec()
	}
	// A bare argument of an aggregate is the field it aggregates, which may be
	// an expression: sum(bytes * 8).
	return p.parseValue(ParamField)
}

// skipPast advances the token stream past every token the expression consumed.
func (p *argParser) skipPast(offset int) {
	for p.pos < len(p.tokens) && p.tokens[p.pos].Type != TokenEOF && p.tokens[p.pos].End <= offset {
		p.pos++
	}
}

// quotedAggSpec parses an aggregate specification written inside a string.
func quotedAggSpec(text string) (*AggSpec, bool) {
	tokens, err := NewLexer(text).Tokenize()
	if err != nil || len(tokens) == 0 || tokens[0].Type != TokenFunction {
		return nil, false
	}
	p := &argParser{tokens: tokens, spec: &CommandSpec{}}
	arg, err := p.parseAggSpec()
	if err != nil || arg.Agg == nil || !p.done() {
		return nil, false
	}
	return arg.Agg, true
}

// parseBindingRef reads a binding reference written as a whole argument. The
// parser substitutes it once the pipeline is built; what it becomes depends on
// the binding's kind.
func (p *argParser) parseBindingRef() Argument {
	tok := p.advance()
	return Argument{
		Kind: ArgExpr,
		Expr: &ExprNode{Kind: ExprField, Value: tok.Value, Pos: tok.Pos},
		Text: tok.Value,
		Pos:  tok.Pos,
	}
}
