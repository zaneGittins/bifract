package parser

import (
	"fmt"
	"strings"
)

// ExprKind discriminates the expression AST.
type ExprKind int

const (
	ExprString  ExprKind = iota // quoted literal
	ExprNumber                  // numeric literal, kept as text so it round-trips exactly
	ExprField                   // bare identifier: a log field or a computed column
	ExprCall                    // function call
	ExprBinary                  // a <op> b
	ExprUnary                   // <op> a
	ExprBoolean                 // true / false
)

// ExprNode is a scalar expression. It replaces the flat expression string the
// old := handling carried, which could not represent calls or nesting.
type ExprNode struct {
	Kind ExprKind
	// Value holds the literal text, field name, function name or operator,
	// depending on Kind.
	Value string
	Args  []*ExprNode // ExprCall positional arguments
	// Named holds ExprCall arguments written name=value. A name given here may
	// not also be satisfied positionally; bindArgs enforces that.
	Named map[string]*ExprNode
	Left  *ExprNode // ExprBinary
	Right *ExprNode // ExprBinary
	Arg   *ExprNode // ExprUnary
	Pos   int       // offset in the original query, for error messages
}

// binaryPrecedence returns the binding power of an infix operator, or 0 when the
// token does not continue an expression.
func binaryPrecedence(t TokenType) int {
	switch t {
	case TokenOr:
		return 1
	case TokenAnd:
		return 2
	case TokenEqual, TokenNotEqual, TokenGreater, TokenLess, TokenGreaterEqual, TokenLessEqual:
		return 3
	case TokenPlus, TokenMinus:
		return 4
	case TokenMultiply, TokenDivide:
		return 5
	}
	return 0
}

// exprParser is a Pratt parser over a token stream lexed in expression mode.
//
// Expression mode matters: the ordinary lexer folds '*' and '-' into
// identifiers (lexer.go readIdentifier), so `a*b` and `a-b` arrive as one token
// and cannot be parsed as arithmetic. Expression mode splits them, which is
// safe here because a wildcard value and a hyphenated field name are filter
// syntax, never expression syntax.
type exprParser struct {
	tokens []Token
	pos    int
}

// ParseExpressionAt lexes and parses one expression starting at the given offset
// in src. It returns the expression and the offset just past it, so the caller
// can resynchronise its own token stream.
func ParseExpressionAt(src []rune, start int) (*ExprNode, int, error) {
	lexer := NewLexer(string(src[start:]))
	lexer.exprMode = true
	tokens, err := lexer.Tokenize()
	if err != nil {
		return nil, 0, err
	}
	// Rebase onto the original query so error positions and the resync offset are
	// in the caller's coordinates.
	for i := range tokens {
		tokens[i].Pos += start
		tokens[i].End += start
	}
	p := &exprParser{tokens: tokens}
	expr, err := p.parse(0)
	if err != nil {
		return nil, 0, err
	}
	return expr, p.consumedEnd(), nil
}

// consumedEnd is the offset just past the last token the parser accepted.
func (p *exprParser) consumedEnd() int {
	if p.pos == 0 {
		return 0
	}
	return p.tokens[p.pos-1].End
}

func (p *exprParser) current() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *exprParser) peek() Token {
	if p.pos+1 >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos+1]
}

func (p *exprParser) advance() Token {
	tok := p.current()
	p.pos++
	return tok
}

// parse consumes an expression whose operators bind more tightly than minPrec.
func (p *exprParser) parse(minPrec int) (*ExprNode, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for {
		tok := p.current()
		prec := binaryPrecedence(tok.Type)
		if prec == 0 || prec <= minPrec {
			return left, nil
		}
		p.advance()
		right, err := p.parse(prec)
		if err != nil {
			return nil, err
		}
		left = &ExprNode{Kind: ExprBinary, Value: tok.Value, Left: left, Right: right, Pos: tok.Pos}
	}
}

func (p *exprParser) parseUnary() (*ExprNode, error) {
	tok := p.current()
	switch tok.Type {
	case TokenMinus, TokenNot:
		p.advance()
		arg, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &ExprNode{Kind: ExprUnary, Value: tok.Value, Arg: arg, Pos: tok.Pos}, nil
	}
	return p.parsePrimary()
}

func (p *exprParser) parsePrimary() (*ExprNode, error) {
	tok := p.current()
	switch tok.Type {
	case TokenLParen:
		p.advance()
		inner, err := p.parse(0)
		if err != nil {
			return nil, err
		}
		if p.current().Type != TokenRParen {
			return nil, newPosError(p.current(), "expected ')' in expression, got %s", p.current().Type)
		}
		p.advance()
		return inner, nil

	case TokenString:
		p.advance()
		return &ExprNode{Kind: ExprString, Value: tok.Value, Pos: tok.Pos}, nil

	case TokenFunction:
		return p.parseCall()

	case TokenField, TokenValue:
		p.advance()
		kind := ExprField
		switch {
		case isNumericLiteral(tok.Value):
			kind = ExprNumber
		case strings.EqualFold(tok.Value, "true"), strings.EqualFold(tok.Value, "false"):
			// Without this, isPrivateIP(ip) = false compares the condition to a log
			// field named "false", which exists on no row.
			kind = ExprBoolean
		}
		return &ExprNode{Kind: kind, Value: tok.Value, Pos: tok.Pos}, nil
	}
	return nil, newPosError(tok, "expected a value, field or function in expression, got %s", tok.Type)
}

func (p *exprParser) parseCall() (*ExprNode, error) {
	nameTok := p.advance()
	call := &ExprNode{Kind: ExprCall, Value: strings.ToLower(nameTok.Value), Pos: nameTok.Pos}
	if p.current().Type != TokenLParen {
		return nil, newPosError(p.current(), "expected '(' after %s", nameTok.Value)
	}
	p.advance()

	for p.current().Type != TokenRParen {
		if p.current().Type == TokenEOF {
			return nil, newPosError(p.current(), "unclosed call to %s()", call.Value)
		}
		name, arg, err := p.parseCallArg()
		if err != nil {
			return nil, err
		}
		if name == "" {
			if len(call.Named) > 0 {
				return nil, newPosError(p.current(), "%s(): positional arguments must come before named ones", call.Value)
			}
			call.Args = append(call.Args, arg)
		} else {
			if call.Named == nil {
				call.Named = map[string]*ExprNode{}
			}
			if _, dup := call.Named[name]; dup {
				return nil, newPosError(p.current(), "%s(): %s given twice", call.Value, name)
			}
			call.Named[name] = arg
		}
		if p.current().Type == TokenComma {
			p.advance()
		} else if p.current().Type != TokenRParen {
			return nil, newPosError(p.current(), "expected ',' or ')' in %s(), got %s", call.Value, p.current().Type)
		}
	}
	p.advance() // ')'
	return call, nil
}

// parseCallArg reads one argument, returning a non-empty name when it was
// written name=value.
func (p *exprParser) parseCallArg() (string, *ExprNode, error) {
	if p.current().Type == TokenField && p.peek().Type == TokenEqual {
		name := p.advance().Value
		p.advance() // '='
		arg, err := p.parse(0)
		return strings.ToLower(name), arg, err
	}
	arg, err := p.parse(0)
	return "", arg, err
}

func isNumericLiteral(s string) bool {
	if s == "" {
		return false
	}
	dots := 0
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r == '.':
			dots++
			if dots > 1 {
				return false
			}
		default:
			return false
		}
	}
	return s != "."
}

// String renders the expression back to BQL. Used in error messages so a
// complaint about a subexpression can quote it.
func (e *ExprNode) String() string {
	switch e.Kind {
	case ExprString:
		return `"` + e.Value + `"`
	case ExprNumber, ExprField, ExprBoolean:
		return e.Value
	case ExprUnary:
		return e.Value + e.Arg.String()
	case ExprBinary:
		return e.Left.String() + " " + e.Value + " " + e.Right.String()
	case ExprCall:
		parts := make([]string, 0, len(e.Args)+len(e.Named))
		for _, a := range e.Args {
			parts = append(parts, a.String())
		}
		for name, a := range e.Named {
			parts = append(parts, name+"="+a.String())
		}
		return e.Value + "(" + strings.Join(parts, ", ") + ")"
	}
	return fmt.Sprintf("<expr %d>", e.Kind)
}
