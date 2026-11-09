package parser

import (
	"fmt"
	"strings"
	"unicode"
)

type TokenType int

const (
	TokenEOF TokenType = iota
	TokenError
	TokenPipe
	TokenField
	TokenValue
	TokenString
	TokenRegex
	TokenEqual
	TokenNotEqual
	TokenAssign
	TokenAnd
	TokenOr
	TokenNot
	TokenLParen
	TokenRParen
	TokenLBracket
	TokenRBracket
	TokenLBrace
	TokenRBrace
	TokenComma
	TokenSemicolon
	TokenFunction
	TokenGreater
	TokenLess
	TokenGreaterEqual
	TokenLessEqual
	TokenPlus
	TokenMinus
	TokenMultiply
	TokenDivide
	TokenContainsAny      // =~ (substring match any, case-insensitive)
	TokenStartsWithAny    // =^ (prefix match any, case-insensitive)
	TokenEndsWithAny      // =$ (suffix match any, case-insensitive)
	TokenBinding          // &name, a reference to a let binding
	TokenNotContainsAny   // !=~ (negated =~)
	TokenNotStartsWithAny // !=^ (negated =^)
	TokenNotEndsWithAny   // !=$ (negated =$)
)

type Token struct {
	Type  TokenType
	Value string
	Text  string // the token as written: a regex's /pattern/flags, a string's quotes
	Pos   int    // start offset (rune index, inclusive) of the token in the source input
	End   int    // end offset (rune index, exclusive) of the token in the source input
}

type Lexer struct {
	runes    []rune
	pos      int
	ch       rune
	lastType TokenType
	syntax   lexSyntax
	// scanComments makes the lexer skip comment lines and record their [start, end)
	// rune spans in comments. Only StripComments sets it: everything else lexes text
	// with comments already blanked, or a fragment that has no lines of its own.
	scanComments bool
	comments     [][2]int
}

func NewLexer(input string) *Lexer {
	l := &Lexer{runes: []rune(input)}
	l.readChar()
	return l
}

// newExprLexer lexes src in expression mode from start, reporting positions in
// src's own coordinates.
func newExprLexer(src []rune, start int) *Lexer {
	l := &Lexer{runes: src, pos: start, syntax: exprSyntax}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.pos >= len(l.runes) {
		l.ch = 0
	} else {
		l.ch = l.runes[l.pos]
	}
	l.pos++
}

func (l *Lexer) peekChar() rune {
	if l.pos >= len(l.runes) {
		return 0
	}
	return l.runes[l.pos]
}

func (l *Lexer) skipWhitespace() {
	for {
		for unicode.IsSpace(l.ch) {
			l.readChar()
		}
		if !l.scanComments || l.ch != '/' || l.peekChar() != '/' || !l.atLineStart() {
			return
		}
		start := l.pos - 1
		for l.ch != 0 && l.ch != '\n' && l.ch != '\r' {
			l.readChar()
		}
		l.comments = append(l.comments, [2]int{start, l.pos - 1})
	}
}

// atLineStart reports whether only whitespace precedes the current rune on its line.
func (l *Lexer) atLineStart() bool {
	for i := l.pos - 2; i >= 0; i-- {
		r := l.runes[i]
		if r == '\n' || r == '\r' {
			return true
		}
		if !unicode.IsSpace(r) {
			return false
		}
	}
	return true
}

func (l *Lexer) readString(quote rune) string {
	var result strings.Builder
	l.readChar() // skip opening quote

	for l.ch != quote && l.ch != 0 {
		if l.ch == '\\' {
			l.readChar()
			if l.ch != 0 {
				result.WriteRune(l.ch)
			}
		} else {
			result.WriteRune(l.ch)
		}
		l.readChar()
	}

	l.readChar() // skip closing quote
	return result.String()
}

func (l *Lexer) readRegex() string {
	var result strings.Builder
	l.readChar() // skip opening /

	// inClass tracks whether we are inside a character class [...].
	// A literal / inside [...] must not terminate the regex — only an unescaped /
	// outside a character class is the closing delimiter.
	inClass := false
	for l.ch != 0 {
		if l.ch == '\\' {
			result.WriteRune(l.ch)
			l.readChar()
			if l.ch != 0 {
				result.WriteRune(l.ch)
			}
		} else if l.ch == '[' {
			inClass = true
			result.WriteRune(l.ch)
		} else if l.ch == ']' && inClass {
			inClass = false
			result.WriteRune(l.ch)
		} else if l.ch == '/' && !inClass {
			break
		} else {
			result.WriteRune(l.ch)
		}
		l.readChar()
	}

	pattern := result.String()
	l.readChar() // skip closing /

	// Check for regex flags (i for case-insensitive)
	flags := ""
	for l.ch == 'i' || l.ch == 'g' || l.ch == 'm' {
		flags += string(l.ch)
		l.readChar()
	}

	if strings.Contains(flags, "i") {
		pattern = "(?i)" + pattern
	}

	return pattern
}

// readBindingName reads a binding's name. Deliberately stricter than
// readIdentifier, which also takes '.', '-' and '*': a binding is a name the
// author chose, not a JSON path or a wildcard, and &a.b reading as one name
// would make the column it produces depend on punctuation.
func (l *Lexer) readBindingName() string {
	var result strings.Builder
	for unicode.IsLetter(l.ch) || unicode.IsDigit(l.ch) || l.ch == '_' {
		result.WriteRune(l.ch)
		l.readChar()
	}
	return result.String()
}

// lexSyntax is the grammar a lexer reads, which decides what continues a word.
type lexSyntax int

const (
	// filterSyntax folds '-' and '*' into a word: a hyphenated field name, a
	// wildcard value.
	filterSyntax lexSyntax = iota
	// exprSyntax keeps them operators, so arithmetic splits into tokens, except a
	// '-' wedged between two words (user-agent).
	exprSyntax
)

// continuesWord reports whether ch, followed by next, continues a word.
func (s lexSyntax) continuesWord(ch, next rune) bool {
	switch {
	case unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_' || ch == '.':
		return true
	case s == filterSyntax:
		return ch == '-' || ch == '*'
	}
	return ch == '-' && isHyphenatedName(next)
}

func (l *Lexer) readIdentifier() string {
	var result strings.Builder
	for l.syntax.continuesWord(l.ch, l.peekChar()) {
		result.WriteRune(l.ch)
		l.readChar()
	}
	return result.String()
}

// isHyphenatedName reports whether a '-' continues an identifier rather than
// starting a subtraction. A hyphen wedged directly between two words is part of
// the name (user-agent); one before a digit, or with a space on either side, is
// the operator.
func isHyphenatedName(next rune) bool {
	return unicode.IsLetter(next) || next == '_'
}

// operators are the punctuation tokens, longest first so no spelling shadows a
// longer one it begins. '/' is not here: it is a division, a regex or an error by
// what precedes it (see slash).
var operators = []struct {
	text string
	typ  TokenType
}{
	{"!=~", TokenNotContainsAny}, {"!=^", TokenNotStartsWithAny}, {"!=$", TokenNotEndsWithAny},
	{":=", TokenAssign}, {"=~", TokenContainsAny}, {"=^", TokenStartsWithAny}, {"=$", TokenEndsWithAny},
	{"!=", TokenNotEqual}, {">=", TokenGreaterEqual}, {"<=", TokenLessEqual},
	{"|", TokenPipe}, {"=", TokenEqual}, {"!", TokenNot}, {">", TokenGreater}, {"<", TokenLess},
	{"(", TokenLParen}, {")", TokenRParen}, {"[", TokenLBracket}, {"]", TokenRBracket},
	{"{", TokenLBrace}, {"}", TokenRBrace}, {",", TokenComma}, {";", TokenSemicolon},
	{"*", TokenMultiply}, {"+", TokenPlus}, {"-", TokenMinus},
}

// keywords are the words that are operators, matched in any case.
var keywords = map[string]TokenType{"AND": TokenAnd, "OR": TokenOr, "NOT": TokenNot}

// NextToken reads the next token, its span and the text it was written as.
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()
	// l.ch is the rune at index l.pos-1.
	start := l.pos - 1
	tok, end := l.scan()
	l.lastType = tok.Type
	// For EOF nothing is consumed, so End == Pos: a zero-width caret at the end.
	tok.Pos, tok.End = start, max(end, start)
	// An unterminated string or regex reads past the end of the input.
	tok.Text = string(l.runes[min(start, len(l.runes)):min(tok.End, len(l.runes))])
	return tok
}

// scan reads one token at l.ch, and returns it with its exclusive end offset.
func (l *Lexer) scan() (Token, int) {
	switch c := l.ch; {
	case c == 0:
		return Token{Type: TokenEOF}, l.pos - 1
	case c == '"' || c == '\'':
		return Token{Type: TokenString, Value: l.readString(c)}, l.pos - 1
	case c == '/':
		return l.slash(), l.pos - 1
	case c == '&':
		return l.binding(), l.pos - 1
	case unicode.IsLetter(c) || c == '_':
		return l.word()
	case unicode.IsDigit(c):
		return Token{Type: TokenValue, Value: l.readIdentifier()}, l.pos - 1
	}
	for _, op := range operators {
		if l.hasPrefix(op.text) {
			l.advance(len(op.text))
			return Token{Type: op.typ, Value: op.text}, l.pos - 1
		}
	}
	// A lone ':' is no operator; the parser reports it where it is out of place.
	if l.ch == ':' {
		l.readChar()
		return Token{Type: TokenValue, Value: ":"}, l.pos - 1
	}
	c := l.ch
	l.readChar()
	return Token{Type: TokenError, Value: string(c)}, l.pos - 1
}

// hasPrefix reports whether the input continues with s at l.ch.
func (l *Lexer) hasPrefix(s string) bool {
	i := l.pos - 1
	for _, r := range s {
		if i >= len(l.runes) || l.runes[i] != r {
			return false
		}
		i++
	}
	return true
}

// advance reads n runes.
func (l *Lexer) advance(n int) {
	for range n {
		l.readChar()
	}
}

// slash reads a token at '/': a division after an operand, else a regex. An empty
// regex is never intended; it is a comment that does not start its line, and would
// otherwise silently match everything.
func (l *Lexer) slash() Token {
	switch {
	case l.lastType == TokenRParen || l.lastType == TokenRBracket || l.lastType == TokenValue || l.lastType == TokenField:
		l.readChar()
		return Token{Type: TokenDivide, Value: "/"}
	case l.peekChar() == '/':
		l.advance(2)
		return Token{Type: TokenError, Value: "//"}
	}
	return Token{Type: TokenRegex, Value: l.readRegex()}
}

// binding reads &name, a let binding. The sigil is kept in the value so a binding
// and a log field of the same name can never collide.
func (l *Lexer) binding() Token {
	l.readChar()
	if !unicode.IsLetter(l.ch) && l.ch != '_' {
		l.readChar()
		return Token{Type: TokenError, Value: "&"}
	}
	return Token{Type: TokenBinding, Value: "&" + l.readBindingName()}
}

// word reads an identifier: a keyword, a function name when '(' follows (case
// takes '{'), or a field. Its end is taken before the lookahead skips whitespace.
func (l *Lexer) word() (Token, int) {
	ident := l.readIdentifier()
	end := l.pos - 1
	if typ, ok := keywords[strings.ToUpper(ident)]; ok {
		return Token{Type: typ, Value: ident}, end
	}
	opens := '('
	if ident == "case" {
		opens = '{'
	}
	l.skipWhitespace()
	if l.ch == opens {
		return Token{Type: TokenFunction, Value: ident}, end
	}
	return Token{Type: TokenField, Value: ident}, end
}

func (l *Lexer) Tokenize() ([]Token, error) {
	var tokens []Token
	for {
		tok := l.NextToken()
		if tok.Type == TokenError {
			return nil, lexError(tok)
		}
		if tok.Type == TokenEOF && len(tokens) > 0 {
			// End of query reads as just after the last token, not after
			// trailing whitespace or blanked comment lines.
			tok.Pos, tok.End = tokens[len(tokens)-1].End, tokens[len(tokens)-1].End
		}
		tokens = append(tokens, tok)
		if tok.Type == TokenEOF {
			break
		}
	}
	return tokens, nil
}

// lexError reports a token the lexer could not read.
func lexError(tok Token) error {
	if tok.Value == "//" {
		return newPosError(tok, "unexpected '//': a comment goes on its own line")
	}
	return newPosError(tok, "unexpected character %q in query", tok.Value)
}

// tokenNames are the token kinds as a user reads them in an error message: an
// operator by its spelling, anything else by what it is.
var tokenNames = func() map[TokenType]string {
	names := map[TokenType]string{
		TokenEOF:      "end of query",
		TokenError:    "invalid character",
		TokenField:    "field name",
		TokenValue:    "value",
		TokenString:   "string",
		TokenRegex:    "regex",
		TokenFunction: "function name",
		TokenBinding:  "binding",
		TokenDivide:   "'/'",
	}
	for _, op := range operators {
		names[op.typ] = "'" + op.text + "'"
	}
	for word, typ := range keywords {
		names[typ] = word
	}
	return names
}()

// String names the token kind as a user would read it in an error message.
func (t TokenType) String() string {
	if name, ok := tokenNames[t]; ok {
		return name
	}
	return fmt.Sprintf("token %d", t)
}
