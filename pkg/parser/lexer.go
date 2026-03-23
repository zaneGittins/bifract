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
)

type Token struct {
	Type  TokenType
	Value string
}

type Lexer struct {
	runes    []rune
	pos      int
	ch       rune
	lastType TokenType
}

func NewLexer(input string) *Lexer {
	l := &Lexer{runes: []rune(input)}
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
	for unicode.IsSpace(l.ch) {
		l.readChar()
	}
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

	for l.ch != '/' && l.ch != 0 {
		if l.ch == '\\' {
			result.WriteRune(l.ch)
			l.readChar()
			if l.ch != 0 {
				result.WriteRune(l.ch)
			}
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

func (l *Lexer) readIdentifier() string {
	var result strings.Builder
	for unicode.IsLetter(l.ch) || unicode.IsDigit(l.ch) || l.ch == '_' || l.ch == '-' || l.ch == '.' || l.ch == '*' {
		result.WriteRune(l.ch)
		l.readChar()
	}
	return result.String()
}

func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	var tok Token

	switch l.ch {
	case 0:
		tok = Token{Type: TokenEOF, Value: ""}
	case '|':
		tok = Token{Type: TokenPipe, Value: "|"}
		l.readChar()
	case ':':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TokenAssign, Value: ":="}
			l.readChar()
		} else {
			// Single colon is not a valid token in our language
			tok = Token{Type: TokenValue, Value: ":"}
			l.readChar()
		}
	case '=':
		tok = Token{Type: TokenEqual, Value: "="}
		l.readChar()
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TokenNotEqual, Value: "!="}
			l.readChar()
		} else {
			tok = Token{Type: TokenNot, Value: "!"}
			l.readChar()
		}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TokenGreaterEqual, Value: ">="}
			l.readChar()
		} else {
			tok = Token{Type: TokenGreater, Value: ">"}
			l.readChar()
		}
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TokenLessEqual, Value: "<="}
			l.readChar()
		} else {
			tok = Token{Type: TokenLess, Value: "<"}
			l.readChar()
		}
	case '(':
		tok = Token{Type: TokenLParen, Value: "("}
		l.readChar()
	case ')':
		tok = Token{Type: TokenRParen, Value: ")"}
		l.readChar()
	case '[':
		tok = Token{Type: TokenLBracket, Value: "["}
		l.readChar()
	case ']':
		tok = Token{Type: TokenRBracket, Value: "]"}
		l.readChar()
	case '{':
		tok = Token{Type: TokenLBrace, Value: "{"}
		l.readChar()
	case '}':
		tok = Token{Type: TokenRBrace, Value: "}"}
		l.readChar()
	case ',':
		tok = Token{Type: TokenComma, Value: ","}
		l.readChar()
	case ';':
		tok = Token{Type: TokenSemicolon, Value: ";"}
		l.readChar()
	case '"':
		tok = Token{Type: TokenString, Value: l.readString('"')}
	case '\'':
		tok = Token{Type: TokenString, Value: l.readString('\'')}
	case '/':
		// After ), ], value, or field, treat / as division, not regex
		if l.lastType == TokenRParen || l.lastType == TokenRBracket || l.lastType == TokenValue || l.lastType == TokenField {
			tok = Token{Type: TokenDivide, Value: "/"}
			l.readChar()
		} else {
			tok = Token{Type: TokenRegex, Value: l.readRegex()}
		}
	case '*':
		tok = Token{Type: TokenMultiply, Value: "*"}
		l.readChar()
	case '+':
		tok = Token{Type: TokenPlus, Value: "+"}
		l.readChar()
	case '-':
		tok = Token{Type: TokenMinus, Value: "-"}
		l.readChar()
	default:
		if unicode.IsLetter(l.ch) || l.ch == '_' {
			ident := l.readIdentifier()
			// Check if it's a keyword
			switch strings.ToUpper(ident) {
			case "AND":
				tok = Token{Type: TokenAnd, Value: ident}
			case "OR":
				tok = Token{Type: TokenOr, Value: ident}
			case "NOT":
				tok = Token{Type: TokenNot, Value: ident}
			default:
				// Special handling for case function which uses { } instead of ( )
				if ident == "case" {
					l.skipWhitespace()
					if l.ch == '{' {
						tok = Token{Type: TokenFunction, Value: ident}
					} else {
						tok = Token{Type: TokenField, Value: ident}
					}
				} else {
					// Check if next char is '(' to determine if it's a function
					l.skipWhitespace()
					if l.ch == '(' {
						tok = Token{Type: TokenFunction, Value: ident}
					} else {
						tok = Token{Type: TokenField, Value: ident}
					}
				}
			}
		} else if unicode.IsDigit(l.ch) {
			tok = Token{Type: TokenValue, Value: l.readIdentifier()}
		} else {
			tok = Token{Type: TokenError, Value: string(l.ch)}
			l.readChar()
		}
	}

	l.lastType = tok.Type
	return tok
}

func (l *Lexer) Tokenize() ([]Token, error) {
	var tokens []Token
	for {
		tok := l.NextToken()
		if tok.Type == TokenError {
			return nil, fmt.Errorf("unexpected character %q in query", tok.Value)
		}
		tokens = append(tokens, tok)
		if tok.Type == TokenEOF {
			break
		}
	}
	return tokens, nil
}

func (t TokenType) String() string {
	names := map[TokenType]string{
		TokenEOF:      "EOF",
		TokenError:    "ERROR",
		TokenPipe:     "PIPE",
		TokenField:    "FIELD",
		TokenValue:    "VALUE",
		TokenString:   "STRING",
		TokenRegex:    "REGEX",
		TokenEqual:       "EQUAL",
		TokenNotEqual:    "NOTEQUAL",
		TokenAssign:      "ASSIGN",
		TokenAnd:         "AND",
		TokenOr:          "OR",
		TokenNot:         "NOT",
		TokenLParen:      "LPAREN",
		TokenRParen:      "RPAREN",
		TokenLBracket:    "LBRACKET",
		TokenRBracket:    "RBRACKET",
		TokenLBrace:      "LBRACE",
		TokenRBrace:      "RBRACE",
		TokenComma:       "COMMA",
		TokenSemicolon:   "SEMICOLON",
		TokenFunction:    "FUNCTION",
		TokenGreater:     "GREATER",
		TokenLess:        "LESS",
		TokenGreaterEqual: "GREATEREQUAL",
		TokenLessEqual:   "LESSEQUAL",
		TokenPlus:        "PLUS",
		TokenMinus:       "MINUS",
		TokenMultiply:    "MULTIPLY",
		TokenDivide:      "DIVIDE",
	}
	if name, ok := names[t]; ok {
		return name
	}
	return fmt.Sprintf("UNKNOWN(%d)", t)
}
