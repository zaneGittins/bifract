package parser

import "strings"

// StripComments blanks // comment lines, keeping every other rune where it was so
// error offsets still index the text as written. A comment starts where only
// whitespace precedes it on its line and runs to the line end; \n, \r and \r\n all
// end a line. A // inside a string or regex is literal text. It also returns how
// many runes it blanked, so len(result)-blanked is the length of the query without
// comments. Comments after a token the lexer rejects are left for that error to report.
func StripComments(query string) (string, int) {
	if !strings.Contains(query, "//") {
		return query, 0
	}
	// Only the lexer knows where a string or regex ends.
	l := NewLexer(query)
	l.scanComments = true
	for {
		if t := l.NextToken().Type; t == TokenEOF || t == TokenError {
			break
		}
	}
	blanked := 0
	for _, c := range l.comments {
		for i := c[0]; i < c[1]; i++ {
			l.runes[i] = ' '
		}
		blanked += c[1] - c[0]
	}
	return string(l.runes), blanked
}
