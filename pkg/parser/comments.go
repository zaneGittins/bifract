package parser

import (
	"strings"
	"unicode"
)

// StripComments blanks // comment lines so the query parses, keeping every
// other rune where it was so error offsets still index the text as written. A
// comment starts where only whitespace precedes it on its line and runs to the
// line end; \n, \r and \r\n all end a line. It also returns how many runes it
// blanked, so len(result)-blanked is the length of the query without comments.
func StripComments(query string) (string, int) {
	if !strings.Contains(query, "//") {
		return query, 0
	}
	runes := []rune(query)
	blanked := 0
	lineStart := true
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch {
		case r == '\n' || r == '\r':
			lineStart = true
		case lineStart && r == '/' && i+1 < len(runes) && runes[i+1] == '/':
			for ; i < len(runes) && runes[i] != '\n' && runes[i] != '\r'; i++ {
				blanked++
				runes[i] = ' '
			}
			i-- // hand the line break back to the loop
		case !unicode.IsSpace(r):
			lineStart = false
		}
	}
	return string(runes), blanked
}
