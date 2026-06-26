package parser

import (
	"errors"
	"fmt"
)

// ParseError is an error that carries the source span (rune offsets) of the
// offending text so editors can underline the exact location. Start is
// inclusive, End is exclusive; both are rune (code point) indices into the
// original query string, NOT byte offsets. A zero-width span (Start == End)
// points at a caret position, e.g. an unexpected end of input.
type ParseError struct {
	Msg   string
	Start int
	End   int
}

func (e *ParseError) Error() string { return e.Msg }

// newPosError builds a ParseError spanning a single token. When the token has
// no usable end (synthetic EOF), the span collapses to a caret at Start.
func newPosError(tok Token, format string, args ...interface{}) *ParseError {
	start := tok.Pos
	end := tok.End
	if end < start {
		end = start
	}
	return &ParseError{Msg: fmt.Sprintf(format, args...), Start: start, End: end}
}

// ErrorPosition extracts the rune span from an error if it carries one. ok is
// false for errors without position information (e.g. translator semantic
// errors), in which case the editor should fall back to a non-positioned
// message banner instead of underlining.
func ErrorPosition(err error) (start, end int, ok bool) {
	var pe *ParseError
	if errors.As(err, &pe) {
		return pe.Start, pe.End, true
	}
	return 0, 0, false
}
