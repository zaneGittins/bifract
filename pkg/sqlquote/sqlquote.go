// Package sqlquote is the one way a value enters ClickHouse SQL as text: String for
// a string literal and Ident for an identifier. Each escapes what it is given, so no
// code writes a quote around a value by hand, which is how an escape gets forgotten
// (see TestValuesEnterSQLOnlyThroughConstructors).
package sqlquote

import "strings"

var literalEscaper = strings.NewReplacer("\\", "\\\\", "'", "\\'", "\n", "\\n", "\r", "\\r", "\t", "\\t", "\x00", "\\0")

// String is s as a ClickHouse string literal: quoted, with quotes, backslashes and
// control characters escaped.
func String(s string) string {
	return "'" + literalEscaper.Replace(s) + "'"
}

// Ident is name as a backtick-quoted identifier, whatever characters it holds.
// Backslashes are escaped first: ClickHouse honours a backslash-escaped backtick
// inside the quotes, so doubling only backticks would let a trailing backslash
// escape the closing quote.
func Ident(name string) string {
	return "`" + strings.ReplaceAll(strings.ReplaceAll(name, "\\", "\\\\"), "`", "``") + "`"
}

// PGString is s as a PostgreSQL string literal. Postgres reads a backslash
// literally (standard_conforming_strings), so only the quote is doubled.
func PGString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
