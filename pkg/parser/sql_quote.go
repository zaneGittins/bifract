package parser

import "bifract/pkg/sqlquote"

// User text reaches SQL only through these and quoteIdent: each escapes what it is
// given, so no caller writes a quote around a value by hand (see
// TestUserTextEntersSQLOnlyThroughConstructors).

// sqlString is s as a ClickHouse string literal: quoted, with quotes, backslashes
// and control characters escaped.
func sqlString(s string) string {
	return sqlquote.String(s)
}

// backtickIdent is name as a quoted identifier, whatever characters it holds.
func backtickIdent(name string) string {
	return sqlquote.Ident(name)
}
