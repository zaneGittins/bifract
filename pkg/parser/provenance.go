package parser

import "fmt"

// Provenance (pgr) abstraction helpers. proc_freq stores ABSTRACTED behavioral keys so
// "the same behavior" collapses to one row (a per-user temp path, a churning DHCP host,
// etc. become one key). The abstraction MUST be byte-identical on the MV write side and
// the pgr() read side, so both sides call abstractExpr -- the single source of truth.
//
// The regex escaping here is validated against ClickHouse 26.6: raw-string literals hold
// the exact SQL text CH receives, so CH's string-literal unescape (\\ -> \) leaves the
// intended RE2 pattern. Do not "simplify" the backslashes without re-testing in CH.

const (
	// AbstractPath: lowercase; mask user home dirs (users/home segment -> *); collapse
	// GUIDs and long digit runs (random temp names). Keeps the full path otherwise, so a
	// masqueraded c:\temp\powershell.exe stays distinct from c:\windows\system32\powershell.exe.
	AbstractPath = "path"
	// AbstractIP: external addresses kept as-is (where C2 lives); internal v4 -> its /24
	// subnet (absorbs per-host DHCP churn, keeps segment distinctions for lateral movement);
	// internal v6 (loopback/link-local/ULA) -> 'internal'.
	AbstractIP = "ip"
)

// abstractExpr returns a ClickHouse SQL expression that abstracts colSQL per kind
// (AbstractPath or AbstractIP). colSQL is any SQL expression yielding the raw String
// (e.g. "fields.image::String" or a proc_lineage column name).
func abstractExpr(colSQL, kind string) string {
	switch kind {
	case AbstractIP:
		return fmt.Sprintf(ipAbstractTmpl, colSQL)
	default: // AbstractPath
		return fmt.Sprintf(pathAbstractTmpl, colSQL)
	}
}

// pathAbstractTmpl: %s is substituted once with the column expression.
const pathAbstractTmpl = `lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(%s, ` +
	`'(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), ` +
	`'\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), ` +
	`'[0-9]{6,}', '*'))`

// ipAbstractTmpl: %[1]s is the column expression, referenced multiple times.
const ipAbstractTmpl = `multiIf(` +
	`match(%[1]s, '^(10\\.|172\\.(1[6-9]|2[0-9]|3[01])\\.|192\\.168\\.|127\\.|169\\.254\\.)'), ` +
	`concat(replaceRegexpOne(%[1]s, '\\.[0-9]{1,3}$', ''), '.0/24'), ` +
	`match(%[1]s, '^(::1$|fe80:|fc|fd)'), 'internal', ` +
	`%[1]s)`
