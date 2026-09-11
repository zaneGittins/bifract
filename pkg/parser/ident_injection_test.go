package parser

import (
	"strings"
	"testing"
	"time"
)

// A field name can carry an arbitrary string (via a quoted argument), and
// ClickHouse honours a backslash-escaped backtick inside a backtick-quoted
// identifier. Escaping only backticks would let `\` + a backtick close the
// identifier early and run the remainder as SQL. These assert the escaping
// keeps any such payload inside the quotes across every command that quotes a
// field name from user input.
func TestBacktickIdentifierNoBreakout(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID: "f1", MaxRows: 100,
	}
	// Each BQL query embeds a backslash+backtick breakout attempt in a field name.
	queries := []string{
		"* | sort(\"a\\\\`::String, (SELECT sleep(9)) -- \")",
		"* | dedup(\"a\\\\`::String, (SELECT sleep(9)) -- \")",
		"* | concat(a, \"a\\\\`::String, (SELECT sleep(9)) -- \", as=b)",
		"* | coalesce(a, \"a\\\\`::String FROM system.tables -- \", as=b)",
		"* | levenshtein(a, \"a\\\\`::String, (SELECT sleep(9)) -- \", as=b)",
	}
	for _, q := range queries {
		p, err := ParseQuery(q)
		if err != nil {
			continue
		}
		res, err := TranslateToSQLWithOrder(p, opts)
		if err != nil {
			continue
		}
		// The backslash must be doubled so ClickHouse reads it as a literal and the
		// following backtick stays inside the identifier. A lone `\` before a
		// doubled backtick (`\`` in output) is the breakout signature.
		if strings.Contains(res.SQL, "`a\\`") && !strings.Contains(res.SQL, "`a\\\\`") {
			t.Fatalf("identifier breakout not escaped for %q:\n%s", q, res.SQL)
		}
	}
}

func TestEscapeCHBacktickIdent(t *testing.T) {
	cases := map[string]string{
		"plain":   "plain",
		"a`b":     "a``b",
		"a\\":     "a\\\\",
		"a\\`b":   "a\\\\``b",
		"a\\\\`b": "a\\\\\\\\``b",
	}
	for in, want := range cases {
		if got := EscapeCHBacktickIdent(in); got != want {
			t.Errorf("EscapeCHBacktickIdent(%q) = %q, want %q", in, got, want)
		}
	}
}
