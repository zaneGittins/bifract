package parser

import (
	"os"
	"strings"
	"testing"
)

// TestAbstractExprMatchesMVs guards write/read parity: the abstraction inlined into the
// static proc_freq MVs (db/init-clickhouse.sql and migration 008) must be byte-identical to
// what abstractExpr() emits for the pgr() read side, or the freq-join keys will silently
// mismatch. If this fails, one side was edited without the other.
func TestAbstractExprMatchesMVs(t *testing.T) {
	files := []string{
		"../../db/init-clickhouse.sql",
		"../../db/migrations/clickhouse/008_proc_freq.sql",
	}
	uses := []struct{ col, kind string }{
		{"fields.parent_image::String", AbstractPath}, // spawn src
		{"fields.image::String", AbstractPath},        // spawn target / file+net src
		{"fields.artifact::String", AbstractPath},     // file target
		{"fields.dst_ip::String", AbstractIP},         // net target
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		sql := string(b)
		for _, u := range uses {
			if want := abstractExpr(u.col, u.kind); !strings.Contains(sql, want) {
				t.Errorf("%s missing/drifted abstraction for %s (%s):\n  want substring: %s", f, u.col, u.kind, want)
			}
		}
	}
}

// TestAbstractExpr pins the exact SQL these helpers emit. The strings below were validated
// directly against ClickHouse 26.6 (see the abstraction review): paths collapse user dirs
// /GUIDs/temp-numbers, internal v4 -> /24, internal v6 -> 'internal', external kept. If a
// change alters this SQL, re-validate in CH AND update the static MV DDL to match (the MV
// write side and this read side must stay byte-identical for the freq join keys to line up).
func TestAbstractExpr(t *testing.T) {
	wantPath := `lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, ` +
		`'(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), ` +
		`'\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), ` +
		`'[0-9]{6,}', '*'))`
	if got := abstractExpr("fields.image::String", AbstractPath); got != wantPath {
		t.Errorf("path abstraction drifted:\n got:  %s\n want: %s", got, wantPath)
	}

	wantIP := `multiIf(` +
		`match(fields.dst_ip::String, '^(10\\.|172\\.(1[6-9]|2[0-9]|3[01])\\.|192\\.168\\.|127\\.|169\\.254\\.)'), ` +
		`concat(replaceRegexpOne(fields.dst_ip::String, '\\.[0-9]{1,3}$', ''), '.0/24'), ` +
		`match(fields.dst_ip::String, '^(::1$|fe80:|fc|fd)'), 'internal', ` +
		`fields.dst_ip::String)`
	if got := abstractExpr("fields.dst_ip::String", AbstractIP); got != wantIP {
		t.Errorf("ip abstraction drifted:\n got:  %s\n want: %s", got, wantIP)
	}
}
