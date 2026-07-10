package parser

import (
	"os"
	"strings"
	"testing"
)

// TestAbstractExprMatchesMVs guards write/read parity: the abstraction inlined into the
// static proc_freq MVs must be byte-identical to what abstractExpr() emits for the pgr()
// read side, or the freq-join keys will silently mismatch. If this fails, one side was
// edited without the other.
//
// init-clickhouse.sql is the canonical full set (fresh installs) and must carry every use.
// The numbered migrations carry only their delta: 009 adds the file (target_file) fix plus
// the remote_thread/process_access/dns MVs, so it must carry those columns. 008 is released
// and immutable (its file MV keys off the now-superseded fields.artifact), so it is not
// re-checked here beyond its own frozen contents.
func TestAbstractExprMatchesMVs(t *testing.T) {
	type use struct{ col, kind string }
	all := []use{
		{"fields.parent_image::String", AbstractPath}, // spawn src
		{"fields.image::String", AbstractPath},        // spawn target / file+net+dns src / p2p actor src
		{"fields.target_file::String", AbstractPath},  // file target
		{"fields.dst_ip::String", AbstractIP},         // net target
		{"fields.target_image::String", AbstractPath}, // remote_thread/process_access target
		{"fields.query::String", AbstractDomain},      // dns target
	}
	// migration 009 introduces/repairs these MVs (image src, file target, p2p target, dns query).
	delta009 := []use{
		{"fields.image::String", AbstractPath},
		{"fields.target_file::String", AbstractPath},
		{"fields.target_image::String", AbstractPath},
		{"fields.query::String", AbstractDomain},
	}
	checks := []struct {
		file string
		uses []use
	}{
		{"../../db/init-clickhouse.sql", all},
		{"../../db/migrations/clickhouse/009_proc_freq_events.sql", delta009},
	}
	for _, c := range checks {
		b, err := os.ReadFile(c.file)
		if err != nil {
			t.Fatalf("read %s: %v", c.file, err)
		}
		sql := string(b)
		for _, u := range c.uses {
			if want := abstractExpr(u.col, u.kind); !strings.Contains(sql, want) {
				t.Errorf("%s missing/drifted abstraction for %s (%s):\n  want substring: %s", c.file, u.col, u.kind, want)
			}
		}
	}
}

// TestAbstractExpr pins the exact SQL these helpers emit. The strings below were validated
// directly against ClickHouse 26.6 (see the abstraction review): paths collapse user dirs
// /GUIDs/temp-numbers, internal v4 -> /24, internal v6 -> 'internal', external kept, domains
// lowercased with the FQDN root dot stripped. If a change alters this SQL, re-validate in CH
// AND update the static MV DDL to match (the MV write side and this read side must stay
// byte-identical for the freq join keys to line up).
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

	wantDomain := `lower(replaceRegexpOne(fields.query::String, '\\.$', ''))`
	if got := abstractExpr("fields.query::String", AbstractDomain); got != wantDomain {
		t.Errorf("domain abstraction drifted:\n got:  %s\n want: %s", got, wantDomain)
	}
}
