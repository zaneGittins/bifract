package storage

import (
	"strings"
	"testing"
)

// TestFieldsTypeHintSQLIsLazy guards the single most dangerous regression in
// schema reconciliation. Without allow_experimental_json_lazy_type_hints, this
// exact statement stops being metadata-only and queues a mutation that rewrites
// every part in the logs table, which on a cluster holding billions of rows
// saturates CPU for hours. Dropping the setting would look harmless in review
// and would not fail any integration test that runs against a small dataset.
func TestFieldsTypeHintSQLIsLazy(t *testing.T) {
	for _, table := range []string{"logs", "logs_hot"} {
		sql := buildFieldsTypeHintSQL(table, []string{"src_ip", "user"})
		if !strings.Contains(sql, "allow_experimental_json_lazy_type_hints=1") {
			t.Fatalf("%s: MODIFY COLUMN would mutate every existing part: %s", table, sql)
		}
		if !strings.HasPrefix(sql, "ALTER TABLE "+table+" MODIFY COLUMN fields JSON(") {
			t.Errorf("%s: unexpected statement shape: %s", table, sql)
		}
	}
}

// TestFieldsTypeHintSQLIsDeterministic pins the ordering. The input is built from
// a Go map, so without an explicit sort the emitted hint list is reordered on
// every process start, churning the DDL and defeating any comparison against the
// column's current type string.
func TestFieldsTypeHintSQLIsDeterministic(t *testing.T) {
	a := buildFieldsTypeHintSQL("logs", []string{"user", "src_ip", "dst_ip"})
	b := buildFieldsTypeHintSQL("logs", []string{"dst_ip", "user", "src_ip"})
	if a != b {
		t.Fatalf("hint order is input-dependent:\n%s\n---\n%s", a, b)
	}
	if strings.Index(a, "`dst_ip`") > strings.Index(a, "`src_ip`") {
		t.Errorf("hints are not sorted: %s", a)
	}
}

// TestFieldsTypeHintSQLEscapesBackticks keeps a field name from breaking out of
// its quoted identifier and injecting DDL.
func TestFieldsTypeHintSQLEscapesBackticks(t *testing.T) {
	sql := buildFieldsTypeHintSQL("logs", []string{"ev`il"})
	if !strings.Contains(sql, "`ev``il` String") {
		t.Errorf("backtick not escaped: %s", sql)
	}
}

// TestParseJSONTypeHintsAcceptsBothSpellings guards the bug that made schema
// reconciliation authoritative by accident.
//
// Hints are WRITTEN with backticks but system.columns REPORTS them bare, so a
// backtick-only pattern parsed nothing. ReconcileSchemaFields then saw an empty
// existing set, which meant it dropped every hint outside the current
// defaults-plus-custom list and re-issued MODIFY COLUMN on every startup. Before
// lazy type hints that ALTER was a full-table mutation, so a plain restart
// rewrote every part on the cluster.
func TestParseJSONTypeHintsAcceptsBothSpellings(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{
			name: "bare, as system.columns reports it",
			in:   "JSON(artifact String, bifract_category String, src_ip String)",
			want: []string{"artifact", "bifract_category", "src_ip"},
		},
		{
			name: "backticked, as the DDL writes it",
			in:   "JSON(max_dynamic_paths=1024, `src_ip` String, `user` String)",
			want: []string{"src_ip", "user"},
		},
		{
			name: "settings must not be read as a path",
			in:   "JSON(max_dynamic_paths=1024, src_ip String)",
			want: []string{"src_ip"},
		},
		{
			name: "non-String types are still paths",
			in:   "JSON(user_id UInt64, score Float64, seen DateTime)",
			want: []string{"user_id", "score", "seen"},
		},
		{
			name: "no declared paths",
			in:   "JSON(max_dynamic_paths=1024)",
			want: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseJSONTypeHints(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("index %d: got %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// TestParseJSONTypeHintsRoundTripsOurOwnSQL pins the two halves together: what
// buildFieldsTypeHintSQL emits must be readable by the parser, or reconciliation
// cannot tell an existing hint from a new one.
func TestParseJSONTypeHintsRoundTripsOurOwnSQL(t *testing.T) {
	in := []string{"src_ip", "user", "bifract_category"}
	sql := buildFieldsTypeHintSQL("logs", in)
	got := parseJSONTypeHints(sql)
	if len(got) != len(in) {
		t.Fatalf("round trip lost fields: emitted %v, parsed %v", in, got)
	}
	for _, want := range in {
		found := false
		for _, g := range got {
			if g == want {
				found = true
			}
		}
		if !found {
			t.Errorf("field %q did not survive the round trip: %v", want, got)
		}
	}
}
