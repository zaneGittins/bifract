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
