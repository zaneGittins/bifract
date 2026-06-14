package storage

import (
	"os"
	"strings"
	"testing"
)

func TestSplitSQLOnTopLevelSemicolons(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "plain statements",
			sql:  "CREATE TABLE a (x Int); ALTER TABLE a ADD COLUMN y Int;",
			want: []string{"CREATE TABLE a (x Int)", " ALTER TABLE a ADD COLUMN y Int"},
		},
		{
			name: "semicolon inside line comment does not split (regression: init-clickhouse.sql)",
			sql:  "CREATE TABLE logs (\n  x Int -- preserves ':' delimiter; hasToken cannot\n);",
			want: []string{"CREATE TABLE logs (\n  x Int -- preserves ':' delimiter; hasToken cannot\n)"},
		},
		{
			name: "semicolon inside block comment does not split",
			sql:  "CREATE TABLE a (\n  x Int /* writes locally; reads cross-shard */\n); SELECT 1;",
			want: []string{"CREATE TABLE a (\n  x Int /* writes locally; reads cross-shard */\n)", " SELECT 1"},
		},
		{
			name: "semicolon inside string literal does not split",
			sql:  "INSERT INTO a VALUES ('foo; bar'); SELECT 1;",
			want: []string{"INSERT INTO a VALUES ('foo; bar')", " SELECT 1"},
		},
		{
			name: "escaped quote inside string literal",
			sql:  "INSERT INTO a VALUES ('it\\'s; fine'); SELECT 2;",
			want: []string{"INSERT INTO a VALUES ('it\\'s; fine')", " SELECT 2"},
		},
		{
			name: "doubled-quote escape inside string literal",
			sql:  "INSERT INTO a VALUES ('it''s; fine'); SELECT 3;",
			want: []string{"INSERT INTO a VALUES ('it''s; fine')", " SELECT 3"},
		},
		{
			name: "trailing whitespace-only segment dropped",
			sql:  "SELECT 1;   \n  ",
			want: []string{"SELECT 1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitSQLOnTopLevelSemicolons(tt.sql)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d segments %#v; want %d %#v", len(got), got, len(tt.want), tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("segment %d = %q; want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// Guards against a ';' inside a comment or literal in the embedded init SQL silently
// truncating the CREATE TABLE block (ClickHouse error 62, unmatched parentheses).
func TestSplitClickHouseSQLKeepsBalancedParens(t *testing.T) {
	sql := `CREATE TABLE logs (
  field_tokens String DEFAULT '',
  -- splitByString keeps the pair as one token; hasToken cannot be used here.
  INDEX field_tokens_text field_tokens TYPE text(tokenizer = splitByString) GRANULARITY 1
) ENGINE = MergeTree();`
	for _, stmt := range splitClickHouseSQL(sql) {
		if strings.Count(stmt, "(") != strings.Count(stmt, ")") {
			t.Errorf("unbalanced parentheses in split statement:\n%s", stmt)
		}
	}
}

// Guards the real embedded init SQL: every split statement must have balanced
// parentheses, which catches a stray ';' in a comment truncating CREATE TABLE.
func TestRealInitClickHouseSQLSplitsCleanly(t *testing.T) {
	sql, err := os.ReadFile("../../db/init-clickhouse.sql")
	if err != nil {
		t.Skipf("cannot read init-clickhouse.sql: %v", err)
	}
	stmts := splitClickHouseSQL(string(sql))
	if len(stmts) == 0 {
		t.Fatal("no statements parsed from init-clickhouse.sql")
	}
	for _, stmt := range stmts {
		if strings.Count(stmt, "(") != strings.Count(stmt, ")") {
			t.Errorf("unbalanced parentheses in statement:\n%s", stmt)
		}
	}
}
