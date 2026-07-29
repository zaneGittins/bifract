//go:build normlogesc

// End-to-end check that bare-term search patterns actually match the rows they
// should once ClickHouse has serialized norm_log. Unit tests pin the escaping
// convention from the Go side; this pins it against the real server, which is
// the only authority on toString(fields).
//
//	docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d clickhouse
//	go test -tags normlogesc ./pkg/parser/ -run TestNormLogEscapingAgainstClickHouse -v
//
// Override the DSN with BIFRACT_TEST_CH_DSN.
package parser

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const chTestTable = "normlog_esc_e2e"

func chTestConn(t *testing.T) clickhouse.Conn {
	t.Helper()
	dsn := os.Getenv("BIFRACT_TEST_CH_DSN")
	if dsn == "" {
		dsn = "clickhouse://default:bifract@localhost:9000/default"
	}
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatalf("ping clickhouse (is the dev container up?): %v", err)
	}
	return conn
}

// seedRows mirrors the production logs schema closely enough for serialization
// to be identical: a typed JSON `fields` column and norm_log DEFAULT toString(fields).
func seedRows(t *testing.T, conn clickhouse.Conn, values []string) {
	t.Helper()
	ctx := context.Background()
	exec := func(q string) {
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	exec("DROP TABLE IF EXISTS " + chTestTable)
	exec(fmt.Sprintf(`CREATE TABLE %s (
		id UInt32,
		fields JSON(max_dynamic_paths=1024, `+"`image`"+` String),
		norm_log String DEFAULT toString(fields) CODEC(ZSTD(3)),
		INDEX norm_log_ngram_lc lower(norm_log) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1
	) ENGINE = MergeTree() ORDER BY id`, chTestTable))

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+chTestTable+" (id, fields)")
	if err != nil {
		t.Fatalf("prepare batch: %v", err)
	}
	for i, v := range values {
		if err := batch.Append(uint32(i), map[string]string{"image": v}); err != nil {
			t.Fatalf("append %q: %v", v, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send batch: %v", err)
	}
}

// matchingIDs runs the WHERE fragment the translator produced for a bare term
// and returns the ids it selects.
func matchingIDs(t *testing.T, conn clickhouse.Conn, where string) []uint32 {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		fmt.Sprintf("SELECT id FROM %s WHERE %s ORDER BY id", chTestTable, where))
	if err != nil {
		t.Fatalf("query %q: %v", where, err)
	}
	defer rows.Close()
	var out []uint32
	for rows.Next() {
		var id uint32
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, id)
	}
	return out
}

// bareTermWhere extracts the norm_log predicate the translator emits for a query.
func bareTermWhere(t *testing.T, query string) string {
	t.Helper()
	p, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse %q: %v", query, err)
	}
	sql, err := TranslateToSQL(p, QueryOptions{TableName: "logs", MaxRows: 10})
	if err != nil {
		t.Fatalf("translate %q: %v", query, err)
	}
	const marker = "match("
	i := strings.Index(sql, marker)
	if i < 0 {
		t.Fatalf("no match() in translated SQL for %q: %s", query, sql)
	}
	// Scan to the closing paren of the match() call.
	depth := 0
	for j := i; j < len(sql); j++ {
		switch sql[j] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return sql[i : j+1]
			}
		}
	}
	t.Fatalf("unbalanced match() in %q: %s", query, sql)
	return ""
}

// predicateAfterTimestamps strips the generated timestamp bounds and returns the
// remaining user predicate, so it can run against the fixture table.
func predicateAfterTimestamps(t *testing.T, sql string) string {
	t.Helper()
	const anchor = "00:00:00' AND "
	i := strings.LastIndex(sql, anchor)
	if i < 0 {
		t.Fatalf("no timestamp bounds in generated SQL: %s", sql)
	}
	rest := sql[i+len(anchor):]
	j := strings.Index(rest, " ORDER BY")
	if j < 0 {
		t.Fatalf("could not isolate predicate from: %s", sql)
	}
	pred := rest[:j]
	// The predicate is followed by the closing parens of the enclosing subqueries.
	// Drop trailing ')' until the fragment is balanced on its own.
	for {
		depth := 0
		for k := 0; k < len(pred); k++ {
			switch pred[k] {
			case '(':
				depth++
			case ')':
				depth--
			}
		}
		if depth >= 0 || len(pred) == 0 {
			break
		}
		pred = strings.TrimSuffix(pred, ")")
	}
	return pred
}

func TestNormLogEscapingAgainstClickHouse(t *testing.T) {
	conn := chTestConn(t)
	defer conn.Close()

	// Row order defines the expected ids below.
	values := []string{
		"/usr/bin/curl",               // 0
		`C:\Windows\System32\cmd.exe`, // 1
		"https://example.com/a/b",     // 2
		`he said "hi" loudly`,         // 3
		"café привет 日本語",             // 4
		"a<b>&c",                      // 5
		"PowerShell.EXE -Enc AAA",     // 6
		"plain-old-value",             // 7
		"exampleXcom",                 // 8 negative control for '.'
		"usr/bin/no-leading-slash",    // 9 negative control for leading '/'
	}
	seedRows(t, conn, values)

	tests := []struct {
		name  string
		query string
		want  []uint32
	}{
		{"unix path", `"/usr/bin/curl"`, []uint32{0}},
		{"unix path prefix", `"/usr/bin"`, []uint32{0}},
		{"windows path", `"C:\\Windows\\System32"`, []uint32{1}},
		{"windows path single segment", `"System32"`, []uint32{1}},
		{"url", `"https://example.com"`, []uint32{2}},
		{"embedded quotes", `"said \"hi\""`, []uint32{3}},
		{"non-ascii", `"привет"`, []uint32{4}},
		{"html chars", `"a<b>&c"`, []uint32{5}},
		{"case insensitive", `"powershell.exe"`, []uint32{6}},
		{"plain", `"plain-old-value"`, []uint32{7}},
		// '.' must stay literal: must not also match row 8 "exampleXcom".
		{"dot is literal", `"example.com"`, []uint32{2}},
		// A leading '/' is real: must not also match row 9.
		{"leading slash is literal", `"/usr/bin/curl"`, []uint32{0}},
		{"no match", `"definitely-absent-xyzzy"`, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			where := bareTermWhere(t, tt.query)
			got := matchingIDs(t, conn, where)
			if fmt.Sprint(got) != fmt.Sprint(tt.want) {
				t.Errorf("query %s\n  predicate: %s\n  got ids:  %v\n  want ids: %v",
					tt.query, where, got, tt.want)
			}
		})
	}

	// Field-qualified operators read the JSON sub-column, which holds the raw
	// unescaped value. These must keep working untouched by the norm_log escaping.
	fieldTests := []struct {
		name  string
		query string
		want  []uint32
	}{
		// Row 9 is "usr/bin/no-leading-slash": the leading '/' must stay literal
		// on the field path too, so it is correctly excluded.
		{"contains unix path", `image=~"/usr/bin"`, []uint32{0}},
		{"contains slashless substring", `image=~"usr/bin"`, []uint32{0, 9}},
		{"contains windows path", `image=~"C:\\Windows\\System32"`, []uint32{1}},
		{"contains url", `image=~"https://example.com"`, []uint32{2}},
		{"contains quotes", `image=~"said \"hi\""`, []uint32{3}},
		{"exact equality", `image="/usr/bin/curl"`, []uint32{0}},
		{"prefix", `image=^"/usr/bin"`, []uint32{0}},
		{"suffix", `image=$"cmd.exe"`, []uint32{1}},
		{"multi-value contains", `image=~"cmd.exe","curl"`, []uint32{0, 1}},
		{"non-ascii", `image=~"привет"`, []uint32{4}},
		{"case insensitive", `image=~"powershell.exe"`, []uint32{6}},
	}
	for _, tt := range fieldTests {
		t.Run("field/"+tt.name, func(t *testing.T) {
			p, err := ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("parse %q: %v", tt.query, err)
			}
			sql, err := TranslateToSQL(p, QueryOptions{TableName: "logs", MaxRows: 10})
			if err != nil {
				t.Fatalf("translate %q: %v", tt.query, err)
			}
			// Pull the predicate out of the generated WHERE and retarget the JSON
			// sub-column ref at this fixture's column.
			where := predicateAfterTimestamps(t, sql)
			got := matchingIDs(t, conn, where)
			if fmt.Sprint(got) != fmt.Sprint(tt.want) {
				t.Errorf("query %s\n  predicate: %s\n  got ids:  %v\n  want ids: %v",
					tt.query, where, got, tt.want)
			}
		})
	}

	t.Run("cleanup", func(t *testing.T) {
		if err := conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+chTestTable); err != nil {
			t.Logf("cleanup: %v", err)
		}
	})
}
