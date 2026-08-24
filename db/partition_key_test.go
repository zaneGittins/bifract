package db

import (
	"regexp"
	"strings"
	"testing"
)

// createTableRe finds a CREATE TABLE and the engine clause that follows it, so a
// table's PARTITION BY / ORDER BY / TTL can be read out of the init SQL.
var createTableRe = regexp.MustCompile(`(?m)^CREATE TABLE (?:IF NOT EXISTS )?(\w+) \(`)

// stripSQLComments removes -- to end-of-line, so a semicolon inside a comment
// does not look like a statement terminator.
func stripSQLComments(sql string) string {
	lines := strings.Split(sql, "\n")
	for i, l := range lines {
		if c := strings.Index(l, "--"); c >= 0 {
			lines[i] = l[:c]
		}
	}
	return strings.Join(lines, "\n")
}

// tableClauses returns the text between a table's CREATE and the next statement
// terminator, which is where PARTITION BY, ORDER BY and TTL live.
func tableClauses(t *testing.T, name string) string {
	t.Helper()
	sql := stripSQLComments(ClickHouseSQL)
	loc := createTableRe.FindStringSubmatchIndex(sql)
	for loc != nil {
		if sql[loc[2]:loc[3]] == name {
			rest := sql[loc[1]:]
			end := strings.Index(rest, ";")
			if end < 0 {
				t.Fatalf("CREATE TABLE %s is not terminated", name)
			}
			return rest[:end]
		}
		next := createTableRe.FindStringSubmatchIndex(sql[loc[1]:])
		if next == nil {
			break
		}
		for i := range next {
			if next[i] >= 0 {
				next[i] += loc[1]
			}
		}
		loc = next
	}
	t.Fatalf("no CREATE TABLE %s in init-clickhouse.sql", name)
	return ""
}

// partitionOf extracts a table's PARTITION BY expression from its clauses.
func partitionOf(t *testing.T, clauses, name string) string {
	t.Helper()
	i := strings.Index(clauses, "PARTITION BY ")
	if i < 0 {
		t.Fatalf("%s has no PARTITION BY", name)
	}
	rest := clauses[i+len("PARTITION BY "):]
	if nl := strings.Index(rest, "\n"); nl >= 0 {
		rest = rest[:nl]
	}
	return strings.TrimSpace(rest)
}

// The logs partition axis is ingest time. On event time a batch spanning several
// event days writes a part into each of those already-merged partitions and drags
// every one back through a merge, which is the tax this key exists to remove.
// Reverting it also silently breaks the logs_histogram rollup prune, which matches
// a dropped partition's day against ingest_day.
func TestLogsPartitionByIngestTime(t *testing.T) {
	for _, name := range []string{"logs", "logs_raw"} {
		got := partitionOf(t, tableClauses(t, name), name)
		if got != "(fractal_id, toDate(ingest_timestamp))" {
			t.Errorf("%s PARTITION BY = %q, want (fractal_id, toDate(ingest_timestamp))", name, got)
		}
	}
}

// logs_raw drops whole parts on TTL (ttl_only_drop_parts). That only works while
// the TTL expression and the partition key name the same column, so a TTL left on
// event time would silently revert to rewriting parts during merges, which is the
// cost migration 013 split the table out to avoid.
func TestLogsRawTTLMatchesPartitionAxis(t *testing.T) {
	clauses := tableClauses(t, "logs_raw")

	i := strings.Index(clauses, "TTL ")
	if i < 0 {
		t.Fatal("logs_raw has no TTL")
	}
	ttl := clauses[i:]
	if nl := strings.Index(ttl, "\n"); nl >= 0 {
		ttl = ttl[:nl]
	}
	if !strings.Contains(ttl, "ingest_timestamp") {
		t.Errorf("logs_raw TTL = %q, want it on ingest_timestamp to match the partition key", ttl)
	}
	if !strings.Contains(clauses, "ttl_only_drop_parts = 1") {
		t.Error("logs_raw must keep ttl_only_drop_parts = 1")
	}
}

// The rollup is pruned by matching a dropped logs partition's day against
// ingest_day. Keyed on the event minute alone the prune deletes counts for rows
// still present and leaves counts for rows just dropped.
func TestHistogramRollupCarriesIngestDay(t *testing.T) {
	clauses := tableClauses(t, "logs_histogram")
	if !strings.Contains(clauses, "ingest_day") {
		t.Error("logs_histogram must carry ingest_day so the partition prune is exact")
	}
	if !strings.Contains(clauses, "ORDER BY (fractal_id, ingest_day, minute)") {
		t.Error("logs_histogram ORDER BY must lead fractal_id, ingest_day, minute")
	}
	if !strings.Contains(ClickHouseSQL, "toDate(ingest_timestamp) AS ingest_day") {
		t.Error("logs_histogram_mv must populate ingest_day from ingest_timestamp")
	}
}
