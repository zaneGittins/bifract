package db

import (
	"regexp"
	"strings"
	"testing"
)

var mvCreateRe = regexp.MustCompile(`(?m)^CREATE MATERIALIZED VIEW (?:IF NOT EXISTS )?(\w+)`)

// Every materialized view over logs must run as DEFINER. Without the clause the view
// executes with the inserting user's privileges, and the least-privilege ingest identity
// holds only INSERT on logs.* -- so the MV push fails with code 497 while the base row is
// already committed. The insert still reports an error, so the ingest queue retries and
// commits a duplicate on every attempt (observed: 3 copies of every row, plus
// consecutive-failure backpressure on an otherwise idle ClickHouse).
func TestInitClickHouseMaterializedViewsUseDefiner(t *testing.T) {
	checkDefiner(t, "init-clickhouse.sql", ClickHouseSQL)
}

// Migrations that create a view have the same requirement: a view created by a migration
// without the clause is just as broken as one from init.
func TestClickHouseMigrationMaterializedViewsUseDefiner(t *testing.T) {
	entries, err := ClickHouseMigrations.ReadDir("migrations/clickhouse")
	if err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		body, err := ClickHouseMigrations.ReadFile("migrations/clickhouse/" + e.Name())
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		checkDefiner(t, e.Name(), string(body))
	}
}

// checkDefiner asserts that each CREATE MATERIALIZED VIEW is followed by the security
// clause before its AS body begins.
func checkDefiner(t *testing.T, file, sql string) {
	t.Helper()
	for _, m := range mvCreateRe.FindAllStringSubmatchIndex(sql, -1) {
		name := sql[m[2]:m[3]]
		rest := sql[m[1]:]
		// The clause must appear before the view body, i.e. before the first " AS".
		asIdx := strings.Index(rest, " AS")
		if asIdx == -1 {
			asIdx = len(rest)
		}
		if !strings.Contains(rest[:asIdx], "SQL SECURITY DEFINER") {
			t.Errorf("%s: materialized view %q is missing DEFINER; add "+
				"\"DEFINER = default SQL SECURITY DEFINER\" before AS", file, name)
		}
	}
}
