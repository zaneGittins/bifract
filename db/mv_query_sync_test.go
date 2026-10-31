package db

import (
	"strings"
	"testing"
)

// A migration that swaps a view's SELECT with MODIFY QUERY must leave it identical to the
// init SQL's, or an upgraded install ingests differently from a fresh one.
func TestModifyQueryMatchesInit(t *testing.T) {
	for _, c := range []struct{ migration, view string }{
		{"019_provenance_access_paths.sql", "proc_lineage_mv"},
	} {
		mig, err := ClickHouseMigrations.ReadFile(ClickHouseMigrationsDir + "/" + c.migration)
		if err != nil {
			t.Fatal(err)
		}
		got := viewSelect(t, string(mig), "ALTER TABLE "+c.view+" MODIFY QUERY")
		want := viewSelect(t, ClickHouseSQL, "CREATE MATERIALIZED VIEW IF NOT EXISTS "+c.view+" ")
		if got != want {
			t.Errorf("%s %s:\n%s\ninit:\n%s", c.migration, c.view, got, want)
		}
	}
}

// viewSelect returns the SELECT that follows header, up to its terminator, with
// whitespace collapsed.
func viewSelect(t *testing.T, sql, header string) string {
	t.Helper()
	sql = stripSQLComments(sql)
	i := strings.Index(sql, header)
	if i < 0 {
		t.Fatalf("%q not found", header)
	}
	rest := sql[i:]
	s := strings.Index(rest, "SELECT")
	end := strings.Index(rest, ";")
	if s < 0 || end < s {
		t.Fatalf("%q has no terminated SELECT", header)
	}
	return strings.Join(strings.Fields(rest[s:end]), " ")
}
