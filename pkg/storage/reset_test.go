package storage

import (
	"strings"
	"testing"
)

// The reset drops by explicit name. Matching system.tables by pattern instead would
// catch dictionary tables, which hold user-uploaded data a log wipe does not
// invalidate, so the scope is pinned here rather than left to review.
func TestResetLogDataStatementsScope(t *testing.T) {
	stmts := ResetLogDataStatements(ModelCHObjectNames("11111111-2222-3333-4444-555555555555"))
	all := strings.Join(stmts, "\n")

	for _, want := range []string{
		"`logs`", "`logs_raw`", "`logs_hot`", "`logs_histogram`",
		"`proc_lineage`", "`proc_freq`", "`process_edges`",
		"`logs_distributed`", "`logs_raw_distributed`",
		"`_bifract_migrations`", "`_bifract_migration_steps`",
		"`logs_histogram_mv`", "`logs_hot_mv`", "`process_edges_mv`",
		// Every shape a model owns, including the two Postgres never records.
		"`model_11111111_2222_3333_4444_555555555555`",
		"`model_mv_11111111_2222_3333_4444_555555555555`",
		"`model_state_11111111_2222_3333_4444_555555555555`",
		"`model_diststate_11111111_2222_3333_4444_555555555555`",
		"`model_dist_11111111_2222_3333_4444_555555555555`",
	} {
		if !strings.Contains(all, want) {
			t.Errorf("reset does not drop %s", want)
		}
	}

	// Dictionaries share the database and must survive, which is also why the reset
	// never issues DROP DATABASE.
	for _, forbidden := range []string{"DROP DATABASE", "dict_", "lookup_"} {
		if strings.Contains(all, forbidden) {
			t.Errorf("reset must not reference %q:\n%s", forbidden, all)
		}
	}

	// The ledger is per-shard and has no distributed twin; asking for one is a sign
	// the _distributed suffix is being applied indiscriminately.
	if strings.Contains(all, "_bifract_migrations_distributed") {
		t.Error("bookkeeping tables have no _distributed twin")
	}
}

// Views must all precede tables: an MV's dependency is registered by name, so one
// left behind would point at a table that is already gone.
func TestResetDropsViewsBeforeTables(t *testing.T) {
	stmts := ResetLogDataStatements(ModelCHObjectNames("abc"))
	seenTable := false
	for _, s := range stmts {
		switch {
		case strings.HasPrefix(s, "DROP TABLE"):
			seenTable = true
		case strings.HasPrefix(s, "DROP VIEW"):
			if seenTable {
				t.Fatalf("view dropped after a table: %s", s)
			}
		default:
			t.Fatalf("unexpected reset statement: %s", s)
		}
	}
}

// The logs table exceeds max_table_size_to_drop (50GB) on any real install, and the
// usual escape hatch is a flag file on the server's disk that a client connection
// cannot create. Without the per-statement override the drop fails with code 359.
func TestResetLiftsTableSizeGuard(t *testing.T) {
	for _, s := range ResetLogDataStatements(nil, nil) {
		if strings.HasPrefix(s, "DROP TABLE") && !strings.Contains(s, "max_table_size_to_drop = 0") {
			t.Errorf("table drop does not lift the size guard: %s", s)
		}
	}
}

// Names reach ClickHouse from Postgres, so the quoting is the guard on the one
// interpolated token.
func TestQuoteCHIdent(t *testing.T) {
	if got := quoteCHIdent("model_a"); got != "`model_a`" {
		t.Errorf("quoteCHIdent = %q", got)
	}
	if got := quoteCHIdent("a`b"); got != "`a``b`" {
		t.Errorf("quoteCHIdent did not escape a backtick: %q", got)
	}
}
