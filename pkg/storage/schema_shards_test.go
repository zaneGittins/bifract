package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"bifract/internal/fakedb"
)

// columnsOf scripts system.columns for one table: name, type, default_kind,
// default_expression.
func columnsOf(rows ...[4]string) fakedb.CHResult {
	r := fakedb.CHResult{Cols: []fakedb.Col{{Column: "name", Type: "String"}, {Column: "type", Type: "String"}, {Column: "default_kind", Type: "String"}, {Column: "default_expression", Type: "String"}}}
	for _, row := range rows {
		r.Rows = append(r.Rows, []any{row[0], row[1], row[2], row[3]})
	}
	return r
}

const columnsQuery = "SELECT name, type, default_kind, default_expression"

// A Distributed table snapshots its columns at creation. The sync must add what a
// later migration added to the local table, carrying its DEFAULT, and repair a
// column whose default drifted: the Distributed layer materializes an omitted
// column from its OWN default and forwards it, overriding the local one (this is
// what once left norm_log empty on every row). It never drops a column and never
// touches fields, whose type-hint list ReconcileSchemaFields owns.
func TestSyncDistributedColumns(t *testing.T) {
	f := fakedb.NewCH()
	f.OnArg(columnsQuery, "logs", columnsOf(
		[4]string{"timestamp", "DateTime64(3, 'UTC')", "", ""},
		[4]string{"fields", "JSON(max_dynamic_paths=1024, user String)", "", ""},
		[4]string{"norm_log", "String", "DEFAULT", "toString(fields)"},
		[4]string{"normalizer", "LowCardinality(String)", "DEFAULT", "''"},
		[4]string{"ingest_day", "Date", "MATERIALIZED", "toDate(ingest_timestamp)"},
	))
	f.OnArg(columnsQuery, "logs_distributed", columnsOf(
		[4]string{"timestamp", "DateTime64(3, 'UTC')", "", ""},
		[4]string{"fields", "JSON(max_dynamic_paths=1024)", "", ""},
		[4]string{"norm_log", "String", "", ""}, // default missing
		[4]string{"legacy", "String", "", ""},   // dist-only: must survive
	))

	// The database is bound separately from the table name; "bifract" keeps the
	// two arguments distinguishable.
	if err := syncDistributedColumns(context.Background(), f, "bifract", "logs_distributed", "logs"); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"ALTER TABLE logs_distributed MODIFY COLUMN `norm_log` String DEFAULT toString(fields)",
		"ALTER TABLE logs_distributed ADD COLUMN IF NOT EXISTS `normalizer` LowCardinality(String) DEFAULT ''",
		"ALTER TABLE logs_distributed ADD COLUMN IF NOT EXISTS `ingest_day` Date MATERIALIZED toDate(ingest_timestamp)",
	}
	if got := f.Executed(); !reflect.DeepEqual(got, want) {
		t.Errorf("executed:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestSyncDistributedColumnsInSyncIsNoop(t *testing.T) {
	f := fakedb.NewCH()
	cols := columnsOf([4]string{"norm_log", "String", "DEFAULT", "toString(fields)"})
	f.OnArg(columnsQuery, "logs", cols)
	f.OnArg(columnsQuery, "logs_distributed", cols)
	if err := syncDistributedColumns(context.Background(), f, "bifract", "logs_distributed", "logs"); err != nil {
		t.Fatal(err)
	}
	if got := f.Executed(); len(got) != 0 {
		t.Errorf("executed %v on an in-sync table", got)
	}
}

func TestSyncDistributedColumnsErrors(t *testing.T) {
	f := fakedb.NewCH()
	f.On(columnsQuery, fakedb.CHResult{Err: errors.New("denied")})
	if err := syncDistributedColumns(context.Background(), f, "logs", "d", "l"); err == nil {
		t.Error("expected the read error")
	}

	f = fakedb.NewCH()
	f.OnArg(columnsQuery, "l", columnsOf([4]string{"x", "String", "", ""}))
	f.FailExec("ADD COLUMN", errors.New("code 44"))
	if err := syncDistributedColumns(context.Background(), f, "logs", "d", "l"); err == nil || !strings.Contains(err.Error(), "add column x to d") {
		t.Errorf("err = %v, want the failing column named", err)
	}
}

// Model tables have generated names no numbered migration can reach, so the app
// adds late columns at startup. It must not touch a table that does not exist on
// this node, and must bring the companion Distributed table along.
func TestEnsureColumn(t *testing.T) {
	ctx := context.Background()

	t.Run("absent table is left alone", func(t *testing.T) {
		c, f := newFakeClient()
		f.On("system.tables", fakedb.Count(0))
		if err := c.EnsureColumn(ctx, "model_x", "model_dist_x", "c", "String"); err != nil {
			t.Fatal(err)
		}
		if len(f.Executed()) != 0 {
			t.Errorf("executed %v", f.Executed())
		}
	})

	t.Run("adds the column and syncs the companion", func(t *testing.T) {
		c, f := newFakeClient()
		c.Database = "" // logsDatabase() falls back to logs
		f.On("system.tables", fakedb.Count(1))
		f.On("SELECT count() FROM system.columns", fakedb.Count(0))
		f.OnArg(columnsQuery, "model_x", columnsOf([4]string{"late", "UInt8", "DEFAULT", "0"}))
		f.OnArg(columnsQuery, "model_dist_x", columnsOf())
		if err := c.EnsureColumn(ctx, "model_x", "model_dist_x", "late", "UInt8 DEFAULT 0"); err != nil {
			t.Fatal(err)
		}
		want := []string{
			"ALTER TABLE `model_x` ADD COLUMN IF NOT EXISTS `late` UInt8 DEFAULT 0",
			"ALTER TABLE model_dist_x ADD COLUMN IF NOT EXISTS `late` UInt8 DEFAULT 0",
		}
		if got := f.Executed(); !reflect.DeepEqual(got, want) {
			t.Errorf("executed %q, want %q", got, want)
		}
	})

	t.Run("present column, no companion", func(t *testing.T) {
		c, f := newFakeClient()
		f.On("system.tables", fakedb.Count(1))
		f.On("SELECT count() FROM system.columns", fakedb.Count(1))
		if err := c.EnsureColumn(ctx, "model_x", "", "late", "UInt8"); err != nil {
			t.Fatal(err)
		}
		if len(f.Executed()) != 0 {
			t.Errorf("executed %v", f.Executed())
		}
	})

	t.Run("identifiers are escaped", func(t *testing.T) {
		c, f := newFakeClient()
		f.On("system.tables", fakedb.Count(1))
		f.On("SELECT count() FROM system.columns", fakedb.Count(0))
		c.EnsureColumn(ctx, "m`x", "", "c`\\", "String")
		if got := f.Executed(); len(got) != 1 || got[0] != "ALTER TABLE `m``x` ADD COLUMN IF NOT EXISTS `c``\\\\` String" {
			t.Errorf("executed %q", got)
		}
	})

	t.Run("errors surface", func(t *testing.T) {
		c, f := newFakeClient()
		f.On("system.tables", fakedb.Count(1))
		f.On("SELECT count() FROM system.columns", fakedb.Count(0))
		f.FailExec("ADD COLUMN", errors.New("readonly"))
		if err := c.EnsureColumn(ctx, "model_x", "", "late", "UInt8"); err == nil {
			t.Error("expected the ALTER failure")
		}
	})
}

// Every sharded log table gets a Distributed companion, named for the cluster and
// created idempotently. The reset drops "<table>_distributed" for exactly the same
// set, so a table added to one list and not the other either survives a reset or
// is never created.
func TestDistributedTableDDLMatchesResetSet(t *testing.T) {
	c := &ClickHouseClient{topo: Topology{DDLCluster: "it's"}}
	got := map[string]bool{}
	for _, stmt := range c.distributedTableDDL() {
		got[stmt] = true
	}
	if len(got) != len(resetShardedTables) {
		t.Fatalf("%d statements for %d sharded tables", len(got), len(resetShardedTables))
	}
	for _, table := range resetShardedTables {
		want := "CREATE TABLE IF NOT EXISTS " + table + "_distributed AS " + table +
			" ENGINE = Distributed('it\\'s', currentDatabase(), '" + table + "', rand())"
		if !got[want] {
			t.Errorf("missing %q", want)
		}
	}
}
