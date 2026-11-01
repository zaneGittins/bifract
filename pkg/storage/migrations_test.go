package storage

import (
	"context"
	"embed"
	"errors"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"

	dbsql "bifract/db"
)

// testMigrations is a synthetic migration set: 1_first carries a SELECT the runner
// must skip, 10_tenth checks numeric (not lexical) ordering, and the unnumbered and
// non-SQL files must be ignored.
//
//go:embed testdata/chmigrations/*
var testMigrations embed.FS

const testMigrationsDir = "testdata/chmigrations"

// migrationState scripts what the bookkeeping tables report.
func migrationState(f *fakedb.CH, maxApplied uint32) {
	f.On("SELECT max(number) FROM logs._bifract_migrations", fakedb.One("max(number)", "UInt32", maxApplied))
}

func TestLoadClickHouseMigrationsOrdersNumericallyAndSkipsStrays(t *testing.T) {
	got, err := loadClickHouseMigrations(testMigrations, testMigrationsDir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, m := range got {
		names = append(names, m.name)
	}
	if want := []string{"1_first", "2_second", "10_tenth"}; !reflect.DeepEqual(names, want) {
		t.Errorf("migrations = %v, want %v", names, want)
	}
	if got[2].number != 10 || !strings.Contains(got[2].sql, "INSERT INTO t1") {
		t.Errorf("10_tenth = %+v", got[2])
	}
}

// A fresh run applies every DDL/write statement, skips anything else, and records
// progress per statement and then per file, in that order: a crash between the two
// must leave the statement recorded so a restart resumes rather than repeats.
func TestRunMigrationsAppliesAndRecordsInOrder(t *testing.T) {
	f := fakedb.NewCH()
	migrationState(f, 0)

	n, err := runMigrationsOnConn(context.Background(), f, nil, false, testMigrations, testMigrationsDir)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("applied = %d, want 3", n)
	}

	var got []string
	for _, s := range f.Executed() {
		if strings.Contains(s, "CREATE TABLE IF NOT EXISTS logs._bifract_") {
			continue // bookkeeping, checked separately
		}
		got = append(got, strings.Join(strings.Fields(s), " "))
	}
	want := []string{
		"CREATE TABLE IF NOT EXISTS t1 (x UInt8) ENGINE = MergeTree() ORDER BY x",
		"INSERT INTO logs._bifract_migration_steps (number, statement_index, name) VALUES (1, 0, '1_first')",
		"ALTER TABLE t1 ADD COLUMN IF NOT EXISTS y String",
		"INSERT INTO logs._bifract_migration_steps (number, statement_index, name) VALUES (1, 2, '1_first')",
		"INSERT INTO logs._bifract_migrations (number, name) VALUES (1, '1_first')",
		"ALTER TABLE t1 ADD COLUMN IF NOT EXISTS z String",
		"INSERT INTO logs._bifract_migration_steps (number, statement_index, name) VALUES (2, 0, '2_second')",
		"ALTER TABLE t1 ADD COLUMN IF NOT EXISTS w String",
		"INSERT INTO logs._bifract_migration_steps (number, statement_index, name) VALUES (2, 1, '2_second')",
		"INSERT INTO logs._bifract_migrations (number, name) VALUES (2, '2_second')",
		"INSERT INTO t1 (x) SELECT 1 WHERE 0",
		"INSERT INTO logs._bifract_migration_steps (number, statement_index, name) VALUES (10, 0, '10_tenth')",
		"INSERT INTO logs._bifract_migrations (number, name) VALUES (10, '10_tenth')",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("executed:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if len(f.ExecutedMatching("CREATE TABLE IF NOT EXISTS logs._bifract_")) != 2 {
		t.Error("both bookkeeping tables must be created on every run")
	}
}

// An interrupted run resumes: applied files are skipped whole, and within the
// in-progress file the statements already recorded are not re-executed.
func TestRunMigrationsResumesFromRecordedSteps(t *testing.T) {
	f := fakedb.NewCH()
	migrationState(f, 1)
	// 2_second's first statement completed before the interruption.
	f.OnOnce("FROM logs._bifract_migration_steps", fakedb.One("statement_index", "UInt32", uint32(0)))

	n, err := runMigrationsOnConn(context.Background(), f, nil, false, testMigrations, testMigrationsDir)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("applied = %d, want 2 (2_second and 10_tenth)", n)
	}
	if got := f.ExecutedMatching("ADD COLUMN IF NOT EXISTS z"); len(got) != 0 {
		t.Errorf("a recorded step re-ran: %v", got)
	}
	if got := f.ExecutedMatching("ADD COLUMN IF NOT EXISTS w"); len(got) != 1 {
		t.Errorf("the unrecorded step did not run: %v", f.Executed())
	}
	if got := f.ExecutedMatching("CREATE TABLE IF NOT EXISTS t1"); len(got) != 0 {
		t.Errorf("an applied migration re-ran: %v", got)
	}
}

// A failing statement stops the run without recording its file, and the error
// names the file and the statement so the operator can find it.
func TestRunMigrationsStopsOnFailure(t *testing.T) {
	f := fakedb.NewCH()
	migrationState(f, 0)
	f.FailExec("ADD COLUMN IF NOT EXISTS w", errors.New("replica down"))

	n, err := runMigrationsOnConn(context.Background(), f, nil, false, testMigrations, testMigrationsDir)
	if err == nil || !strings.Contains(err.Error(), "migration 2_second (statement 1)") {
		t.Fatalf("err = %v, want it to name 2_second statement 1", err)
	}
	if n != 1 {
		t.Errorf("applied = %d, want 1", n)
	}
	if got := f.ExecutedMatching("_bifract_migrations (number, name) VALUES (2,"); len(got) != 0 {
		t.Error("a failed file must not be recorded as applied")
	}
	if got := f.ExecutedMatching("10_tenth"); len(got) != 0 {
		t.Error("later files must not run after a failure")
	}
}

func TestRunMigrationsSurfacesBookkeepingErrors(t *testing.T) {
	f := fakedb.NewCH()
	f.FailExec("_bifract_migration_steps (", errors.New("no keeper"))
	if _, err := runMigrationsOnConn(context.Background(), f, nil, false, testMigrations, testMigrationsDir); err == nil {
		t.Error("expected the bookkeeping failure")
	}

	f = fakedb.NewCH()
	f.On("SELECT max(number)", fakedb.CHResult{Err: errors.New("unreadable")})
	if _, err := runMigrationsOnConn(context.Background(), f, nil, false, testMigrations, testMigrationsDir); err == nil {
		t.Error("expected the migration state failure")
	}
}

// In cluster mode every statement, including the bookkeeping DDL, goes through the
// engine rewrite so each shard gets Replicated tables.
func TestRunMigrationsAppliesTransform(t *testing.T) {
	f := fakedb.NewCH()
	migrationState(f, 2)
	c := &ClickHouseClient{topo: Topology{ReplicatedEngines: true}}
	if _, err := runMigrationsOnConn(context.Background(), f, c.RewriteEngine, true, testMigrations, testMigrationsDir); err != nil {
		t.Fatal(err)
	}
	for _, s := range f.ExecutedMatching("CREATE TABLE IF NOT EXISTS logs._bifract_") {
		if !strings.Contains(s, "ReplicatedReplacingMergeTree(") {
			t.Errorf("bookkeeping DDL not rewritten: %s", s)
		}
	}
}

// Every statement in the shipped migrations must be one the runner executes. The
// runner silently skips anything that does not start with a DDL or write verb, so
// a statement starting with a block comment or an unlisted verb would be recorded
// as applied without ever running.
func TestShippedMigrationsHaveNoSilentlySkippedStatements(t *testing.T) {
	all, err := loadClickHouseMigrations(dbsql.ClickHouseMigrations, "migrations/clickhouse")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) == 0 {
		t.Fatal("no shipped migrations found")
	}
	f := fakedb.NewCH()
	migrationState(f, 0)
	if _, err := runMigrationsOnConn(context.Background(), f, nil, false, dbsql.ClickHouseMigrations, "migrations/clickhouse"); err != nil {
		t.Fatal(err)
	}
	executed := map[string]bool{}
	for _, s := range f.Executed() {
		executed[s] = true
	}
	for _, m := range all {
		for i, stmt := range splitClickHouseSQL(m.sql) {
			// Neutralized placeholders are deliberately no-ops.
			if stmt == "SELECT 1" {
				continue
			}
			if !executed[strings.TrimSpace(stmt)] {
				t.Errorf("%s statement %d would be skipped by the runner: %.80q", m.name, i, stmt)
			}
		}
	}
}

func TestSetMigrationsBaseline(t *testing.T) {
	f := fakedb.NewCH()
	if err := setClickHouseMigrationsBaseline(context.Background(), f, nil, testMigrations, testMigrationsDir, 2); err != nil {
		t.Fatal(err)
	}
	got := f.ExecutedMatching("INSERT INTO logs._bifract_migrations")
	if len(got) != 2 || !strings.Contains(got[1], "(2, '2_second')") {
		t.Errorf("baseline through 2 recorded %v", got)
	}
	if len(f.ExecutedMatching("CREATE TABLE IF NOT EXISTS t1")) != 0 {
		t.Error("a baseline must never execute a migration")
	}

	f = fakedb.NewCH()
	setClickHouseMigrationsBaseline(context.Background(), f, nil, testMigrations, testMigrationsDir, 0)
	if got := f.ExecutedMatching("INSERT INTO logs._bifract_migrations"); len(got) != 3 {
		t.Errorf("upTo 0 recorded %d migrations, want all 3", len(got))
	}
}

func TestRecordCompletedStepEscapesName(t *testing.T) {
	f := fakedb.NewCH()
	recordCompletedStep(context.Background(), f, 7, 3, "o'brien")
	if got := f.Executed()[0]; !strings.Contains(got, "VALUES (7, 3, 'o''brien')") {
		t.Errorf("stmt = %q", got)
	}
}

// A schema provisioned from the init SQL but never stamped is past migration 13
// (no logs.raw_log) while recording less. It must be stamped through 13 before any
// replay, or 004/012 fail selecting raw_log.
func TestStampProvisionedSchema(t *testing.T) {
	ctx := context.Background()
	t.Run("already recorded", func(t *testing.T) {
		f := fakedb.NewCH()
		migrationState(f, dbsql.RawLogSplitMigration)
		if err := stampProvisionedSchema(ctx, f, testMigrations, testMigrationsDir); err != nil {
			t.Fatal(err)
		}
		if len(f.ExecutedMatching("INSERT INTO")) != 0 {
			t.Error("nothing to stamp")
		}
	})
	t.Run("genuinely old schema", func(t *testing.T) {
		f := fakedb.NewCH()
		migrationState(f, 3)
		f.On("name = ?", fakedb.Count(1)) // raw_log still present
		stampProvisionedSchema(ctx, f, testMigrations, testMigrationsDir)
		if len(f.ExecutedMatching("INSERT INTO")) != 0 {
			t.Error("a schema that still has raw_log must replay, not be stamped")
		}
	})
	t.Run("provisioned from init SQL", func(t *testing.T) {
		f := fakedb.NewCH()
		migrationState(f, 0)
		f.On("name = ?", fakedb.Count(0))
		if err := stampProvisionedSchema(ctx, f, testMigrations, testMigrationsDir); err != nil {
			t.Fatal(err)
		}
		// The synthetic set tops out at 10, all at or below the split migration.
		if got := f.ExecutedMatching("INSERT INTO logs._bifract_migrations"); len(got) != 3 {
			t.Errorf("stamped %d, want every migration through %d", len(got), dbsql.RawLogSplitMigration)
		}
	})
}

func TestCheckPartitionKey(t *testing.T) {
	ctx := context.Background()
	f := fakedb.NewCH()
	f.On("partition_key", fakedb.Strs("partition_key", "(fractal_id, toDate(timestamp))"))
	if err := checkPartitionKey(ctx, f); !errors.Is(err, ErrIncompatibleSchema) {
		t.Errorf("event-time key: err = %v, want ErrIncompatibleSchema", err)
	}
	f = fakedb.NewCH()
	f.On("partition_key", fakedb.Strs("partition_key", "(fractal_id, toDate(ingest_timestamp))"))
	if err := checkPartitionKey(ctx, f); err != nil {
		t.Errorf("ingest-time key: err = %v", err)
	}
	// No answer is not a verdict: it must not read as an incompatible schema.
	f = fakedb.NewCH()
	if err := checkPartitionKey(ctx, f); err == nil || errors.Is(err, ErrIncompatibleSchema) {
		t.Errorf("missing row: err = %v, want a read error", err)
	}
	c := &ClickHouseClient{conn: f}
	if err := c.CheckLogsPartitionKey(ctx); err == nil {
		t.Error("CheckLogsPartitionKey must report the read failure")
	}
}

// A fresh single node is provisioned from the init SQL and stamped, never migrated:
// replaying history against a current schema fails.
func TestInitializeFreshSingleNode(t *testing.T) {
	c, f := newFakeClient()
	c.schemaReady = make(chan struct{})
	f.On("system.tables WHERE database = currentDatabase() AND name = ?", fakedb.Count(0))
	initSQL := "CREATE DATABASE IF NOT EXISTS logs; USE logs; CREATE TABLE IF NOT EXISTS a (x UInt8) ENGINE = MergeTree() ORDER BY x;"

	if err := c.Initialize(context.Background(), initSQL, testMigrations, testMigrationsDir); err != nil {
		t.Fatal(err)
	}
	if len(f.ExecutedMatching("CREATE DATABASE")) != 0 || len(f.ExecutedMatching("USE ")) != 0 {
		t.Error("CREATE DATABASE and USE must be skipped; the database is managed by the environment")
	}
	if len(f.ExecutedMatching("CREATE TABLE IF NOT EXISTS a ")) != 1 {
		t.Errorf("init SQL not applied: %v", f.Executed())
	}
	if got := f.ExecutedMatching("INSERT INTO logs._bifract_migrations"); len(got) != 3 {
		t.Errorf("stamped %d migrations, want all 3", len(got))
	}
	if len(f.ExecutedMatching("CREATE TABLE IF NOT EXISTS t1")) != 0 {
		t.Error("a fresh install must not replay migrations")
	}
	select {
	case <-c.schemaReady:
	default:
		t.Error("schema must be marked ready")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c.WaitForSchemaReady(ctx) // returns immediately once ready
	if ctx.Err() != nil {
		t.Error("WaitForSchemaReady blocked after the schema was ready")
	}
}

// An existing event-time schema is refused before anything is touched.
func TestInitializeRefusesIncompatibleSchema(t *testing.T) {
	c, f := newFakeClient()
	f.On("AND name = ?", fakedb.Count(1))
	f.On("partition_key", fakedb.Strs("partition_key", "toYYYYMMDD(timestamp)"))
	err := c.Initialize(context.Background(), "", testMigrations, testMigrationsDir)
	if !errors.Is(err, ErrIncompatibleSchema) {
		t.Fatalf("err = %v, want ErrIncompatibleSchema", err)
	}
	if len(f.Executed()) != 0 {
		t.Errorf("executed %v before refusing", f.Executed())
	}
}

func TestInitializeUpgradeAppliesPending(t *testing.T) {
	c, f := newFakeClient()
	f.On("system.tables WHERE database = currentDatabase() AND name = ?", fakedb.Count(1))
	f.On("partition_key", fakedb.Strs("partition_key", "(fractal_id, toDate(ingest_timestamp))"))
	migrationState(f, dbsql.RawLogSplitMigration+100) // everything synthetic is applied
	if err := c.Initialize(context.Background(), "", testMigrations, testMigrationsDir); err != nil {
		t.Fatal(err)
	}
	if len(f.ExecutedMatching("t1")) != 0 {
		t.Errorf("applied migrations re-ran: %v", f.Executed())
	}

	c, f = newFakeClient()
	f.On("AND name = ?", fakedb.CHResult{Err: errors.New("down")})
	if err := c.Initialize(context.Background(), "", testMigrations, testMigrationsDir); err == nil {
		t.Error("an unreadable table list must fail initialization")
	}
}

func TestMigrationStmtTimeout(t *testing.T) {
	t.Setenv("BIFRACT_MIGRATION_STMT_TIMEOUT", "")
	if got := migrationStmtTimeout(); got != 30*time.Minute {
		t.Errorf("default = %v", got)
	}
	t.Setenv("BIFRACT_MIGRATION_STMT_TIMEOUT", "90")
	if got := migrationStmtTimeout(); got != 90*time.Second {
		t.Errorf("override = %v", got)
	}
	for _, bad := range []string{"0", "-5", "soon"} {
		t.Setenv("BIFRACT_MIGRATION_STMT_TIMEOUT", bad)
		if got := migrationStmtTimeout(); got != 30*time.Minute {
			t.Errorf("%q = %v, want the default", bad, got)
		}
	}
}

func TestRewriteEngine(t *testing.T) {
	const path = "'/clickhouse/tables/{shard}/{database}/{table}', '{replica}'"
	cluster := &ClickHouseClient{topo: Topology{ReplicatedEngines: true}}
	cases := []struct{ in, want string }{
		{"ENGINE = MergeTree() ORDER BY x", "ENGINE = ReplicatedMergeTree(" + path + ") ORDER BY x"},
		{"engine=MergeTree()", "ENGINE = ReplicatedMergeTree(" + path + ")"},
		{"ENGINE = ReplacingMergeTree()", "ENGINE = ReplicatedReplacingMergeTree(" + path + ")"},
		{"ENGINE = ReplacingMergeTree(version)", "ENGINE = ReplicatedReplacingMergeTree(" + path + ", version)"},
		{"ENGINE = SummingMergeTree()", "ENGINE = ReplicatedSummingMergeTree(" + path + ")"},
		{"ENGINE = SummingMergeTree((a, b))", "ENGINE = ReplicatedSummingMergeTree(" + path + ", (a, b))"}, // the original closing paren survives the match
		{"ENGINE = AggregatingMergeTree()", "ENGINE = ReplicatedAggregatingMergeTree(" + path + ")"},
		{"ENGINE = AggregatingMergeTree(x)", "ENGINE = ReplicatedAggregatingMergeTree(" + path + ", x)"},
		{"ENGINE = Distributed('c', db, 't', rand())", "ENGINE = Distributed('c', db, 't', rand())"},
	}
	for _, tc := range cases {
		if got := cluster.RewriteEngine(tc.in); !strings.HasPrefix(got, tc.want) {
			t.Errorf("RewriteEngine(%q) = %q, want prefix %q", tc.in, got, tc.want)
		}
	}
	single := &ClickHouseClient{}
	if got := single.RewriteEngine("ENGINE = MergeTree()"); got != "ENGINE = MergeTree()" {
		t.Errorf("single node rewrote the engine: %q", got)
	}
}

// A retried logs insert is only dropped where the table keeps a dedup window, and its
// materialized views re-run on the retry, so logs and every view target need one. Replicated
// engines keep a window by default; this covers single node, fresh and upgraded.
func TestInsertDedupWindowOnLogsAndViewTargets(t *testing.T) {
	const setting = "non_replicated_deduplication_window"
	viewTarget := regexp.MustCompile(`(?i)CREATE MATERIALIZED VIEW IF NOT EXISTS \S+ TO (\w+)`)
	tables := map[string]bool{"logs": true}
	for _, m := range viewTarget.FindAllStringSubmatch(dbsql.ClickHouseSQL, -1) {
		tables[m[1]] = true
	}
	if len(tables) < 2 {
		t.Fatal("found no materialized view targets in init-clickhouse.sql")
	}
	migs, err := loadClickHouseMigrations(dbsql.ClickHouseMigrations, dbsql.ClickHouseMigrationsDir)
	if err != nil {
		t.Fatal(err)
	}
	var migrated []string
	for _, m := range migs {
		migrated = append(migrated, splitClickHouseSQL(m.sql)...)
	}
	for table := range tables {
		ddl, err := initTableDDL(table)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(ddl, setting) {
			t.Errorf("init-clickhouse.sql: %s has no %s", table, setting)
		}
		upgraded := slices.ContainsFunc(migrated, func(stmt string) bool {
			return strings.Contains(stmt, setting) && (strings.Contains(stmt, "ALTER TABLE "+table+" ") ||
				strings.Contains(stmt, "CREATE TABLE IF NOT EXISTS "+table+" ("))
		})
		if !upgraded {
			t.Errorf("no migration sets %s on %s for upgraded installs", setting, table)
		}
	}
}

// Endpoint-analysis views are permanently detached while the feature is off, and
// ClickHouse cannot MODIFY QUERY a detached view. The runner attaches it around the
// statement and detaches it again, so the upgrade applies whatever the toggle says.
func TestMigrationModifiesDetachedViewQuery(t *testing.T) {
	const stmt = "ALTER TABLE proc_freq_spawn_mv MODIFY QUERY\nSELECT 1 AS x FROM logs"
	for _, c := range []struct {
		name     string
		detached bool
		failOn   string
		want     []string
	}{
		{"attached", false, "", []string{stmt}},
		{"detached", true, "", []string{"ATTACH TABLE proc_freq_spawn_mv", stmt, "DETACH TABLE proc_freq_spawn_mv PERMANENTLY"}},
		{"detached and failing", true, "MODIFY QUERY", []string{"ATTACH TABLE proc_freq_spawn_mv", stmt, "DETACH TABLE proc_freq_spawn_mv PERMANENTLY"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := fakedb.NewCH()
			if c.detached {
				f.On("FROM system.detached_tables", fakedb.One("table", "String", "proc_freq_spawn_mv"))
			}
			if c.failOn != "" {
				f.FailExec(c.failOn, errors.New("boom"))
			}
			err := execMigrationStmt(context.Background(), f, stmt)
			if (err != nil) != (c.failOn != "") {
				t.Fatalf("err = %v", err)
			}
			if got := f.Executed(); !reflect.DeepEqual(got, c.want) {
				t.Errorf("executed %q, want %q", got, c.want)
			}
		})
	}
	f := fakedb.NewCH()
	if err := execMigrationStmt(context.Background(), f, "ALTER TABLE proc_freq ADD COLUMN IF NOT EXISTS y String"); err != nil {
		t.Fatal(err)
	}
	if q := f.Queried(); len(q) != 0 {
		t.Errorf("only MODIFY QUERY needs the detached check, ran %q", q)
	}
}
