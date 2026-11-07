//go:build chrepartition

// Converts tables provisioned before their per-fractal partition keys against a real
// ClickHouse, including resuming after a crash part way and a clear landing mid-copy.
//
//	docker run -d --name bfrepart -e CLICKHOUSE_PASSWORD=bifract -p 19011:9000 \
//	  clickhouse/clickhouse-server:26.8.34.13-alpine
//	go test -tags chrepartition ./pkg/storage/ -run TestConvertLayouts -v
package storage

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const repartAddr = "localhost:19011"

// repartFixture inserts rows for fractals a, b, the default fractal's unscoped "" and a
// deleted fractal z, each with an exact per-fractal total that a conversion must keep.
var repartFixture = map[string]string{
	"logs_histogram": `INSERT INTO logs_histogram SELECT ['a','b','','z'][number % 4 + 1], toDate('2026-01-01') + number % 5, toDateTime('2026-01-01 00:00:00') + number * 60, 1 FROM numbers(4000)`,
	"proc_lineage":   `INSERT INTO proc_lineage (fractal_id, timestamp, ingest_timestamp, log_id, process_guid) SELECT ['a','b','','z'][number % 4 + 1], now64(), now64(), toString(number), toString(number) FROM numbers(4000)`,
	"proc_freq":      `INSERT INTO proc_freq (fractal_id, src_image, event_type, target_norm, day, first_day, event_count) SELECT ['a','b','','z'][number % 4 + 1], 'img', 'spawn', toString(number % 50), today(), today(), 1 FROM numbers(4000)`,
	"process_edges":  `INSERT INTO process_edges (fractal_id, process_guid, event_type, dst_node, cnt, ingest_timestamp) SELECT ['a','b','','z'][number % 4 + 1], toString(number % 100), 'dns_query', toString(number % 7), 1, now64() FROM numbers(4000)`,
}

// repartTotal is each table's per-fractal measure that merging must not change.
var repartTotal = map[string]string{
	"logs_histogram": "sum(cnt)",
	"proc_lineage":   "uniqExact(process_guid)",
	"proc_freq":      "sum(event_count)",
	"process_edges":  "sum(cnt)",
}

func repartClient(t *testing.T) (*ClickHouseClient, driver.Conn) {
	t.Helper()
	ctx := context.Background()
	root, err := openClickHouseConn(ConnOptions{Addrs: []string{repartAddr}, Database: "default", User: "default", Password: "bifract", Pool: ClickHousePoolConfig{MaxOpenConns: 1, MaxIdleConns: 1}})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	for _, stmt := range []string{"DROP DATABASE IF EXISTS logs SYNC", "CREATE DATABASE logs"} {
		if err := root.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	root.Close()
	conn, err := openClickHouseConn(ConnOptions{Addrs: []string{repartAddr}, Database: "logs", User: "default", Password: "bifract", Pool: ClickHousePoolConfig{MaxOpenConns: 1, MaxIdleConns: 1}})
	if err != nil {
		t.Fatalf("connect to logs: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	// The pre-change schema: today's DDL without its PARTITION BY.
	for _, target := range repartitionTargets {
		ddl, err := initTableDDL(target.table)
		if err != nil {
			t.Fatal(err)
		}
		ddl = strings.Replace(ddl, "\nPARTITION BY "+target.partitionKey+"\n", "\n", 1)
		if err := conn.Exec(ctx, ddl); err != nil {
			t.Fatalf("create %s: %v", target.table, err)
		}
		if err := conn.Exec(ctx, repartFixture[target.table]); err != nil {
			t.Fatalf("seed %s: %v", target.table, err)
		}
	}
	return &ClickHouseClient{conn: conn}, conn
}

func repartTotals(t *testing.T, conn driver.Conn, table string) map[string]uint64 {
	t.Helper()
	rows, err := conn.Query(context.Background(), fmt.Sprintf("SELECT fractal_id, toUInt64(%s) FROM %s GROUP BY fractal_id", repartTotal[table], table))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]uint64{}
	for rows.Next() {
		var f string
		var n uint64
		if err := rows.Scan(&f, &n); err != nil {
			t.Fatal(err)
		}
		out[f] = n
	}
	return out
}

func assertConverted(t *testing.T, conn driver.Conn) {
	t.Helper()
	for _, target := range repartitionTargets {
		info, err := describeTable(context.Background(), conn, target.table)
		if err != nil {
			t.Fatal(err)
		}
		if info.partitionKey != target.partitionKey {
			t.Errorf("%s partition key = %q, want %q", target.table, info.partitionKey, target.partitionKey)
		}
		for _, suffix := range []string{repartOld, repartStage, repartDone, repartSkip} {
			if w, _ := describeTable(context.Background(), conn, target.table+suffix); w.exists {
				t.Errorf("%s%s left behind", target.table, suffix)
			}
		}
	}
}

var repartKeep = map[string]bool{"a": true, "b": true, "": true}

func TestConvertLayouts(t *testing.T) {
	c, conn := repartClient(t)
	ctx := context.Background()
	before := map[string]map[string]uint64{}
	for _, target := range repartitionTargets {
		before[target.table] = repartTotals(t, conn, target.table)
	}

	if err := c.ConvertLayouts(ctx, repartKeep); err != nil {
		t.Fatalf("convert: %v", err)
	}
	assertConverted(t, conn)
	for _, target := range repartitionTargets {
		after := repartTotals(t, conn, target.table)
		for f := range repartKeep {
			if after[f] != before[target.table][f] {
				t.Errorf("%s fractal %q: %d after, %d before", target.table, f, after[f], before[target.table][f])
			}
		}
		if after["z"] != 0 {
			t.Errorf("%s copied %d rows of deleted fractal z", target.table, after["z"])
		}
	}

	// Idempotent: a second start finds nothing to do.
	if err := c.ConvertLayouts(ctx, repartKeep); err != nil {
		t.Fatalf("second convert: %v", err)
	}
	assertConverted(t, conn)
}

// A crash after the swap, with one fractal's copy half written and another copied but
// never moved, must still end with every row exactly once, plus rows that arrived after
// the swap.
func TestConvertLayoutsResumes(t *testing.T) {
	c, conn := repartClient(t)
	ctx := context.Background()
	before := map[string]map[string]uint64{}
	for _, target := range repartitionTargets {
		before[target.table] = repartTotals(t, conn, target.table)
		live, err := describeTable(ctx, conn, target.table)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.swapIn(ctx, conn, target, live, false); err != nil {
			t.Fatalf("swap %s: %v", target.table, err)
		}
		// Ingest after the swap lands in the new table through the views.
		if err := conn.Exec(ctx, repartFixture[target.table]); err != nil {
			t.Fatal(err)
		}
		stage, old := target.table+repartStage, target.table+repartOld
		cols, err := columnList(ctx, conn, stage, old)
		if err != nil {
			t.Fatal(err)
		}
		// a: half a copy, never recorded.
		if err := conn.Exec(ctx, "INSERT INTO "+stage+" ("+cols+") SELECT "+cols+" FROM "+old+" WHERE fractal_id = 'a' LIMIT 100"); err != nil {
			t.Fatal(err)
		}
		// b: a full copy, recorded, not yet moved.
		if err := conn.Exec(ctx, "INSERT INTO "+stage+" ("+cols+") SELECT "+cols+" FROM "+old+" WHERE fractal_id = 'b'"); err != nil {
			t.Fatal(err)
		}
		if err := conn.Exec(ctx, "INSERT INTO "+target.table+repartDone+" VALUES ('b')"); err != nil {
			t.Fatal(err)
		}
	}

	if err := c.ConvertLayouts(ctx, repartKeep); err != nil {
		t.Fatalf("resume: %v", err)
	}
	assertConverted(t, conn)
	for _, target := range repartitionTargets {
		after := repartTotals(t, conn, target.table)
		for _, f := range []string{"a", "b", ""} {
			// proc_lineage dedups re-ingested process_guids; the others sum.
			want := before[target.table][f] * 2
			if target.table == "proc_lineage" {
				want = before[target.table][f]
			}
			if after[f] != want {
				t.Errorf("%s fractal %q: %d after resume, want %d", target.table, f, after[f], want)
			}
		}
	}
}

// Clearing a fractal while a conversion is in progress must not have its earlier rows
// copied back afterwards.
func TestConvertLayoutsSkipsClearedFractal(t *testing.T) {
	c, conn := repartClient(t)
	ctx := context.Background()
	before := map[string]map[string]uint64{}
	for _, target := range repartitionTargets {
		before[target.table] = repartTotals(t, conn, target.table)
		live, err := describeTable(ctx, conn, target.table)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.swapIn(ctx, conn, target, live, false); err != nil {
			t.Fatalf("swap %s: %v", target.table, err)
		}
	}
	if err := c.DeleteLogsByFractalID(ctx, "a", false); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if err := c.ConvertLayouts(ctx, repartKeep); err != nil {
		t.Fatalf("convert: %v", err)
	}
	assertConverted(t, conn)
	for _, target := range repartitionTargets {
		after := repartTotals(t, conn, target.table)
		if after["a"] != 0 {
			t.Errorf("%s: cleared fractal a has %d after conversion", target.table, after["a"])
		}
		if after["b"] != before[target.table]["b"] {
			t.Errorf("%s fractal b: %d, want %d", target.table, after["b"], before[target.table]["b"])
		}
	}

	// Once converted, clearing a fractal is a partition drop.
	if err := c.DeleteLogsByFractalID(ctx, "b", false); err != nil {
		t.Fatal(err)
	}
	for _, target := range repartitionTargets {
		if n := repartTotals(t, conn, target.table)["b"]; n != 0 {
			t.Errorf("%s: fractal b has %d after clearing", target.table, n)
		}
	}
	var mutations uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM system.mutations WHERE database = 'logs'").Scan(&mutations); err != nil {
		t.Fatal(err)
	}
	if mutations != 0 {
		t.Errorf("%d mutations ran; conversion and clears must only move and drop partitions", mutations)
	}
}

// An upgrade landing mid-conversion migrates the live table, but the stage and old tables
// an earlier release created keep its columns. The stage gains the new columns, so a
// copied-but-unmoved fractal still moves, and columns old lacks take their defaults.
func TestConvertLayoutsResumesAcrossMigration(t *testing.T) {
	c, conn := repartClient(t)
	ctx := context.Background()
	target := repartitionTargets[1]
	if target.table != "proc_lineage" {
		t.Fatalf("target %s, want proc_lineage", target.table)
	}
	before := repartTotals(t, conn, target.table)
	live, err := describeTable(ctx, conn, target.table)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.swapIn(ctx, conn, target, live, false); err != nil {
		t.Fatal(err)
	}
	stage, old := target.table+repartStage, target.table+repartOld
	// The earlier release's shape: no proc_user, no lookup columns.
	for _, tbl := range []string{stage, old} {
		for _, stmt := range []string{
			"ALTER TABLE " + tbl + " DROP PROJECTION by_parent", "ALTER TABLE " + tbl + " DROP PROJECTION by_image",
			"ALTER TABLE " + tbl + " DROP COLUMN proc_user, DROP COLUMN child_image, DROP COLUMN child_cmd_key, DROP COLUMN image_lc",
		} {
			if err := conn.Exec(ctx, stmt); err != nil {
				t.Fatalf("%s: %v", stmt, err)
			}
		}
	}
	cols, err := columnList(ctx, conn, stage, old)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, "INSERT INTO "+stage+" ("+cols+") SELECT "+cols+" FROM "+old+" WHERE fractal_id = 'b'"); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, "INSERT INTO "+target.table+repartDone+" VALUES ('b')"); err != nil {
		t.Fatal(err)
	}

	if err := c.ConvertLayouts(ctx, repartKeep); err != nil {
		t.Fatalf("resume: %v", err)
	}
	assertConverted(t, conn)
	after := repartTotals(t, conn, target.table)
	for f := range repartKeep {
		if after[f] != before[f] {
			t.Errorf("fractal %q: %d after, %d before", f, after[f], before[f])
		}
	}
}

// previousLayoutDDL is a table as the release before proc_key created it: no key columns,
// sorted and projected by GUID, and optionally from before its per-fractal partition key.
func previousLayoutDDL(t *testing.T, table string, partitioned bool) string {
	t.Helper()
	ddl, err := initTableDDL(table)
	if err != nil {
		t.Fatal(err)
	}
	cut := func(from, to string) {
		t.Helper()
		if !strings.Contains(ddl, from) {
			t.Fatalf("%s: no %q to revert", table, from[:min(60, len(from))])
		}
		ddl = strings.Replace(ddl, from, to, 1)
	}
	for _, col := range []string{"proc_key", "parent_key"} {
		if line := keyColumnDDL(ddl, col); line != "" {
			cut(line, "")
		}
	}
	switch table {
	case "proc_lineage":
		cut("PROJECTION by_parent (SELECT fractal_id, parent_key, parent_guid, process_guid, timestamp, child_image, child_cmd_key ORDER BY (fractal_id, parent_key))",
			"PROJECTION by_parent (SELECT fractal_id, parent_guid, process_guid, timestamp, child_image, child_cmd_key ORDER BY (fractal_id, parent_guid))")
		cut("ORDER BY (fractal_id, proc_key)\n", "ORDER BY (fractal_id, process_guid)\n")
	case "process_edges":
		cut("ORDER BY (fractal_id, proc_key, process_guid, event_type, dst_node)\n", "ORDER BY (fractal_id, process_guid, event_type, dst_node)\n")
	}
	if !partitioned {
		cut("\nPARTITION BY fractal_id\n", "\n")
	}
	return strings.Replace(ddl, "index_granularity = 1024", "index_granularity = 8192", 1)
}

// keyColumnDDL returns col's definition line in ddl, through its trailing comma and newline.
func keyColumnDDL(ddl, col string) string {
	i := strings.Index(ddl, "    "+col+" ")
	if i < 0 {
		return ""
	}
	return ddl[i : i+strings.Index(ddl[i:], ",\n")+2]
}

// procKeyFromDDL is proc_key's expression as init-clickhouse.sql defines it.
func procKeyFromDDL(t *testing.T) string {
	t.Helper()
	ddl, err := initTableDDL("proc_lineage")
	if err != nil {
		t.Fatal(err)
	}
	line := keyColumnDDL(ddl, "proc_key")
	_, expr, ok := strings.Cut(line, " MATERIALIZED ")
	if !ok {
		t.Fatal("proc_key has no MATERIALIZED expression")
	}
	return strings.TrimSuffix(expr, ",\n")
}

// sortKeyFixture seeds Sysmon-shaped and other GUIDs, with repeated edges that must aggregate.
var sortKeyFixture = map[string]string{
	"proc_lineage": `INSERT INTO proc_lineage (fractal_id, timestamp, ingest_timestamp, log_id, process_guid, parent_guid, image) SELECT ['a','b','','z'][number % 4 + 1], now64(), now64(), toString(number), ` +
		`if(number % 5 = 0, hex(number), concat('{63047898-', leftPad(lower(hex(toUInt16(number % 65536))), 4, '0'), '-', leftPad(lower(hex(toUInt16(26720 + intDiv(number, 7)))), 4, '0'), '-0000-', leftPad(toString(number), 12, '0'), '}')), ` +
		`if(number % 5 = 0, '', concat('{63047898-', leftPad(lower(hex(toUInt16((number + 1) % 65536))), 4, '0'), '-6860-0000-000000000001}')), 'x.exe' FROM numbers(SEED_FROM, SEED_COUNT)`,
	"process_edges": `INSERT INTO process_edges (fractal_id, process_guid, event_type, dst_node, cnt, ingest_timestamp) SELECT ['a','b','','z'][number % 4 + 1], ` +
		`concat('{63047898-', leftPad(lower(hex(toUInt16(number % 100))), 4, '0'), '-6860-0000-000000000001}'), 'dns_query', toString(number % 7), 1, now64() FROM numbers(SEED_FROM, SEED_COUNT)`,
}

// seed is a sortKeyFixture insert for count rows numbered from from.
func seed(table string, from, count int) string {
	return strings.NewReplacer("SEED_FROM", fmt.Sprint(from), "SEED_COUNT", fmt.Sprint(count)).Replace(sortKeyFixture[table])
}

// migration022 runs the statements that add the key columns to the live tables.
func migration022(t *testing.T, conn driver.Conn) {
	t.Helper()
	b, err := os.ReadFile("../../db/migrations/clickhouse/022_proc_key.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range splitClickHouseSQL(string(b)) {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if err := conn.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("migration 022: %v\n%s", err, stmt)
		}
	}
}

// finalRows counts a table's rows once merged, for the kept fractals.
func finalRows(t *testing.T, conn driver.Conn, from string) uint64 {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(context.Background(), "SELECT count() FROM "+from+" WHERE fractal_id IN ('a', 'b', '')").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// assertSortKeyed checks a converted provenance table: the target keys, every row's key
// computed from its own GUID, Sysmon GUIDs reordered, and every row found by its key.
func assertSortKeyed(t *testing.T, conn driver.Conn, target repartitionTarget, wantRows uint64) {
	t.Helper()
	ctx := context.Background()
	info, err := describeTable(ctx, conn, target.table)
	if err != nil {
		t.Fatal(err)
	}
	if info.sortingKey != target.sortingKey || info.partitionKey != target.partitionKey {
		t.Errorf("%s keys = (%q, %q), want (%q, %q)", target.table, info.partitionKey, info.sortingKey, target.partitionKey, target.sortingKey)
	}
	key := procKeyFromDDL(t)
	var rows, wrongKey, reordered, found uint64
	q := fmt.Sprintf("SELECT count(), countIf(proc_key != %[1]s), countIf(proc_key != process_guid), "+
		"(SELECT count() FROM %[2]s FINAL WHERE proc_key IN (SELECT %[1]s FROM %[2]s)) FROM %[2]s FINAL", key, target.table)
	if err := conn.QueryRow(ctx, q).Scan(&rows, &wrongKey, &reordered, &found); err != nil {
		t.Fatal(err)
	}
	if rows != wantRows || wrongKey != 0 || reordered == 0 || found != rows {
		t.Errorf("%s: rows %d (want %d), wrong keys %d, reordered %d, found by key %d", target.table, rows, wantRows, wrongKey, reordered, found)
	}
}

// From the previous release's layout, the migration plus a conversion leave each provenance
// table sorted by key with every row and every aggregate kept.
func TestConvertLayoutsSortKey(t *testing.T) {
	c, conn := repartClient(t)
	ctx := context.Background()
	sums, rows := map[string]map[string]uint64{}, map[string]uint64{}
	for _, target := range repartitionTargets {
		if target.sortingKey == "" {
			continue
		}
		rows0 := seed(target.table, 0, 4000)
		for _, stmt := range []string{"DROP TABLE " + target.table + " SYNC", previousLayoutDDL(t, target.table, true), rows0, rows0} {
			if err := conn.Exec(ctx, stmt); err != nil {
				t.Fatalf("%s: %v", target.table, err)
			}
		}
		sums[target.table] = repartTotals(t, conn, target.table)
		rows[target.table] = finalRows(t, conn, target.table+" FINAL")
	}
	migration022(t, conn)
	if err := c.ConvertLayouts(ctx, repartKeep); err != nil {
		t.Fatal(err)
	}
	assertConverted(t, conn)
	for _, target := range repartitionTargets {
		if target.sortingKey == "" {
			continue
		}
		got := repartTotals(t, conn, target.table)
		for f := range repartKeep {
			if got[f] != sums[target.table][f] {
				t.Errorf("%s fractal %q: %d after, %d before", target.table, f, got[f], sums[target.table][f])
			}
		}
		assertSortKeyed(t, conn, target, rows[target.table])
	}
}

// An earlier conversion that swapped and stopped part way is finished before the new layout
// swaps in, so rows still waiting in its __old reach the new table too. Its working tables
// predate the key columns, as on a real upgrade.
func TestConvertLayoutsFinishesEarlierConversion(t *testing.T) {
	c, conn := repartClient(t)
	ctx := context.Background()
	target := repartitionTargets[1]
	stage, old := target.table+repartStage, target.table+repartOld
	named := func(ddl, name string) string {
		return strings.Replace(ddl, "EXISTS proc_lineage (", "EXISTS "+name+" (", 1)
	}
	for _, stmt := range []string{
		"DROP TABLE proc_lineage SYNC",
		previousLayoutDDL(t, target.table, true),
		named(previousLayoutDDL(t, target.table, false), old),
		named(previousLayoutDDL(t, target.table, true), stage),
		"CREATE TABLE " + target.table + repartDone + " (fractal_id String) ENGINE = MergeTree ORDER BY fractal_id",
		"CREATE TABLE " + target.table + repartSkip + " (fractal_id String) ENGINE = MergeTree ORDER BY fractal_id",
		strings.Replace(seed(target.table, 0, 4000), "INSERT INTO proc_lineage ", "INSERT INTO "+old+" ", 1),
		seed(target.table, 4000, 1000),
	} {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%v\n%s", err, stmt[:min(120, len(stmt))])
		}
	}
	want := finalRows(t, conn, "(SELECT * FROM "+old+" UNION ALL SELECT * FROM proc_lineage)")
	migration022(t, conn)
	if err := c.ConvertLayouts(ctx, repartKeep); err != nil {
		t.Fatal(err)
	}
	assertConverted(t, conn)
	assertSortKeyed(t, conn, target, want)
}
