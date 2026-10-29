//go:build chfieldpaths

// Reconciles the field path rollup against logs on a real ClickHouse: history the
// view never saw, an orphaned partition, a key explosion over the cap, and a day
// large enough to be recounted in slices.
//
//	docker run -d --name bffieldpaths -e CLICKHOUSE_PASSWORD=bifract -p 19012:9000 \
//	  clickhouse/clickhouse-server:26.8.34.13-alpine
//	go test -tags chfieldpaths ./pkg/storage/ -run TestReconcileFieldPaths -v
package storage

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	dbsql "bifract/db"
)

func chTestPassword() string {
	if p := os.Getenv("BIFRACT_TEST_CH_PASSWORD"); p != "" {
		return p
	}
	return "bifract"
}

func fieldPathsClient(t *testing.T) (*ClickHouseClient, driver.Conn) {
	t.Helper()
	ctx := context.Background()
	addr := os.Getenv("BIFRACT_TEST_CH_ADDR")
	if addr == "" {
		addr = "localhost:19012"
	}
	opts := func(db string) ConnOptions {
		return ConnOptions{Addrs: []string{addr}, Database: db, User: "default", Password: chTestPassword(),
			Pool: ClickHousePoolConfig{MaxOpenConns: 1, MaxIdleConns: 1}}
	}
	root, err := openClickHouseConn(opts("default"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	for _, stmt := range []string{"DROP DATABASE IF EXISTS logs SYNC", "CREATE DATABASE logs"} {
		if err := root.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	root.Close()
	conn, err := openClickHouseConn(opts("logs"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := runInitSQLOnConn(ctx, conn, dbsql.ClickHouseSQL, nil); err != nil {
		t.Fatalf("init sql: %v", err)
	}
	return &ClickHouseClient{conn: conn}, conn
}

// insertDay writes n rows for a fractal on an ingest day. Every row has src; one in
// three has dst; one row in the day has rare. extraKeys adds that many distinct keys
// per row, named from seed, to simulate sender-controlled field names.
func insertDay(t *testing.T, conn driver.Conn, fractal string, day time.Time, n, extraKeys, seed int) {
	t.Helper()
	extra := "''"
	if extraKeys > 0 {
		extra = fmt.Sprintf("arrayStringConcat(arrayMap(i -> concat(',\"k%d_', toString(number * %d + i), '\":\"x\"'), range(%d)))", seed, extraKeys, extraKeys)
	}
	q := fmt.Sprintf(`INSERT INTO logs (timestamp, log_id, fields, fractal_id, ingest_timestamp)
		SELECT toDateTime64('%[1]s', 3, 'UTC') + number, toString(number), concat('{"src":"s', toString(number %% 5), '"',
			if(number %% 3 = 0, ',"dst":"d"', ''), if(number = 7, ',"rare":"r"', ''), ',"empty":""', %[2]s, '}'),
			'%[3]s', toDateTime64('%[1]s', 3, 'UTC') + number / 1000
		FROM numbers(%[4]d)`, day.Format("2006-01-02 15:04:05"), extra, fractal, n)
	if err := conn.Exec(context.Background(), q); err != nil {
		t.Fatalf("insert %s %s: %v", fractal, day.Format("2006-01-02"), err)
	}
}

func pathCounts(t *testing.T, conn driver.Conn, q string) map[string]uint64 {
	t.Helper()
	rows, err := conn.Query(context.Background(), q)
	if err != nil {
		t.Fatalf("%v\n%s", err, q)
	}
	defer rows.Close()
	out := map[string]uint64{}
	for rows.Next() {
		var k string
		var n uint64
		if err := rows.Scan(&k, &n); err != nil {
			t.Fatal(err)
		}
		out[k] = n
	}
	return out
}

// rollupCounts is the rollup per (partition, path); truthCounts is the same counted
// straight from logs.
func rollupCounts(t *testing.T, conn driver.Conn) map[string]uint64 {
	return pathCounts(t, conn, "SELECT concat(fractal_id, '|', toString(ingest_day), '|', path), sum(cnt) FROM logs_field_paths GROUP BY 1")
}

func truthCounts(t *testing.T, conn driver.Conn) map[string]uint64 {
	return pathCounts(t, conn, "SELECT concat(fractal_id, '|', toString(toDate(ingest_timestamp)), '|', path), count() FROM logs ARRAY JOIN "+
		fieldPathsExpr+" AS path WHERE fractal_id = 'a' GROUP BY 1")
}

func TestReconcileFieldPaths(t *testing.T) {
	c, conn := fieldPathsClient(t)
	ctx := context.Background()
	today := time.Now().UTC().Truncate(24 * time.Hour)
	day := func(n int) time.Time { return today.AddDate(0, 0, -n) }
	exec := func(q string) {
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%v\n%s", err, q)
		}
	}

	// History from before the view existed.
	exec("DROP VIEW logs_field_paths_mv")
	insertDay(t, conn, "a", day(5), 5000, 0, 0)
	insertDay(t, conn, "a", day(4), 300, 0, 0)
	insertDay(t, conn, "a", day(0), 100, 0, 0)
	if err := runInitSQLOnConn(ctx, conn, dbsql.ClickHouseSQL, nil); err != nil {
		t.Fatalf("recreate view: %v", err)
	}

	// Counted by the view: a normal day, a key explosion across three inserts, and today.
	insertDay(t, conn, "a", day(3), 400, 0, 0)
	for seed := range 3 {
		insertDay(t, conn, "b", day(3), 200, 10, seed)
	}
	insertDay(t, conn, "a", day(0), 50, 0, 0)
	// Counts whose logs partition was dropped.
	exec(fmt.Sprintf("INSERT INTO logs_field_paths VALUES ('z', '%s', '', 9), ('z', '%s', 'src', 9)", day(6).Format("2006-01-02"), day(6).Format("2006-01-02")))

	before := rollupCounts(t, conn)
	if n := before["a|"+day(3).Format("2006-01-02")+"|"]; n != 400 {
		t.Fatalf("view counted %d rows for a's day -3, want 400", n)
	}
	if n := before["b|"+day(3).Format("2006-01-02")+"|"]; n != 600 {
		t.Fatalf("view counted %d rows for b's day -3, want 600", n)
	}

	defer func(old uint64) { fieldPathChunkRows = old }(fieldPathChunkRows)
	fieldPathChunkRows = 1000 // day -5 is recounted in five slices

	if err := c.ReconcileFieldPaths(ctx, 1<<30); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got, truth := rollupCounts(t, conn), truthCounts(t, conn)
	for _, d := range []int{5, 4, 3} {
		key := "a|" + day(d).Format("2006-01-02") + "|"
		for _, path := range []string{"", "src", "dst", "rare"} {
			if got[key+path] != truth[key+path] {
				t.Errorf("a day -%d path %q: rollup %d, logs %d", d, path, got[key+path], truth[key+path])
			}
		}
		if _, ok := got[key+"empty"]; ok {
			t.Errorf("a day -%d counted a field whose value is empty", d)
		}
	}
	if got["a|"+day(5).Format("2006-01-02")+"|rare"] != 1 {
		t.Errorf("a single occurrence must survive the rebuild exactly")
	}

	// Today is open: left as the view wrote it, without the 100 rows it never saw.
	if n := got["a|"+today.Format("2006-01-02")+"|"]; n != 50 {
		t.Errorf("today's count = %d, want the view's 50 untouched", n)
	}

	if n := pathCounts(t, conn, "SELECT fractal_id, sum(cnt) FROM logs_field_paths WHERE fractal_id = 'z' GROUP BY 1")["z"]; n != 0 {
		t.Errorf("orphaned counts survived: %d", n)
	}

	if n := pathCounts(t, conn, "SELECT 'n', count() FROM "+fieldPathsStage)["n"]; n != 0 {
		t.Errorf("the stage kept %d rows after the swaps", n)
	}

	bKey := "b|" + day(3).Format("2006-01-02") + "|"
	if got[bKey] != 600 || got[bKey+"src"] != 600 {
		t.Errorf("compaction must keep exact counts for what it keeps: rows %d, src %d", got[bKey], got[bKey+"src"])
	}
	bPaths := pathCounts(t, conn, "SELECT fractal_id, uniqExact(path) FROM logs_field_paths WHERE fractal_id = 'b' GROUP BY 1")["b"]
	if bPaths != FieldPathCap {
		t.Errorf("b's exploded day holds %d paths, want the cap %d", bPaths, FieldPathCap)
	}

	// A second pass finds nothing to do.
	if err := c.ReconcileFieldPaths(ctx, 1<<30); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	again := rollupCounts(t, conn)
	if len(again) != len(got) {
		t.Errorf("second pass changed the rollup: %d keys, was %d", len(again), len(got))
	}
	for k, v := range got {
		if again[k] != v {
			t.Errorf("second pass changed %q: %d, was %d", k, again[k], v)
		}
	}
}

// A day of random keys too large to group in memory is still recounted exactly:
// the recount spills to disk instead of failing every pass.
func TestReconcileFieldPathsSpills(t *testing.T) {
	c, conn := fieldPathsClient(t)
	ctx := context.Background()
	day := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -3)
	if err := conn.Exec(ctx, "DROP VIEW logs_field_paths_mv"); err != nil {
		t.Fatal(err)
	}
	for seed := range 10 {
		insertDay(t, conn, "c", day, 20000, 50, seed)
	}

	if err := c.ReconcileFieldPaths(ctx, 300<<20); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := rollupCounts(t, conn)
	key := "c|" + day.Format("2006-01-02") + "|"
	if got[key] != 200000 || got[key+"src"] != 200000 {
		t.Errorf("rows %d, src %d, want 200000", got[key], got[key+"src"])
	}
	if len(got) != FieldPathCap {
		t.Errorf("the day holds %d paths, want the cap %d", len(got), FieldPathCap)
	}
}
