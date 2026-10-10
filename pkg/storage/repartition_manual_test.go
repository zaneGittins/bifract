//go:build chrepartition

// Converts tables provisioned before their per-fractal partition keys against a real
// ClickHouse, including resuming after a crash part way and a clear landing mid-copy.
//
//	docker run -d --name bfrepart -e CLICKHOUSE_PASSWORD=bifract -p 19011:9000 \
//	  clickhouse/clickhouse-server:26.6.2.81-alpine
//	go test -tags chrepartition ./pkg/storage/ -run TestConvertPartitioning -v
package storage

import (
	"context"
	"fmt"
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
		ddl = strings.Replace(ddl, "\nPARTITION BY "+target.key+"\n", "\n", 1)
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
		if info.partitionKey != target.key {
			t.Errorf("%s partition key = %q, want %q", target.table, info.partitionKey, target.key)
		}
		for _, suffix := range []string{repartOld, repartStage, repartDone, repartSkip} {
			if w, _ := describeTable(context.Background(), conn, target.table+suffix); w.exists {
				t.Errorf("%s%s left behind", target.table, suffix)
			}
		}
	}
}

var repartKeep = map[string]bool{"a": true, "b": true, "": true}

func TestConvertPartitioning(t *testing.T) {
	c, conn := repartClient(t)
	ctx := context.Background()
	before := map[string]map[string]uint64{}
	for _, target := range repartitionTargets {
		before[target.table] = repartTotals(t, conn, target.table)
	}

	if err := c.ConvertPartitioning(ctx, repartKeep); err != nil {
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
	if err := c.ConvertPartitioning(ctx, repartKeep); err != nil {
		t.Fatalf("second convert: %v", err)
	}
	assertConverted(t, conn)
}

// A crash after the swap, with one fractal's copy half written and another copied but
// never moved, must still end with every row exactly once, plus rows that arrived after
// the swap.
func TestConvertPartitioningResumes(t *testing.T) {
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
		cols, err := columnList(ctx, conn, stage)
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

	if err := c.ConvertPartitioning(ctx, repartKeep); err != nil {
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
func TestConvertPartitioningSkipsClearedFractal(t *testing.T) {
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
	if err := c.ConvertPartitioning(ctx, repartKeep); err != nil {
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
