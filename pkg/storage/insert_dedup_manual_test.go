//go:build chdedup

// Retries a logs insert on a real ClickHouse, after one that committed but reported an
// error, and checks no row is written twice in logs or in its materialized views.
//
//	docker run -d --name bfdedup -e CLICKHOUSE_PASSWORD=bifract -p 19014:9000 \
//	  clickhouse/clickhouse-server:26.8.34.13-alpine
//	go test -tags chdedup ./pkg/storage/ -run TestInsertRetryDedup -v
package storage

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	dbsql "bifract/db"
)

func dedupClient(t *testing.T) (*ClickHouseClient, driver.Conn) {
	t.Helper()
	ctx := context.Background()
	addr := os.Getenv("BIFRACT_TEST_CH_ADDR")
	if addr == "" {
		addr = "localhost:19014"
	}
	password := os.Getenv("BIFRACT_TEST_CH_PASSWORD")
	if password == "" {
		password = "bifract"
	}
	opts := func(db string) ConnOptions {
		return ConnOptions{Addrs: []string{addr}, Database: db, User: "default", Password: password,
			Pool: ClickHousePoolConfig{MaxOpenConns: 2, MaxIdleConns: 2}}
	}
	root, err := openClickHouseConn(opts("default"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	// Its own database, so pointing the test at a real server cannot drop logs.
	const db = "bifract_dedup_test"
	for _, stmt := range []string{"DROP DATABASE IF EXISTS " + db + " SYNC", "CREATE DATABASE " + db} {
		if err := root.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	root.Close()
	conn, err := openClickHouseConn(opts(db))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := runInitSQLOnConn(ctx, conn, dbsql.ClickHouseSQL, nil); err != nil {
		t.Fatalf("init sql: %v", err)
	}
	// A view that fails while the flag is set, as a memory limit in a view would: logs
	// commits its part, then the insert reports an error.
	for _, stmt := range []string{
		"CREATE TABLE fail_flag (v UInt8) ENGINE = Memory",
		"CREATE TABLE flaky_target (n UInt64) ENGINE = SummingMergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 10000",
		"CREATE MATERIALIZED VIEW flaky_mv TO flaky_target AS SELECT count() + throwIf((SELECT count() FROM fail_flag) > 0) AS n FROM logs",
	} {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	return &ClickHouseClient{conn: conn}, conn
}

type dedupCounts struct{ logs, histogram, hot, flaky uint64 }

func readDedupCounts(t *testing.T, conn driver.Conn) dedupCounts {
	t.Helper()
	var c dedupCounts
	err := conn.QueryRow(context.Background(), `SELECT
		(SELECT count() FROM logs),
		(SELECT toUInt64(sum(cnt)) FROM logs_histogram),
		(SELECT count() FROM logs_hot),
		(SELECT toUInt64(sum(n)) FROM flaky_target)`).Scan(&c.logs, &c.histogram, &c.hot, &c.flaky)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func dedupBatch(n int) []LogEntry {
	// Recent, or logs_hot expires the rows by TTL.
	ts := time.Now().UTC().Truncate(time.Second)
	out := make([]LogEntry, n)
	for i := range out {
		out[i] = LogEntry{Timestamp: ts, LogID: fmt.Sprintf("id-%d", i), FractalID: "F", IngestTimestamp: ts,
			Fields: map[string]string{"i": fmt.Sprint(i)}}
	}
	return out
}

func TestInsertRetryDedup(t *testing.T) {
	for _, async := range []int{0, 1} {
		t.Run(fmt.Sprintf("async_insert=%d", async), func(t *testing.T) {
			c, conn := dedupClient(t)
			c.insertSettings = clickhouse.Settings{"async_insert": async, "wait_for_async_insert": 1}
			ctx := context.Background()
			batch := dedupBatch(100)
			want := func(step string, n uint64) {
				t.Helper()
				if got := readDedupCounts(t, conn); got != (dedupCounts{n, n, n, n}) {
					t.Errorf("%s: counts %+v, want %d everywhere", step, got, n)
				}
			}

			// Committed but unacknowledged, then retried under the same token.
			tokA := InsertTokenContext(ctx, "batch-a")
			for range 2 {
				if err := c.InsertLogs(tokA, batch); err != nil {
					t.Fatal(err)
				}
			}
			want("retry after an unacknowledged success", 100)

			// logs commits and a view fails: the retry fills only the view that missed it.
			if err := conn.Exec(ctx, "INSERT INTO fail_flag VALUES (1)"); err != nil {
				t.Fatal(err)
			}
			batch2 := dedupBatch(150)[100:]
			tokB := InsertTokenContext(ctx, "batch-b")
			if err := c.InsertLogs(tokB, batch2); err == nil {
				t.Fatal("insert succeeded with the failing view armed")
			}
			if err := conn.Exec(ctx, "TRUNCATE TABLE fail_flag"); err != nil {
				t.Fatal(err)
			}
			if err := c.InsertLogs(tokB, batch2); err != nil {
				t.Fatal(err)
			}
			want("retry after a partial insert", 150)

			// The same rows in a new batch are new events and are kept.
			if err := c.InsertLogs(ctx, batch); err != nil {
				t.Fatal(err)
			}
			want("identical rows in a new batch", 250)
		})
	}
}
