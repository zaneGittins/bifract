//go:build chlineage

// Needs a single-node ClickHouse on lineageAddr:
//
//	go test -tags chlineage ./pkg/storage -run BackfillLineageFromLogs -v

package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

const lineageAddr = "localhost:19011"

// Process creations already in logs reach lineage with the view's columns, a second run adds
// nothing, and a late row in a copied partition is copied on the next run.
func TestBackfillLineageFromLogs(t *testing.T) {
	ctx := context.Background()
	root, err := openClickHouseConn(ConnOptions{Addrs: []string{lineageAddr}, Database: "default", User: "default", Password: "bifract", Pool: ClickHousePoolConfig{MaxOpenConns: 1, MaxIdleConns: 1}})
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{"DROP DATABASE IF EXISTS logs SYNC", "CREATE DATABASE logs"} {
		if err := root.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	root.Close()
	conn, err := openClickHouseConn(ConnOptions{Addrs: []string{lineageAddr}, Database: "logs", User: "default", Password: "bifract", Pool: ClickHousePoolConfig{MaxOpenConns: 2, MaxIdleConns: 2}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, table := range []string{"logs", "proc_lineage"} {
		ddl, err := initTableDDL(table)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.Exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	var rows []string
	for d := 0; d < 3; d++ {
		for i := 0; i < 50; i++ {
			n := d*50 + i
			rows = append(rows, fmt.Sprintf(`('f1', toDateTime64('2026-10-0%d 01:00:00', 3, 'UTC'), toDateTime64('2026-10-0%d 02:00:00', 3, 'UTC'), 'l%d', `+
				`'{"bifract_category": "process_creation", "process_guid": "g%d", "parent_process_guid": "g%d", "image": "C:\\\\p%d.exe", "commandline": "p%d -x", "user": "CORP\\\\u", "hash": "SHA256=%d", "computer_name": "WS1"}')`,
				d+1, d+1, n, n, n/2, n%5, n, n))
		}
	}
	rows = append(rows, `('f1', toDateTime64('2026-10-01 01:00:00', 3, 'UTC'), toDateTime64('2026-10-01 02:00:00', 3, 'UTC'), 'x', '{"message": "not a process"}')`)
	if err := conn.Exec(ctx, "INSERT INTO logs (fractal_id, timestamp, ingest_timestamp, log_id, fields) VALUES "+strings.Join(rows, ", ")); err != nil {
		t.Fatal(err)
	}
	sel, err := initViewSelect("proc_lineage_mv")
	if err != nil {
		t.Fatal(err)
	}
	for run := 0; run < 2; run++ {
		if err := backfillLineageOnShard(ctx, conn, sel); err != nil {
			t.Fatal(err)
		}
	}
	// New rows in an already copied partition are copied on the next run.
	if err := conn.Exec(ctx, `INSERT INTO logs (fractal_id, timestamp, ingest_timestamp, log_id, fields) VALUES ('f1', toDateTime64('2026-10-01 01:00:00', 3, 'UTC'), toDateTime64('2026-10-01 03:00:00', 3, 'UTC'), 'late', '{"bifract_category": "process_creation", "process_guid": "g999", "image": "late.exe", "commandline": "late", "computer_name": "WS1"}')`); err != nil {
		t.Fatal(err)
	}
	if err := backfillLineageOnShard(ctx, conn, sel); err != nil {
		t.Fatal(err)
	}
	var late, copies uint64
	if err := conn.QueryRow(ctx, "SELECT countIf(process_guid = 'g999'), count() FROM proc_lineage").Scan(&late, &copies); err != nil || late != 1 || copies != 151 {
		t.Errorf("late row copied %d times, %d lineage rows before merging (want 1 and 151): %v", late, copies, err)
	}
	var n, wrong uint64
	if err := conn.QueryRow(ctx, "SELECT count(), countIf(commandline != concat('p', substring(process_guid, 2), ' -x') OR proc_user != 'CORP\\\\u' OR hash != concat('SHA256=', substring(process_guid, 2)) OR computer_name != 'WS1') FROM proc_lineage FINAL WHERE process_guid != 'g999'").Scan(&n, &wrong); err != nil {
		t.Fatal(err)
	}
	if n != 150 || wrong != 0 {
		t.Errorf("lineage rows %d (want 150), %d with fields unlike the log", n, wrong)
	}
}
