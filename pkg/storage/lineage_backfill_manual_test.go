//go:build chancestry

package storage

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// Process creations already in logs reach lineage with the view's columns, a second run adds
// nothing, and the ancestry builder is moved back so it reflects them.
func TestBackfillLineageFromLogs(t *testing.T) {
	ctx := context.Background()
	root, err := openClickHouseConn(ConnOptions{Addrs: []string{ancestryAddr}, Database: "default", User: "default", Password: "bifract", Pool: ClickHousePoolConfig{MaxOpenConns: 1, MaxIdleConns: 1}})
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{"DROP DATABASE IF EXISTS logs SYNC", "CREATE DATABASE logs"} {
		if err := root.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	root.Close()
	conn, err := openClickHouseConn(ConnOptions{Addrs: []string{ancestryAddr}, Database: "logs", User: "default", Password: "bifract", Pool: ClickHousePoolConfig{MaxOpenConns: 2, MaxIdleConns: 2}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, table := range []string{"logs", "proc_lineage", procAncestryTable, procAncestryWatermarkTable} {
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
	c := &ClickHouseClient{conn: conn}
	if err := c.setProcAncestryWatermark(ctx, time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	sel, err := initViewSelect("proc_lineage_mv")
	if err != nil {
		t.Fatal(err)
	}
	var rewound []time.Time
	rewind := func(at time.Time) error {
		rewound = append(rewound, at)
		return c.rewindProcAncestry(ctx, at)
	}
	for run := 0; run < 2; run++ {
		if err := backfillLineageOnShard(ctx, conn, sel, rewind); err != nil {
			t.Fatal(err)
		}
	}
	earliest := slices.MinFunc(rewound, func(a, b time.Time) int { return a.Compare(b) })
	if len(rewound) != 3 || !earliest.Equal(time.Date(2026, 10, 1, 2, 0, 0, 0, time.UTC)) {
		t.Errorf("rewound %v, want once per partition from the first, and nothing on the second run", rewound)
	}
	// New rows in an already copied partition are copied on the next run, and only they.
	if err := conn.Exec(ctx, `INSERT INTO logs (fractal_id, timestamp, ingest_timestamp, log_id, fields) VALUES ('f1', toDateTime64('2026-10-01 01:00:00', 3, 'UTC'), toDateTime64('2026-10-01 03:00:00', 3, 'UTC'), 'late', '{"bifract_category": "process_creation", "process_guid": "g999", "image": "late.exe", "commandline": "late", "computer_name": "WS1"}')`); err != nil {
		t.Fatal(err)
	}
	rewound = nil
	if err := backfillLineageOnShard(ctx, conn, sel, rewind); err != nil || len(rewound) != 1 || !rewound[0].Equal(time.Date(2026, 10, 1, 3, 0, 0, 0, time.UTC)) {
		t.Errorf("late row: rewound %v (%v), want once to its own ingest", rewound, err)
	}
	var n, wrong uint64
	if err := conn.QueryRow(ctx, "SELECT count(), countIf(commandline != concat('p', substring(process_guid, 2), ' -x') OR proc_user != 'CORP\\\\u' OR hash != concat('SHA256=', substring(process_guid, 2)) OR computer_name != 'WS1') FROM proc_lineage FINAL WHERE process_guid != 'g999'").Scan(&n, &wrong); err != nil {
		t.Fatal(err)
	}
	if n != 150 || wrong != 0 {
		t.Errorf("lineage rows %d (want 150), %d with fields unlike the log", n, wrong)
	}
	if w, _ := c.procAncestryWatermark(ctx); !w.Equal(time.Date(2026, 10, 1, 2, 0, 0, 0, time.UTC)) {
		t.Errorf("ancestry watermark %s, want it moved back to the earliest backfilled row", w)
	}
	slice := time.Hour
	if err := c.reflectProcAncestry(ctx, time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), &slice); err != nil {
		t.Fatal(err)
	}
	var status uint64
	if err := conn.QueryRow(ctx, "SELECT uniqExact(proc_key) FROM proc_ancestry WHERE depth = 0").Scan(&status); err != nil || status != 151 {
		t.Errorf("ancestry status rows %d (%v), want one per backfilled process", status, err)
	}
}
