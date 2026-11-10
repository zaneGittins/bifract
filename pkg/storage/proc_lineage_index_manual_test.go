//go:build chcluster

package storage

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Lineage written before migration 024 added idx_ingest_ts has no index data, so every
// ingest-time filter the ancestry builder runs reads all of it. An upgrade must index it.
func TestClusterUpgradeIndexesOldLineage(t *testing.T) {
	dropEverything(t)
	initialize(t)
	// The pre-024 state: lineage on each shard merged into one part spanning ten days, which
	// per-part statistics cannot skip, then the index added.
	for _, addr := range chTestHosts {
		execOnShard(t, addr, "ALTER TABLE logs.proc_lineage DROP INDEX idx_ingest_ts")
		for day := 0; day < 10; day++ {
			execOnShard(t, addr, fmt.Sprintf("INSERT INTO logs.proc_lineage (fractal_id, timestamp, ingest_timestamp, process_guid, parent_guid, image, parent_image) "+
				"SELECT 'f', t, t, concat('p-%d-', toString(number)), concat('p-%d-', toString(intDiv(number, 2))), 'c.exe', 'c.exe' "+
				"FROM (SELECT number, toDateTime64('2026-10-01 00:00:00', 3, 'UTC') + toIntervalDay(%d) + toIntervalSecond(number) AS t FROM numbers(20000))",
				day, day, day))
		}
		execOnShard(t, addr, "OPTIMIZE TABLE logs.proc_lineage FINAL")
		execOnShard(t, addr, "ALTER TABLE logs.proc_lineage ADD INDEX IF NOT EXISTS idx_ingest_ts ingest_timestamp TYPE minmax GRANULARITY 1")
		rewindMigrations(t, addr, 25)
	}
	const recent = "SELECT count() FROM logs.proc_lineage WHERE ingest_timestamp >= toDateTime64('2026-10-10 00:00:00', 3, 'UTC')"
	for _, addr := range chTestHosts {
		if kept, total := ingestIndexGranules(t, addr, recent); kept*2 < total {
			t.Fatalf("setup: %s already skips by ingest time (%d/%d granules)", addr, kept, total)
		}
	}

	initialize(t)
	for _, addr := range chTestHosts {
		waitForMutations(t, addr, "proc_lineage")
		if kept, total := ingestIndexGranules(t, addr, recent); kept*5 > total {
			t.Errorf("%s: after upgrade a one-day filter still reads %d of %d granules", addr, kept, total)
		} else {
			t.Logf("%s: a one-day filter reads %d of %d granules", addr, kept, total)
		}
	}
}

var (
	readGranulesRe  = regexp.MustCompile(`Parts: \d+ \| Granules: (\d+)`)
	totalGranulesRe = regexp.MustCompile(`Granules: \d+/(\d+)`)
)

// ingestIndexGranules reports the granules query reads on one shard, of the table's total.
func ingestIndexGranules(t *testing.T, addr, query string) (kept, total int) {
	t.Helper()
	conn, err := openClickHouseConn(ConnOptions{Addrs: []string{addr}, Database: "logs", User: "default", Password: "bifract", Pool: chTestPool()})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rows, err := conn.Query(context.Background(), "EXPLAIN indexes = 1 "+query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, line)
	}
	text := strings.Join(plan, "\n")
	r, a := readGranulesRe.FindStringSubmatch(text), totalGranulesRe.FindStringSubmatch(text)
	if r == nil || a == nil {
		t.Fatalf("%s: no granule counts in the plan:\n%s", addr, text)
	}
	kept, _ = strconv.Atoi(r[1])
	total, _ = strconv.Atoi(a[1])
	return kept, total
}

func waitForMutations(t *testing.T, addr, table string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for shardUint(t, addr, "SELECT count() FROM system.mutations WHERE database = 'logs' AND table = '"+table+"' AND NOT is_done") > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%s: mutations on %s did not finish", addr, table)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
