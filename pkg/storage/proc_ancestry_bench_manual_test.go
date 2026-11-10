//go:build chcluster

package storage

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// How much lineage one 30s builder cycle reflects while backfilling a month of it. Logs only;
// timing varies by host.
//
//	BIFRACT_ANCESTRY_BENCH=1 go test -tags chcluster ./pkg/storage -run AncestryBackfillRate -v -timeout 30m
func TestAncestryBackfillRate(t *testing.T) {
	if os.Getenv("BIFRACT_ANCESTRY_BENCH") == "" {
		t.Skip("BIFRACT_ANCESTRY_BENCH not set")
	}
	dropEverything(t)
	initialize(t)
	c := newTestClusterClient(t)
	defer c.Close()
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	const days, hosts, perHost = 30, 200, 250
	// Sysmon-shaped GUIDs: host, then the time split as Sysmon writes it, so proc_key keeps a
	// host's processes in time order. A parent is the same host's process one to three earlier.
	guid := func(s string) string {
		return "concat('{', leftPad(hex(m), 8, '0'), '-', leftPad(hex(" + s + " % 65536), 4, '0'), '-', leftPad(hex(intDiv(" + s + ", 65536)), 4, '0'), " +
			"'-0000-', leftPad(hex(" + s + " * 256 + m), 12, '0'), '}')"
	}
	for _, addr := range chTestHosts {
		for d := 0; d < days; d++ {
			execOnShard(t, addr, fmt.Sprintf("INSERT INTO logs.proc_lineage (fractal_id, timestamp, ingest_timestamp, process_guid, parent_guid, image, parent_image) "+
				"SELECT 'f', t, t, %s, if(i %% 40 = 0, '', %s), concat('C:\\\\w\\\\p', toString(i %% 23), '.exe'), concat('C:\\\\w\\\\p', toString((i + 1) %% 23), '.exe') "+
				"FROM (SELECT number %% %d AS m, intDiv(number, %d) AS i, %d * 86400 + i * 345 + m AS s, s - 345 * (1 + number %% 3) AS ps, "+
				"toDateTime64('%s', 3, 'UTC') + toIntervalSecond(s) AS t FROM numbers(%d))",
				guid("s"), guid("ps"), hosts, hosts, d, base.Format("2006-01-02 15:04:05"), hosts*perHost))
		}
		execOnShard(t, addr, "OPTIMIZE TABLE logs.proc_lineage FINAL")
	}
	t.Logf("lineage rows: %d", shardUint(t, chTestHosts[0], "SELECT count() FROM logs.proc_lineage")+shardUint(t, chTestHosts[1], "SELECT count() FROM logs.proc_lineage"))

	slice := 5 * time.Minute
	hi := base.Add(days * 24 * time.Hour)
	for i := 0; i < 6; i++ {
		before, _ := c.procAncestryWatermark(context.Background())
		if before.IsZero() {
			before = base
		}
		ctx, cancel := context.WithTimeout(context.Background(), procAncestryCycleBudget)
		start := time.Now()
		err := c.reflectProcAncestry(ctx, hi, &slice)
		cancel()
		after, _ := c.procAncestryWatermark(context.Background())
		n, _ := c.lineageRowsBetween(context.Background(), before, after)
		t.Logf("cycle %d: %d lineage rows (%s of ingest) in %s; slice now %s; err %v",
			i, n, after.Sub(before), time.Since(start).Round(time.Second), slice, err)
	}
}
