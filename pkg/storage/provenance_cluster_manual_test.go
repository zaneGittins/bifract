//go:build chcluster

package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"bifract/pkg/parser"
)

// pgr's generated SQL against the Distributed tables of two shards: every pass must run with
// its external tables, and a process's identical children on different shards collapse into
// one node whose command line and user come from the lineage rows.
func TestProvenanceSQLAcrossShards(t *testing.T) {
	dropEverything(t)
	initialize(t)
	c := newTestClusterClient(t)
	defer c.Close()
	ctx := context.Background()

	const cols = "INSERT INTO logs.proc_lineage (fractal_id, timestamp, ingest_timestamp, log_id, process_guid, parent_guid, image, parent_image, commandline, proc_user, computer_name) VALUES "
	execOnShard(t, chTestHosts[0], cols+`('f1', '2026-10-01 00:00:00', now64(), 'l0', 'root', '', 'C:\\w.exe', '', 'w.exe', 'CORP\\a', 'WS1'), `+
		`('f1', '2026-10-01 00:00:01', now64(), 'l1', 'c1', 'root', 'C:\\Users\\bob\\t.exe', 'C:\\w.exe', 't.exe 1234567', 'CORP\\a', 'WS1')`)
	execOnShard(t, chTestHosts[1], cols+`('f1', '2026-10-01 00:00:02', now64(), 'l2', 'c2', 'root', 'C:\\Users\\amy\\t.exe', 'C:\\w.exe', 't.exe 7654321', '', 'WS2')`)
	// c3 predates the user column: its lineage row reads '\0' and its user is in its log.
	// The view is detached so the log does not write a second, current lineage row.
	execOnShard(t, chTestHosts[1], "DETACH TABLE logs.proc_lineage_mv")
	execOnShard(t, chTestHosts[1], `INSERT INTO logs.logs (timestamp, log_id, fields, fractal_id, ingest_timestamp) VALUES `+
		`('2026-10-01 00:00:03', 'l3', '{"bifract_category":"process_creation","process_guid":"c3","user":"CORP\\\\b"}', 'f1', now64())`)
	execOnShard(t, chTestHosts[1], "ATTACH TABLE logs.proc_lineage_mv")
	execOnShard(t, chTestHosts[0], "INSERT INTO logs.proc_lineage (fractal_id, timestamp, ingest_timestamp, log_id, process_guid, parent_guid, image, parent_image, commandline, computer_name) VALUES "+
		`('f1', '2026-10-01 00:00:03', now64(), 'l3', 'c3', 'root', 'C:\\x.exe', 'C:\\w.exe', 'x.exe', 'WS2')`)
	// Baseline rows on both shards, so the summary's freq kinds have something to agree on.
	for i, addr := range chTestHosts {
		execOnShard(t, addr, fmt.Sprintf("INSERT INTO logs.proc_freq SELECT 'f1', 'img%[1]d', 'spawn', 't', toDate('2026-09-0%[2]d'), toDate('2026-09-0%[2]d'), 1, groupUniqArrayState(256)(h) "+
			"FROM (SELECT arrayJoin(['WS1', 'WS%[2]d']) AS h)", i, i+2))
	}

	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	opts := parser.QueryOptions{
		FractalID: "f1", StartTime: start, EndTime: start.Add(24 * time.Hour), MaxRows: 100,
		TableName: c.ReadTable(), ProcLineageTable: c.ProcLineageReadTable(), ProcFreqTable: c.ProcFreqReadTable(),
		ProcEdgesTable: c.ProcEdgesReadTable(), ProvenanceSummaryTable: c.ProvenanceSummaryReadTable(),
	}

	frontier := ExternalTable{Name: parser.ProvenanceFrontierTable, Columns: []string{"guid", "node"}, Rows: [][]string{{"root", "root"}}}
	hop, err := c.QueryProvenance(ctx, parser.BuildProcessGroupHopSQL(opts), frontier)
	if err != nil {
		t.Fatalf("hop: %v", err)
	}
	if len(hop) != 2 || hop[0]["rep"] != "c1" || hop[0]["n"] != uint64(2) {
		t.Errorf("children on both shards must collapse into one group led by c1: %v", hop)
	}
	// Each shard aggregates its own children; only partial states cross the network.
	plan, err := c.QueryProvenance(ctx, "EXPLAIN "+parser.BuildProcessGroupHopSQL(opts), frontier)
	if err != nil {
		t.Fatalf("explain hop: %v", err)
	}
	if fmt.Sprint(plan) == "" || !strings.Contains(fmt.Sprint(plan), "MergingAggregated") {
		t.Errorf("hop must aggregate on the shards:\n%v", plan)
	}

	tree := ExternalTable{Name: parser.ProvenanceTreeTable, Columns: []string{"guid", "node"}, Rows: [][]string{{"root", "root"}, {"c1", "c1"}, {"c2", "c1"}, {"c3", "c3"}}}
	for name, sql := range map[string]string{
		"edge keys":    parser.BuildProvenanceEdgeKeysSQL(nil, opts),
		"reconnection": parser.BuildReconnectionSQL(parser.ProvenanceParams{Reconnect: true}, 2, 2, opts),
		"ptg rows":     parser.BuildProcessTreeRowsSQL(opts),
		"diffuse":      parser.BuildProvenanceScoringSQL(0, nil, true, parser.ProvenanceBaseline{TotalHosts: 2}, opts),
		"span literal": parser.BuildProvenanceScoringSQL(0, nil, false, parser.ProvenanceBaseline{FirstDay: start}, opts),
	} {
		if _, err := c.QueryProvenance(ctx, sql, tree); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}

	rows, err := c.QueryProvenance(ctx, parser.BuildProvenanceScoringSQL(0, nil, false, parser.ProvenanceBaseline{TotalHosts: 2}, opts), tree)
	if err != nil {
		t.Fatalf("scoring: %v", err)
	}
	want := map[string][2]string{"c1": {"t.exe 1234567", `CORP\a`}, "c3": {"x.exe", `CORP\b`}}
	for _, r := range rows {
		if w, ok := want[fmt.Sprint(r["child"])]; ok && r["parent"] == "root" {
			if r["command_line"] != w[0] || r["proc_user"] != w[1] {
				t.Errorf("node %v: command line/user %q %q, want %q", r["child"], r["command_line"], r["proc_user"], w)
			}
			delete(want, fmt.Sprint(r["child"]))
		}
	}
	if len(want) != 0 {
		t.Errorf("missing spawn edges to %v in %v", want, rows)
	}

	// Each shard summarizes its own rows; the summary must agree with the scans across both.
	one := func(sql string) map[string]interface{} {
		got, err := c.QueryProvenance(ctx, sql)
		if err != nil || len(got) != 1 {
			t.Fatalf("%s: %v %v", sql, got, err)
		}
		return got[0]
	}
	sum := one(parser.BuildProvenanceSummarySQL(opts))
	scanHosts := one(parser.BuildProvenanceTotalHostsSQL(opts))["total_hosts"]
	recon := one(parser.BuildReconnectionTotalsSQL(opts))
	first := one("SELECT toString(min(if(first_day = toDate(0), day, first_day))) AS d FROM " + c.ProcFreqReadTable() + " WHERE fractal_id = 'f1'")["d"]
	for name, pair := range map[string][2]interface{}{
		"total_hosts":  {sum["total_hosts"], scanHosts},
		"recon_hosts":  {sum["recon_hosts"], recon["total_hosts"]},
		"recon_images": {sum["recon_images"], recon["total_images"]},
		"first_day":    {sum["first_day"], first},
	} {
		if pair[0] != pair[1] {
			t.Errorf("%s: summary %v, scan %v", name, pair[0], pair[1])
		}
	}
	if sum["total_hosts"] != uint64(2) || sum["recon_hosts"] != uint64(3) || sum["first_day"] != "2026-09-02" {
		t.Errorf("summary %v", sum)
	}
}

// An upgraded cluster's history: parts on both shards from before the projections, and no
// summary rows. The backfill fills the summary to match the scans, records it, and leaves
// every part with its projections.
func TestBackfillProvenancePathsAcrossShards(t *testing.T) {
	dropEverything(t)
	initialize(t)
	c := newTestClusterClient(t)
	defer c.Close()
	ctx := context.Background()
	oldMin := provenanceBackfillMinPoll
	provenanceBackfillMinPoll = 100 * time.Millisecond
	defer func() { provenanceBackfillMinPoll = oldMin }()

	for i, addr := range chTestHosts {
		for _, p := range provenanceProjections {
			for _, name := range p.names {
				execOnShard(t, addr, "ALTER TABLE logs."+p.table+" DROP PROJECTION "+name)
			}
		}
		for _, f := range []string{"f1", "f2"} {
			execOnShard(t, addr, fmt.Sprintf("INSERT INTO logs.proc_lineage (fractal_id, timestamp, ingest_timestamp, log_id, process_guid, parent_guid, image, parent_image, commandline, computer_name) "+
				"SELECT '%[1]s', now64(), now64(), toString(number), concat('%[1]s-%[2]d-', toString(number)), 'p', 'C:\\\\a.exe', 'C:\\\\p.exe', 'a', concat('WS', toString(number %% 5 + %[2]d)) FROM numbers(1000)", f, i))
			execOnShard(t, addr, fmt.Sprintf("INSERT INTO logs.proc_freq SELECT '%[1]s', 'img%[2]d', 'spawn', 't', today() - %[2]d, today() - %[2]d, 1, groupUniqArrayState(256)(h) FROM (SELECT arrayJoin(['WS1', 'WS9']) AS h)", f, i))
			execOnShard(t, addr, fmt.Sprintf("INSERT INTO logs.process_edges (fractal_id, process_guid, event_type, dst_node, cnt, ingest_timestamp) VALUES ('%s', 'g', 'dns_query', 'dns:x', 1, now64())", f))
		}
		execOnShard(t, addr, "TRUNCATE TABLE logs.provenance_summary")
		execOnShard(t, addr, "ALTER TABLE logs.proc_lineage ADD PROJECTION by_parent (SELECT fractal_id, parent_guid, process_guid, timestamp, child_image, child_cmd_key ORDER BY (fractal_id, parent_guid))")
		execOnShard(t, addr, "ALTER TABLE logs.proc_lineage ADD PROJECTION by_image (SELECT _part_offset ORDER BY (fractal_id, image_lc))")
		execOnShard(t, addr, "ALTER TABLE logs.process_edges ADD PROJECTION by_target (SELECT _part_offset ORDER BY (fractal_id, event_type, fkey_tgt))")
		execOnShard(t, addr, "ALTER TABLE logs.proc_freq ADD PROJECTION by_target (SELECT _part_offset ORDER BY (fractal_id, event_type, target_norm))")
	}

	marked := 0
	if err := c.BackfillProvenancePaths(ctx, false, func(context.Context) error { marked++; return nil }); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if marked != 1 {
		t.Errorf("marked %d times", marked)
	}
	for _, addr := range chTestHosts {
		conn, err := openClickHouseConn(ConnOptions{Addrs: []string{addr}, Database: "logs", User: "default", Password: "bifract", Pool: chTestPool()})
		if err != nil {
			t.Fatal(err)
		}
		var lacking uint64
		if err := conn.QueryRow(ctx, "SELECT countIf(NOT hasAll(projections, if(table = 'proc_lineage', ['by_parent', 'by_image'], ['by_target']))) FROM system.parts "+
			"WHERE database = 'logs' AND active AND table IN ('proc_lineage', 'process_edges', 'proc_freq')").Scan(&lacking); err != nil {
			t.Fatal(err)
		}
		conn.Close()
		if lacking != 0 {
			t.Errorf("%s: %d parts still lack a projection", addr, lacking)
		}
	}

	for _, f := range []string{"f1", "f2"} {
		opts := parser.QueryOptions{FractalID: f, ProcLineageTable: c.ProcLineageReadTable(), ProcFreqTable: c.ProcFreqReadTable(), ProvenanceSummaryTable: c.ProvenanceSummaryReadTable()}
		row := func(sql string) map[string]interface{} {
			got, err := c.QueryProvenance(ctx, sql)
			if err != nil || len(got) != 1 {
				t.Fatalf("%s: %v %v", sql, got, err)
			}
			return got[0]
		}
		sum, recon := row(parser.BuildProvenanceSummarySQL(opts)), row(parser.BuildReconnectionTotalsSQL(opts))
		first := row("SELECT toString(min(if(first_day = toDate(0), day, first_day))) AS d FROM " + c.ProcFreqReadTable() + " WHERE fractal_id = '" + f + "'")["d"]
		if sum["total_hosts"] != row(parser.BuildProvenanceTotalHostsSQL(opts))["total_hosts"] || sum["recon_hosts"] != recon["total_hosts"] ||
			sum["recon_images"] != recon["total_images"] || sum["first_day"] != first || sum["total_hosts"] != uint64(6) {
			t.Errorf("%s: summary %v, scans %v first %v", f, sum, recon, first)
		}
	}

	// Done is done: a second run issues nothing.
	if err := c.BackfillProvenancePaths(ctx, true, nil); err != nil {
		t.Fatalf("second backfill: %v", err)
	}
}

// A clear recorded after a fractal's read began makes the fill drop and refill that
// fractal; one recorded before it does not.
func TestProvenanceFillSeesClears(t *testing.T) {
	dropEverything(t)
	initialize(t)
	ctx := context.Background()
	conn, err := openClickHouseConn(ConnOptions{Addrs: []string{chTestHosts[0]}, Database: "logs", User: "default", Password: "bifract", Pool: chTestPool()})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	execOnShard(t, chTestHosts[0], "INSERT INTO logs.proc_lineage (fractal_id, timestamp, ingest_timestamp, log_id, process_guid, computer_name) VALUES ('f1', now64(), now64(), 'l', 'g', 'WS1')")
	execOnShard(t, chTestHosts[0], "CREATE TABLE logs."+provenanceFillCleared+" (fractal_id String, at DateTime64(3)) ENGINE = MergeTree ORDER BY fractal_id")
	selects := make([]string, len(provenanceSummaryViews))
	for i, v := range provenanceSummaryViews {
		if selects[i], err = initViewSelect(v); err != nil {
			t.Fatal(err)
		}
	}
	if err := markProvenanceFillClear(ctx, conn, []string{"f1"}); err != nil {
		t.Fatal(err)
	}
	if err := fillFractalSummary(ctx, conn, "f1", selects); err != nil {
		t.Errorf("a clear before the read forced a refill: %v", err)
	}
	execOnShard(t, chTestHosts[0], "INSERT INTO logs."+provenanceFillCleared+" SELECT '*', now64(3) + INTERVAL 1 HOUR")
	if err := fillFractalSummary(ctx, conn, "f1", selects); err == nil || !strings.Contains(err.Error(), "cleared during every attempt") {
		t.Errorf("a clear after the read began must refill: %v", err)
	}
}
