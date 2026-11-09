//go:build chmodels

package models

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"bifract/pkg/parser"
)

// netResult is one results row scored at a pass, kept until expires.
func netResult(src, dst string, final float64, pass, expires time.Time) resultRow {
	return resultRow{fractalID: "f1", src: src, dst: dst, port: "443", final: final, connCount: 10,
		firstSeen: pass.Add(-time.Hour), lastSeen: pass, scoredAt: pass, expiresOn: expires}
}

// writeResults inserts each row as its own part, as separate passes land.
func (d *chDB) writeResults(table string, rows ...resultRow) {
	d.t.Helper()
	for _, r := range rows {
		b := newResultBuffer(d.c, table)
		b.add(r)
		if err := b.flush(context.Background()); err != nil {
			d.t.Fatal(err)
		}
	}
}

func pairsOf(rows []map[string]interface{}) []string {
	var out []string
	for _, r := range rows {
		out = append(out, fmt.Sprintf("%s>%s %.2f", getString(r, "src_ip"), getString(r, "dst_ip"), getFloat64(r, "final_score")))
	}
	return out
}

// Pair a scores in both passes, b went quiet after the first, c scores low in
// the latest. The findings, their count, the listing's count and the alert's
// lookup are a alone, at its newest score; history holds every pair once.
func TestCHNetFindingsAreTheCurrentPass(t *testing.T) {
	d := newCHDB(t)
	d.netState(ModelDefinition{}, 1)
	d.exec(BuildNetResultsTableDDL("r"))
	now := time.Now().UTC().Truncate(time.Millisecond)
	p1, p2 := now.Add(-2*time.Hour), now.Add(-time.Hour)
	d.writeResults("r",
		netResult("10.0.0.1", "203.0.113.1", 0.90, p1, netResultsExpiry(p1)),
		netResult("10.0.0.2", "203.0.113.2", 0.90, p1, netResultsExpiry(p1)),
		netResult("10.0.0.1", "203.0.113.1", 0.95, p2, netResultsExpiry(p2)),
		netResult("10.0.0.3", "203.0.113.3", 0.30, p2, netResultsExpiry(p2)),
	)
	m := &Manager{ch: d.c}
	mo := &Model{ModelType: ModelTypeBeacon, CHTableName: "r", ScoredPassAt: &p2}
	ctx := context.Background()

	findings, total, err := m.GetData(ctx, mo, "f1", DataQuery{View: DataViewFindings})
	if err != nil || total != 1 || !reflect.DeepEqual(pairsOf(findings), []string{"10.0.0.1>203.0.113.1 0.95"}) {
		t.Errorf("findings %v (%d) %v", pairsOf(findings), total, err)
	}
	all, total, err := m.GetData(ctx, mo, "f1", DataQuery{View: DataViewAll})
	want := []string{"10.0.0.1>203.0.113.1 0.95", "10.0.0.2>203.0.113.2 0.90", "10.0.0.3>203.0.113.3 0.30"}
	if err != nil || total != 3 || !reflect.DeepEqual(pairsOf(all), want) {
		t.Errorf("history %v (%d) %v", pairsOf(all), total, err)
	}
	for _, r := range all {
		if cur := getUint64(r, "current") == 1; cur != (getString(r, "src_ip") != "10.0.0.2") {
			t.Errorf("%s current = %v", getString(r, "src_ip"), cur)
		}
	}
	st, err := m.GetStats(ctx, mo, "f1")
	if err != nil || getUint64(st, "findings") != 1 || getUint64(st, "total_pairs") != 3 {
		t.Errorf("stats %v %v", st, err)
	}
	sum := d.rows(healthSummarySQL(ModelTypeBeacon, ModelDefinition{}, "r", "s", "f1", "", time.Time{}, now, &p2))
	if got := getUint64(sum[0], "flagged"); got != 1 {
		t.Errorf("listing counts %d findings", got)
	}
	// Before its first pass a model has no findings, whatever the table holds.
	if _, total, _ := m.GetData(ctx, &Model{ModelType: ModelTypeBeacon, CHTableName: "r"}, "f1", DataQuery{View: DataViewFindings}); total != 0 {
		t.Errorf("no pass: %d findings", total)
	}

	d.exec("ALTER TABLE logs ADD COLUMN log_id String DEFAULT toString(generateUUIDv4())")
	for _, pair := range [][2]string{{"10.0.0.1", "203.0.113.1"}, {"10.0.0.2", "203.0.113.2"}} {
		d.exec(fmt.Sprintf(`INSERT INTO logs (fractal_id, timestamp, ingest_timestamp, fields) SELECT 'f1', now64(3, 'UTC') - toIntervalMinute(number + 1), now64(3, 'UTC'),
    '{"src_ip":"%s","dst_ip":"%s","dst_port":"443"}' FROM numbers(3)`, pair[0], pair[1]))
	}
	opts := parser.QueryOptions{StartTime: now.Add(-time.Hour), EndTime: now.Add(time.Minute), MaxRows: 100, FractalID: "f1",
		Models: map[string]parser.AnalyticsModelInfo{"b": {ID: "b", TableName: "r", ModelType: "beacon", FractalID: "f1",
			Keys: []string{"src_ip", "dst_ip", "dst_port"}, ScoredPassAt: &p2}}}
	for _, q := range []string{
		`* | model_lookup(model="b", key=[src_ip, dst_ip, dst_port]) | beacon_score > 0.8 | table(src_ip, beacon_score)`,
		`* | model_lookup(model="b", key=[src_ip, dst_ip, dst_port], require=false) | beacon_score > 0.8 | table(src_ip, beacon_score)`,
	} {
		if got, want := d.lookupRows(q, opts), []string{"10.0.0.1|0.95", "10.0.0.1|0.95", "10.0.0.1|0.95"}; !reflect.DeepEqual(got, want) {
			t.Errorf("%s: %v, want %v", q, got, want)
		}
	}
}

// Expired rows leave a whole part at a time: each expiry day is its own
// partition, so the TTL drops parts and never rewrites a surviving row.
func TestCHNetResultsExpireWholeParts(t *testing.T) {
	d := newCHDB(t)
	d.exec(BuildNetResultsTableDDL("r"))
	now := time.Now().UTC().Truncate(time.Millisecond)
	old := now.AddDate(0, 0, -netResultsRetentionDays-2)
	d.writeResults("r",
		netResult("10.0.0.1", "203.0.113.1", 0.9, old, netResultsExpiry(old)),
		netResult("10.0.0.2", "203.0.113.2", 0.9, now, netResultsExpiry(now)),
		netResult("10.0.0.3", "203.0.113.3", 0.9, old, netResultsNever),
	)
	// An emptied part stays listed until cleanup, so count the parts with rows.
	parts := func() uint64 {
		return getUint64(d.rows("SELECT count() AS n FROM system.parts WHERE active AND rows > 0 AND database = currentDatabase() AND table = 'r'")[0], "n")
	}
	d.exec("OPTIMIZE TABLE r FINAL")
	got := pairsOf(d.rows("SELECT src_ip, dst_ip, final_score FROM r ORDER BY src_ip"))
	if want := []string{"10.0.0.2>203.0.113.2 0.90", "10.0.0.3>203.0.113.3 0.90"}; !reflect.DeepEqual(got, want) {
		t.Errorf("rows after TTL %v, want %v", got, want)
	}
	if n := parts(); n != 2 {
		t.Errorf("%d parts with rows after TTL, want the expired one dropped", n)
	}
	if n := getUint64(d.rows("SELECT count() AS n FROM (SELECT _partition_id FROM r GROUP BY _partition_id HAVING uniqExact(expires_on) > 1)")[0], "n"); n != 0 {
		t.Errorf("%d parts mix expiry days, so the TTL would rewrite them", n)
	}
}

// A results table from before the expiry partitions is recreated in them with
// each pair's newest row; a dataset's rows are kept for good. A current table
// is left as it is.
func TestCHEnsureNetResultsLayout(t *testing.T) {
	for _, dataset := range []bool{false, true} {
		d := newCHDB(t)
		d.exec(`CREATE TABLE r (fractal_id LowCardinality(String), src_ip String, dst_ip String, dst_port String,
    regularity_score Float64, ts_score Float64, ds_score Float64, dur_score Float64, hist_score Float64,
    prevalence Float64, prevalence_total UInt64, prevalence_score Float64, first_seen_score Float64, threat_intel_score Float64,
    final_score Float64, conn_count UInt64, total_duration Float64,
    first_seen DateTime64(3, 'UTC'), last_seen DateTime64(3, 'UTC'), scored_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(scored_at) ORDER BY (fractal_id, src_ip, dst_ip, dst_port)`)
		now := time.Now().UTC().Truncate(time.Millisecond)
		p1, p2 := now.Add(-2*time.Hour), now.Add(-time.Hour)
		for _, r := range []struct {
			final float64
			at    time.Time
		}{{0.5, p1}, {0.9, p2}} {
			d.exec(fmt.Sprintf(`INSERT INTO r (fractal_id, src_ip, dst_ip, dst_port, final_score, conn_count, first_seen, last_seen, scored_at)
VALUES ('f1', '10.0.0.1', '203.0.113.1', '443', %g, 7, toDateTime64('%s', 3, 'UTC'), toDateTime64('%s', 3, 'UTC'), toDateTime64('%s', 3, 'UTC'))`,
				r.final, p1.Add(-time.Hour).Format(chTimeLayout), r.at.Format(chTimeLayout), r.at.Format(chTimeLayout)))
		}
		m := &Manager{ch: d.c}
		mo := &Model{ID: "m1", FractalID: "f1", ModelType: ModelTypeBeacon, CHTableName: "r"}
		wantExpiry := netResultsExpiry(time.Now()).Format("2006-01-02")
		if dataset {
			mo.DatasetEnd = &now
			wantExpiry = netResultsNever.Format("2006-01-02")
		}
		ctx := context.Background()
		if ok, err := m.netResultsLayoutOK(ctx, "r"); err != nil || ok {
			t.Fatalf("an old layout passed: %v %v", ok, err)
		}
		if err := m.ensureNetResultsLayout(ctx, mo); err != nil {
			t.Fatal(err)
		}
		if ok, err := m.netResultsLayoutOK(ctx, "r"); err != nil || !ok {
			t.Fatalf("recreated layout: %v %v", ok, err)
		}
		rows := d.rows("SELECT src_ip, final_score, conn_count, toString(expires_on) AS exp, toUnixTimestamp64Milli(scored_at) AS ms FROM r")
		if len(rows) != 1 || getFloat64(rows[0], "final_score") != 0.9 || getUint64(rows[0], "conn_count") != 7 ||
			getString(rows[0], "exp") != wantExpiry || getUint64(rows[0], "ms") != uint64(p2.UnixMilli()) {
			t.Errorf("dataset=%v: carried over %v, want the newest row expiring %s", dataset, rows, wantExpiry)
		}
		d.exec("INSERT INTO r (fractal_id, src_ip, dst_ip, dst_port, scored_at, expires_on) VALUES ('f1', 'x', 'y', '1', now64(3), today() + 1)")
		if err := m.ensureNetResultsLayout(ctx, mo); err != nil {
			t.Fatal(err)
		}
		if n := getUint64(d.rows("SELECT count() AS n FROM r")[0], "n"); n != 2 {
			t.Errorf("a current table was recreated: %d rows", n)
		}
	}
}

// A score just over the threshold is a finding in the count and on the page
// alike, though the page shows it rounded to the threshold.
func TestCHNetFindingsPageMatchesTheCount(t *testing.T) {
	d := newCHDB(t)
	d.exec(BuildNetResultsTableDDL("r"))
	pass := time.Now().UTC().Truncate(time.Millisecond)
	d.writeResults("r", netResult("10.0.0.1", "203.0.113.1", 0.8004, pass, netResultsExpiry(pass)))
	m := &Manager{ch: d.c}
	mo := &Model{ModelType: ModelTypeBeacon, CHTableName: "r", ScoredPassAt: &pass}
	st, err := m.GetStats(context.Background(), mo, "f1")
	if err != nil {
		t.Fatal(err)
	}
	rows, total, err := m.GetData(context.Background(), mo, "f1", DataQuery{View: DataViewFindings})
	if err != nil || getUint64(st, "findings") != 1 || total != 1 || len(rows) != 1 || getFloat64(rows[0], "final_score") != 0.8 {
		t.Errorf("count %v, page %v (%d) %v", st["findings"], rows, total, err)
	}
}
