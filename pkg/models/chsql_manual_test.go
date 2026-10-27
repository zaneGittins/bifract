//go:build chmodels

// Runs the model SQL builders against a real ClickHouse, each test in its own
// scratch database:
//
//	go test -tags chmodels ./pkg/models/ -run CH -v
//
// BIFRACT_TEST_CLICKHOUSE (default localhost:9000) and
// BIFRACT_TEST_CLICKHOUSE_PASSWORD (default bifract) point at the server.
package models

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"bifract/pkg/parser"
	"bifract/pkg/storage"
)

type chDB struct {
	t        *testing.T
	c        *storage.ClickHouseClient
	db, pass string
}

func newCHDB(t *testing.T) *chDB {
	t.Helper()
	addr := os.Getenv("BIFRACT_TEST_CLICKHOUSE")
	if addr == "" {
		addr = "localhost:9000"
	}
	pass, ok := os.LookupEnv("BIFRACT_TEST_CLICKHOUSE_PASSWORD")
	if !ok {
		pass = "bifract"
	}
	connect := func(db string) *storage.ClickHouseClient {
		c, err := storage.NewClickHouseClient(storage.ClientOptions{Conn: storage.ConnOptions{
			Addrs: storage.HostAddrs([]string{addr}, 9000), Database: db, User: "default", Password: pass,
		}})
		if err != nil {
			t.Fatalf("connect %s: %v", addr, err)
		}
		return c
	}
	admin := connect("default")
	db := fmt.Sprintf("models_sqltest_%d", time.Now().UnixNano())
	if err := admin.Conn().Exec(context.Background(), "CREATE DATABASE "+db); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		_ = admin.Conn().Exec(context.Background(), "DROP DATABASE IF EXISTS "+db)
		admin.Close()
	})
	d := &chDB{t: t, c: connect(db), db: db, pass: pass}
	t.Cleanup(func() { _ = d.c.Close() })
	d.exec(`CREATE TABLE logs (
    fractal_id LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    ingest_timestamp DateTime64(3, 'UTC'),
    fields JSON
) ENGINE = MergeTree ORDER BY timestamp`)
	return d
}

func (d *chDB) exec(stmts ...string) {
	d.t.Helper()
	for _, s := range stmts {
		if err := d.c.Conn().Exec(context.Background(), s); err != nil {
			d.t.Fatalf("exec: %v\n%s", err, s)
		}
	}
}

func (d *chDB) rows(q string) []map[string]interface{} {
	d.t.Helper()
	rows, err := d.c.Query(context.Background(), q)
	if err != nil {
		d.t.Fatalf("query: %v\n%s", err, q)
	}
	return rows
}

// connLogs inserts one connection log per (offset back from now, src, dst).
func (d *chDB) connLogs(src, dst string, every time.Duration, n int) {
	d.t.Helper()
	d.exec(fmt.Sprintf(`INSERT INTO logs SELECT 'f1', now64(3, 'UTC') - toIntervalSecond(number * %d + 30), now64(3, 'UTC'),
    '{"src_ip":"%s","dst_ip":"%s","dst_port":"443","orig_bytes":"' || toString(number %% 997) || '","duration":"1"}'
FROM numbers(%d)`, int(every.Seconds()), src, dst, n))
}

// connLogsAt inserts n connection logs every interval from first, each lasting
// dur seconds.
func (d *chDB) connLogsAt(src, dst string, first time.Time, every time.Duration, n, dur int) {
	d.t.Helper()
	d.exec(fmt.Sprintf(`INSERT INTO logs SELECT 'f1', toDateTime64('%s', 3, 'UTC') + toIntervalSecond(number * %d), now64(3, 'UTC'),
    '{"src_ip":"%s","dst_ip":"%s","dst_port":"443","orig_bytes":"' || toString(number %% 997) || '","duration":"%d"}'
FROM numbers(%d)`, first.UTC().Format(chTimeLayout), int(every.Seconds()), src, dst, dur, n))
}

// netState builds the network state table for a window and fills it from logs.
func (d *chDB) netState(def ModelDefinition, days int) {
	d.t.Helper()
	ins, err := BuildNetStateInsert(def, "s", "logs", "", "f1")
	if err != nil {
		d.t.Fatal(err)
	}
	d.exec(BuildNetStateTableDDL("s", days), ins)
}

// A busy pair keeps the hourly cap on every hour of the window, timestamps and
// sizes stay paired, and pairs outside the gate never reach the scorer.
func TestCHBeaconReadKeepsEveryHour(t *testing.T) {
	d := newCHDB(t)
	d.connLogs("10.0.0.1", "198.51.100.1", 30*time.Second, 7*2880) // 120 an hour, over the cap
	d.connLogs("10.0.0.2", "198.51.100.2", 2*time.Second, 7*43200) // a strobe
	d.connLogs("10.0.0.3", "198.51.100.3", time.Hour, 2)           // too few
	def := ModelDefinition{Beacon: &BeaconParams{StrobeLimit: 100000}}
	d.netState(def, 7)

	w := netWindowAt(time.Now(), 7)
	rows := d.rows(BuildNetScoreReadQuery(def, ModelTypeBeacon, "s", "f1", w, "") + scorerReadSettings())
	if len(rows) != 1 || getString(rows[0], "src") != "10.0.0.1" {
		t.Fatalf("gated pairs: %v", rows)
	}
	p := pairFromRow(rows[0], w.Start.Unix())
	// The current hour may hold fewer than the cap.
	if len(p.TsList) < 167*netStateHourCap || len(p.TsList) > 168*netStateHourCap {
		t.Errorf("%d points, want the hourly cap on each of 167-168 hours", len(p.TsList))
	}
	if span := p.LastTs - p.FirstTs; span < 6*86400 {
		t.Errorf("points span %v, want the whole window", time.Duration(span)*time.Second)
	}
	if len(p.SizeList) != len(p.TsList) {
		t.Errorf("%d sizes for %d timestamps", len(p.SizeList), len(p.TsList))
	}
}

// A beacon too busy to keep every point still keeps the earliest run of every
// hour, so it covers all 24 hours of each day and its intervals stay its own.
func TestCHBeaconStateCoversEveryHour(t *testing.T) {
	d := newCHDB(t)
	w := netWindowAt(time.Now(), 3)
	d.connLogsAt("10.0.0.1", "198.51.100.1", w.Start, 10*time.Second, 3*8640, 1)
	def := ModelDefinition{}
	d.exec(BuildNetStateTableDDL("s", 3))
	// One insert per day, latest first, as a catching-up maintainer might.
	for day := 2; day >= 0; day-- {
		from := w.Start.Add(time.Duration(day) * 24 * time.Hour)
		where := fmt.Sprintf("timestamp >= '%s' AND timestamp < '%s'",
			from.Format(chTimeLayout), from.Add(24*time.Hour).Format(chTimeLayout))
		ins, err := BuildNetStateInsert(def, "s", "logs", where, "f1")
		if err != nil {
			t.Fatal(err)
		}
		d.exec(ins)
	}

	rows := d.rows(BuildNetScoreReadQuery(def, ModelTypeBeacon, "s", "f1", w, "") + scorerReadSettings())
	if len(rows) != 1 {
		t.Fatalf("pairs: %v", rows)
	}
	p := pairFromRow(rows[0], w.Start.Unix())
	if p.Count != 3*8640 {
		t.Errorf("count %d, want every connection", p.Count)
	}
	hours := map[int64]int{}
	for _, ts := range p.TsList {
		hours[ts/3600]++
	}
	if len(hours) != 72 {
		t.Errorf("points in %d hours, want all 72", len(hours))
	}
	for h, n := range hours {
		if n != netStateHourCap {
			t.Errorf("hour %v keeps %d points, want %d", time.Unix(h*3600, 0).UTC(), n, netStateHourCap)
		}
	}
	rs := ScoreBeacon(p, def.Beacon.WithDefaults(3*86400), 3*86400)
	if rs.TsScore < 0.99 || rs.HistScore < 0.99 || rs.DurScore < 0.99 {
		t.Errorf("a 10s beacon scored %+v", rs)
	}
}

// The gate counts the span the scorer scores: connections before the window
// start neither qualify a pair nor push it over the strobe limit, and a long
// connection before it adds no duration.
func TestCHNetGateMatchesTheWindow(t *testing.T) {
	d := newCHDB(t)
	w := netWindowAt(time.Now(), 1)
	before := w.Start.Add(-30 * time.Minute)
	d.connLogsAt("10.0.0.1", "198.51.100.1", before, time.Minute, 10, 1) // qualifies on old points only
	d.connLogsAt("10.0.0.1", "198.51.100.1", w.Start, time.Hour, 2, 1)
	d.connLogsAt("10.0.0.2", "198.51.100.2", before, time.Second, 600, 1) // a burst before the window
	d.connLogsAt("10.0.0.2", "198.51.100.2", w.Start, 10*time.Minute, 100, 1)
	d.connLogsAt("10.0.0.3", "198.51.100.3", before, time.Minute, 1, 7200) // long, but before
	d.connLogsAt("10.0.0.3", "198.51.100.3", w.Start, time.Minute, 1, 60)
	d.connLogsAt("10.0.0.4", "198.51.100.4", w.Start, time.Minute, 1, 7200)
	d.connLogsAt("10.0.0.5", "198.51.100.5", before, time.Minute, 1, 1) // seen only before

	def := ModelDefinition{Beacon: &BeaconParams{StrobeLimit: 500}}
	d.netState(def, 1)

	pairs := func(mt ModelType) map[string]PairAgg {
		out := map[string]PairAgg{}
		for _, r := range d.rows(BuildNetScoreReadQuery(def, mt, "s", "f1", w, "") + scorerReadSettings()) {
			p := pairFromRow(r, w.Start.Unix())
			out[p.Src] = p
		}
		return out
	}
	beacons := pairs(ModelTypeBeacon)
	if _, ok := beacons["10.0.0.1"]; ok {
		t.Error("a pair qualified on connections before the window")
	}
	if p, ok := beacons["10.0.0.2"]; !ok || p.Count != 100 || len(p.TsList) != 100 {
		t.Errorf("a pair under the strobe limit in the window: %+v (present %v)", p, ok)
	}
	long := pairs(ModelTypeLongConnection)
	if _, ok := long["10.0.0.3"]; ok {
		t.Error("a long connection before the window qualified")
	}
	if p, ok := long["10.0.0.4"]; !ok || p.TotalDuration != 7200 {
		t.Errorf("a long connection in the window: %+v (present %v)", p, ok)
	}

	if rows := d.rows(BuildNetNetworkSizeQuery("s", "f1", w)); getUint64(rows[0], "network_size") != 4 {
		t.Errorf("network size %v, want the 4 sources in the window", rows)
	}
	for _, r := range d.rows(BuildNetPrevalenceQuery("s", "f1", w)) {
		if getString(r, "dst") == "198.51.100.5" {
			t.Error("prevalence counted a destination seen only before the window")
		}
	}
}

// A preview longer than the longest window keeps each pair inside the point
// budget, counting the extra calendar day a range off midnight touches.
func TestCHBeaconPreviewStaysInBudget(t *testing.T) {
	d := newCHDB(t)
	d.connLogs("10.0.0.1", "198.51.100.1", 30*time.Second, 31*2880)
	end := time.Now().UTC()
	b := NetPreviewBounds{Start: end.Add(-30 * 24 * time.Hour), End: end, Days: 30}
	q, err := BuildNetPreviewAgg(ModelDefinition{}, ModelTypeBeacon, "logs", "f1", b, "")
	if err != nil {
		t.Fatal(err)
	}
	rows := d.rows(q)
	if len(rows) != 1 {
		t.Fatalf("pairs: %v", rows)
	}
	if n := len(getInt64Slice(rows[0], "ts_list")); n > netPairPointBudget || n < netPairPointBudget*9/10 {
		t.Errorf("%d points, want close to and within the %d budget", n, netPairPointBudget)
	}
	if !strings.Contains(q, "HAVING") {
		t.Error("preview is not gated")
	}
}

// fsRow is one first_seen state row of a windowed table, recorded live (fr set)
// or seeded (fr zero, the epoch).
type fsRow struct {
	key     string
	daysAgo int
	fr      time.Duration // how long ago the model recorded it; 0 seeds it
}

func (d *chDB) firstSeenRows(table string, rows ...fsRow) {
	d.t.Helper()
	vals := make([]string, len(rows))
	for i, r := range rows {
		fr := "toDateTime64(0, 3, 'UTC')"
		if r.fr > 0 {
			fr = fmt.Sprintf("now64(3, 'UTC') - toIntervalSecond(%d)", int(r.fr.Seconds()))
		}
		vals[i] = fmt.Sprintf("('%s', toDate(now(), 'UTC') - %d, %s)", r.key, r.daysAgo, fr)
	}
	d.exec(fmt.Sprintf(`INSERT INTO %s (fractal_id, entity_key, first_seen, last_seen, event_count, first_recorded, _day)
SELECT 'f1', k, toDateTime64(d, 3, 'UTC') + INTERVAL 1 HOUR, toDateTime64(d, 3, 'UTC') + INTERVAL 1 HOUR, 1, fr, d
FROM (SELECT tupleElement(r, 1) AS k, tupleElement(r, 2) AS d, tupleElement(r, 3) AS fr FROM (SELECT arrayJoin([%s]) AS r))`,
		table, strings.Join(vals, ", ")))
}

// A windowed first_seen entity is new only on a return after at least N days
// away, and stays so however its days arrive.
func TestCHFirstSeenHistoryReturns(t *testing.T) {
	d := newCHDB(t)
	def := ModelDefinition{KeyFields: []string{"k"}, HistoryDays: 7}
	ddl, err := GenerateDDL(def, ModelTypeFirstSeen, "fs")
	if err != nil {
		t.Fatal(err)
	}
	d.exec(ddl)
	day := 24 * time.Hour
	var steady []fsRow
	for i := 0; i < 14; i++ {
		steady = append(steady, fsRow{"steady", i, time.Duration(i)*day + time.Hour})
	}
	d.firstSeenRows("fs", steady...)
	d.firstSeenRows("fs",
		fsRow{"returned", 10, 10 * day}, fsRow{"returned", 0, 10 * time.Minute},
		fsRow{"brandnew", 0, 10 * time.Minute},
		fsRow{"oldonly", 12, 12 * day},
		// Seen on the window's first day, then late yesterday: that day leaving
		// the window must not make it new.
		fsRow{"midnight", 6, 6 * day}, fsRow{"midnight", 1, 10 * time.Minute},
		// Away exactly N days is a return; one day less is not.
		fsRow{"away7", 8, 8 * day}, fsRow{"away7", 0, 10 * time.Minute},
		fsRow{"away6", 7, 7 * day}, fsRow{"away6", 0, 10 * time.Minute},
		// Returned two days ago, then a late log for the day before arrives now:
		// still the same return, recorded two days ago.
		fsRow{"late", 13, 13 * day}, fsRow{"late", 2, 2 * day}, fsRow{"late", 3, time.Minute},
		// Seeded history, and a duplicate part for the same day.
		fsRow{"seeded", 0, 0}, fsRow{"seeded", 0, 5 * time.Minute},
	)

	type want struct {
		present bool
		isNew   bool // recorded within the last hour
	}
	wants := map[string]want{
		"steady": {true, false}, "returned": {true, true}, "brandnew": {true, true},
		"oldonly": {false, false}, "midnight": {true, false},
		"away7": {true, true}, "away6": {true, false},
		"late": {true, false}, "seeded": {true, false},
	}
	for _, final := range []string{" FINAL", ""} {
		agg := firstSeenAgg("`fs`"+final, fidScope("f1"), "entity_key", def.HistoryDays, true, false)
		rows := d.rows(`SELECT entity_key, toUInt8(recorded_at >= now() - INTERVAL 1 HOUR) AS is_new FROM (` + agg + `)`)
		got := map[string]bool{}
		for _, r := range rows {
			got[getString(r, "entity_key")] = fmt.Sprint(r["is_new"]) == "1"
		}
		for k, w := range wants {
			isNew, present := got[k]
			if present != w.present || isNew != w.isNew {
				t.Errorf("%q (FINAL %v): present %v new %v, want present %v new %v", k, final != "", present, isNew, w.present, w.isNew)
			}
		}
	}
}

// A windowed rarity pair is scored over the window, and its since is the return
// day, the epoch for one that never left.
func TestCHRarityHistoryWindow(t *testing.T) {
	d := newCHDB(t)
	def := ModelDefinition{PartitionKey: "h", ValueKey: "v", HistoryDays: 7}
	ddl, _ := GenerateDDL(def, ModelTypeRarity, "r")
	d.exec(ddl, `INSERT INTO r (fractal_id, partition_val, value_val, event_count, _day)
SELECT 'f1', 'h', v, 1, toDate(now(), 'UTC') - n FROM (
    SELECT 'steady' AS v, number AS n FROM numbers(14)
    UNION ALL SELECT 'returned', 10 UNION ALL SELECT 'returned', 0
    UNION ALL SELECT 'oldonly', 12)`)
	rows := d.rows("SELECT value_val, model_count, model_total, toString(since) AS since FROM (" + buildRarityScoredSQL("`r` FINAL", "f1", 7) + ")")
	got := map[string]map[string]interface{}{}
	for _, r := range rows {
		got[getString(r, "value_val")] = r
	}
	today := time.Now().UTC().Format("2006-01-02")
	if r := got["steady"]; r == nil || getUint64(r, "model_count") != 7 || getUint64(r, "model_total") != 7 || getString(r, "since") != "1970-01-01" {
		t.Errorf("steady: %v", r)
	}
	if r := got["returned"]; r == nil || getUint64(r, "model_count") != 1 || getString(r, "since") != today {
		t.Errorf("returned: %v", r)
	}
	if _, ok := got["oldonly"]; ok {
		t.Error("a pair seen only before the window was scored")
	}
}

// A table left in another layout, or with another window's TTL, is recreated;
// a missing one is not mistaken for a wrong one.
func TestCHEnsureStateLayout(t *testing.T) {
	d := newCHDB(t)
	m := &Manager{ch: d.c}
	ctx := context.Background()
	engine := func(table string) string {
		rows := d.rows("SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = '" + table + "'")
		if len(rows) == 0 {
			return ""
		}
		return getString(rows[0], "engine_full")
	}
	check := func(name string, have, want ModelDefinition) {
		t.Helper()
		if have.KeyFields != nil {
			old, _ := GenerateDDL(have, ModelTypeFirstSeen, name)
			d.exec(old)
		}
		ddl, _ := GenerateDDL(want, ModelTypeFirstSeen, name)
		if err := m.ensureStateLayout(ctx, "id-"+name, name, "mv-"+name, want, ModelTypeFirstSeen, ddl); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}

	check("t_none", ModelDefinition{}, ModelDefinition{KeyFields: []string{"k"}, HistoryDays: 7})
	if engine("t_none") != "" {
		t.Error("a missing table was created by the layout check")
	}

	check("t_add", ModelDefinition{KeyFields: []string{"k"}}, ModelDefinition{KeyFields: []string{"k"}, HistoryDays: 7})
	if e := engine("t_add"); !strings.Contains(e, "TTL _day + toIntervalDay(15)") {
		t.Errorf("unwindowed table kept: %s", e)
	}

	check("t_ttl", ModelDefinition{KeyFields: []string{"k"}, HistoryDays: 30}, ModelDefinition{KeyFields: []string{"k"}, HistoryDays: 90})
	if e := engine("t_ttl"); !strings.Contains(e, "toIntervalDay(181)") {
		t.Errorf("old window's TTL kept: %s", e)
	}

	check("t_drop", ModelDefinition{KeyFields: []string{"k"}, HistoryDays: 7}, ModelDefinition{KeyFields: []string{"k"}})
	if e := engine("t_drop"); strings.Contains(e, "_day") {
		t.Errorf("windowed table kept for an unwindowed model: %s", e)
	}
}

// On a cluster the layout check reads every replica, and a Distributed companion
// left without the day column is a mismatch too.
func TestCHEnsureStateLayoutChecksTheDistributedTable(t *testing.T) {
	d := newCHDB(t)
	topo := storage.Topology{DistributedTables: true, FanoutCluster: "default"}
	m := &Manager{ch: storage.NewClickHouseClientFromConn(d.c.Conn(), d.db, topo)}
	ctx := context.Background()
	want := ModelDefinition{KeyFields: []string{"k"}, HistoryDays: 7}
	ddl, _ := GenerateDDL(want, ModelTypeFirstSeen, "t_local")
	old, _ := GenerateDDL(ModelDefinition{KeyFields: []string{"k"}}, ModelTypeFirstSeen, "t_old")
	dist := chModelDistName("m1")
	d.exec(ddl, old, fmt.Sprintf("CREATE TABLE `%s` AS t_old ENGINE = Distributed('default', currentDatabase(), t_local)", dist))

	if ok, err := m.stateLayoutOK(ctx, "m1", "t_local", want, ModelTypeFirstSeen); err != nil || ok {
		t.Fatalf("an old Distributed companion passed: %v %v", ok, err)
	}
	if err := m.ensureStateLayout(ctx, "m1", "t_local", "mv", want, ModelTypeFirstSeen, ddl); err != nil {
		t.Fatal(err)
	}
	if rows := d.rows("SELECT name FROM system.tables WHERE database = currentDatabase() AND name = '" + dist + "'"); len(rows) != 0 {
		t.Error("the old Distributed companion was kept")
	}
	if ok, err := m.stateLayoutOK(ctx, "m1", "t_local", want, ModelTypeFirstSeen); err != nil || !ok {
		t.Errorf("recreated layout: %v %v", ok, err)
	}
}

// A network state table left in an older layout is recreated in the current
// one, and one already current keeps its rows.
func TestCHEnsureNetStateLayout(t *testing.T) {
	d := newCHDB(t)
	m := &Manager{ch: d.c}
	ctx := context.Background()
	state := chModelStateName("m1")
	d.exec(fmt.Sprintf("CREATE TABLE `%s` (fractal_id String, day Date, ts_state AggregateFunction(groupArray(2000), UInt32)) ENGINE = AggregatingMergeTree ORDER BY (fractal_id, day)", state))
	if ok, err := m.netStateLayoutOK(ctx, "m1"); err != nil || ok {
		t.Fatalf("an old layout passed: %v %v", ok, err)
	}
	if err := m.ensureNetStateLayout(ctx, "m1", 7); err != nil {
		t.Fatal(err)
	}
	if ok, err := m.netStateLayoutOK(ctx, "m1"); err != nil || !ok {
		t.Fatalf("recreated layout: %v %v", ok, err)
	}
	d.exec(fmt.Sprintf("INSERT INTO `%s` (fractal_id, src, dst, port, hour, conn_count) VALUES ('f1', 'a', 'b', '443', now(), 1)", state))
	if err := m.ensureNetStateLayout(ctx, "m1", 7); err != nil {
		t.Fatal(err)
	}
	if rows := d.rows(fmt.Sprintf("SELECT count() AS n FROM `%s`", state)); getUint64(rows[0], "n") != 1 {
		t.Error("a current state table was recreated")
	}
}

// On a cluster a Distributed state companion in the old layout is a mismatch
// too, and is dropped so it can be recreated over the current table.
func TestCHEnsureNetStateLayoutChecksTheDistributedTable(t *testing.T) {
	d := newCHDB(t)
	topo := storage.Topology{DistributedTables: true, FanoutCluster: "default"}
	m := &Manager{ch: storage.NewClickHouseClientFromConn(d.c.Conn(), d.db, topo)}
	ctx := context.Background()
	state, dist := chModelStateName("m1"), chModelStateDistName("m1")
	d.exec(BuildNetStateTableDDL("`"+state+"`", 7),
		"CREATE TABLE t_old (fractal_id String, day Date) ENGINE = MergeTree ORDER BY day",
		fmt.Sprintf("CREATE TABLE `%s` AS t_old ENGINE = Distributed('default', currentDatabase(), t_old)", dist))
	if ok, err := m.netStateLayoutOK(ctx, "m1"); err != nil || ok {
		t.Fatalf("an old Distributed companion passed: %v %v", ok, err)
	}
	if err := m.ensureNetStateLayout(ctx, "m1", 7); err != nil {
		t.Fatal(err)
	}
	if rows := d.rows("SELECT name FROM system.tables WHERE database = currentDatabase() AND name = '" + dist + "'"); len(rows) != 0 {
		t.Error("the old Distributed companion was kept")
	}
}

// Every state insert path writes into the table its definition creates.
func TestCHStateInsertsMatchTheirTables(t *testing.T) {
	d := newCHDB(t)
	d.exec(`INSERT INTO logs SELECT 'f1', now64(3, 'UTC') - toIntervalDay(number), now64(3, 'UTC'),
    '{"host":"h1","port":"443","image":"c:/a/svchost.exe","tlsh":"T1` + strings.Repeat("A", 70) + `"}' FROM numbers(5)`)
	ext := []ExtractionStep{{FromField: "image", Pattern: `([a-z]+)\.exe`, OutputField: "ext"}}
	for i, c := range []struct {
		mt  ModelType
		def ModelDefinition
	}{
		{ModelTypeRarity, ModelDefinition{PartitionKey: "host", ValueKey: "port"}},
		{ModelTypeRarity, ModelDefinition{PartitionKey: "host", ValueKey: "port", HistoryDays: 30}},
		{ModelTypeRarity, ModelDefinition{PartitionKey: "host", ValueKey: "ext", HistoryDays: 30, Extractions: ext}},
		{ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"host", "image"}}},
		{ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"host", "image"}, HistoryDays: 30}},
		{ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"host", "ext"}, HistoryDays: 30, Extractions: ext}},
		{ModelTypeTLSH, ModelDefinition{KeyFields: []string{"tlsh"}}},
		{ModelTypeVolumeBaseline, ModelDefinition{KeyFields: []string{"host"}}},
	} {
		table := fmt.Sprintf("t%d", i)
		ddl, err := GenerateDDL(c.def, c.mt, table)
		if err != nil {
			t.Fatal(err)
		}
		ins, err := BuildBackfillInsert(c.def, c.mt, table, "logs", "", "f1")
		if err != nil {
			t.Fatal(err)
		}
		d.exec(ddl, ins)
		if rows := d.rows("SELECT count() AS n FROM " + table); getUint64(rows[0], "n") == 0 {
			t.Errorf("%s %+v wrote nothing", c.mt, c.def)
		}
	}
}

// The threat intel column marks exactly the pairs whose destination the lookup
// holds, and the first-seen read returns each candidate's first sighting.
func TestCHBeaconModifierReads(t *testing.T) {
	d := newCHDB(t)
	d.connLogs("10.0.0.1", "203.0.113.10", time.Minute, 600)
	d.connLogs("10.0.0.2", "198.51.100.20", time.Minute, 600)
	def := ModelDefinition{}
	d.netState(def, 1)
	w := netWindowAt(time.Now(), 1)
	d.exec("CREATE TABLE iocs (ip String) ENGINE = MergeTree ORDER BY ip",
		"INSERT INTO iocs VALUES ('203.0.113.10')",
		fmt.Sprintf(`CREATE DICTIONARY d_ioc (ip String) PRIMARY KEY ip
SOURCE(CLICKHOUSE(TABLE 'iocs' DB '%s' USER 'default' PASSWORD '%s')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0)`, d.db, d.pass))

	hit, err := parserLookup(d.db)
	if err != nil {
		t.Fatal(err)
	}
	rows := d.rows(BuildNetScoreReadQuery(def, ModelTypeBeacon, "s", "f1", w, hit) + scorerReadSettings())
	for _, r := range rows {
		p := pairFromRow(r, 0)
		if want := p.Dst == "203.0.113.10"; p.ThreatIntelHit != want {
			t.Errorf("%s: threat intel %v, want %v", p.Dst, p.ThreatIntelHit, want)
		}
	}
	if len(rows) != 2 {
		t.Fatalf("pairs: %v", rows)
	}

	fsDef := ModelDefinition{KeyFields: []string{"dst_ip"}}
	fsDDL, _ := GenerateDDL(fsDef, ModelTypeFirstSeen, "fs")
	fsIns, _ := BuildBackfillInsert(fsDef, ModelTypeFirstSeen, "fs", "logs", "", "f1")
	d.exec(fsDDL, fsIns)
	fs := d.rows(firstSeenContextSQL("fs", "f1", false, BuildNetPairsQuery(def, ModelTypeBeacon, "s", "f1", w)))
	if len(fs) != 2 {
		t.Fatalf("first sightings: %v", fs)
	}
	for _, r := range fs {
		if age := time.Since(time.Unix(int64(getUint64(r, "fs")), 0)); age < 9*time.Hour || age > 11*time.Hour {
			t.Errorf("%s first seen %v ago, want about 10h", getString(r, "k"), age)
		}
	}
}

func parserLookup(db string) (string, error) {
	return parser.LookupMembership("dst", "iocs", "ip", parser.QueryOptions{
		DictionaryDatabase: db,
		Dictionaries:       map[string]map[string]string{"iocs": {"ip": "d_ioc"}},
	})
}
