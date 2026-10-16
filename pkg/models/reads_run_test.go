package models

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
)

// A built model's data, stats and histogram readers through the fake.

func clusterModel(env *fakeEnv, mt ModelType, def ModelDefinition) *Model {
	r := seedModel(env, mt, def, nil)
	mo, err := env.mgr.Get(context.Background(), r.id)
	if err != nil {
		panic(err)
	}
	return mo
}

// A page is a count and a slice of the same rows: on a cluster both read the
// Distributed companion, the findings view narrows both with the alert's rule,
// a search is escaped, and paging is clamped.
func TestGetDataRarityFindingsPage(t *testing.T) {
	env := newClusterEnv(t)
	mo := clusterModel(env, ModelTypeRarity, rarityDef)
	env.ch.On("SELECT count() FROM (", fakedb.Count(3))
	env.ch.On(" ORDER BY ", fakedb.CHResult{Cols: cols("partition_val", "String", "value_val", "String", "coverage", "Float64"),
		Rows: [][]any{{"web01", "nc.exe", 0.95}}})

	rows, total, err := env.mgr.GetData(context.Background(), mo, fracA,
		DataQuery{View: DataViewFindings, Search: "x' OR 1=1 --", Sort: "nope", Limit: 9999, Offset: -4})
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 || len(rows) != 1 || rows[0]["value_val"] != "nc.exe" {
		t.Fatalf("total %d rows %v", total, rows)
	}
	qs := env.ch.Queried()
	if len(qs) != 2 {
		t.Fatalf("queries: %v", qs)
	}
	dist := "`" + chModelDistName(mo.ID) + "` FINAL"
	for _, q := range qs {
		if !strings.Contains(q, dist) || !strings.Contains(q, "fractal_id = '"+fracA+"'") {
			t.Errorf("not the fractal's distributed read: %s", q)
		}
		if !strings.Contains(q, `ILIKE '%x\' OR 1=1 --%'`) {
			t.Errorf("search not escaped: %s", q)
		}
		if !strings.Contains(q, "WHERE model_total >= 1 AND coverage > 0.9") {
			t.Errorf("findings rule missing: %s", q)
		}
	}
	if !strings.HasSuffix(qs[1], "ORDER BY percent ASC, coverage DESC, partition_val, value_val LIMIT 500 OFFSET 0") {
		t.Errorf("page: %s", qs[1][max(0, len(qs[1])-120):])
	}
}

// first_seen days arrive as dates and leave as strings; the default sort is the
// newest first sighting.
func TestGetDataFirstSeen(t *testing.T) {
	env := newEnv(t)
	mo := clusterModel(env, ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"user"}})
	day := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	env.ch.On("SELECT count() FROM (", fakedb.Count(1))
	env.ch.On(" ORDER BY ", fakedb.CHResult{Cols: cols("entity_key", "String", "days", "Array(Date)"),
		Rows: [][]any{{"alice", []time.Time{day}}}})
	rows, _, err := env.mgr.GetData(context.Background(), mo, fracA, DataQuery{Order: "asc", Sort: "event_count"})
	if err != nil {
		t.Fatal(err)
	}
	if d, ok := rows[0]["days"].([]string); !ok || len(d) != 1 || d[0] != "2026-10-03" {
		t.Errorf("days: %#v", rows[0]["days"])
	}
	if q := env.ch.QueriedMatching(" ORDER BY "); len(q) != 1 || !strings.Contains(q[0], "ORDER BY event_count ASC LIMIT 50 OFFSET 0") ||
		!strings.Contains(q[0], "min(first_recorded) AS recorded_at") {
		t.Errorf("page: %v", q)
	}
}

func TestGetDataOtherTypes(t *testing.T) {
	cases := []struct {
		mt   ModelType
		def  ModelDefinition
		want []string
	}{
		{ModelTypeTLSH, ModelDefinition{KeyFields: []string{"tlsh"}}, []string{"GROUP BY digest", "ORDER BY first_seen DESC"}},
		{ModelTypeVolumeBaseline, ModelDefinition{KeyFields: []string{"host"}}, []string{"ORDER BY abs(z_score) DESC", "entity_val ILIKE '%db%'"}},
		{ModelTypeBeacon, ModelDefinition{}, []string{"round(final_score,3) AS final_score", "src_ip ILIKE '%db%' OR dst_ip ILIKE '%db%'", "ORDER BY final_score DESC"}},
	}
	for _, c := range cases {
		env := newEnv(t)
		mo := clusterModel(env, c.mt, c.def)
		env.ch.On("SELECT count() FROM (", fakedb.Count(0))
		if _, _, err := env.mgr.GetData(context.Background(), mo, fracA, DataQuery{Search: "db"}); err != nil {
			t.Fatalf("%s: %v", c.mt, err)
		}
		page := env.ch.QueriedMatching(" LIMIT 50 OFFSET 0")
		if len(page) != 1 {
			t.Fatalf("%s: %v", c.mt, env.ch.Queried())
		}
		for _, w := range c.want {
			if !strings.Contains(page[0], w) {
				t.Errorf("%s page lacks %q: %s", c.mt, w, page[0])
			}
		}
		if c.mt == ModelTypeTLSH && strings.Contains(page[0], "recorded_at") {
			t.Error("a tlsh table has no first_recorded column")
		}
	}
}

func TestGetDataErrors(t *testing.T) {
	env := newEnv(t)
	mo := clusterModel(env, ModelTypeRarity, rarityDef)
	env.ch.On("SELECT count() FROM (", fakedb.CHResult{Err: errors.New("unknown table")})
	if _, _, err := env.mgr.GetData(context.Background(), mo, fracA, DataQuery{}); err == nil || !strings.Contains(err.Error(), "count rarity data") {
		t.Errorf("count: %v", err)
	}
	env.ch.ClearRules()
	env.ch.On("SELECT count() FROM (", fakedb.Count(1))
	env.ch.On(" ORDER BY ", fakedb.CHResult{Err: errors.New("timeout")})
	if _, _, err := env.mgr.GetData(context.Background(), mo, fracA, DataQuery{}); err == nil || !strings.Contains(err.Error(), "query rarity data") {
		t.Errorf("page: %v", err)
	}
}

func dayStr(daysAgo int) string {
	return time.Now().UTC().AddDate(0, 0, -daysAgo).Format("2006-01-02")
}

// Rarity stats: findings are the pairs the alert would raise, and the series
// counts pairs by the first day each was seen, zero-filled to 30 days ending today.
func TestGetStatsRarity(t *testing.T) {
	env := newEnv(t)
	mo := clusterModel(env, ModelTypeRarity, rarityDef)
	env.ch.On("AS total_rows", fakedb.CHResult{Cols: cols("total_rows", "UInt64", "distinct_partitions", "UInt64"), Rows: [][]any{{uint64(40), uint64(4)}}})
	env.ch.On("LIMIT 5", fakedb.CHResult{Cols: cols("partition_val", "String", "cnt", "UInt64"), Rows: [][]any{{"web01", uint64(9)}}})
	env.ch.On("AS first_day", fakedb.CHResult{Cols: cols("first_day", "String", "n", "UInt64", "flagged", "UInt64"), Rows: [][]any{
		{dayStr(0), uint64(2), uint64(1)},
		{dayStr(3), uint64(5), uint64(2)},
		{dayStr(10), uint64(7), uint64(0)},
		{dayStr(90), uint64(26), uint64(1)}, // outside the series, still a finding
	}})
	st, err := env.mgr.GetStats(context.Background(), mo, fracA)
	if err != nil {
		t.Fatal(err)
	}
	if st["total_rows"] != uint64(40) || st["findings"] != uint64(4) || st["new_week"] != uint64(7) || st["findings_rule"] != "coverage > 0.9" {
		t.Errorf("stats: %v", st)
	}
	series := st["series"].([]dayCount)
	if len(series) != discoveryDays || series[len(series)-1].Count != 2 || series[len(series)-11].Count != 7 {
		t.Errorf("series: %v", series)
	}
	if q := env.ch.QueriedMatching("AS first_day"); len(q) != 1 || !strings.Contains(q[0], "countIf(model_total >= 1 AND coverage > 0.9)") {
		t.Errorf("discovery: %v", q)
	}
}

// first_seen findings are what the model first recorded this week; seeded history
// (recorded at the epoch) never counts.
func TestGetStatsFirstSeen(t *testing.T) {
	env := newEnv(t)
	mo := clusterModel(env, ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"user"}})
	env.ch.On("AS total_entities", fakedb.CHResult{Cols: cols("total_entities", "UInt64", "new_today", "UInt64"), Rows: [][]any{{uint64(12), uint64(1)}}})
	env.ch.On("AS week", fakedb.CHResult{Cols: cols("day", "String", "n", "UInt64", "week", "UInt64", "hour", "UInt64"), Rows: [][]any{
		{dayStr(0), uint64(3), uint64(3), uint64(1)},
		{dayStr(2), uint64(4), uint64(4), uint64(0)},
	}})
	st, err := env.mgr.GetStats(context.Background(), mo, fracA)
	if err != nil {
		t.Fatal(err)
	}
	if st["total_entities"] != uint64(12) || st["findings"] != uint64(7) || st["new_hour"] != uint64(1) || st["new_week"] != uint64(7) {
		t.Errorf("stats: %v", st)
	}
	if q := env.ch.QueriedMatching("AS week"); len(q) != 1 || !strings.Contains(q[0], "HAVING recorded_at >= toDateTime64('"+discoveryWindow(time.Now()).Format("2006-01-02")) {
		t.Errorf("discovery window: %v", q)
	}
}

func TestGetStatsOtherTypes(t *testing.T) {
	env := newEnv(t)
	ctx := context.Background()

	vol := clusterModel(env, ModelTypeVolumeBaseline, ModelDefinition{KeyFields: []string{"host"}, Alert: &AlertConfig{ZThreshold: 5}})
	env.ch.On("AS anomalous", fakedb.CHResult{Cols: cols("total_entities", "UInt64", "anomalous", "UInt64", "findings", "UInt64", "max_z", "Float64", "latest_bucket", "String"),
		Rows: [][]any{{uint64(10), uint64(2), uint64(1), 7.5, "2026-10-14"}}})
	st, err := env.mgr.GetStats(ctx, vol, fracA)
	if err != nil || st["findings"] != uint64(1) || st["max_z"] != 7.5 || st["findings_rule"] != "z-score > 5" {
		t.Errorf("volume: %v %v", st, err)
	}
	if len(env.ch.QueriedMatching("countIf(abs(z_score) > 5)")) != 1 {
		t.Error("volume anomalies not counted at the model's threshold")
	}

	net := clusterModel(env, ModelTypeLongConnection, ModelDefinition{})
	env.ch.On("AS total_pairs", fakedb.CHResult{Cols: cols("total_pairs", "UInt64", "flagged", "UInt64", "critical", "UInt64", "max_score", "Float64", "scored_at", "String"),
		Rows: [][]any{{uint64(9), uint64(3), uint64(1), 0.91, "2026-10-14 00:00:00"}}})
	st, err = env.mgr.GetStats(ctx, net, fracA)
	if err != nil || st["findings"] != uint64(3) || st["critical"] != uint64(1) || st["findings_rule"] != "score > 0.5" {
		t.Errorf("network: %v %v", st, err)
	}

	idx := clusterModel(env, ModelTypeTLSH, ModelDefinition{KeyFields: []string{"tlsh"}})
	env.ch.On("AS total_entities", fakedb.CHResult{Cols: cols("total_entities", "UInt64"), Rows: [][]any{{uint64(12)}}})
	st, err = env.mgr.GetStats(ctx, idx, fracA)
	if err != nil || st["total_entities"] != uint64(12) {
		t.Errorf("tlsh: %v %v", st, err)
	}
	if _, ok := st["findings"]; ok {
		t.Error("an index model reports findings it cannot have")
	}
}

func TestGetStatsErrors(t *testing.T) {
	for _, c := range []struct {
		mt    ModelType
		def   ModelDefinition
		match string
	}{
		{ModelTypeRarity, rarityDef, "AS total_rows"},
		{ModelTypeRarity, rarityDef, "AS first_day"},
		{ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"u"}}, "AS total_entities"},
		{ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"u"}}, "AS week"},
		{ModelTypeVolumeBaseline, ModelDefinition{KeyFields: []string{"u"}}, "AS anomalous"},
		{ModelTypeBeacon, ModelDefinition{}, "AS total_pairs"},
	} {
		env := newEnv(t)
		mo := clusterModel(env, c.mt, c.def)
		env.ch.On(c.match, fakedb.CHResult{Err: errors.New("boom")})
		if _, err := env.mgr.GetStats(context.Background(), mo, fracA); err == nil {
			t.Errorf("%s %s: error swallowed", c.mt, c.match)
		}
	}
}

func TestGetHistogram(t *testing.T) {
	env := newEnv(t)
	ctx := context.Background()

	rar := clusterModel(env, ModelTypeRarity, rarityDef)
	env.ch.On("AS flagged FROM (", fakedb.CHResult{Cols: cols("total", "UInt64", "flagged", "UInt64"), Rows: [][]any{{uint64(50), uint64(4)}}})
	h, err := env.mgr.GetHistogram(ctx, rar, fracA)
	if err != nil || h["flagged"] != uint64(4) || h["total"] != uint64(50) || h["criterion"] != "coverage > 0.9" {
		t.Errorf("rarity: %v %v", h, err)
	}

	fs := clusterModel(env, ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"user"}})
	env.ch.On("AS cnt\nFROM (", fakedb.CHResult{Cols: cols("day", "String", "cnt", "UInt64"), Rows: [][]any{{"2026-10-13", uint64(2)}}})
	h, err = env.mgr.GetHistogram(ctx, fs, fracA)
	if s, _ := h["series"].([]map[string]interface{}); err != nil || len(s) != 1 || s[0]["label"] != "2026-10-13" || s[0]["count"] != uint64(2) {
		t.Errorf("first_seen: %v %v", h, err)
	}

	env.ch.On("AS bucket, count() AS cnt", fakedb.CHResult{Cols: cols("bucket", "UInt64", "cnt", "UInt64"), Rows: [][]any{{uint64(5), uint64(3)}, {uint64(9), uint64(1)}}})
	vol := clusterModel(env, ModelTypeVolumeBaseline, ModelDefinition{KeyFields: []string{"h"}})
	h, err = env.mgr.GetHistogram(ctx, vol, fracA)
	if b, _ := h["buckets"].([]histBucket); err != nil || len(b) != len(volumeHistLabels) || b[5].Count != 3 {
		t.Errorf("volume: %v %v", h, err)
	}
	net := clusterModel(env, ModelTypeBeacon, ModelDefinition{})
	h, err = env.mgr.GetHistogram(ctx, net, fracA)
	if b, _ := h["buckets"].([]histBucket); err != nil || len(b) != 10 || b[5].Count != 3 || b[9].Count != 1 || h["metric"] != "final_score" {
		t.Errorf("network: %v %v", h, err)
	}

	env.ch.ClearRules()
	env.ch.On("SELECT", fakedb.CHResult{Err: errors.New("boom")})
	for _, mo := range []*Model{rar, fs, vol, net} {
		if _, err := env.mgr.GetHistogram(ctx, mo, fracA); err == nil {
			t.Errorf("%s: error swallowed", mo.ModelType)
		}
	}
}

func TestTestExtractionRunsTheProbe(t *testing.T) {
	env := newEnv(t)
	env.ch.On("LIMIT 50", fakedb.CHResult{Cols: cols("domain", "String", "cnt", "UInt64"), Rows: [][]any{{"example.org", uint64(3)}}})
	rows, sql, err := env.mgr.TestExtraction(context.Background(), fracA, nil,
		[]ExtractionStep{{FromField: "url", OutputField: "domain", Pattern: `https?://([^/]+)`}})
	if err != nil || len(rows) != 1 || rows[0]["domain"] != "example.org" || !strings.Contains(sql, "FROM logs") {
		t.Errorf("%v %v %s", rows, err, sql)
	}
}

// The listing's health comes from one bounded read of each model's state.
func TestReadStateSummary(t *testing.T) {
	env := newClusterEnv(t)
	mo := clusterModel(env, ModelTypeLongConnection, ModelDefinition{})
	newest := uint64(time.Now().Add(-time.Hour).Unix())
	env.ch.On("AS newest", fakedb.CHResult{Cols: cols("oldest", "UInt64", "newest", "UInt64", "flagged", "UInt64"),
		Rows: [][]any{{newest - 86400, newest, uint64(2)}}})
	sum, err := env.mgr.readStateSummary(context.Background(), mo, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if sum.newest.Unix() != int64(newest) || sum.findings == nil || *sum.findings != 2 {
		t.Errorf("summary: %+v", sum)
	}
	if q := env.ch.Queried(); len(q) != 1 || !strings.Contains(q[0], chModelStateDistName(mo.ID)) {
		t.Errorf("a network model's summary reads its distributed state: %v", q)
	}
	if _, err := env.mgr.readStateSummary(context.Background(), &Model{ID: "x", ModelType: "nope"}, time.Now()); err == nil {
		t.Error("unknown type summarized")
	}
}
