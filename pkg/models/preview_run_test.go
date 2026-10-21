package models

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
)

// The pre-save preview through the fake: the shape, metrics and top-rows queries
// it runs over the raw-log window, and how their answers become the estimate.

const previewSettings = " SETTINGS max_execution_time=25, max_threads=4, max_bytes_before_external_group_by=2000000000"

func lastDay() PreviewRange {
	end := time.Now().UTC()
	return PreviewRange{Start: end.Add(-24 * time.Hour), End: end, Label: "1d"}
}

// Every preview query is bounded by the preview settings and reads only the
// model's own fractal within the window.
func assertPreviewQueries(t *testing.T, ch *chFake, rng PreviewRange, n int) {
	t.Helper()
	qs := ch.Queried()
	if len(qs) != n {
		t.Fatalf("%d queries, want %d:\n%s", len(qs), n, strings.Join(qs, "\n---\n"))
	}
	window := rng.Start.Format("2006-01-02 15:04:05")
	for _, q := range qs {
		if !strings.HasSuffix(q, previewSettings) && !strings.HasSuffix(q, previewSettings+", max_block_size=256") {
			t.Errorf("unbounded preview query: ...%s", q[max(0, len(q)-120):])
		}
		if !strings.Contains(q, fracA) || !strings.Contains(q, window) {
			t.Errorf("query not scoped to the fractal and window: %s", q)
		}
	}
}

func TestPreviewRarity(t *testing.T) {
	env := newEnv(t)
	rng := lastDay()
	env.ch.On("AS n\nFROM (", fakedb.CHResult{Cols: cols("c", "Int64", "p", "Int64", "h", "Int64", "s", "Int64", "n", "Int64"), Rows: [][]any{
		{int64(95), int64(3), int64(9), int64(1), int64(4)},
		{int64(95), int64(3), int64(9), int64(0), int64(2)}, // group below min history: charted, not a cell
		{int64(50), int64(50), int64(5), int64(1), int64(10)},
	}})
	env.ch.On("AS would_flag", fakedb.CHResult{
		Cols: cols("scored_values", "UInt64", "partitions", "UInt64", "avg_coverage", "Float64", "max_coverage", "Float64", "would_flag", "UInt64"),
		Rows: [][]any{{uint64(16), uint64(3), 0.7, 0.95, uint64(4)}},
	})
	env.ch.On("\nLIMIT 25", fakedb.CHResult{
		Cols: cols("partition_val", "String", "value_val", "String", "model_count", "UInt64", "percent", "Float64", "coverage", "Float64"),
		Rows: [][]any{{"web01", "nc.exe", uint64(1), 3.3, 0.95}},
	})

	res, err := env.mgr.Preview(context.Background(), fracA, ModelTypeRarity, rarityDef, rng)
	if err != nil {
		t.Fatal(err)
	}
	assertPreviewQueries(t, env.ch, rng, 3)
	if res.Histogram[9].Count != 6 || res.Histogram[5].Count != 10 {
		t.Errorf("histogram: %+v", res.Histogram)
	}
	if want := [][]int64{{95, 3, 4}, {50, 50, 10}}; !reflect.DeepEqual(res.Distribution.Cells, want) {
		t.Errorf("cells %v, want %v", res.Distribution.Cells, want)
	}
	if res.Distribution.Truncated || res.WouldFlag != 4 || res.FlagBasis != "coverage > 0.9" || res.Metric != "coverage" {
		t.Errorf("result: %+v", res)
	}
	if res.Stats["scored_values"] != uint64(16) || res.Stats["max_coverage"] != 0.95 || len(res.Top) != 1 {
		t.Errorf("stats %v top %v", res.Stats, res.Top)
	}
	if len(env.ch.Executed()) != 0 {
		t.Error("a preview created something")
	}
}

// Past the cell cap the distribution says it is incomplete, so the editor re-runs
// the preview instead of recounting from a partial grid.
func TestPreviewDistributionTruncation(t *testing.T) {
	env := newEnv(t)
	rows := make([][]any, previewDistCellCap+1)
	for i := range rows {
		rows[i] = []any{int64(i % 100), int64(i / 100), int64(1), int64(1), int64(1)}
	}
	env.ch.On("AS n\nFROM (", fakedb.CHResult{Cols: cols("c", "Int64", "p", "Int64", "h", "Int64", "s", "Int64", "n", "Int64"), Rows: rows})
	res, err := env.mgr.Preview(context.Background(), fracA, ModelTypeRarity, rarityDef, lastDay())
	if err != nil {
		t.Fatal(err)
	}
	if !res.Distribution.Truncated {
		t.Error("truncation not reported")
	}
}

func TestPreviewFirstSeen(t *testing.T) {
	for _, alertOnNew := range []bool{false, true} {
		env := newEnv(t)
		rng := lastDay()
		env.ch.On("AS bucket, count() AS cnt", fakedb.CHResult{Cols: cols("bucket", "UInt64", "cnt", "UInt64"),
			Rows: [][]any{{uint64(0), uint64(7)}, {uint64(3), uint64(2)}}})
		env.ch.On("AS new_recent", fakedb.CHResult{Cols: cols("entities", "UInt64", "new_recent", "UInt64"),
			Rows: [][]any{{uint64(9), uint64(2)}}})
		def := ModelDefinition{KeyFields: []string{"user"}, Alert: &AlertConfig{AlertOnNew: alertOnNew}}

		res, err := env.mgr.Preview(context.Background(), fracA, ModelTypeFirstSeen, def, rng)
		if err != nil {
			t.Fatal(err)
		}
		assertPreviewQueries(t, env.ch, rng, 3)
		if res.Histogram[0].Count != 7 || res.Histogram[3].Count != 2 || len(res.Histogram) != len(firstSeenHistLabels) {
			t.Errorf("histogram: %+v", res.Histogram)
		}
		want := uint64(9)
		if alertOnNew {
			want = 2
		}
		if res.WouldFlag != want {
			t.Errorf("alertOnNew=%v: would flag %d, want %d", alertOnNew, res.WouldFlag, want)
		}
		recent := rng.End.Add(-24 * time.Hour).Format("2006-01-02 15:04:05")
		if len(env.ch.QueriedMatching("first_seen >= toDateTime64('"+recent+"', 3, 'UTC')")) != 1 {
			t.Error("new-entity rate is not taken over the window's last day")
		}
	}
}

// A volume preview of a past range scores as of that range's end, not as of now.
func TestPreviewVolumeScoresAsOfTheRangeEnd(t *testing.T) {
	env := newEnv(t)
	end := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Second)
	rng := PreviewRange{Start: end.Add(-7 * 24 * time.Hour), End: end, Label: "custom"}
	env.ch.On("AS n\nFROM (", fakedb.CHResult{Cols: cols("z", "Int64", "h", "Int64", "n", "Int64"),
		Rows: [][]any{{int64(42), int64(4), int64(3)}, {int64(-5), int64(0), int64(8)}}})
	env.ch.On("AS would_flag", fakedb.CHResult{Cols: cols("entities_scored", "UInt64", "max_z", "Float64", "would_flag", "UInt64"),
		Rows: [][]any{{uint64(11), 4.2, uint64(3)}}})
	def := ModelDefinition{KeyFields: []string{"host"}, Alert: &AlertConfig{ZThreshold: 4}}

	res, err := env.mgr.Preview(context.Background(), fracA, ModelTypeVolumeBaseline, def, rng)
	if err != nil {
		t.Fatal(err)
	}
	asOf := "toDateTime('" + end.Format("2006-01-02 15:04:05") + "', 'UTC')"
	if n := len(env.ch.QueriedMatching(asOf)); n != 3 {
		t.Errorf("%d of 3 queries score as of the range end", n)
	}
	if len(env.ch.QueriedMatching("countIf(z_score > 4)")) != 1 || res.FlagBasis != "z-score > 4" || res.WouldFlag != 3 {
		t.Errorf("threshold: %+v", res)
	}
	if res.Histogram[4].Count != 3 || res.Histogram[0].Count != 8 {
		t.Errorf("histogram: %+v", res.Histogram)
	}
	if want := [][]int64{{42, 3}, {-5, 8}}; !reflect.DeepEqual(res.Distribution.Cells, want) {
		t.Errorf("cells %v", res.Distribution.Cells)
	}
	if res.Stats["min_buckets"] == nil || res.Stats["max_z"] != 4.2 {
		t.Errorf("stats: %v", res.Stats)
	}
}

// One failed query fails the preview with which one it was.
func TestPreviewQueryFailure(t *testing.T) {
	env := newEnv(t)
	env.ch.On("AS would_flag", fakedb.CHResult{Err: errors.New("TIMEOUT_EXCEEDED")})
	_, err := env.mgr.Preview(context.Background(), fracA, ModelTypeRarity, rarityDef, lastDay())
	if err == nil || !strings.Contains(err.Error(), "preview metrics") || !strings.Contains(err.Error(), "TIMEOUT_EXCEEDED") {
		t.Errorf("err = %v", err)
	}
	env = newEnv(t)
	env.ch.On("\nLIMIT 25", fakedb.CHResult{Err: errors.New("boom")})
	if _, err := env.mgr.Preview(context.Background(), fracA, ModelTypeVolumeBaseline,
		ModelDefinition{KeyFields: []string{"h"}}, lastDay()); err == nil || !strings.Contains(err.Error(), "preview top rows") {
		t.Errorf("err = %v", err)
	}
	env = newEnv(t)
	env.ch.On("AS bucket, count() AS cnt", fakedb.CHResult{Err: errors.New("boom")})
	if _, err := env.mgr.Preview(context.Background(), fracA, ModelTypeTLSH,
		ModelDefinition{KeyFields: []string{"tlsh"}}, lastDay()); err == nil || !strings.Contains(err.Error(), "preview distribution") {
		t.Errorf("err = %v", err)
	}
}

// The network preview scores raw-log pairs with the scorer's own functions, so the
// same long-connection pairs as TestScorerLongConnectionEndToEnd score the same:
// 0.65, 0.639 and 0.25, plus a pair under the base duration at 0.
func TestPreviewNetworkLongConnection(t *testing.T) {
	env := newEnv(t)
	rng := lastDay()
	now := uint32(rng.End.Unix())
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(100)))
	env.ch.On("AS prev_total", prevalenceRows(map[string]uint64{"203.0.113.5": 1, "10.0.0.53": 60, "198.51.100.7": 10}))
	env.ch.On("AS ts_list", fakedb.CHResult{Cols: stateCols, Rows: [][]any{
		statePair("10.1.1.3", "198.51.100.7", 1, []uint32{now - 60}, []float64{1}, 14400),
		statePair("10.1.1.1", "203.0.113.5", 3, []uint32{now - 600}, []float64{1}, 28800),
		statePair("10.1.1.4", "192.0.2.9", 1, []uint32{now - 60}, []float64{1}, 3600),
		statePair("10.1.1.2", "10.0.0.53", 2, []uint32{now - 60}, []float64{5}, 50000),
	}})

	res, err := env.mgr.Preview(context.Background(), fracA, ModelTypeLongConnection, ModelDefinition{}, rng)
	if err != nil {
		t.Fatal(err)
	}
	assertPreviewQueries(t, env.ch, rng, 3)
	var scores []float64
	for _, row := range res.Top {
		scores = append(scores, row["score"].(float64))
	}
	if want := []float64{0.65, 0.639, 0.25, 0}; !reflect.DeepEqual(scores, want) {
		t.Errorf("top scores %v, want %v (highest first)", scores, want)
	}
	if res.WouldFlag != 2 || res.FlagBasis != "final score >= 0.5" || res.Metric != "longconn_score" {
		t.Errorf("flagging: %d %q %q", res.WouldFlag, res.FlagBasis, res.Metric)
	}
	if res.Histogram[6].Count != 2 || res.Histogram[2].Count != 1 || res.Histogram[0].Count != 1 {
		t.Errorf("histogram: %+v", res.Histogram)
	}
	if want := [][]int64{{0, 1}, {25, 1}, {63, 1}, {65, 1}}; !reflect.DeepEqual(res.Distribution.Cells, want) {
		t.Errorf("cells %v, want %v", res.Distribution.Cells, want)
	}
	if res.Stats["pairs_scanned"] != uint64(4) || res.Stats["max_score"] != 0.65 || res.Stats["network_size"] != uint64(100) {
		t.Errorf("stats: %v", res.Stats)
	}
}

// An empty window reads no prevalence and scores nothing.
func TestPreviewNetworkEmptyWindow(t *testing.T) {
	env := newEnv(t)
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(0)))
	res, err := env.mgr.Preview(context.Background(), fracA, ModelTypeBeacon, ModelDefinition{}, lastDay())
	if err != nil {
		t.Fatal(err)
	}
	if len(env.ch.QueriedMatching("AS prev_total")) != 0 || res.WouldFlag != 0 || len(res.Top) != 0 || res.Metric != "beacon_score" {
		t.Errorf("empty window: %+v", res)
	}
}

func TestPreviewNetworkFailures(t *testing.T) {
	for name, script := range map[string]func(*chFake){
		"network size": func(ch *chFake) { ch.On("AS network_size", fakedb.CHResult{Err: errors.New("x")}) },
		"prevalence": func(ch *chFake) {
			ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(3)))
			ch.On("AS prev_total", fakedb.CHResult{Err: errors.New("x")})
		},
		"aggregation": func(ch *chFake) {
			ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(3)))
			ch.On("AS ts_list", fakedb.CHResult{Err: errors.New("x")})
		},
	} {
		env := newEnv(t)
		script(env.ch)
		if _, err := env.mgr.Preview(context.Background(), fracA, ModelTypeBeacon, ModelDefinition{}, lastDay()); err == nil || !strings.Contains(err.Error(), "preview "+name) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
