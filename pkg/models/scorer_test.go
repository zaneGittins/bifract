package models

import (
	"reflect"
	"testing"
	"time"
)

// A pair-read row becomes a pair as read: its points in their order, and first
// and last seen from the row rather than its kept points.
func TestPairFromRow(t *testing.T) {
	p := pairFromRow(map[string]interface{}{
		"src": "10.0.0.1", "dst": "10.0.0.2", "port": "443", "cnt": uint64(9),
		"ts_list": []uint32{100, 200, 300}, "size_list": []float64{1, 2, 3},
		"total_duration": float64(7), "first_ts": uint32(90), "last_ts": uint32(950), "ti_hit": uint64(1),
	})
	want := PairAgg{Src: "10.0.0.1", Dst: "10.0.0.2", Port: "443", Count: 9,
		TsList: []int64{100, 200, 300}, SizeList: []float64{1, 2, 3},
		TotalDuration: 7, FirstTs: 90, LastTs: 950, ThreatIntelHit: true}
	if !reflect.DeepEqual(p, want) {
		t.Fatalf("pair = %+v\nwant %+v", p, want)
	}
}

// The driver hands back different scan types for the same column depending on
// the query shape; the accessors must read each one rather than yield zero.
func TestRowAccessorsTolerateDriverTypes(t *testing.T) {
	s := "x"
	row := map[string]interface{}{
		"s": "a", "sp": &s, "snil": (*string)(nil),
		"u64": uint64(7), "u32": uint32(7), "i64": int64(7), "neg": int64(-3), "f64": float64(7.9),
		"f32": float32(1.5), "fu": uint64(2), "fi": int64(-2),
		"ai64": []int64{1, 2}, "au32": []uint32{1, 2}, "au64": []uint64{1, 2}, "af": []float64{1.5},
	}
	if getString(row, "s") != "a" || getString(row, "sp") != "x" || getString(row, "snil") != "" || getString(row, "u64") != "" {
		t.Error("getString")
	}
	for _, k := range []string{"u64", "u32", "i64"} {
		if getUint64(row, k) != 7 {
			t.Errorf("getUint64(%s) = %d", k, getUint64(row, k))
		}
	}
	if getUint64(row, "f64") != 7 || getUint64(row, "neg") != 0 || getUint64(row, "s") != 0 {
		t.Error("getUint64 truncation / negative / non-numeric")
	}
	if getFloat64(row, "f32") != 1.5 || getFloat64(row, "fu") != 2 || getFloat64(row, "fi") != -2 || getFloat64(row, "s") != 0 {
		t.Error("getFloat64")
	}
	for _, k := range []string{"ai64", "au32", "au64"} {
		if v := getInt64Slice(row, k); len(v) != 2 || v[1] != 2 {
			t.Errorf("getInt64Slice(%s) = %v", k, v)
		}
	}
	if getInt64Slice(row, "af") != nil || getFloat64Slice(row, "ai64") != nil || len(getFloat64Slice(row, "af")) != 1 {
		t.Error("slice accessors must reject the wrong element type")
	}
}

func TestUnixToTime(t *testing.T) {
	if got := unixToTime(0); !got.Equal(time.Unix(0, 0)) || got.Location() != time.UTC {
		t.Errorf("zero = %v", got)
	}
	if got := unixToTime(-5); !got.Equal(time.Unix(0, 0)) {
		t.Errorf("negative must clamp to the epoch, got %v", got)
	}
	if got := unixToTime(1_700_000_000); got.Unix() != 1_700_000_000 || got.Location() != time.UTC {
		t.Errorf("got %v", got)
	}
}

// A model rescores roughly once per window-day in hours, bounded so a 1d model is
// not rescored every tick and a 14d model is still rescored several times a day.
func TestRescoreCadence(t *testing.T) {
	cases := map[string]time.Duration{"1d": time.Hour, "7d": scorerMaxRescore, "14d": scorerMaxRescore, "": time.Hour}
	for window, want := range cases {
		r := scheduledModelRow{def: ModelDefinition{Window: window}}
		if got := r.rescoreInterval(); got != want {
			t.Errorf("window %q: interval %v, want %v", window, got, want)
		}
	}

	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	r := scheduledModelRow{def: ModelDefinition{Window: "1d"}}
	if !r.due(now) {
		t.Error("a never-scored model is due")
	}
	justNow := now.Add(-59 * time.Minute)
	r.lastScored = &justNow
	if r.due(now) {
		t.Error("a model scored inside its interval is not due")
	}
	onTime := now.Add(-time.Hour)
	r.lastScored = &onTime
	if !r.due(now) {
		t.Error("a model is due once exactly its interval has passed")
	}
}
