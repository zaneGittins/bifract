package models

import (
	"testing"
	"time"
)

// groupArray keeps neither list in time order. Timestamps and sizes are parallel,
// so trimming to the window must keep the sizes of the connections inside it.
func TestPairFromRowKeepsSizesWithTheirTimestamps(t *testing.T) {
	cutoff := int64(150)
	p := pairFromRow(map[string]interface{}{
		"src": "10.0.0.1", "dst": "10.0.0.2", "port": "443", "cnt": uint64(4),
		"ts_list":   []uint32{300, 100, 200, 50},
		"size_list": []float64{3, 1, 2, 0.5},
	}, cutoff)
	if len(p.TsList) != 2 || p.TsList[0] != 200 || p.TsList[1] != 300 {
		t.Fatalf("ts = %v, want [200 300]", p.TsList)
	}
	if len(p.SizeList) != 2 || p.SizeList[0] != 2 || p.SizeList[1] != 3 {
		t.Fatalf("sizes = %v, want [2 3]: the sizes of the in-window connections", p.SizeList)
	}
	if p.FirstTs != 200 || p.LastTs != 300 || p.Src != "10.0.0.1" || p.Count != 4 {
		t.Fatalf("pair = %+v", p)
	}
}

// A size list that is not parallel (it should always be) is trimmed by position
// rather than trusted, and one shorter than the trim is dropped.
func TestPairFromRowMismatchedSizes(t *testing.T) {
	p := pairFromRow(map[string]interface{}{
		"ts_list":   []int64{10, 20, 30},
		"size_list": []float64{1},
	}, 25)
	if len(p.TsList) != 1 || p.SizeList != nil {
		t.Fatalf("ts %v sizes %v", p.TsList, p.SizeList)
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
