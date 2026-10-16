package query

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"bifract/pkg/storage"
)

func partRow(fractal, day string, raw, disk, rows interface{}) map[string]interface{} {
	return map[string]interface{}{
		"partition": fmt.Sprintf("('%s','%s')", fractal, day),
		"raw_bytes": raw, "disk_bytes": disk, "rows": rows,
	}
}

func dayTotals(resp map[string]interface{}) map[string]float64 {
	out := map[string]float64{}
	for _, d := range resp["days"].([]map[string]interface{}) {
		out[d["day"].(string)] = d["raw_bytes"].(float64)
	}
	return out
}

func dayKeys(resp map[string]interface{}) []string {
	var out []string
	for _, d := range resp["days"].([]map[string]interface{}) {
		out = append(out, d["day"].(string))
	}
	return out
}

func TestIngestDailyResponse(t *testing.T) {
	now := time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC)
	rows := []map[string]interface{}{
		partRow("f1", "2026-10-15", uint64(100), uint64(10), uint64(5)),
		partRow("f1", "2026-10-14", float64(50), int64(5), 2),
		partRow("f2", "2026-10-15", int64(30), float32(3), uint64(1)),
		partRow("", "2026-10-15", uint64(7), uint64(1), uint64(1)),      // the default fractal's legacy rows
		partRow("f1", "2026-10-16", uint64(4), uint64(1), uint64(1)),    // writer clock just past midnight
		partRow("f1", "2026-10-01", uint64(1000), uint64(1), uint64(1)), // before the window
		{"partition": "tuple()", "raw_bytes": uint64(999)},              // not a logs partition
	}

	resp := ingestDailyResponse(rows, "", 3, now)
	// Three requested days plus one day of slack for the future-dated part.
	if got := fmt.Sprint(dayKeys(resp)); got != "[2026-10-13 2026-10-14 2026-10-15 2026-10-16]" {
		t.Fatalf("day window = %s", got)
	}
	totals := dayTotals(resp)
	if totals["2026-10-13"] != 0 || totals["2026-10-14"] != 50 || totals["2026-10-15"] != 137 || totals["2026-10-16"] != 4 {
		t.Errorf("per-day raw totals = %v", totals)
	}
	day15 := resp["days"].([]map[string]interface{})[2]
	if day15["disk_bytes"].(float64) != 14 || day15["rows"].(float64) != 7 {
		t.Errorf("2026-10-15 disk/rows = %v/%v, want 14/7", day15["disk_bytes"], day15["rows"])
	}

	// Stacked series, largest contributor first; the empty id is a real fractal.
	series := resp["series"].([]map[string]interface{})
	var order []string
	for _, s := range series {
		order = append(order, s["fractal_id"].(string))
		if n := len(s["raw_bytes"].([]float64)); n != 4 {
			t.Errorf("series %q has %d points, want one per day", s["fractal_id"], n)
		}
	}
	if fmt.Sprint(order) != "[f1 f2 ]" {
		t.Errorf("series order = %q", order)
	}
	if f1 := series[0]["raw_bytes"].([]float64); fmt.Sprint(f1) != "[0 50 100 4]" {
		t.Errorf("f1 series = %v", f1)
	}
}

// A clock far ahead of real time must not stretch the chart: the window allows
// one day of slack past today and no more.
func TestIngestDailyResponseClampsFutureDays(t *testing.T) {
	now := time.Date(2026, 10, 15, 23, 59, 0, 0, time.UTC)
	resp := ingestDailyResponse([]map[string]interface{}{partRow("f1", "2027-01-01", uint64(1), uint64(1), uint64(1))}, "", 2, now)
	if got := fmt.Sprint(dayKeys(resp)); got != "[2026-10-14 2026-10-15 2026-10-16]" {
		t.Errorf("day window = %s", got)
	}
}

func TestIngestDailyResponseFractalFilter(t *testing.T) {
	now := time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC)
	resp := ingestDailyResponse([]map[string]interface{}{
		partRow("f1", "2026-10-15", uint64(10), uint64(1), uint64(1)),
		partRow("f2", "2026-10-15", uint64(99), uint64(1), uint64(1)),
	}, "f1", 1, now)
	if resp["fractal"] != "f1" || dayTotals(resp)["2026-10-15"] != 10 {
		t.Errorf("filtered totals = %v", dayTotals(resp))
	}
	if _, ok := resp["series"]; ok {
		t.Error("a single-fractal view has no per-fractal breakdown")
	}
}

// Beyond the top eight, fractals roll into one "__other__" series, and equal
// totals rank by id so the legend does not reshuffle between polls.
func TestIngestDailyResponseTopNAndOther(t *testing.T) {
	now := time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC)
	var rows []map[string]interface{}
	for i := 0; i < 10; i++ {
		rows = append(rows, partRow(fmt.Sprintf("f%02d", i), "2026-10-15", uint64(100-i), uint64(1), uint64(1)))
	}
	rows = append(rows, partRow("tie-b", "2026-10-15", uint64(500), uint64(1), uint64(1)))
	rows = append(rows, partRow("tie-a", "2026-10-15", uint64(500), uint64(1), uint64(1)))

	for i := 0; i < 20; i++ {
		series := ingestDailyResponse(rows, "", 1, now)["series"].([]map[string]interface{})
		if len(series) != 9 {
			t.Fatalf("%d series, want 8 plus other", len(series))
		}
		if series[0]["fractal_id"] != "tie-a" || series[1]["fractal_id"] != "tie-b" {
			t.Fatalf("tied fractals ordered %v, %v", series[0]["fractal_id"], series[1]["fractal_id"])
		}
		other := series[8]
		// f00..f05 make the top eight with the ties; f06..f09 (94+93+92+91) are other.
		if other["fractal_id"] != "__other__" || other["raw_bytes"].([]float64)[0] != 370 {
			t.Fatalf("other = %v %v", other["fractal_id"], other["raw_bytes"])
		}
	}
}

func TestHotTableStats(t *testing.T) {
	oldest := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	newest := oldest.Add(90*time.Minute + 20*time.Second)
	for name, pair := range map[string][2]interface{}{
		"time.Time":  {oldest, newest},
		"*time.Time": {&oldest, &newest},
		"string":     {"2026-10-15 09:00:00", "2026-10-15 10:30:20"},
	} {
		t.Run(name, func(t *testing.T) {
			s := hotTableStats(map[string]interface{}{
				"partition_count": uint64(4), "row_count": uint64(1000), "disk_bytes": uint64(2048),
				"oldest": pair[0], "newest": pair[1],
			})
			if s["partition_count"] != 4.0 || s["row_count"] != 1000.0 || s["disk_bytes"] != 2048.0 {
				t.Errorf("counts = %v", s)
			}
			if s["coverage_minutes"] != int64(90) || s["oldest"] != "2026-10-15 09:00:00" || s["newest"] != "2026-10-15 10:30:20" {
				t.Errorf("coverage = %v %v..%v", s["coverage_minutes"], s["oldest"], s["newest"])
			}
		})
	}

	// An empty table has no readable bounds; say nothing rather than a bogus span.
	var nilTime *time.Time
	for name, row := range map[string]map[string]interface{}{
		"zero":    {"oldest": time.Time{}, "newest": newest},
		"nil ptr": {"oldest": nilTime, "newest": newest},
		"garbage": {"oldest": "yesterday", "newest": newest},
		"missing": {"oldest": oldest},
	} {
		if _, ok := hotTableStats(row)["coverage_minutes"]; ok {
			t.Errorf("%s: coverage reported without both bounds", name)
		}
	}
}

func TestMetricRange(t *testing.T) {
	cases := map[string]struct {
		since  time.Duration
		bucket int
	}{
		"":      {time.Hour, 60},
		"1h":    {time.Hour, 60},
		"8h":    {8 * time.Hour, 300},
		"24h":   {24 * time.Hour, 900},
		"7d":    {7 * 24 * time.Hour, 3600},
		"30d":   {30 * 24 * time.Hour, 14400},
		"bogus": {time.Hour, 60},
	}
	for in, want := range cases {
		since, bucket := MetricRange(in)
		if since != want.since || bucket != want.bucket {
			t.Errorf("MetricRange(%q) = %v/%d, want %v/%d", in, since, bucket, want.since, want.bucket)
		}
		if pts := int(since.Seconds()) / bucket; pts < 50 || pts > 200 {
			t.Errorf("MetricRange(%q) draws %d points", in, pts)
		}
	}
}

func TestCPUPoints(t *testing.T) {
	ts := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	got := cpuPoints([]storage.MetricPoint{{Bucket: ts, Value: 12.345}, {Bucket: ts.Add(time.Minute), Value: 99.96}})
	if len(got) != 2 || got[0]["time"] != ts.Unix() || got[0]["value"] != 12.3 || got[1]["value"] != 100.0 {
		t.Errorf("got %v", got)
	}
	if got := cpuPoints(nil); got == nil || len(got) != 0 {
		t.Error("no points must encode as an empty array, not null")
	}
}

func TestToFloat64(t *testing.T) {
	for in, want := range map[interface{}]float64{float64(1.5): 1.5, float32(2.5): 2.5, int64(-3): -3, 4: 4, uint64(5): 5, "6": 0, nil: 0} {
		if got := toFloat64(in); got != want {
			t.Errorf("toFloat64(%#v) = %v, want %v", in, got, want)
		}
	}
}

// Query ids end up in a KILL statement, so anything but the id alphabet is
// refused before ClickHouse is involved.
func TestHandleKillQueryValidation(t *testing.T) {
	h := &PerformanceHandler{}
	for _, q := range []string{"", "?query_id=", "?query_id=abc%27%20OR%201%3D1", "?query_id=a%3Bb", "?query_id=a%20b"} {
		rec := httptest.NewRecorder()
		h.HandleKillQuery(rec, httptest.NewRequest(http.MethodPost, "/"+q, nil))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%q: status %d, want 400", q, rec.Code)
		}
	}
}
