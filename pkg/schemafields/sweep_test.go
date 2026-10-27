package schemafields

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/storage/storagetest"
)

// The strata must tile the window with no gap or overlap, or rows are sampled
// twice or never. The first leans on the window predicate's lower bound and the
// last is open so rows ingested during the sweep still fall in a slice.
func TestWindowStrataTileTheWindow(t *testing.T) {
	anchor := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	strata := windowStrata(anchor, 8*time.Hour, 4)
	if len(strata) != 4 {
		t.Fatalf("want 4 strata, got %d: %v", len(strata), strata)
	}
	want := []string{
		"ingest_timestamp < toDateTime64('2026-01-01 18:00:00.000', 3, 'UTC')",
		"ingest_timestamp >= toDateTime64('2026-01-01 18:00:00.000', 3, 'UTC') AND ingest_timestamp < toDateTime64('2026-01-01 20:00:00.000', 3, 'UTC')",
		"ingest_timestamp >= toDateTime64('2026-01-01 20:00:00.000', 3, 'UTC') AND ingest_timestamp < toDateTime64('2026-01-01 22:00:00.000', 3, 'UTC')",
		"ingest_timestamp >= toDateTime64('2026-01-01 22:00:00.000', 3, 'UTC')",
	}
	for i := range want {
		if strata[i] != want[i] {
			t.Errorf("stratum %d:\n got %s\nwant %s", i, strata[i], want[i])
		}
	}
}

// sweepFakes scripts two fractals, a and b, whose sample query fails for b.
func sweepFakes(t *testing.T) (*Sweeper, *fakedb.PG, *fakedb.CH) {
	t.Helper()
	ch, chf := storagetest.ClickHouse()
	pg, pgf := storagetest.Postgres()
	now := time.Now().UTC()
	chf.Respond(func(q string, _ []any) (fakedb.CHResult, bool) {
		switch {
		case strings.Contains(q, "recent_rows"):
			return fakedb.CHResult{
				Cols: cols("fractal", "String", "newest", "DateTime", "total_bytes", "UInt64", "recent_rows", "UInt64"),
				Rows: [][]any{{"a", now, uint64(0), uint64(0)}, {"b", now, uint64(0), uint64(0)}},
			}, true
		case strings.Contains(q, "ARRAY JOIN paths"):
			return fakedb.CHResult{Cols: cols("fractal", "String", "field", "String", "bytes", "UInt64")}, true
		case strings.Contains(q, "approx_top_k"):
			if strings.Contains(q, "fractal_id = 'b'") {
				return fakedb.CHResult{Err: errors.New("memory limit exceeded")}, true
			}
			return fakedb.CHResult{
				Cols: cols("key", "String", "present", "UInt64", "cardinality", "UInt64", "top_values", "Array(String)", "top_counts", "Array(UInt64)", "top_errors", "Array(UInt64)", "distinct_hashes", "Array(UInt64)"),
				Rows: [][]any{{"user", uint64(90), uint64(4), []string{}, []uint64{}, []uint64{}, []uint64{1, 2, 3, 4}}},
			}, true
		case strings.Contains(q, "SELECT count() AS n"):
			return fakedb.One("n", "UInt64", uint64(100)), true
		case strings.Contains(q, "JSONDynamicPaths"):
			return fakedb.One("n", "UInt64", uint64(0)), true
		}
		return fakedb.CHResult{}, false
	})
	return NewSweeper(ch, pg, nil, nil), pgf, chf
}

func cols(nameType ...string) []fakedb.Col {
	out := make([]fakedb.Col, 0, len(nameType)/2)
	for i := 0; i+1 < len(nameType); i += 2 {
		out = append(out, fakedb.Col{Column: nameType[i], Type: nameType[i+1]})
	}
	return out
}

func insertsFor(pgf *fakedb.PG, table, fractal string) [][]driver.Value {
	var out [][]driver.Value
	for _, c := range pgf.Matching("INSERT INTO " + table) {
		if len(c.Args) > 0 && c.Args[0] == fractal {
			out = append(out, c.Args)
		}
	}
	return out
}

// A fractal whose sample fails this pass keeps its last measurement. Writing it
// as zero rows made its fields read 0% present until a later pass succeeded,
// which on large clusters is where samples hit their memory cap most.
func TestSweepKeepsLastSampleWhenOneFails(t *testing.T) {
	s, pgf, _ := sweepFakes(t)
	sampledAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pgf.On("FROM schema_fractal_stats WHERE fractal_id", fakedb.Row(int64(80), int64(5), sampledAt))
	pgf.On("FROM schema_field_stats WHERE fractal_id", fakedb.RowsOf(
		[]driver.Value{"image", int64(40), int64(3), `[{"value":"cmd.exe","count":40}]`},
	))

	s.sweepStats(context.Background())

	fb := insertsFor(pgf, "schema_fractal_stats", "b")
	if len(fb) != 1 {
		t.Fatalf("want one fractal row for b, got %v", fb)
	}
	if fb[0][1] != int64(80) || fb[0][2] != int64(5) {
		t.Errorf("b sampled_rows/max_paths = %v/%v, want the previous 80/5", fb[0][1], fb[0][2])
	}
	if got, ok := fb[0][4].(time.Time); !ok || !got.Equal(sampledAt) {
		t.Errorf("b sampled_at = %v, want the previous %v", fb[0][4], sampledAt)
	}
	fields := insertsFor(pgf, "schema_field_stats", "b")
	if len(fields) != 1 || fields[0][1] != "image" || fields[0][2] != int64(40) || fields[0][3] != int64(3) {
		t.Fatalf("b fields = %v, want the previous image row", fields)
	}
	if !strings.Contains(fields[0][4].(string), "cmd.exe") {
		t.Errorf("b top values lost: %v", fields[0][4])
	}

	// The fractal that sampled cleanly is written from this pass: one query per
	// stratum, each answering 90 rows.
	if fa := insertsFor(pgf, "schema_field_stats", "a"); len(fa) != 1 || fa[0][1] != "user" || fa[0][2] != int64(90*sampleStrata) {
		t.Errorf("a fields = %v, want this pass's user row", fa)
	}
}

// With nothing measured before, a failed sample is still recorded as unmeasured
// rather than blocking the other fractals' results.
func TestSweepFailedSampleWithoutHistory(t *testing.T) {
	s, pgf, _ := sweepFakes(t)
	s.sweepStats(context.Background())

	fb := insertsFor(pgf, "schema_fractal_stats", "b")
	if len(fb) != 1 || fb[0][1] != int64(0) {
		t.Fatalf("b = %v, want one unmeasured row", fb)
	}
	if at, ok := fb[0][4].(time.Time); !ok || at.IsZero() {
		t.Errorf("b sampled_at = %v, want this pass's time", fb[0][4])
	}
	if fa := insertsFor(pgf, "schema_field_stats", "a"); len(fa) != 1 {
		t.Errorf("a fields = %v, want this pass's row", fa)
	}
}

// The probe's count is the coverage denominator, so it must count the stratified
// scan the sample reads rather than a single LIMIT over the window.
func TestProbeCountsTheStratifiedScan(t *testing.T) {
	ch, chf := storagetest.ClickHouse()
	var probe string
	chf.Respond(func(q string, _ []any) (fakedb.CHResult, bool) {
		probe = q
		return fakedb.One("n", "UInt64", uint64(0)), true
	})
	s := NewSweeper(ch, nil, nil, nil)
	anchor := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	if _, err := s.probeCount(context.Background(), "a", anchor, 8*time.Hour, 800); err != nil {
		t.Fatal(err)
	}
	if strings.Count(probe, "UNION ALL") != sampleStrata-1 {
		t.Errorf("probe does not count the stratified scan:\n%s", probe)
	}
}

func tv(v string, n uint64) TopValue { return TopValue{Value: v, Count: n} }

// Slices are sampled in separate queries and merged here: presence sums, and the
// distinct count must be exact below the set-index threshold, because that is
// the only decision it drives.
func TestMergeSlicesCountsAndCardinality(t *testing.T) {
	got := mergeSlices([]map[string]*fieldSample{
		{"user": {Present: 10, Cardinality: 2, distinct: []uint64{1, 2}}},
		{"user": {Present: 5, Cardinality: 2, distinct: []uint64{2, 3}}},
		{},
	}, 5, 5, 500)
	u := got["user"]
	if u == nil || u.Present != 15 || u.Cardinality != 3 {
		t.Fatalf("user = %+v, want present 15 and 3 distinct", u)
	}

	full := make([]uint64, 257)
	for i := range full {
		full[i] = uint64(i)
	}
	got = mergeSlices([]map[string]*fieldSample{
		{"id": {Present: 300, Cardinality: 300, distinct: full}},
		{"id": {Present: 1, Cardinality: 1, distinct: []uint64{0}}},
	}, 5, 5, 500)
	if c := got["id"].Cardinality; c <= 256 {
		t.Errorf("a capped field must stay above the set threshold, got %d", c)
	}
}

// A merged count is exact only if every slice that saw the field either listed
// the value or listed all its values; otherwise the value may hide in a slice's
// tail and the count is a lower bound.
func TestMergeSlicesTopValues(t *testing.T) {
	got := mergeSlices([]map[string]*fieldSample{
		{"img": {Present: 8, Top: []TopValue{tv("x", 5), tv("y", 3)}}},
		{"img": {Present: 5, Top: []TopValue{tv("z", 4), tv("x", 1)}}},
		{"other": {Present: 1}},
	}, 2, 2, 500)
	top := got["img"].Top
	want := []TopValue{{Value: "x", Count: 6}, {Value: "z", Count: 4, Approx: true}}
	if len(top) != len(want) {
		t.Fatalf("top = %+v, want %+v", top, want)
	}
	for i := range want {
		if top[i] != want[i] {
			t.Errorf("top[%d] = %+v, want %+v", i, top[i], want[i])
		}
	}

	// Short lists are exhaustive, so a value missing from one is a true zero.
	got = mergeSlices([]map[string]*fieldSample{
		{"img": {Present: 3, Top: []TopValue{tv("x", 3)}}},
		{"img": {Present: 2, Top: []TopValue{tv("y", 2)}}},
	}, 5, 5, 500)
	for _, v := range got["img"].Top {
		if v.Approx {
			t.Errorf("%q marked approximate though every list was exhaustive", v.Value)
		}
	}

	// An estimator's own error carries through the merge.
	got = mergeSlices([]map[string]*fieldSample{
		{"img": {Present: 3, Top: []TopValue{{Value: "x", Count: 3, Approx: true}}}},
	}, 5, 5, 500)
	if !got["img"].Top[0].Approx {
		t.Error("estimator error lost in the merge")
	}
}

// Each slice returns up to MaxFields fields, so the merge must cap the union or
// a fractal persists up to sampleStrata times as many rows.
func TestMergeSlicesCapsFields(t *testing.T) {
	got := mergeSlices([]map[string]*fieldSample{
		{"a": {Present: 9}, "b": {Present: 1}},
		{"c": {Present: 5}},
	}, 5, 5, 2)
	if len(got) != 2 || got["a"] == nil || got["c"] == nil {
		t.Errorf("want the two most present fields, got %v", got)
	}
}

// Each stratum is sampled in its own query for its share of the rows. Sampling
// them through one UNION ALL kept every reader open at once and measured over
// four times the memory of a single scan.
func TestSampleQueriesEachStratumSeparately(t *testing.T) {
	s, _, chf := sweepFakes(t)
	s.sweepStats(context.Background())

	var forA []string
	for _, q := range chf.Queried() {
		if strings.Contains(q, "approx_top_k") && strings.Contains(q, "fractal_id = 'a'") {
			forA = append(forA, q)
		}
	}
	if len(forA) != sampleStrata {
		t.Fatalf("want %d sample queries for a, got %d", sampleStrata, len(forA))
	}
	share := fmt.Sprintf("LIMIT %d", sliceShare(sampleSize()))
	for i, q := range forA {
		if strings.Contains(q, "UNION ALL") || !strings.Contains(q, share) {
			t.Errorf("query %d must scan one stratum for %s:\n%s", i, share, q)
		}
	}
}

// Slices list more values than the tab shows, so a field with a handful more
// distinct values than it displays still merges to exact counts.
func TestMergeSlicesListsBeyondWhatIsShown(t *testing.T) {
	six := []TopValue{tv("a", 6), tv("b", 5), tv("c", 4), tv("d", 3), tv("e", 2), tv("f", 1)}
	got := mergeSlices([]map[string]*fieldSample{
		{"lvl": {Present: 21, Top: six}},
		{"lvl": {Present: 21, Top: six}},
	}, 20, 5, 500)
	top := got["lvl"].Top
	if len(top) != 5 || top[0] != (TopValue{Value: "a", Count: 12}) {
		t.Fatalf("top = %+v, want five exact values led by a=12", top)
	}
	for _, v := range top {
		if v.Approx {
			t.Errorf("%q approximate though each slice listed every value", v.Value)
		}
	}
}

// The slice queries must ask for the longer list the merge relies on.
func TestSliceQueriesListBeyondWhatIsShown(t *testing.T) {
	for _, q := range sliceSampleSQL("logs", "a", time.Now(), time.Hour, 800) {
		if !strings.Contains(q, fmt.Sprintf("approx_top_k(%d,", sampleSliceTopN)) {
			t.Fatalf("slice query does not list %d values:\n%s", sampleSliceTopN, q)
		}
	}
}
