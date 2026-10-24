package query

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"bifract/internal/fakedb"
)

// A filtered search scans each histogram chunk once: the scan that counts the
// timeline also locates the rows still owed, which a keyed fetch then reads. These
// tests pin that the rows, their order and the histogram match what two separate
// scans per chunk would return.
//
// The 24h test window has 96 quarter-hour buckets in four chunks, newest first:
// [22:00, 24:00), [18:00, 22:00), [10:00, 18:00), [00:00, 10:00).

func isNewestSQL(sql string) bool { return strings.Contains(sql, "AS newest_ms") }
func isFetchSQL(sql string) bool  { return strings.Contains(sql, "timestamp IN (") }

func queriesWhere(e *testEnv, keep func(string) bool) []string {
	var out []string
	for _, q := range e.ch.Queried() {
		if keep(q) {
			out = append(out, q)
		}
	}
	return out
}

func plainRowScans(e *testEnv) []string {
	return queriesWhere(e, func(q string) bool { return !isHistogramSQL(q) && !isNewestSQL(q) && !isFetchSQL(q) })
}

// newestScan answers a single-scan chunk: bucket counts indexed from the chunk
// start, and the newest matching event times as the negated milliseconds
// groupArraySorted returns, newest first.
func newestScan(size int, counts map[int]uint64, times ...string) []map[string]interface{} {
	c := make([]uint64, size)
	for i, n := range counts {
		c[i] = n
	}
	ms := make([]int64, len(times))
	for i, ts := range times {
		t, err := time.Parse("2006-01-02 15:04:05.000", ts)
		if err != nil {
			panic(err)
		}
		ms[i] = -t.UnixMilli()
	}
	return []map[string]interface{}{{"hist_counts": c, "newest_ms": ms}}
}

const (
	chunk0Window = "timestamp >= '2026-10-01 22:00:00.000' AND timestamp <= '2026-10-02 00:00:00.000'"
	chunk1Window = "timestamp >= '2026-10-01 18:00:00.000' AND timestamp <= '2026-10-01 22:00:00.000'"
	chunk2Window = "timestamp >= '2026-10-01 10:00:00.000' AND timestamp <= '2026-10-01 18:00:00.000'"
	chunk3Window = "timestamp >= '2026-10-01 00:00:00.000' AND timestamp <= '2026-10-01 10:00:00.000'"
)

func TestSingleScanSparseSearch(t *testing.T) {
	e := newTestEnv(t, 1000)
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		switch {
		case isHistogramSQL(sql):
			if !strings.Contains(sql, chunk0Window) {
				t.Errorf("plain histogram scan of an older chunk: %s", sql)
			}
			return []map[string]interface{}{
				{"bucket": "2026-10-01 23:00:00", "cnt": uint64(1)},
				{"bucket": "2026-10-01 22:45:00", "cnt": uint64(1)},
			}, nil
		case isNewestSQL(sql):
			switch {
			case strings.Contains(sql, chunk1Window):
				return newestScan(16, map[int]uint64{12: 3}, "2026-10-01 21:10:00.500", "2026-10-01 21:05:00.000", "2026-10-01 21:05:00.000"), nil
			case strings.Contains(sql, chunk2Window):
				return newestScan(32, nil), nil
			case strings.Contains(sql, chunk3Window):
				return newestScan(40, map[int]uint64{39: 1}, "2026-10-01 09:45:00.000"), nil
			}
		case isFetchSQL(sql):
			switch {
			case strings.Contains(sql, "timestamp IN ('2026-10-01 21:10:00.500', '2026-10-01 21:05:00.000')"):
				return []map[string]interface{}{
					logRow("2026-10-01 21:10:00.500", "b", "f1"),
					logRow("2026-10-01 21:05:00.000", "a2", "f1"),
					logRow("2026-10-01 21:05:00.000", "a1", "f1"),
				}, nil
			case strings.Contains(sql, "timestamp IN ('2026-10-01 09:45:00.000')"):
				return []map[string]interface{}{logRow("2026-10-01 09:45:00.000", "c", "f1")}, nil
			}
		case strings.Contains(sql, chunk0Window):
			return logRows(2, testEndTime.Add(-time.Hour), "f1"), nil
		}
		t.Errorf("unexpected query: %s", sql)
		return []map[string]interface{}{}, nil
	})

	frames := runStream(t, e, QueryRequest{Query: "level=error", FractalID: "f1"})

	var got []string
	for _, r := range streamedRows(frames) {
		got = append(got, r["timestamp"].(string)+"/"+r["log_id"].(string))
	}
	want := []string{
		"2026-10-01 23:00:00.000/lid-2026-10-01 23:00:00.000",
		"2026-10-01 22:59:59.000/lid-2026-10-01 22:59:59.000",
		"2026-10-01 21:10:00.500/b",
		"2026-10-01 21:05:00.000/a2",
		"2026-10-01 21:05:00.000/a1",
		"2026-10-01 09:45:00.000/c",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("rows:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}

	// The newest chunk is read as today: an in-order row scan beside its own
	// histogram. Every older chunk is one scan that collects only the rows still
	// owed, plus a fetch when it found any.
	if scans := plainRowScans(e); len(scans) != 1 || !strings.Contains(scans[0], chunk0Window) {
		t.Errorf("plain row scans = %v", scans)
	}
	if h := e.ch.QueriedMatching("AS bucket"); len(h) != 1 {
		t.Errorf("plain histogram scans = %d, want only the newest chunk", len(h))
	}
	newest := queriesWhere(e, isNewestSQL)
	if len(newest) != 3 {
		t.Fatalf("single scans = %d, want one per older chunk", len(newest))
	}
	for i, n := range []string{"998", "995", "995"} {
		if !strings.Contains(newest[i], "groupArraySortedIf("+n+")") {
			t.Errorf("single scan %d does not ask for the %s rows still owed: %s", i, n, newest[i])
		}
		// Rows must never come from the query cache: a late event in an old chunk
		// would be hidden from a re-run.
		if strings.Contains(newest[i], "use_query_cache") {
			t.Errorf("single scan %d is cached: %s", i, newest[i])
		}
	}
	fetches := queriesWhere(e, isFetchSQL)
	if len(fetches) != 2 {
		t.Fatalf("fetches = %v", fetches)
	}
	if !strings.Contains(fetches[0], "timestamp >= '2026-10-01 21:05:00.000' AND timestamp <= '2026-10-01 21:10:00.500'") ||
		!strings.HasSuffix(fetches[0], "LIMIT 998") {
		t.Errorf("fetch is not bounded to the located rows: %s", fetches[0])
	}

	hist := framesOf(frames, "histogram")
	if len(hist) != 4 {
		t.Fatalf("%d histogram frames, want one per chunk", len(hist))
	}
	prevFrom := 97.0
	for _, h := range hist {
		from := h["covered_from"].(float64)
		if from >= prevFrom {
			t.Errorf("covered_from not decreasing: %v after %v", from, prevFrom)
		}
		prevFrom = from
	}
	last := hist[len(hist)-1]
	b := last["buckets"].([]interface{})
	if last["complete"] != true || b[92] != 1.0 || b[91] != 1.0 || b[84] != 3.0 || b[39] != 1.0 {
		t.Errorf("final histogram complete=%v [92]=%v [91]=%v [84]=%v [39]=%v", last["complete"], b[92], b[91], b[84], b[39])
	}
	done := framesOf(frames, "done")
	if len(done) != 1 || done[0]["count"] != 6.0 || done[0]["limit_hit"] != "" {
		t.Errorf("done = %v", done)
	}
	if len(framesOf(frames, "histogram_end")) != 0 || len(framesOf(frames, "error")) != 0 {
		t.Errorf("frames = %v", frameKinds(frames))
	}
}

// A dense search fills its rows from the newest chunk, so older chunks only need
// counting: they get the plain cached histogram and no row work at all.
func TestSingleScanDenseSearchOnlyCounts(t *testing.T) {
	e := newTestEnv(t, 3)
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		switch {
		case isNewestSQL(sql) || isFetchSQL(sql):
			t.Errorf("row work after the budget was full: %s", sql)
		case isHistogramSQL(sql):
			return []map[string]interface{}{}, nil
		case strings.Contains(sql, chunk0Window):
			return logRows(3, testEndTime, "f1"), nil
		}
		return []map[string]interface{}{}, nil
	})
	frames := runStream(t, e, QueryRequest{Query: "level=error", FractalID: "f1"})
	if h := e.ch.QueriedMatching("AS bucket"); len(h) != 4 {
		t.Errorf("histogram scans = %d, want one per chunk", len(h))
	}
	if hist := framesOf(frames, "histogram"); len(hist) != 4 || hist[3]["complete"] != true {
		t.Errorf("histogram frames = %v", hist)
	}
	if done := framesOf(frames, "done"); len(done) != 1 || done[0]["count"] != 3.0 || done[0]["limit_hit"] != "search" {
		t.Errorf("done = %v", done)
	}
}

// Once a single scan fills the budget, the chunks behind it are only counted.
func TestSingleScanStopsRowWorkAtBudget(t *testing.T) {
	e := newTestEnv(t, 4)
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		switch {
		case isHistogramSQL(sql):
			return []map[string]interface{}{}, nil
		case isNewestSQL(sql):
			if !strings.Contains(sql, chunk1Window) {
				t.Errorf("single scan after the budget was full: %s", sql)
			}
			return newestScan(16, map[int]uint64{15: 5}, "2026-10-01 21:59:00.000", "2026-10-01 21:58:00.000"), nil
		case isFetchSQL(sql):
			if !strings.HasSuffix(sql, "LIMIT 2") {
				t.Errorf("fetch limit: %s", sql)
			}
			return []map[string]interface{}{
				logRow("2026-10-01 21:59:00.000", "x", "f1"),
				logRow("2026-10-01 21:58:00.000", "y", "f1"),
			}, nil
		case strings.Contains(sql, chunk0Window):
			return logRows(2, testEndTime, "f1"), nil
		}
		return []map[string]interface{}{}, nil
	})
	frames := runStream(t, e, QueryRequest{Query: "level=error", FractalID: "f1"})
	newest := queriesWhere(e, isNewestSQL)
	if len(newest) != 1 || !strings.Contains(newest[0], "groupArraySortedIf(2)") {
		t.Errorf("single scans = %v", newest)
	}
	if h := e.ch.QueriedMatching("AS bucket"); len(h) != 3 {
		t.Errorf("plain histogram scans = %d, want the newest chunk and the two behind the full budget", len(h))
	}
	if hist := framesOf(frames, "histogram"); len(hist) != 4 || hist[3]["complete"] != true {
		t.Errorf("histogram frames = %v", hist)
	}
	if done := framesOf(frames, "done"); len(done) != 1 || done[0]["count"] != 4.0 || done[0]["limit_hit"] != "search" {
		t.Errorf("done = %v", done)
	}
}

// A single scan that fails costs neither the rows nor the timeline: the rows fall
// back to plain chunk scans and the remaining chunks are counted the plain way.
func TestSingleScanFailureFallsBackToPlainScans(t *testing.T) {
	e := newTestEnv(t, 1000)
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		switch {
		case isHistogramSQL(sql):
			return []map[string]interface{}{{"bucket": "2026-10-01 23:45:00", "cnt": uint64(1)}}, nil
		case isNewestSQL(sql):
			return nil, fakedb.Exception(241, "memory limit exceeded")
		case strings.Contains(sql, chunk0Window):
			return logRows(1, testEndTime, "f1"), nil
		case strings.Contains(sql, "timestamp >= '2026-10-01 18:00:00.000' AND timestamp < '2026-10-01 22:00:00.000'"):
			return logRows(1, testEndTime.Add(-3*time.Hour), "f1"), nil
		}
		return []map[string]interface{}{}, nil
	})
	frames := runStream(t, e, QueryRequest{Query: "level=error", FractalID: "f1"})
	if n := len(queriesWhere(e, isNewestSQL)); n != 1 {
		t.Errorf("single scans = %d, want the one that failed", n)
	}
	if scans := plainRowScans(e); len(scans) != 4 {
		t.Errorf("plain row scans = %d, want every chunk", len(scans))
	}
	if h := e.ch.QueriedMatching("AS bucket"); len(h) != 4 {
		t.Errorf("plain histogram scans = %d, want every chunk", len(h))
	}
	if hist := framesOf(frames, "histogram"); len(hist) != 4 || hist[3]["complete"] != true {
		t.Errorf("histogram frames = %v", hist)
	}
	if n := len(framesOf(frames, "histogram_end")); n != 0 {
		t.Errorf("timeline reported incomplete: %v", frameKinds(frames))
	}
	if done := framesOf(frames, "done"); len(done) != 1 || done[0]["count"] != 2.0 {
		t.Errorf("done = %v", done)
	}
}

// The newest chunk's histogram failing ends the timeline as it always has, and
// leaves the rows to plain chunk scans.
func TestSingleScanNewestHistogramFailure(t *testing.T) {
	e := newTestEnv(t, 1000)
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		switch {
		case isHistogramSQL(sql):
			return nil, fakedb.Exception(241, "memory limit exceeded")
		case isNewestSQL(sql) || isFetchSQL(sql):
			t.Errorf("single scan without a timeline to fill: %s", sql)
		case strings.Contains(sql, chunk0Window):
			return logRows(1, testEndTime, "f1"), nil
		}
		return []map[string]interface{}{}, nil
	})
	frames := runStream(t, e, QueryRequest{Query: "level=error", FractalID: "f1"})
	if scans := plainRowScans(e); len(scans) != 4 {
		t.Errorf("plain row scans = %d, want every chunk", len(scans))
	}
	if hist := framesOf(frames, "histogram"); len(hist) != 0 {
		t.Errorf("histogram frames = %v", hist)
	}
	if last := frames[len(frames)-1]; last.kind() != "histogram_end" || last["complete"] != false {
		t.Errorf("last frame = %v (%v)", last, frameKinds(frames))
	}
	if done := framesOf(frames, "done"); len(done) != 1 || done[0]["count"] != 1.0 {
		t.Errorf("done = %v", done)
	}
}

// doneWatcher closes done once the done frame has been written, so a test can
// hold back a query until the handler has answered.
type doneWatcher struct {
	*httptest.ResponseRecorder
	once sync.Once
	done chan struct{}
}

func (w *doneWatcher) Write(b []byte) (int, error) {
	n, err := w.ResponseRecorder.Write(b)
	if bytes.Contains(b, []byte(`"type":"done"`)) {
		w.once.Do(func() { close(w.done) })
	}
	return n, err
}

// A dense search answers from the newest chunk's row scan alone: the done frame
// does not wait for that chunk's histogram, which reads the whole chunk.
func TestSingleScanDenseDoneDoesNotWaitForHistogram(t *testing.T) {
	e := newTestEnv(t, 3)
	w := &doneWatcher{ResponseRecorder: httptest.NewRecorder(), done: make(chan struct{})}
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		switch {
		case isHistogramSQL(sql) && strings.Contains(sql, chunk0Window):
			select {
			case <-w.done:
			case <-time.After(5 * time.Second):
				t.Error("the done frame waited for the newest chunk's histogram")
			}
			return []map[string]interface{}{{"bucket": "2026-10-01 23:45:00", "cnt": uint64(3)}}, nil
		case isHistogramSQL(sql):
			return []map[string]interface{}{}, nil
		case strings.Contains(sql, chunk0Window):
			return logRows(3, testEndTime, "f1"), nil
		}
		return []map[string]interface{}{}, nil
	})
	e.h.HandleQueryStream(w, testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: "level=error", FractalID: "f1"}), adminCtx()))
	frames := decodeFrames(t, w.ResponseRecorder)
	kinds := strings.Join(frameKinds(frames), " ")
	if !strings.HasPrefix(kinds, "meta rows done histogram") {
		t.Errorf("frames = %s", kinds)
	}
	if hist := framesOf(frames, "histogram"); len(hist) != 4 || hist[3]["complete"] != true {
		t.Errorf("histogram frames = %v", hist)
	}
}

// The oldest chunk's rows start at the requested start, not at the bucket the
// histogram snapped it to.
func TestSingleScanClipsRowsToRequestedStart(t *testing.T) {
	e := newTestEnv(t, 1000)
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		switch {
		case isNewestSQL(sql):
			switch {
			case strings.Contains(sql, chunk1Window):
				return newestScan(16, nil), nil
			case strings.Contains(sql, chunk2Window):
				return newestScan(32, nil), nil
			}
			return newestScan(40, nil), nil
		}
		return []map[string]interface{}{}, nil
	})
	runStream(t, e, QueryRequest{Query: "level=error", FractalID: "f1", Start: "2026-10-01T00:07:00Z", End: testEnd})
	newest := queriesWhere(e, isNewestSQL)
	if len(newest) != 3 {
		t.Fatalf("single scans = %d", len(newest))
	}
	oldest := newest[2]
	if !strings.Contains(oldest, chunk3Window) ||
		!strings.Contains(oldest, "timestamp >= '2026-10-01 00:07:00.000' AND timestamp < '2026-10-01 10:00:00.000') AS newest_ms") {
		t.Errorf("oldest chunk not clipped to the requested start: %s", oldest)
	}
}

// A scan projecting a value as timestamp shadows the event time the single scan
// collects, so such a search keeps its plain row chunks.
func TestSingleScanSkipsShadowedTimestamp(t *testing.T) {
	e := newTestEnv(t, 1000)
	e.respond(func(sql string) ([]map[string]interface{}, error) { return []map[string]interface{}{}, nil })
	runStream(t, e, QueryRequest{Query: "level=error | lowercase(user, timestamp)", FractalID: "f1"})
	if n := len(queriesWhere(e, func(q string) bool { return isNewestSQL(q) || isFetchSQL(q) })); n != 0 {
		t.Errorf("a shadowed timestamp used the single scan (%d queries)", n)
	}
	if h := e.ch.QueriedMatching("AS bucket"); len(h) != 4 {
		t.Errorf("plain histogram scans = %d, want every chunk", len(h))
	}
}

// With no deployment row cap the response is a cursor page: older chunks are
// scanned until the page fills, and the row past it sets has_more.
func TestSingleScanUncappedFillsCursorPage(t *testing.T) {
	e := newTestEnv(t, 0)
	var older []string
	for i := 0; i < 60; i++ {
		older = append(older, time.Date(2026, 10, 1, 21, 0, 0, 0, time.UTC).Add(-time.Duration(i)*time.Second).Format("2006-01-02 15:04:05.000"))
	}
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		switch {
		case isHistogramSQL(sql):
			return []map[string]interface{}{}, nil
		case isNewestSQL(sql):
			if !strings.Contains(sql, chunk1Window) || !strings.Contains(sql, "groupArraySortedIf(51)") {
				t.Errorf("single scan: %s", sql)
			}
			return newestScan(16, map[int]uint64{11: 60}, older...), nil
		case isFetchSQL(sql):
			rows := make([]map[string]interface{}, len(older))
			for i, ts := range older {
				rows[i] = logRow(ts, "lid-"+ts, "f1")
			}
			return rows, nil
		case strings.Contains(sql, chunk0Window):
			return logRows(150, testEndTime, "f1"), nil
		}
		return []map[string]interface{}{}, nil
	})
	frames := runStream(t, e, QueryRequest{Query: "level=error", FractalID: "f1"})
	if got := len(streamedRows(frames)); got != cursorPageSize {
		t.Fatalf("streamed %d rows, want a page of %d", got, cursorPageSize)
	}
	done := framesOf(frames, "done")[0]
	cur, err := decodeCursor(done["next_cursor"].(string))
	if done["has_more"] != true || err != nil || cur.LID != "lid-"+older[49] {
		t.Errorf("done = %v (cursor %+v, %v)", done, cur, err)
	}
	if n := len(queriesWhere(e, isNewestSQL)); n != 1 {
		t.Errorf("single scans = %d, want none past the full page", n)
	}
}

// A histogram answered from the per-minute rollup costs no log scan, so there is
// nothing to share: an unfiltered search keeps its plain row chunks.
func TestSingleScanSkipsRollupHistogram(t *testing.T) {
	e := newTestEnv(t, 1000)
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		if isHistogramSQL(sql) {
			return []map[string]interface{}{}, nil
		}
		return []map[string]interface{}{}, nil
	})
	runStream(t, e, QueryRequest{Query: "*", FractalID: "f1"})
	if n := len(queriesWhere(e, func(q string) bool { return isNewestSQL(q) || isFetchSQL(q) })); n != 0 {
		t.Errorf("an unfiltered search used the single scan (%d queries)", n)
	}
	if h := e.ch.QueriedMatching("logs_histogram"); len(h) == 0 {
		t.Error("the rollup histogram was not used")
	}
}
