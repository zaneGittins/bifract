package query

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
)

// These tests drive the NDJSON streaming search against the scripted driver and
// pin its frame protocol: meta first, rows newest-first, one done frame carrying
// the outcome, histogram frames that fill in newest-first and end complete (or say
// they did not), and an error frame in place of done when the scan fails.
//
// The row-chunk tests search "*": its histogram reads the per-minute rollup, so
// its rows keep their own chunks. A filtered search shares its scans with the
// histogram instead (search_singlescan_test.go).

func runStream(t *testing.T, e *testEnv, req QueryRequest) []frame {
	t.Helper()
	rec := httptest.NewRecorder()
	e.h.HandleQueryStream(rec, testRequest(http.MethodPost, searchBody(t, req), adminCtx()))
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/x-ndjson" {
		t.Fatalf("status %d content-type %q: %s", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}
	return decodeFrames(t, rec)
}

// streamedRows concatenates the rows frames.
func streamedRows(frames []frame) []map[string]interface{} {
	var out []map[string]interface{}
	for _, f := range framesOf(frames, "rows") {
		for _, r := range f["data"].([]interface{}) {
			out = append(out, r.(map[string]interface{}))
		}
	}
	return out
}

// rowScans returns the row-scan queries (not histogram chunks) in the order issued.
func rowScans(ch *fakedb.CH) []string {
	var out []string
	for _, q := range ch.Queried() {
		if !isHistogramSQL(q) {
			out = append(out, q)
		}
	}
	return out
}

// The newest stream chunk of the 24h test window starts here; older chunks
// double in span behind it (see streamChunks).
const newestChunk = "timestamp >= '2026-10-01 22:30:00.000'"
const secondChunk = "timestamp >= '2026-10-01 19:30:00.000'"

func TestHandleQueryStreamProtocol(t *testing.T) {
	e := newTestEnv(t, 1000)
	newest := logRows(3, testEndTime, "f1")
	older := logRows(2, testEndTime.Add(-2*time.Hour), "f1") // 22:00, inside the second chunk
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		if isHistogramSQL(sql) {
			// One count per chunk, in the chunk's own newest bucket.
			switch {
			case strings.Contains(sql, "minute >= '2026-10-01 22:00:00'"):
				return []map[string]interface{}{{"bucket": "2026-10-01 23:45:00", "cnt": uint64(3)}}, nil
			case strings.Contains(sql, "minute >= '2026-10-01 18:00:00'"):
				return []map[string]interface{}{{"bucket": "2026-10-01 21:45:00", "cnt": uint64(2)}}, nil
			}
			return []map[string]interface{}{}, nil
		}
		switch {
		case strings.Contains(sql, newestChunk):
			return newest, nil
		case strings.Contains(sql, secondChunk):
			return older, nil
		}
		return []map[string]interface{}{}, nil
	})

	frames := runStream(t, e, QueryRequest{Query: "*", FractalID: "f1"})
	kinds := frameKinds(frames)
	if kinds[0] != "meta" {
		t.Fatalf("first frame %q, want meta (%v)", kinds[0], kinds)
	}
	meta := frames[0]
	if meta["streaming"] != true || !strings.Contains(meta["sql"].(string), "fractal_id = 'f1'") {
		t.Errorf("meta = %v", meta)
	}
	if hm, _ := meta["histogram_meta"].(map[string]interface{}); hm == nil || hm["bucket_count"] != 96.0 || hm["bucket_seconds"] != 900.0 {
		t.Errorf("histogram_meta = %v", meta["histogram_meta"])
	}

	// Rows arrive newest-first across chunks, and all of them before done.
	doneAt := -1
	for i, k := range kinds {
		switch k {
		case "done":
			if doneAt >= 0 {
				t.Fatalf("two done frames: %v", kinds)
			}
			doneAt = i
		case "rows":
			if doneAt >= 0 {
				t.Errorf("rows after done: %v", kinds)
			}
		case "error", "histogram_end":
			t.Errorf("unexpected %s frame: %v", k, frames)
		}
	}
	rows := streamedRows(frames)
	if len(rows) != 5 {
		t.Fatalf("streamed %d rows, want 5", len(rows))
	}
	if !sort.SliceIsSorted(rows, func(i, j int) bool { return rows[i]["timestamp"].(string) > rows[j]["timestamp"].(string) }) {
		t.Errorf("rows not newest-first: %v", rows)
	}
	done := frames[doneAt]
	if done["count"] != 5.0 || done["has_more"] != false || done["limit_hit"] != "" || done["next_cursor"] != "" {
		t.Errorf("done = %v", done)
	}

	// Every chunk was scanned, newest first, the older ones with an exclusive upper
	// bound so adjacent chunks never count a row twice.
	scans := rowScans(e.ch)
	if len(scans) != 4 || !strings.Contains(scans[0], newestChunk) || !strings.Contains(scans[1], secondChunk) {
		t.Fatalf("scans = %d, first %s", len(scans), scans)
	}
	if strings.Contains(scans[0], "timestamp < '") || !strings.Contains(scans[1], "timestamp < '2026-10-01 22:30:00.000'") {
		t.Errorf("chunk bounds: newest %q, second %q", scans[0], scans[1])
	}

	// The histogram fills right to left, cumulatively, and ends complete.
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
	buckets := last["buckets"].([]interface{})
	if last["complete"] != true || last["covered_from"] != 0.0 || buckets[95] != 3.0 || buckets[87] != 2.0 {
		t.Errorf("final histogram complete=%v from=%v [95]=%v [87]=%v", last["complete"], last["covered_from"], buckets[95], buckets[87])
	}
	if first := hist[0]["buckets"].([]interface{}); first[87] != 0.0 || hist[0]["complete"] != false {
		t.Errorf("first frame already holds older chunks: %v", hist[0])
	}

	// Only historical chunks may come from the query cache: the newest one is
	// where fresh logs land.
	histQ := e.ch.QueriedMatching("AS bucket")
	for i, q := range histQ {
		if cached := strings.Contains(q, "use_query_cache = 1"); cached != (i > 0) {
			t.Errorf("histogram chunk %d cached=%v: %s", i, cached, q)
		}
	}
}

// Each chunk carries only the remaining row budget, and a chunk that fills the
// budget ends the scan: older chunks are never queried.
func TestHandleQueryStreamRowBudget(t *testing.T) {
	e := newTestEnv(t, 3)
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		switch {
		case isHistogramSQL(sql):
			return []map[string]interface{}{}, nil
		case strings.Contains(sql, newestChunk):
			return logRows(2, testEndTime, "f1"), nil
		case strings.Contains(sql, secondChunk):
			return logRows(1, testEndTime.Add(-2*time.Hour), "f1"), nil
		}
		t.Errorf("scanned past a full budget: %s", sql)
		return []map[string]interface{}{}, nil
	})
	frames := runStream(t, e, QueryRequest{Query: "*", FractalID: "f1"})
	scans := rowScans(e.ch)
	if len(scans) != 2 || !strings.HasSuffix(scans[0], "LIMIT 3") || !strings.HasSuffix(scans[1], "LIMIT 1") {
		t.Errorf("scans = %v", scans)
	}
	done := framesOf(frames, "done")
	if len(done) != 1 || done[0]["count"] != 3.0 || done[0]["limit_hit"] != "search" {
		t.Errorf("done = %v", done)
	}
}

// With no deployment row cap the response is a cursor page, so a sparse search
// keeps scanning older chunks rather than stopping after the newest.
func TestHandleQueryStreamUncappedScansEveryChunk(t *testing.T) {
	e := newTestEnv(t, 0)
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		switch {
		case isHistogramSQL(sql):
			return []map[string]interface{}{}, nil
		case strings.Contains(sql, newestChunk):
			return logRows(1, testEndTime, "f1"), nil
		case strings.Contains(sql, secondChunk):
			return logRows(1, testEndTime.Add(-2*time.Hour), "f1"), nil
		}
		return []map[string]interface{}{}, nil
	})
	frames := runStream(t, e, QueryRequest{Query: "*", FractalID: "f1"})
	if scans := rowScans(e.ch); len(scans) != 4 {
		t.Errorf("row scans = %d, want every chunk", len(scans))
	}
	if got := len(streamedRows(frames)); got != 2 {
		t.Errorf("streamed %d rows, want 2", got)
	}
	if done := framesOf(frames, "done")[0]; done["has_more"] != false {
		t.Errorf("done = %v", done)
	}
}

// Aggregations and explicit sorts cannot stream: they arrive as one rows frame
// through the same protocol.
func TestHandleQueryStreamBuffered(t *testing.T) {
	e := newTestEnv(t, 1000)
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		if isHistogramSQL(sql) {
			return []map[string]interface{}{{"bucket": "2026-10-01 00:00:00", "cnt": uint64(4)}}, nil
		}
		return logRows(4, testEndTime, "f1"), nil
	})
	frames := runStream(t, e, QueryRequest{Query: "level=error | sort(user)", FractalID: "f1"})
	if frames[0]["streaming"] != false {
		t.Errorf("meta = %v", frames[0])
	}
	if got := framesOf(frames, "rows"); len(got) != 1 || len(got[0]["data"].([]interface{})) != 4 {
		t.Errorf("rows frames = %v", got)
	}
	if scans := rowScans(e.ch); len(scans) != 1 {
		t.Errorf("a buffered query was chunked: %v", scans)
	}
	done := framesOf(frames, "done")
	if len(done) != 1 || done[0]["count"] != 4.0 {
		t.Errorf("done = %v", done)
	}
	if h := framesOf(frames, "histogram"); len(h) == 0 || h[len(h)-1]["complete"] != true {
		t.Errorf("histogram frames = %v", h)
	}
}

// With no deployment cap the stream pages by cursor: the row past the page marks
// has_more, and the cursor resumes after the last row sent.
func TestHandleQueryStreamCursorPage(t *testing.T) {
	e := newTestEnv(t, 0)
	rows := logRows(cursorPageSize+1, testEndTime, "f1")
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		if isHistogramSQL(sql) {
			return []map[string]interface{}{}, nil
		}
		return rows, nil
	})
	frames := runStream(t, e, QueryRequest{Query: "level=error", FractalID: "f1"})
	if got := len(streamedRows(frames)); got != cursorPageSize {
		t.Errorf("streamed %d rows, want a page of %d", got, cursorPageSize)
	}
	done := framesOf(frames, "done")[0]
	cur, err := decodeCursor(done["next_cursor"].(string))
	if done["has_more"] != true || err != nil || cur.LID != rows[cursorPageSize-1]["log_id"] {
		t.Errorf("done = %v (cursor %+v, %v)", done, cur, err)
	}
	if done["limit_hit"] != "" {
		t.Errorf("a cursor page reported limit_hit %v", done["limit_hit"])
	}
}

// A failed scan ends the stream with an error frame and no done frame, worded
// without the exception text.
func TestHandleQueryStreamErrorFrame(t *testing.T) {
	for name, q := range map[string]string{"streamed": "level=error", "buffered": "level=error | count()"} {
		t.Run(name, func(t *testing.T) {
			e := newTestEnv(t, 1000)
			e.respond(func(sql string) ([]map[string]interface{}, error) {
				if isHistogramSQL(sql) {
					return []map[string]interface{}{}, nil
				}
				return nil, fakedb.Exception(47, "Unknown identifier secret_col")
			})
			frames := runStream(t, e, QueryRequest{Query: q, FractalID: "f1"})
			kinds := frameKinds(frames)
			last := frames[len(frames)-1]
			if last.kind() != "error" || last["error_type"] != "execution" || !strings.Contains(last["error"].(string), "unknown field") {
				t.Errorf("frames %v, last %v", kinds, last)
			}
			if len(framesOf(frames, "done")) != 0 || strings.Contains(last["error"].(string), "secret_col") {
				t.Errorf("frames = %v", frames)
			}
		})
	}
}

// A histogram chunk that fails keeps what earlier chunks delivered and says the
// timeline is incomplete, so the client marks the rest unscanned instead of zero.
func TestHandleQueryStreamPartialHistogram(t *testing.T) {
	e := newTestEnv(t, 1000)
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		if isHistogramSQL(sql) {
			if strings.Contains(sql, "use_query_cache") {
				return nil, fakedb.Exception(241, "oom")
			}
			return []map[string]interface{}{{"bucket": "2026-10-01 23:45:00", "cnt": uint64(1)}}, nil
		}
		return logRows(1, testEndTime, "f1"), nil
	})
	frames := runStream(t, e, QueryRequest{Query: "*", FractalID: "f1"})
	hist := framesOf(frames, "histogram")
	if len(hist) != 1 || hist[0]["complete"] != false {
		t.Errorf("histogram frames = %v", hist)
	}
	if last := frames[len(frames)-1]; last.kind() != "histogram_end" || last["complete"] != false {
		t.Errorf("last frame = %v (%v)", last, frameKinds(frames))
	}
	if len(framesOf(frames, "done")) != 1 {
		t.Errorf("rows were fine but no done frame: %v", frameKinds(frames))
	}
}

// A client that leaves mid-scan gets no further frames, and its queries are killed.
func TestHandleQueryStreamClientGone(t *testing.T) {
	e := newTestEnv(t, 1000)
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		if isHistogramSQL(sql) {
			return []map[string]interface{}{}, nil
		}
		return nil, context.Canceled
	})
	r := testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: "level=error", FractalID: "f1"}), adminCtx())
	ctx, cancel := context.WithCancel(r.Context())
	cancel()
	rec := httptest.NewRecorder()
	e.h.HandleQueryStream(rec, r.WithContext(ctx))
	if kinds := frameKinds(decodeFrames(t, rec)); len(kinds) != 1 || kinds[0] != "meta" {
		t.Errorf("frames after the client left: %v", kinds)
	}
	waitFor(t, "the search to be killed", func() bool { return len(killedSearches(e)) > 0 })
}

type noFlushWriter struct{ http.ResponseWriter }

func TestHandleQueryStreamNeedsFlusher(t *testing.T) {
	e := newTestEnv(t, 1000)
	rec := httptest.NewRecorder()
	e.h.HandleQueryStream(noFlushWriter{rec}, testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: "level=error", FractalID: "f1"}), adminCtx()))
	if rec.Code != http.StatusInternalServerError || len(e.ch.Queried()) != 0 {
		t.Errorf("got %d after %d queries", rec.Code, len(e.ch.Queried()))
	}
}

// Notices about what a query could not cover reach the streamed search too, in the meta frame,
// so the client can flag the result before rows arrive: here pgr() hitting its row limit.
func TestHandleQueryStreamCarriesWarnings(t *testing.T) {
	e := newTestEnv(t, 1000)
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		switch {
		case strings.HasPrefix(sql, "SELECT process_guid, parent_guid, toUnixTimestamp64Milli"):
			return []map[string]interface{}{{"process_guid": "root", "parent_guid": "", "ts_ms": int64(1)}}, nil
		case strings.HasPrefix(sql, "SELECT * FROM (") && strings.Contains(sql, ") AS _d ORDER BY"):
			return []map[string]interface{}{spawnRow("root", "a"), spawnRow("root", "b")}, nil
		}
		return nil, nil
	})
	frames := runStream(t, e, QueryRequest{Query: `pgr(start="root", limit=1, reconnect=false)`, FractalID: "f1"})
	ws, _ := frames[0]["warnings"].([]interface{})
	if frames[0]["type"] != "meta" || len(ws) != 1 || !strings.Contains(ws[0].(string), "row limit reached") {
		t.Errorf("meta = %v", frames[0])
	}
}

// A query whose rows depend on the others in range (dedup, limit, a median-based
// score) reads it in one scan: per-chunk copies would each repeat the selection,
// returning a dedup key once per chunk or limit(n) rows per chunk, or score each
// chunk against its own median.
func TestHandleQueryStreamRangeDependentRowsScanOnce(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  int
	}{
		{"* | dedup(user)", 2},
		{"* | limit(2)", 2},
		{"* | madoutlier(bytes)", 2},
	} {
		t.Run(tc.query, func(t *testing.T) {
			e := newTestEnv(t, 1000)
			e.respond(func(sql string) ([]map[string]interface{}, error) {
				if isHistogramSQL(sql) {
					return []map[string]interface{}{}, nil
				}
				rows := logRows(2, testEndTime, "f1")
				rows[0]["user"], rows[1]["user"] = "alice", "bob"
				return rows, nil
			})

			rows := streamedRows(runStream(t, e, QueryRequest{Query: tc.query, FractalID: "f1"}))
			if scans := rowScans(e.ch); len(scans) != 1 {
				t.Fatalf("%d row scans, want 1: %v", len(scans), scans)
			}
			if len(rows) != tc.want {
				t.Errorf("streamed %d rows, want %d: %v", len(rows), tc.want, rows)
			}
		})
	}
}
