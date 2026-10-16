package query

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// These tests run the buffered search handler past preparation, against the
// scripted ClickHouse driver: what comes back, how the histogram lands, how
// ClickHouse failures reach the user, and what is left behind server-side.

func runQuery(t *testing.T, e *testEnv, req QueryRequest, vals ctxVals) (*httptest.ResponseRecorder, QueryResponse) {
	t.Helper()
	rec := httptest.NewRecorder()
	e.h.HandleQuery(rec, testRequest(http.MethodPost, searchBody(t, req), vals))
	return rec, decodeQueryResponse(t, rec)
}

var testEndTime = time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)

func TestHandleQueryReturnsRowsAndHistogram(t *testing.T) {
	e := newTestEnv(t, 1000)
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		if isHistogramSQL(sql) {
			// The last quarter hour of the window, and one row outside it that a
			// misaligned fold would write past the end of the array.
			return []map[string]interface{}{
				{"bucket": "2026-10-01 23:45:00", "cnt": uint64(7)},
				{"bucket": "2026-10-01 00:00:00", "cnt": uint64(2)},
				{"bucket": "2026-10-02 00:15:00", "cnt": uint64(99)},
			}, nil
		}
		return logRows(3, testEndTime, "f1"), nil
	})

	rec, resp := runQuery(t, e, QueryRequest{Query: "level=error", FractalID: "f1"}, adminCtx())
	if rec.Code != http.StatusOK || !resp.Success {
		t.Fatalf("got %d %+v", rec.Code, resp)
	}
	if resp.Count != 3 || len(resp.Results) != 3 || resp.Results[0]["log_id"] == "" {
		t.Errorf("results = %d %v", resp.Count, resp.Results)
	}
	if resp.LimitHit != "" || resp.HasMore || resp.NextCursor != "" {
		t.Errorf("limit_hit=%q has_more=%v cursor=%q for a short result", resp.LimitHit, resp.HasMore, resp.NextCursor)
	}
	if !strings.Contains(resp.SQL, "fields.`level`::String = 'error'") || !strings.Contains(resp.SQL, "fractal_id = 'f1'") {
		t.Errorf("sql = %s", resp.SQL)
	}
	if len(resp.Histogram) != 96 || resp.Histogram[95] != 7 || resp.Histogram[0] != 2 || resp.BucketSec != 900 {
		t.Errorf("histogram = %v (bucket %ds)", resp.Histogram, resp.BucketSec)
	}
	if resp.TimeStart != "2026-10-01T00:00:00Z" || resp.TimeEnd != "2026-10-02T00:00:00Z" {
		t.Errorf("histogram window %s..%s", resp.TimeStart, resp.TimeEnd)
	}
	// The buffered path's single-shot histogram is the profiler's view of the
	// query's cost, so it must never be served from the query cache.
	for _, q := range e.ch.QueriedMatching("AS bucket") {
		if strings.Contains(q, "use_query_cache") {
			t.Errorf("buffered histogram cached: %s", q)
		}
	}
}

// limit_hit tells the UI the result was cut at the deployment cap, and whether the
// cap came from a full-text regex scan (which the UI explains differently).
func TestHandleQueryReportsRowCap(t *testing.T) {
	for q, want := range map[string]string{"level=error": "search", "/denied/": "bloom"} {
		t.Run(q, func(t *testing.T) {
			e := newTestEnv(t, 3)
			e.respond(func(sql string) ([]map[string]interface{}, error) {
				if isHistogramSQL(sql) {
					return []map[string]interface{}{}, nil
				}
				return logRows(3, testEndTime, "f1"), nil
			})
			_, resp := runQuery(t, e, QueryRequest{Query: q, FractalID: "f1"}, adminCtx())
			if resp.LimitHit != want {
				t.Errorf("limit_hit = %q, want %q", resp.LimitHit, want)
			}
		})
	}
}

// With no deployment row cap the search pages by cursor: one row past the page
// says there is more, and the cursor resumes after the last row returned.
func TestHandleQueryCursorPage(t *testing.T) {
	e := newTestEnv(t, 0)
	rows := logRows(cursorPageSize+1, testEndTime, "f1")
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		if isHistogramSQL(sql) {
			return []map[string]interface{}{}, nil
		}
		return rows, nil
	})
	_, resp := runQuery(t, e, QueryRequest{Query: "level=error", FractalID: "f1"}, adminCtx())
	if resp.Count != cursorPageSize || !resp.HasMore {
		t.Fatalf("count=%d has_more=%v", resp.Count, resp.HasMore)
	}
	cur, err := decodeCursor(resp.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	last := rows[cursorPageSize-1]
	if cur.LID != last["log_id"] || cur.TSMilli != parseLogTimestamp(last["timestamp"].(string)).UnixMilli() {
		t.Errorf("cursor %+v does not resume after the last returned row %v", cur, last)
	}
	if resp.LimitHit != "" {
		t.Errorf("a cursor page reported limit_hit %q", resp.LimitHit)
	}
}

// ClickHouse exception text can carry column names and sampled values, so it never
// reaches the user: known codes get a fixed message, others only their number.
func TestHandleQueryExecutionErrors(t *testing.T) {
	const secret = "Column `fields.ssn` value '123-45-6789'"
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"mapped code", fakedb.Exception(241, secret), "Query exceeded memory limits"},
		{"unmapped code", fakedb.Exception(999, secret), "Query execution failed (ClickHouse error 999)"},
		{"not a server exception", errors.New(secret), "Query execution failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEnv(t, 1000)
			e.respond(func(sql string) ([]map[string]interface{}, error) {
				if isHistogramSQL(sql) {
					return []map[string]interface{}{}, nil
				}
				return nil, tc.err
			})
			rec, resp := runQuery(t, e, QueryRequest{Query: "level=error", FractalID: "f1"}, adminCtx())
			if rec.Code != http.StatusInternalServerError || resp.Success || resp.ErrorType != "execution" {
				t.Fatalf("got %d %+v", rec.Code, resp)
			}
			if !strings.HasPrefix(resp.Error, tc.want) || strings.Contains(rec.Body.String(), "123-45-6789") {
				t.Errorf("error = %q, want %q and no exception text", resp.Error, tc.want)
			}
			if resp.SQL != "" {
				t.Errorf("a failed query echoed its SQL: %s", resp.SQL)
			}
		})
	}
}

// The timeline is non-essential: a failed histogram leaves the rows intact.
func TestHandleQueryHistogramFailureIsNotFatal(t *testing.T) {
	e := newTestEnv(t, 1000)
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		if isHistogramSQL(sql) {
			return nil, fakedb.Exception(241, "oom")
		}
		return logRows(2, testEndTime, "f1"), nil
	})
	rec, resp := runQuery(t, e, QueryRequest{Query: "level=error", FractalID: "f1"}, adminCtx())
	if rec.Code != http.StatusOK || resp.Count != 2 || resp.Histogram != nil || resp.BucketSec != 0 {
		t.Errorf("got %d count=%d histogram=%v", rec.Code, resp.Count, resp.Histogram)
	}
}

// killedSearches returns the KILL statements issued so far.
func killedSearches(e *testEnv) []string { return e.ch.ExecutedMatching("KILL QUERY") }

// A histogram still running when the rows are back is dropped from the response
// and, since nothing will read it, killed server-side by the search's id prefix.
func TestHandleQueryDropsAndKillsSlowHistogram(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t, 1000)
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		if isHistogramSQL(sql) {
			time.Sleep(800 * time.Millisecond)
			return []map[string]interface{}{}, nil
		}
		return logRows(1, testEndTime, "f1"), nil
	})
	rec, resp := runQuery(t, e, QueryRequest{Query: "level=error", FractalID: "f1"}, adminCtx())
	if rec.Code != http.StatusOK || resp.Count != 1 || resp.Histogram != nil {
		t.Fatalf("got %d count=%d histogram=%v", rec.Code, resp.Count, resp.Histogram)
	}
	waitFor(t, "the abandoned histogram to be killed", func() bool { return len(killedSearches(e)) > 0 })
	if k := killedSearches(e)[0]; !strings.Contains(k, "startsWith(query_id, 'bif-search-") || !strings.HasSuffix(k, "ASYNC") {
		t.Errorf("kill = %s", k)
	}
}

// A search that completes normally leaves nothing to kill.
func TestHandleQueryCleanSearchKillsNothing(t *testing.T) {
	e := newTestEnv(t, 1000)
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		if isHistogramSQL(sql) {
			return []map[string]interface{}{}, nil
		}
		return logRows(1, testEndTime, "f1"), nil
	})
	runQuery(t, e, QueryRequest{Query: "level=error", FractalID: "f1"}, adminCtx())
	time.Sleep(20 * time.Millisecond) // the kill is detached; give a wrong one time to land
	if k := killedSearches(e); len(k) != 0 {
		t.Errorf("clean search issued %v", k)
	}
}

// A client that left gets no response, and its search is killed rather than left
// scanning for nobody.
func TestHandleQueryClientGone(t *testing.T) {
	e := newTestEnv(t, 1000)
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		if isHistogramSQL(sql) {
			return []map[string]interface{}{}, nil
		}
		return nil, context.Canceled
	})
	r := testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: "level=error", FractalID: "f1"}), adminCtx())
	ctx, cancel := context.WithCancel(r.Context())
	r = r.WithContext(ctx)
	cancel()
	rec := httptest.NewRecorder()
	e.h.HandleQuery(rec, r)
	if rec.Body.Len() != 0 {
		t.Errorf("wrote a response to a departed client: %s", rec.Body.String())
	}
	waitFor(t, "the search to be killed", func() bool { return len(killedSearches(e)) > 0 })
}

// Every search lands in the audit fractal with who ran what, where, and how much
// came back.
func TestHandleQueryWritesAuditRecord(t *testing.T) {
	e := newTestEnv(t, 1000)
	e.set(func(e *testEnv) { e.auditFractal = "faudit" })
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		if isHistogramSQL(sql) {
			return []map[string]interface{}{}, nil
		}
		return logRows(4, testEndTime, "f1"), nil
	})
	runQuery(t, e, QueryRequest{Query: "level=error", FractalID: "f1"}, ctxVals{"user": &storage.User{Username: "ana", IsAdmin: true}})

	// The logs_raw batch is prepared only after the logs batch is sent, so once it
	// exists the audit row is complete.
	waitFor(t, "the audit write", func() bool { return len(e.ch.Batches()) >= 2 })
	b := e.ch.Batches()[0]
	if !strings.HasPrefix(b.Query, "INSERT INTO logs (") || !b.Sent || len(b.Appended) != 1 {
		t.Fatalf("audit batch = %q sent=%v rows=%d", b.Query, b.Sent, len(b.Appended))
	}
	row := b.Appended[0]
	fields := row[2].(map[string]string)
	if row[3] != "faudit" || fields["user"] != "ana" || fields["fractal_id"] != "f1" ||
		fields["result_count"] != "4" || fields["query"] != "level=error" || fields["event"] != "query" {
		t.Errorf("audit row fractal=%v fields=%v", row[3], fields)
	}
}

// The response size guard: a result estimated past 50MB is cut to 1000 rows with
// the cut said out loud, rather than a body the browser cannot parse. The search
// is still audited: the audit trail cannot depend on how big the answer was.
func TestHandleQueryTruncatesHugeResults(t *testing.T) {
	e := newTestEnv(t, 2000)
	e.set(func(e *testEnv) { e.auditFractal = "faudit" })
	big := strings.Repeat("x", 52*1024)
	rows := logRows(1001, testEndTime, "f1")
	for _, r := range rows {
		r["norm_log"] = big
	}
	e.respond(func(sql string) ([]map[string]interface{}, error) {
		if isHistogramSQL(sql) {
			return []map[string]interface{}{}, nil
		}
		return rows, nil
	})
	_, resp := runQuery(t, e, QueryRequest{Query: "level=error", FractalID: "f1"}, adminCtx())
	if !resp.Success || resp.Count != 1000 || resp.LimitHit != "truncated" || !strings.Contains(resp.Error, "truncated to 1000 rows") {
		t.Errorf("success=%v count=%d limit_hit=%q error=%q", resp.Success, resp.Count, resp.LimitHit, resp.Error)
	}
	if resp.BucketSec != 900 || len(resp.Histogram) != 96 {
		t.Errorf("truncated response lost its histogram: %d buckets of %ds", len(resp.Histogram), resp.BucketSec)
	}
	waitFor(t, "the audit write", func() bool { return len(e.ch.Batches()) >= 2 })
	if f := e.ch.Batches()[0].Appended[0][2].(map[string]string); f["result_count"] != "1000" {
		t.Errorf("audit fields = %v", f)
	}
}

// Profiling reads the query's per-shard cost back from system.query_log once the
// query is logged there.
func TestHandleQueryProfile(t *testing.T) {
	t.Parallel()
	for _, cluster := range []bool{false, true} {
		name := "single node"
		if cluster {
			name = "cluster"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db, ch := storagetest.ClickHouse()
			if cluster {
				topo, err := storage.DeriveTopology(storage.TopologySpec{Cluster: "bf"})
				if err != nil {
					t.Fatal(err)
				}
				db, ch = storagetest.ClickHouseWith(topo)
			}
			e := newTestEnvOn(t, 1000, db, ch)
			e.respond(func(sql string) ([]map[string]interface{}, error) {
				switch {
				case strings.Contains(sql, "SELECT count() AS cnt FROM system.query_log"):
					if !strings.Contains(sql, "-main'") {
						t.Errorf("profile polls the wrong query id: %s", sql)
					}
					return []map[string]interface{}{{"cnt": uint64(1)}}, nil
				case strings.Contains(sql, "AS marks_read"):
					return []map[string]interface{}{{"shard": "n2", "marks_read": uint64(5), "marks_skipped": uint64(15), "total_marks": uint64(20), "pct_marks_surviving": 25.0}}, nil
				case strings.Contains(sql, "AS bytes_from_disk"):
					return []map[string]interface{}{
						{"shard": "n1", "coordinator": uint64(1), "duration_ms": uint64(40), "read_bytes": "1.00 MiB", "read_rows": uint64(100), "marks_selected": uint64(3)},
					}, nil
				case isHistogramSQL(sql):
					return []map[string]interface{}{}, nil
				}
				return logRows(1, testEndTime, "f1"), nil
			})
			_, resp := runQuery(t, e, QueryRequest{Query: "level=error", FractalID: "f1", Profile: true}, adminCtx())
			p := resp.Profile
			if p == nil || !strings.HasPrefix(p.QueryID, "bif-search-") || len(p.Shards) != 1 {
				t.Fatalf("profile = %+v", p)
			}
			if s := p.Shards[0]; s.Shard != "n1" || s.Coordinator != 1 || s.DurationMs != 40 || s.ReadBytes != "1.00 MiB" || s.MarksSelected != 3 {
				t.Errorf("shard = %+v", s)
			}
			// Skip-index effectiveness only means something when the query fanned out.
			if cluster != (len(p.SkipIndex) == 1) {
				t.Errorf("skip index = %+v on cluster=%v", p.SkipIndex, cluster)
			}
			if cluster && (p.SkipIndex[0].PctMarksSurviving != 25 || p.SkipIndex[0].TotalMarks != 20) {
				t.Errorf("skip index row = %+v", p.SkipIndex[0])
			}
		})
	}
}
