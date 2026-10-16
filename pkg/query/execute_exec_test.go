package query

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/api"
	"bifract/pkg/parser"
)

// The server-side execution paths built on the same resolution as a search:
// ExecuteBQL (dashboards, notebooks), field statistics, and export.

var execWindow = ExecuteScope{Start: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), End: testEndTime}

func TestExecuteBQLRunsInScope(t *testing.T) {
	cases := []struct {
		name      string
		scope     func(s ExecuteScope) ExecuteScope
		wantScope string
		wantArg   string // scope the dictionary/comment reads were made for
	}{
		{"fractal", func(s ExecuteScope) ExecuteScope { s.FractalID = "f1"; return s }, "fractal_id = 'f1'", "f1"},
		{"default fractal keeps legacy rows", func(s ExecuteScope) ExecuteScope { s.FractalID = "fdef"; return s }, "fractal_id IN ('fdef', '')", "fdef"},
		{"prism", func(s ExecuteScope) ExecuteScope { s.PrismID = "p2"; return s }, "fractal_id IN ('f1', 'fdef', '')", "p2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := scopeEnv(t)
			e.set(func(e *testEnv) {
				e.dictRows = [][]driver.Value{{"d1", "users", "name"}}
				e.commentIDs = []string{"L7"}
			})
			e.respond(func(sql string) ([]map[string]interface{}, error) {
				return []map[string]interface{}{{"tier": "gold", "_count": uint64(3)}}, nil
			})
			res, err := e.h.ExecuteBQL(context.Background(), `comment() | match(dict="users", field=user, column=name, include=[tier]) | groupby(tier)`, tc.scope(execWindow))
			if err != nil {
				t.Fatal(err)
			}
			if res.Count != 1 || res.Results[0]["tier"] != "gold" || !res.IsAggregated || res.FieldOrder == nil || res.ChartConfig == nil {
				t.Errorf("result = %+v", res)
			}
			if !strings.Contains(res.SQL, tc.wantScope) || !strings.Contains(res.SQL, "'L7'") || !strings.Contains(res.SQL, "logs.lookup_d1") {
				t.Errorf("sql = %s", res.SQL)
			}
			if q := e.ch.Queried(); len(q) != 1 || q[0] != res.SQL {
				t.Errorf("ran %v, reported %s", q, res.SQL)
			}
			for _, sub := range []string{"FROM dictionaries", "FROM comments"} {
				calls := e.pg.Matching(sub)
				if len(calls) != 1 || !argsContain(calls[0].Args, tc.wantArg) {
					t.Errorf("%s reads = %+v, want one for %s", sub, calls, tc.wantArg)
				}
			}
		})
	}
}

func argsContain(args []driver.Value, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

// A widget gets the first page of a raw search, stably ordered, when the
// deployment sets no row cap of its own.
func TestExecuteBQLPagesRawSearch(t *testing.T) {
	e := newTestEnv(t, 0)
	e.respond(func(string) ([]map[string]interface{}, error) { return logRows(2, testEndTime, "f1"), nil })
	s := execWindow
	s.FractalID = "f1"
	res, err := e.h.ExecuteBQL(context.Background(), "level=error", s)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(res.SQL, " LIMIT 200") || !strings.Contains(res.SQL, "ORDER BY timestamp DESC, log_id DESC") || res.Count != 2 {
		t.Errorf("sql = %s, count %d", res.SQL, res.Count)
	}
}

func TestExecuteBQLFailures(t *testing.T) {
	s := execWindow
	s.FractalID = "f1"
	t.Run("comment lookup", func(t *testing.T) {
		e := newTestEnv(t, 1000)
		e.set(func(e *testEnv) { e.commentErr = errors.New("pg down") })
		if _, err := e.h.ExecuteBQL(context.Background(), "comment()", s); err == nil || !strings.Contains(err.Error(), "failed to resolve comment filter") {
			t.Errorf("err = %v", err)
		}
		if len(e.ch.Queried()) != 0 {
			t.Error("ran without its comment filter")
		}
	})
	t.Run("execution", func(t *testing.T) {
		e := newTestEnv(t, 1000)
		e.respond(func(string) ([]map[string]interface{}, error) { return nil, fakedb.Exception(241, "oom") })
		if _, err := e.h.ExecuteBQL(context.Background(), "level=error", s); err == nil || !strings.Contains(err.Error(), "query execution failed") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("empty result is an empty list", func(t *testing.T) {
		e := newTestEnv(t, 1000)
		res, err := e.h.ExecuteBQL(context.Background(), "level=error", s)
		if err != nil || res.Results == nil || res.Count != 0 {
			t.Errorf("res = %+v, %v", res, err)
		}
	})
}

func runFieldStats(t *testing.T, e *testEnv, q string) FieldStatsResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	e.h.HandleFieldStats(rec, testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: q, FractalID: "f1"}), adminCtx()))
	var resp FieldStatsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("%d %s: %v", rec.Code, rec.Body.String(), err)
	}
	return resp
}

// Field statistics run their own sampled aggregation over the search's scope; the
// search itself is never executed for them.
func TestHandleFieldStats(t *testing.T) {
	e := newTestEnv(t, 1000)
	e.ch.On("", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "key", Type: "String"}, {Column: "present", Type: "UInt64"}, {Column: "cardinality", Type: "UInt64"}, {Column: "top_values", Type: "Array(String)"}, {Column: "top_counts", Type: "Array(UInt64)"}},
		Rows: [][]any{
			{"host", uint64(9), uint64(2), []string{"a", "b"}, []uint64{3, 6}},
			{"__rows__", uint64(10), uint64(0), []string{}, []uint64{}},
		},
	})
	resp := runFieldStats(t, e, "level=error")
	if !resp.Success || !resp.Supported || resp.SampleSize != 10 || len(resp.Fields) != 1 || resp.Fields[0].Top[0].Value != "b" {
		t.Errorf("resp = %+v", resp)
	}
	q := e.ch.Queried()
	if len(q) != 1 || !strings.Contains(q[0], "fractal_id = 'f1'") || !strings.Contains(q[0], "fields.`level`::String = 'error'") {
		t.Errorf("queries = %v", q)
	}

	e = newTestEnv(t, 1000)
	e.respond(func(string) ([]map[string]interface{}, error) { return nil, errors.New("boom") })
	if resp := runFieldStats(t, e, "level=error"); resp.Success || !resp.Supported {
		t.Errorf("failed stats = %+v", resp)
	}
}

func runExport(t *testing.T, e *testEnv, gz bool) *httptest.ResponseRecorder {
	t.Helper()
	r := testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: "level=error", FractalID: "f1"}), adminCtx())
	if gz {
		r.Header.Set("Accept-Encoding", "gzip")
	}
	rec := httptest.NewRecorder()
	e.h.HandleExport(rec, r)
	return rec
}

func exportRow(ts, id string) map[string]interface{} {
	return map[string]interface{}{"timestamp": ts, "log_id": id, "fractal_id": "f1", "norm_log": `{"level":"error"}`}
}

func TestHandleExportStreamsEveryRow(t *testing.T) {
	for _, gz := range []bool{false, true} {
		name := "plain"
		if gz {
			name = "gzip"
		}
		t.Run(name, func(t *testing.T) {
			e := newTestEnv(t, 1000)
			e.set(func(e *testEnv) { e.auditFractal = "faudit" })
			e.respond(func(string) ([]map[string]interface{}, error) {
				return []map[string]interface{}{exportRow("2026-10-01 12:00:01.000", "L2"), exportRow("2026-10-01 12:00:00.000", "L1")}, nil
			})
			rec := runExport(t, e, gz)
			if rec.Code != http.StatusOK || rec.Header().Get(api.ExportRowsTrailer) != "2" {
				t.Fatalf("got %d, trailer %q", rec.Code, rec.Header().Get(api.ExportRowsTrailer))
			}
			body := rec.Body.Bytes()
			if gz {
				zr, err := gzip.NewReader(bytes.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				if body, err = io.ReadAll(zr); err != nil {
					t.Fatal(err)
				}
			}
			want := `{"timestamp":"2026-10-01T12:00:01.000Z","log_id":"L2","fractal_id":"f1","fields":{"level":"error"}}` + "\n" +
				`{"timestamp":"2026-10-01T12:00:00.000Z","log_id":"L1","fractal_id":"f1","fields":{"level":"error"}}` + "\n"
			if string(body) != want {
				t.Errorf("body =\n%s\nwant\n%s", body, want)
			}
			// An export reads everything: no row cap, no paging.
			if q := e.ch.Queried(); len(q) != 1 || strings.Contains(q[0], "LIMIT") {
				t.Errorf("queries = %v", q)
			}
			waitFor(t, "the export audit", func() bool { return len(e.ch.Batches()) >= 2 })
			f := e.ch.Batches()[0].Appended[0][2].(map[string]string)
			if f["query_type"] != "export" || f["outcome"] != "complete" || f["result_count"] != "2" {
				t.Errorf("audit = %v", f)
			}
		})
	}
}

// Before the first row the failure can still be an HTTP error; after it the
// connection is aborted, so a truncated file never looks complete.
func TestHandleExportFailures(t *testing.T) {
	t.Run("before any row", func(t *testing.T) {
		e := newTestEnv(t, 1000)
		e.respond(func(string) ([]map[string]interface{}, error) { return nil, fakedb.Exception(241, "oom secret") })
		rec := runExport(t, e, false)
		if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "memory limits") || strings.Contains(rec.Body.String(), "secret") {
			t.Errorf("got %d %s", rec.Code, rec.Body.String())
		}
	})
	t.Run("mid-stream", func(t *testing.T) {
		e := newTestEnv(t, 1000)
		res := chResultOf([]map[string]interface{}{exportRow("2026-10-01 12:00:00.000", "L1")})
		res.IterErr = errors.New("connection reset")
		e.ch.On("", res)
		defer func() {
			if r := recover(); r != http.ErrAbortHandler {
				t.Errorf("recovered %v, want http.ErrAbortHandler", r)
			}
			waitFor(t, "the export to be killed", func() bool { return len(killedSearches(e)) > 0 })
		}()
		runExport(t, e, false)
		t.Error("a failed export ended cleanly")
	})
}

// A source command's preferred row cap and order (pgr() ranks spawn structure ahead
// of leaves) hold for a dashboard widget as they do for a search: cut at the generic
// cap and re-sorted by time, the widget drops structure instead of noise.
func TestExecuteBQLHonorsSourcePreferences(t *testing.T) {
	stub := sourceResolvers["pgr"]
	t.Cleanup(func() { sourceResolvers["pgr"] = stub })
	sourceResolvers["pgr"] = func(_ *QueryHandler, _ context.Context, cmd parser.CommandNode, _ parser.QueryOptions) (*resolvedSource, error) {
		rs, err := stubPgrSource(cmd)
		if err == nil {
			rs.Limit = 7
		}
		return rs, err
	}
	e := scopeEnv(t)
	e.respond(func(string) ([]map[string]interface{}, error) { return nil, nil })
	scope := execWindow
	scope.FractalID = "f1"
	res, err := e.h.ExecuteBQL(context.Background(), `pgr(start="g1")`, scope)
	if err != nil {
		t.Fatal(err)
	}
	if want := "ORDER BY " + strings.Join(parser.ProvenanceOrderBy, ", "); !strings.Contains(res.SQL, want) {
		t.Errorf("sql is missing %q: %s", want, res.SQL)
	}
	if !regexp.MustCompile(`\bLIMIT 7\b`).MatchString(res.SQL) {
		t.Errorf("sql does not cap at the source's 7 rows: %s", res.SQL)
	}
}
