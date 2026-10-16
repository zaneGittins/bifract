package query

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"bifract/internal/fakedb"
)

// Single-log reads by id. The client names the log and (optionally) its fractal,
// but the session decides what may be read: the row's own fractal_id is checked
// after the fetch, so a crafted id or fractal cannot read across tenants.

// logLookup scripts the by-id lookups: the stored log's fractal, and the
// arguments each lookup was bound with.
type logLookup struct {
	mu      sync.Mutex
	fractal string // the stored row's fractal_id; "-" means no such row
	err     error
	args    [][]any
}

func (l *logLookup) install(e *testEnv) {
	e.ch.Respond(func(sql string, args []any) (fakedb.CHResult, bool) {
		l.mu.Lock()
		defer l.mu.Unlock()
		switch {
		case strings.Contains(sql, "FROM system.columns"):
			return fakedb.CHResult{}, true // no type hints
		case strings.Contains(sql, "SELECT raw_log FROM logs_raw"):
			return fakedb.Strs("raw_log", "<13>raw line"), true
		case strings.Contains(sql, "WHERE log_id = ?"), strings.Contains(sql, "PREWHERE log_id = ?"):
			l.args = append(l.args, args)
			if l.err != nil {
				return fakedb.CHResult{Err: l.err}, true
			}
			if l.fractal == "-" {
				return fakedb.CHResult{}, true
			}
			if strings.Contains(sql, "PREWHERE") {
				return fakedb.CHResult{
					Cols: []fakedb.Col{{Column: "log_id", Type: "String"}, {Column: "fractal_id", Type: "String"}, {Column: "fields", Type: "String"}, {Column: "normalizer", Type: "String"}, {Column: "ingest_timestamp", Type: "DateTime64(3)"}},
					Rows: [][]any{{"L1", l.fractal, `{"user":"ana","level":"error"}`, "winlog@3", time.Date(2026, 10, 1, 12, 0, 1, 0, time.UTC)}},
				}, true
			}
			return chResultOf([]map[string]interface{}{{
				"timestamp": "2026-10-01 12:00:00.123", "log_id": "L1", "fractal_id": l.fractal,
				"fields": `{"user":"ana"}`, "ingest_timestamp": "2026-10-01 12:00:01.000",
			}}), true
		}
		return fakedb.CHResult{}, false
	})
}

// boundFractal reports whether the most recent lookup was scoped to fractal.
func (l *logLookup) boundFractal(fractal string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.args) == 0 {
		return false
	}
	for _, a := range l.args[len(l.args)-1] {
		if a == fractal {
			return true
		}
	}
	return false
}

func (l *logLookup) lookups() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.args)
}

func TestHandleGetLogByTimestampScope(t *testing.T) {
	viewerOn := func(k, v string) ctxVals { return ctxVals{"user": testViewer, k: v} }
	cases := []struct {
		name        string
		vals        ctxVals
		reqFractal  string
		rowFractal  string
		defaultID   string
		wantCode    int
		wantScoped  string // fractal the lookup must be pruned to, if any
		wantLookups int
	}{
		{name: "own fractal", vals: viewerOn("selected_fractal", "f1"), reqFractal: "f1", rowFractal: "f1", wantCode: 200, wantScoped: "f1", wantLookups: 1},
		// The requested fractal is outside scope, so it is not used even as a filter;
		// the row it finds belongs to that fractal and is refused.
		{name: "other fractal named", vals: viewerOn("selected_fractal", "f1"), reqFractal: "f2", rowFractal: "f2", wantCode: 404, wantLookups: 1},
		{name: "crafted id from another fractal", vals: viewerOn("selected_fractal", "f1"), rowFractal: "f2", wantCode: 404, wantLookups: 1},
		// Rows written before fractal_id existed belong to the default fractal.
		{name: "legacy row in the default fractal", vals: viewerOn("selected_fractal", "f1"), defaultID: "f1", rowFractal: "", wantCode: 200, wantLookups: 1},
		{name: "legacy row outside the default", vals: viewerOn("selected_fractal", "f1"), defaultID: "fdef", rowFractal: "", wantCode: 404, wantLookups: 1},
		{name: "prism member", vals: viewerOn("selected_prism", "p1"), rowFractal: "f2", wantCode: 200, wantLookups: 1},
		{name: "outside the prism", vals: viewerOn("selected_prism", "p1"), rowFractal: "f3", wantCode: 404, wantLookups: 1},
		{name: "admin reads anywhere", vals: adminCtx(), reqFractal: "f9", rowFractal: "f9", wantCode: 200, wantScoped: "f9", wantLookups: 1},
		{name: "no such log", vals: viewerOn("selected_fractal", "f1"), rowFractal: "-", wantCode: 404, wantLookups: 1},
		// A prism with no members has nothing to read; no lookup at all.
		{name: "empty prism", vals: viewerOn("selected_prism", "p0"), rowFractal: "f1", wantCode: 404},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEnv(t, 1000)
			e.set(func(e *testEnv) {
				e.members["p1"] = []string{"f1", "f2"}
				if tc.defaultID != "" {
					e.defaultFractal = tc.defaultID
				}
			})
			l := &logLookup{fractal: tc.rowFractal}
			l.install(e)
			body := jsonBody(t, LogByTimestampRequest{Timestamp: "2026-10-01T12:00:00.123Z", LogID: "L1", FractalID: tc.reqFractal})
			rec := httptest.NewRecorder()
			e.h.HandleGetLogByTimestamp(rec, testRequest(http.MethodPost, body, tc.vals))
			if rec.Code != tc.wantCode {
				t.Fatalf("status %d, want %d: %s", rec.Code, tc.wantCode, rec.Body.String())
			}
			if l.lookups() != tc.wantLookups {
				t.Errorf("%d lookups, want %d", l.lookups(), tc.wantLookups)
			}
			if tc.wantScoped != "" && !l.boundFractal(tc.wantScoped) {
				t.Errorf("lookup not pruned to %s: %v", tc.wantScoped, l.args)
			}
			if tc.reqFractal != "" && tc.wantScoped == "" && l.boundFractal(tc.reqFractal) {
				t.Errorf("an out-of-scope fractal was used as a filter: %v", l.args)
			}
			if tc.wantCode == 200 {
				var resp struct {
					Log map[string]interface{} `json:"log"`
				}
				json.Unmarshal(rec.Body.Bytes(), &resp)
				if f, _ := resp.Log["fields"].(map[string]interface{}); resp.Log["log_id"] != "L1" || f["user"] != "ana" {
					t.Errorf("log = %v", resp.Log)
				}
			}
		})
	}
}

func TestHandleGetLogByTimestampFailures(t *testing.T) {
	t.Run("lookup fails", func(t *testing.T) {
		e := newTestEnv(t, 1000)
		(&logLookup{err: errors.New("boom")}).install(e)
		rec := httptest.NewRecorder()
		e.h.HandleGetLogByTimestamp(rec, testRequest(http.MethodPost, `{"timestamp":"2026-10-01T12:00:00Z","log_id":"L1"}`, adminCtx()))
		if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "Failed to retrieve log") {
			t.Errorf("got %d %s", rec.Code, rec.Body.String())
		}
	})
	t.Run("scope cannot be resolved", func(t *testing.T) {
		e := newTestEnv(t, 1000)
		e.set(func(e *testEnv) { e.defaultFractal = "" })
		rec := httptest.NewRecorder()
		e.h.HandleGetLogByTimestamp(rec, testRequest(http.MethodPost, `{"timestamp":"2026-10-01T12:00:00Z","log_id":"L1"}`, ctxVals{"user": testViewer}))
		if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "Failed to determine scope") {
			t.Errorf("got %d %s", rec.Code, rec.Body.String())
		}
	})
}

func TestHandleGetLogFields(t *testing.T) {
	const ts = "2026-10-01%2012:00:00.123"
	get := func(e *testEnv, query string, vals ctxVals) *httptest.ResponseRecorder {
		r := testRequest(http.MethodGet, "", vals)
		r.URL.RawQuery = query
		rec := httptest.NewRecorder()
		e.h.HandleGetLogFields(rec, r)
		return rec
	}
	viewer := ctxVals{"user": testViewer, "selected_fractal": "f1"}

	t.Run("reads the pruned row with its raw text", func(t *testing.T) {
		e := newTestEnv(t, 1000)
		l := &logLookup{fractal: "f1"}
		l.install(e)
		rec := get(e, "log_id=L1&timestamp="+ts+"&fractal_id=f1&shard_num=3", viewer)
		if rec.Code != http.StatusOK {
			t.Fatalf("got %d %s", rec.Code, rec.Body.String())
		}
		var resp struct {
			Fields   map[string]interface{} `json:"fields"`
			RawLog   string                 `json:"raw_log"`
			IngestTS string                 `json:"ingest_timestamp"`
		}
		json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.Fields["user"] != "ana" || resp.Fields["_normalizer"] != "winlog@3" || resp.RawLog != "<13>raw line" || resp.IngestTS != "2026-10-01 12:00:01.000" {
			t.Errorf("resp = %+v", resp)
		}
		if !l.boundFractal("f1") || !l.boundFractal("2026-10-01 12:00:00.123") {
			t.Errorf("lookup args = %v", l.args)
		}
	})

	// The fractal filter is a pruning hint; the row's own fractal is the check.
	t.Run("row from another fractal is refused", func(t *testing.T) {
		e := newTestEnv(t, 1000)
		(&logLookup{fractal: "f2"}).install(e)
		if rec := get(e, "log_id=L1&timestamp="+ts+"&fractal_id=f1", viewer); rec.Code != http.StatusNotFound {
			t.Errorf("got %d %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("no such log", func(t *testing.T) {
		e := newTestEnv(t, 1000)
		(&logLookup{fractal: "-"}).install(e)
		if rec := get(e, "log_id=L1&timestamp="+ts+"&fractal_id=f1", viewer); rec.Code != http.StatusNotFound {
			t.Errorf("got %d %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("lookup fails", func(t *testing.T) {
		e := newTestEnv(t, 1000)
		(&logLookup{err: errors.New("boom")}).install(e)
		rec := get(e, "log_id=L1&timestamp="+ts+"&fractal_id=f1", viewer)
		if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "Failed to retrieve log fields") {
			t.Errorf("got %d %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("scope cannot be resolved", func(t *testing.T) {
		e := newTestEnv(t, 1000)
		e.set(func(e *testEnv) { e.defaultFractal = "" })
		rec := get(e, "log_id=L1&timestamp="+ts+"&fractal_id=f1", ctxVals{"user": testViewer})
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("got %d %s", rec.Code, rec.Body.String())
		}
	})
}

func getRecent(e *testEnv, handle func(http.ResponseWriter, *http.Request), vals ctxVals) map[string]interface{} {
	rec := httptest.NewRecorder()
	handle(rec, testRequest(http.MethodGet, "", vals))
	var out map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &out)
	out["_status"] = float64(rec.Code)
	return out
}

func TestHandleGetRecentLogs(t *testing.T) {
	session := func(k, v string) ctxVals { return ctxVals{"user": testViewer, "auth_type": "session", k: v} }
	cases := []struct {
		name      string
		vals      ctxVals
		wantScope string // fragment the logs query must carry; "" means no query at all
	}{
		{"fractal", session("selected_fractal", "f1"), "fractal_id = 'f1'"},
		// The default fractal also owns legacy rows with no fractal_id.
		{"default fractal", session("selected_fractal", "fdef"), "fractal_id IN ('fdef', '')"},
		{"prism", session("selected_prism", "p1"), "fractal_id IN ('f1', 'f2')"},
		{"prism holding the default", session("selected_prism", "p2"), "fractal_id IN ('f1', 'fdef', '')"},
		{"empty prism", session("selected_prism", "p0"), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEnv(t, 1000)
			e.set(func(e *testEnv) {
				e.members["p1"] = []string{"f1", "f2"}
				e.members["p2"] = []string{"f1", "fdef"}
			})
			e.respond(func(sql string) ([]map[string]interface{}, error) {
				r := logRow("2026-10-01 12:00:00.000", "L1", "f1")
				r["secret_extra"] = "x"
				return []map[string]interface{}{r}, nil
			})
			out := getRecent(e, e.h.HandleGetRecentLogs, tc.vals)
			if out["_status"] != 200.0 || out["success"] != true {
				t.Fatalf("got %v", out)
			}
			q := e.ch.Queried()
			if tc.wantScope == "" {
				if len(q) != 0 || out["count"] != 0.0 {
					t.Errorf("empty prism queried %v, count %v", q, out["count"])
				}
				return
			}
			if len(q) != 1 || !strings.Contains(q[0], tc.wantScope) || !strings.HasSuffix(q[0], "ORDER BY timestamp DESC LIMIT 50") {
				t.Fatalf("queries = %v", q)
			}
			rows := out["results"].([]interface{})
			if len(rows) != 1 {
				t.Fatalf("results = %v", rows)
			}
			if _, leaked := rows[0].(map[string]interface{})["secret_extra"]; leaked {
				t.Errorf("unprojected column passed through: %v", rows[0])
			}
		})
	}

	t.Run("query fails", func(t *testing.T) {
		e := newTestEnv(t, 1000)
		e.respond(func(string) ([]map[string]interface{}, error) { return nil, errors.New("boom") })
		out := getRecent(e, e.h.HandleGetRecentLogs, session("selected_fractal", "f1"))
		if out["_status"] != 500.0 || out["error"] != "Failed to retrieve recent logs" {
			t.Errorf("got %v", out)
		}
	})
}

func TestHandleGetRecentHistogram(t *testing.T) {
	session := ctxVals{"user": testViewer, "auth_type": "session", "selected_fractal": "f1"}
	// A bucket half a day back lands mid-array, whatever its numeric type.
	bucket := time.Now().UTC().Add(-12 * time.Hour).Truncate(15 * time.Minute).Format("2006-01-02 15:04:05")
	for name, cnt := range map[string]interface{}{"uint64": uint64(9), "int64": int64(9), "float64": float64(9)} {
		t.Run(name, func(t *testing.T) {
			e := newTestEnv(t, 1000)
			e.respond(func(sql string) ([]map[string]interface{}, error) {
				return []map[string]interface{}{{"bucket": bucket, "cnt": cnt}, {"bucket": "garbage", "cnt": cnt}}, nil
			})
			out := getRecent(e, e.h.HandleGetRecentHistogram, session)
			hist := out["histogram"].([]interface{})
			total, at := 0.0, -1
			for i, v := range hist {
				if v.(float64) > 0 {
					total += v.(float64)
					at = i
				}
			}
			if len(hist) != 96 || total != 9 || at < 46 || at > 48 {
				t.Errorf("histogram = %v (count %v at %d)", hist, total, at)
			}
			if q := e.ch.Queried(); len(q) != 1 || !strings.Contains(q[0], "FROM logs_histogram") || !strings.Contains(q[0], "fractal_id = 'f1'") {
				t.Errorf("queries = %v", q)
			}
		})
	}

	// The landing chart is decoration: a failed read is an empty chart, not an error.
	t.Run("query fails", func(t *testing.T) {
		e := newTestEnv(t, 1000)
		e.respond(func(string) ([]map[string]interface{}, error) { return nil, errors.New("boom") })
		out := getRecent(e, e.h.HandleGetRecentHistogram, session)
		if out["_status"] != 200.0 || len(out["histogram"].([]interface{})) != 96 {
			t.Errorf("got %v", out)
		}
	})

	t.Run("empty prism", func(t *testing.T) {
		e := newTestEnv(t, 1000)
		out := getRecent(e, e.h.HandleGetRecentHistogram, ctxVals{"user": testViewer, "auth_type": "session", "selected_prism": "p0"})
		if out["_status"] != 200.0 || len(e.ch.Queried()) != 0 {
			t.Errorf("got %v after %v", out, e.ch.Queried())
		}
	})
}
