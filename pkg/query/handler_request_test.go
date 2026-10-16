package query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bifract/pkg/fractals"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"
)

// These tests drive the search request path (prepareQuery and the handlers built
// on it) with no database behind it. A zero ClickHouseClient answers the
// topology questions (single node, plain table names) and nil managers stand in
// for the Postgres-backed scope lookups, which is exactly what each handler
// already tolerates. Anything that would execute SQL is out of reach, so every
// case here stops at, or before, the point where a query would run.

var (
	testAdmin  = &storage.User{Username: "root", IsAdmin: true}
	testViewer = &storage.User{Username: "vic"}
)

// Fixed 24h window: 15-minute histogram buckets, 96 of them.
const (
	testStart = "2026-10-01T00:00:00Z"
	testEnd   = "2026-10-02T00:00:00Z"
)

type ctxVals map[string]interface{}

// viewerCtx is a non-admin session holding a viewer grant on its fractal.
func viewerCtx() ctxVals { return ctxVals{"user": testViewer, "fractal_role": "viewer"} }

func adminCtx() ctxVals { return ctxVals{"user": testAdmin} }

func newTestQueryHandler(maxRows int) *QueryHandler {
	return NewQueryHandlerFull(&storage.ClickHouseClient{}, maxRows, nil, nil, nil)
}

func testRequest(method, body string, vals ctxVals) *http.Request {
	r := httptest.NewRequest(method, "/api/v1/query", bytes.NewReader([]byte(body)))
	ctx := r.Context()
	for k, v := range vals {
		ctx = context.WithValue(ctx, k, v)
	}
	return r.WithContext(ctx)
}

func jsonBody(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func searchBody(t *testing.T, req QueryRequest) string {
	t.Helper()
	if req.Start == "" && req.End == "" {
		req.Start, req.End = testStart, testEnd
	}
	return jsonBody(t, req)
}

func decodeQueryResponse(t *testing.T, rec *httptest.ResponseRecorder) QueryResponse {
	t.Helper()
	var resp QueryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return resp
}

// prepare runs prepareQuery and fails the test if it wrote a response instead of
// returning a prepared query.
func prepare(t *testing.T, h *QueryHandler, purpose queryPurpose, body string, vals ctxVals) *preparedQuery {
	t.Helper()
	rec := httptest.NewRecorder()
	prep := h.prepareQuery(rec, testRequest(http.MethodPost, body, vals), purpose)
	if prep == nil {
		t.Fatalf("prepareQuery refused the request: %d %s", rec.Code, rec.Body.String())
	}
	return prep
}

// Every refusal prepareQuery can make before SQL exists, with the status and the
// error routing (error_type) the UI keys its display on.
func TestPrepareQueryRejects(t *testing.T) {
	longQuery := "level=" + strings.Repeat("x", 10000)
	cases := []struct {
		name      string
		method    string
		body      string
		vals      ctxVals
		purpose   queryPurpose
		wantCode  int
		wantErr   string
		wantType  string
		wantSpan  bool
		rawRecord bool // response is not a QueryResponse envelope
	}{
		{name: "GET is not a search", method: http.MethodGet, body: "{}", vals: adminCtx(), wantCode: http.StatusMethodNotAllowed, rawRecord: true},
		{name: "anonymous", body: `{"query":"*"}`, vals: ctxVals{}, wantCode: http.StatusUnauthorized, wantErr: "Authentication required"},
		{name: "user with no grant", body: `{"query":"*"}`, vals: ctxVals{"user": testViewer}, wantCode: http.StatusForbidden, wantErr: "Insufficient permissions"},
		{name: "export needs admin, viewer refused", body: `{"query":"*"}`, vals: viewerCtx(), purpose: purposeExport, wantCode: http.StatusForbidden},
		{name: "export needs admin, analyst refused", body: `{"query":"*"}`, vals: ctxVals{"user": testViewer, "fractal_role": "analyst"}, purpose: purposeExport, wantCode: http.StatusForbidden},
		{name: "malformed body", body: `{"query":`, vals: viewerCtx(), wantCode: http.StatusBadRequest, wantErr: "Invalid request body"},
		{name: "empty query", body: `{"query":"   "}`, vals: viewerCtx(), wantCode: http.StatusBadRequest, wantErr: "Query parameter is required"},
		{name: "comment-only query is no query", body: jsonBody(t, QueryRequest{Query: "// just a note"}), vals: viewerCtx(), wantCode: http.StatusBadRequest, wantErr: "Query parameter is required"},
		{name: "too long", body: jsonBody(t, QueryRequest{Query: longQuery}), vals: viewerCtx(), wantCode: http.StatusBadRequest, wantErr: "Query too long (10006 chars"},
		{
			name: "variables expand past the limit",
			body: jsonBody(t, map[string]interface{}{
				"query":     "level=@v",
				"variables": []map[string]string{{"name": "v", "value": strings.Repeat("y", 10000)}},
			}),
			vals: viewerCtx(), wantCode: http.StatusBadRequest, wantErr: "after variable expansion",
		},
		{name: "unparseable start", body: jsonBody(t, QueryRequest{Query: "*", Start: "yesterday"}), vals: viewerCtx(), wantCode: http.StatusBadRequest, wantErr: "Invalid time range"},
		{name: "start after end", body: jsonBody(t, QueryRequest{Query: "*", Start: testEnd, End: testStart}), vals: viewerCtx(), wantCode: http.StatusBadRequest, wantErr: "Invalid time range"},
		{name: "parse error carries a span", body: searchBody(t, QueryRequest{Query: "level=error | eval(x:=)"}), vals: viewerCtx(), wantCode: http.StatusBadRequest, wantErr: "Parse error: ", wantType: "parse", wantSpan: true},
		{name: "translate error", body: searchBody(t, QueryRequest{Query: "level=error | nosuchcommand()"}), vals: viewerCtx(), wantCode: http.StatusBadRequest, wantErr: "Query error: unsupported command: nosuchcommand", wantType: "translate"},
		{name: "tlsh() without a needle", body: searchBody(t, QueryRequest{Query: "tlsh(field=hash)"}), vals: viewerCtx(), wantCode: http.StatusBadRequest, wantErr: "tlsh(): needs hash="},
		// Resolver failures name their fix (here: no analytics models), so they are
		// surfaced as the user's error rather than a generic failure.
		{name: "tlsh() with no model store", body: searchBody(t, QueryRequest{Query: `tlsh(field=hash, hash="T1` + strings.Repeat("A", 70) + `")`, FractalID: "f1"}), vals: viewerCtx(), wantCode: http.StatusBadRequest, wantErr: "tlsh(): analytics models are not available"},
		{name: "pgr() after a filter", body: searchBody(t, QueryRequest{Query: `level=error | pgr(start="g1")`}), vals: viewerCtx(), wantCode: http.StatusBadRequest, wantErr: "pgr() must be the first command", wantType: "translate"},
		{name: "pgr() without start", body: searchBody(t, QueryRequest{Query: `pgr()`, FractalID: "f1"}), vals: viewerCtx(), wantCode: http.StatusBadRequest, wantErr: "pgr() requires start=", wantType: "translate"},
		// pgr() is per-fractal by definition; with no scope resolved it must refuse
		// rather than traverse every tenant's process lineage.
		{name: "pgr() with no scope", body: searchBody(t, QueryRequest{Query: `pgr(start="g1")`}), vals: viewerCtx(), wantCode: http.StatusBadRequest, wantErr: "pgr: fractal scoping is required", wantType: "translate"},
		{name: "pgr() cannot be exported", body: searchBody(t, QueryRequest{Query: `pgr(start="g1")`, FractalID: "f1"}), vals: adminCtx(), purpose: purposeExport, wantCode: http.StatusBadRequest, wantErr: errExportNotFilter},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			method := tc.method
			if method == "" {
				method = http.MethodPost
			}
			rec := httptest.NewRecorder()
			prep := newTestQueryHandler(1000).prepareQuery(rec, testRequest(method, tc.body, tc.vals), tc.purpose)
			if prep != nil {
				t.Fatalf("expected a refusal, got a prepared query: %s", prep.sql)
			}
			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tc.wantCode, rec.Body.String())
			}
			if tc.rawRecord {
				return
			}
			resp := decodeQueryResponse(t, rec)
			if resp.Success {
				t.Error("a refusal reported success")
			}
			if !strings.Contains(resp.Error, tc.wantErr) {
				t.Errorf("error = %q, want it to contain %q", resp.Error, tc.wantErr)
			}
			if resp.ErrorType != tc.wantType {
				t.Errorf("error_type = %q, want %q", resp.ErrorType, tc.wantType)
			}
			if (resp.ErrorPos != nil) != tc.wantSpan {
				t.Errorf("error_pos = %+v, want present=%v", resp.ErrorPos, tc.wantSpan)
			}
		})
	}
}

// Comments are blanked before the length check, so a heavily annotated query is
// not refused for text that never reaches the parser.
func TestPrepareQueryCommentsDoNotCountTowardLength(t *testing.T) {
	q := "// " + strings.Repeat("why this filter exists ", 600) + "\nlevel=error"
	prep := prepare(t, newTestQueryHandler(1000), purposeSearch, searchBody(t, QueryRequest{Query: q}), viewerCtx())
	if !strings.Contains(prep.sql, "fields.`level`::String = 'error'") {
		t.Fatalf("the filter after the comment was lost:\n%s", prep.sql)
	}
}

// Variables substitute before parsing, so the parser and the audit/tag see the
// query that actually ran.
func TestPrepareQuerySubstitutesVariables(t *testing.T) {
	body := jsonBody(t, map[string]interface{}{
		"query": "level=@lvl", "start": testStart, "end": testEnd,
		"variables": []map[string]string{{"name": "lvl", "value": "error"}},
	})
	prep := prepare(t, newTestQueryHandler(1000), purposeSearch, body, viewerCtx())
	if prep.req.Query != "level=error" {
		t.Errorf("req.Query = %q, want the substituted query", prep.req.Query)
	}
	if !strings.Contains(prep.sql, "= 'error'") {
		t.Errorf("substituted value missing from SQL:\n%s", prep.sql)
	}
}

// The plain search shape when the deployment sets no row cap (maxRows 0): the
// translator leaves the LIMIT off, so the handler adds cursor paging (one extra
// row to detect a next page), a log_id tie-breaker, and the histogram.
func TestPrepareQueryCursorPagedSearch(t *testing.T) {
	h := newTestQueryHandler(0)
	prep := prepare(t, h, purposeSearch, searchBody(t, QueryRequest{Query: "level=error", FractalID: "f1"}), viewerCtx())

	if !strings.HasSuffix(prep.sql, " LIMIT 201") {
		t.Errorf("a cursor page must fetch cursorPageSize+1 rows:\n%s", prep.sql)
	}
	if !prep.appliedCursorPaging || !prep.streamable || !prep.chunkable {
		t.Errorf("flags: paging=%v streamable=%v chunkable=%v, want all true", prep.appliedCursorPaging, prep.streamable, prep.chunkable)
	}
	if !strings.Contains(strings.ToUpper(prep.sql), "LOG_ID DESC") {
		t.Errorf("no log_id tie-breaker, so pages drift on equal timestamps:\n%s", prep.sql)
	}
	if prep.selectedIndex != "f1" || prep.translationOpts.FractalID != "f1" {
		t.Errorf("scope = %q / %q, want the requested fractal", prep.selectedIndex, prep.translationOpts.FractalID)
	}
	wantStart, _ := time.Parse(time.RFC3339, testStart)
	if !prep.startTime.Equal(wantStart) {
		t.Errorf("startTime = %v, want %v", prep.startTime, wantStart)
	}

	// Histogram: 24h of 15-minute buckets, scanned from logs because the query
	// filters on content the per-minute rollup does not know.
	if !prep.needsHistogram || prep.histBucketSec != 900 || prep.histBucketCount != 96 {
		t.Errorf("histogram = %v/%ds/%d buckets, want true/900s/96", prep.needsHistogram, prep.histBucketSec, prep.histBucketCount)
	}
	if prep.histRollupTable != "" {
		t.Errorf("a content filter must not read the rollup (%q)", prep.histRollupTable)
	}
	if prep.histogramSQL == "" || len(prep.histChunks) == 0 {
		t.Error("histogram SQL or chunks missing")
	}
	if strings.Contains(prep.histogramSQL, "use_query_cache") {
		t.Error("the single-shot histogram spans the newest bucket and must never be cached")
	}
}

// Page N > 1: the cursor's (timestamp, log_id) bound goes into the scan's WHERE,
// and nothing that only makes sense for a whole-range first page runs.
func TestPrepareQueryAppliesCursor(t *testing.T) {
	ts := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	cursor, err := encodeCursor(map[string]interface{}{"timestamp": ts, "log_id": "x' OR 1=1 --"})
	if err != nil {
		t.Fatal(err)
	}
	prep := prepare(t, newTestQueryHandler(0), purposeSearch, searchBody(t, QueryRequest{Query: "level=error", FractalID: "f1", Cursor: cursor}), viewerCtx())

	want := fmt.Sprintf("AND (toUnixTimestamp64Milli(timestamp), log_id) < (%d, 'x\\' OR 1=1 --') ORDER BY", ts.UnixMilli())
	if !strings.Contains(prep.sql, want) {
		t.Errorf("cursor bound missing or unescaped; want %q in:\n%s", want, prep.sql)
	}
	if prep.chunkable {
		t.Error("a cursor page is not a whole-range scan and must not be chunked")
	}
	if prep.needsHistogram {
		t.Error("the histogram was already drawn by page one")
	}
}

// A cursor that does not decode is ignored rather than failing the search: the
// page is the first page again, not an error.
func TestPrepareQueryIgnoresMalformedCursor(t *testing.T) {
	prep := prepare(t, newTestQueryHandler(0), purposeSearch, searchBody(t, QueryRequest{Query: "level=error", FractalID: "f1", Cursor: "%%%not-base64"}), viewerCtx())
	if strings.Contains(prep.sql, "toUnixTimestamp64Milli") {
		t.Errorf("a malformed cursor injected a bound:\n%s", prep.sql)
	}
	if !strings.HasSuffix(prep.sql, " LIMIT 201") {
		t.Errorf("page size lost:\n%s", prep.sql)
	}
}

// With a deployment row cap the translator writes its own LIMIT, and the handler
// must not stack a second one or claim cursor paging it did not apply.
func TestPrepareQueryKeepsTranslatorLimit(t *testing.T) {
	prep := prepare(t, newTestQueryHandler(1000), purposeSearch, searchBody(t, QueryRequest{Query: "level=error", FractalID: "f1"}), viewerCtx())
	if prep.appliedCursorPaging {
		t.Error("cursor paging reported although the translator's LIMIT governs")
	}
	if strings.Contains(prep.sql, "LIMIT 201") {
		t.Errorf("handler stacked a page LIMIT on the translator's:\n%s", prep.sql)
	}
	if prep.queryMaxRows != 1000 {
		t.Errorf("queryMaxRows = %d, want 1000 (limit_hit detection reads it)", prep.queryMaxRows)
	}
}

func TestPrepareQueryShapes(t *testing.T) {
	cases := []struct {
		name                         string
		query                        string
		maxRows                      int
		wantAgg, wantStream, wantHis bool
		wantBloom                    bool
		wantSuffix                   string
	}{
		{name: "aggregation is fully computed", query: "level=error | groupby(host)", wantAgg: true, wantSuffix: " LIMIT 10000"},
		{name: "explicit sort is not streamable", query: "level=error | sort(host)", wantHis: true},
		{name: "bare regex is a raw-log search", query: "/denied/", wantStream: true, wantHis: true, wantBloom: true},
		{name: "head() opts out of the raw-log limit", query: "/denied/ | head(5)", wantHis: true},
		{name: "table keeps default order", query: "level=error | table(host)", wantStream: true, wantHis: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prep := prepare(t, newTestQueryHandler(tc.maxRows), purposeSearch, searchBody(t, QueryRequest{Query: tc.query, FractalID: "f1"}), viewerCtx())
			if prep.isAggregated != tc.wantAgg || prep.streamable != tc.wantStream || prep.needsHistogram != tc.wantHis || prep.isBloomQuery != tc.wantBloom {
				t.Errorf("agg=%v stream=%v hist=%v bloom=%v, want %v %v %v %v",
					prep.isAggregated, prep.streamable, prep.needsHistogram, prep.isBloomQuery,
					tc.wantAgg, tc.wantStream, tc.wantHis, tc.wantBloom)
			}
			if tc.wantSuffix != "" && !strings.HasSuffix(prep.sql, tc.wantSuffix) {
				t.Errorf("SQL does not end in %q:\n%s", tc.wantSuffix, prep.sql)
			}
			if prep.chunkable && !prep.streamable {
				t.Error("chunkable without streamable")
			}
		})
	}
}

// A histogram filtered by nothing but time and scope is what the per-minute
// rollup stores, so it reads the rollup. Sub-minute buckets (ranges under an
// hour) have no rollup equivalent and scan logs.
func TestPrepareQueryHistogramRollup(t *testing.T) {
	h := newTestQueryHandler(1000)

	prep := prepare(t, h, purposeSearch, searchBody(t, QueryRequest{Query: "*", FractalID: "f1"}), viewerCtx())
	if prep.histRollupTable != "logs_histogram" {
		t.Fatalf("scope-only histogram table = %q, want logs_histogram", prep.histRollupTable)
	}
	for _, want := range []string{"FROM logs_histogram", "minute >= '2026-10-01 00:00:00'", "minute < '2026-10-02 00:00:00'", "fractal_id = 'f1'", "INTERVAL 900 SECOND"} {
		if !strings.Contains(prep.histogramSQL, want) {
			t.Errorf("rollup histogram missing %q:\n%s", want, prep.histogramSQL)
		}
	}

	short := prepare(t, h, purposeSearch, searchBody(t, QueryRequest{Query: "*", FractalID: "f1", Start: "2026-10-01T00:00:00Z", End: "2026-10-01T00:30:00Z"}), viewerCtx())
	if short.histBucketSec != 30 || short.histRollupTable != "" {
		t.Errorf("30-minute range: bucket=%ds rollup=%q, want 30s and no rollup", short.histBucketSec, short.histRollupTable)
	}
}

// Export reads every matching row for a file, so it carries no row cap, no page
// LIMIT, no histogram, and none of the per-row extras a search shows.
func TestPrepareQueryExportIsUnlimited(t *testing.T) {
	prep := prepare(t, newTestQueryHandler(1000), purposeExport, searchBody(t, QueryRequest{Query: "level=error", FractalID: "f1"}), adminCtx())
	if sqlLimitRE.MatchString(prep.sql) {
		t.Errorf("export SQL is limited:\n%s", prep.sql)
	}
	if prep.needsHistogram {
		t.Error("export built a histogram")
	}
	o := prep.translationOpts
	if o.MaxRows != 0 || o.IncludeShardNum || o.IncludeIngestTime {
		t.Errorf("export opts: MaxRows=%d shard=%v ingest=%v, want 0/false/false", o.MaxRows, o.IncludeShardNum, o.IncludeIngestTime)
	}
	if !prep.streamable {
		t.Error("a filter query must remain streamable for export")
	}
}

func TestPrepareQueryPinnedTimezone(t *testing.T) {
	prep := prepare(t, newTestQueryHandler(1000), purposeSearch, searchBody(t, QueryRequest{Query: "*", FractalID: "f1", Timezone: "Europe/Berlin"}), viewerCtx())
	if prep.translationOpts.DisplayTimezone != "Europe/Berlin" {
		t.Errorf("DisplayTimezone = %q, want the pinned zone", prep.translationOpts.DisplayTimezone)
	}
}

// Scope resolution. A prism selected in the session beats a fractal also held
// there (prism-first, or rows written under a prism are listed by fractal and
// vanish); a fractal_id named in the request body is an explicit choice and
// beats both.
func TestPrepareQueryScope(t *testing.T) {
	t.Run("session prism beats session fractal", func(t *testing.T) {
		vals := ctxVals{"user": testAdmin, "selected_fractal": "f1", "selected_prism": "p1"}
		rec := httptest.NewRecorder()
		// The prism has no members (no prism manager), so the prism path answers
		// with an empty result itself. Had the fractal won, a query would have been
		// prepared to run against f1.
		if prep := newTestQueryHandler(1000).prepareQuery(rec, testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: "*"}), vals), purposeSearch); prep != nil {
			t.Fatalf("prepared a query against %q instead of the prism", prep.selectedIndex)
		}
		resp := decodeQueryResponse(t, rec)
		if rec.Code != http.StatusOK || !resp.Success || resp.Count != 0 || resp.SQL != "" {
			t.Fatalf("got %d %+v, want an empty successful prism result", rec.Code, resp)
		}
	})

	t.Run("request fractal beats session prism", func(t *testing.T) {
		vals := ctxVals{"user": testAdmin, "selected_prism": "p1"}
		prep := prepare(t, newTestQueryHandler(1000), purposeSearch, searchBody(t, QueryRequest{Query: "*", FractalID: "f2"}), vals)
		if prep.prismID != "" || prep.selectedIndex != "f2" || prep.translationOpts.FractalID != "f2" {
			t.Errorf("prism=%q index=%q opts=%q, want the requested fractal f2", prep.prismID, prep.selectedIndex, prep.translationOpts.FractalID)
		}
	})

	t.Run("empty prism exports nothing rather than everything", func(t *testing.T) {
		vals := ctxVals{"user": testAdmin, "selected_prism": "p1"}
		prep := prepare(t, newTestQueryHandler(1000), purposeExport, searchBody(t, QueryRequest{Query: "*"}), vals)
		if !prep.emptyScope || prep.prismID != "p1" || prep.sql != "" {
			t.Errorf("emptyScope=%v prism=%q sql=%q, want an empty p1 scope and no SQL", prep.emptyScope, prep.prismID, prep.sql)
		}
	})

	t.Run("prism without a grant is refused", func(t *testing.T) {
		// A fractal grant gets past the first gate; it is not a prism grant.
		vals := ctxVals{"user": testViewer, "fractal_role": "viewer", "selected_prism": "p1"}
		rec := httptest.NewRecorder()
		if prep := newTestQueryHandler(1000).prepareQuery(rec, testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: "*"}), vals), purposeSearch); prep != nil {
			t.Fatal("prism query ran without a prism grant")
		}
		if rec.Code != http.StatusForbidden {
			t.Errorf("status = %d, want 403", rec.Code)
		}
	})

	t.Run("prism role from the auth middleware is honoured", func(t *testing.T) {
		vals := ctxVals{"user": testViewer, "prism_role": "viewer", "selected_prism": "p1"}
		rec := httptest.NewRecorder()
		newTestQueryHandler(1000).prepareQuery(rec, testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: "*"}), vals), purposeSearch)
		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want 200 (%s)", rec.Code, rec.Body.String())
		}
	})
}

// An API key carries its grant in the request context for the one fractal it is
// bound to, so naming that fractal in the body is honoured without a Postgres
// role lookup; a fractal grant is not an admin grant, so export stays closed.
func TestPrepareQueryAPIKeyOwnFractal(t *testing.T) {
	h := newTestQueryHandler(1000)
	h.SetRBACResolver(rbac.NewResolver(nil)) // a role lookup would dereference nil Postgres
	key := &storage.User{Username: "apikey_7"}
	vals := ctxVals{"user": key, "auth_type": "api_key", "selected_fractal": "f1", "fractal_role": "viewer"}

	prep := prepare(t, h, purposeSearch, searchBody(t, QueryRequest{Query: "level=error", FractalID: "f1"}), vals)
	if prep.selectedIndex != "f1" {
		t.Errorf("selectedIndex = %q", prep.selectedIndex)
	}

	rec := httptest.NewRecorder()
	if h.prepareQuery(rec, testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: "*", FractalID: "f1"}), vals), purposeExport) != nil || rec.Code != http.StatusForbidden {
		t.Errorf("viewer key export: %d, want 403", rec.Code)
	}
}

// getSelectedIndex is the scope fallback for requests whose body names no
// fractal. Its contract: an API key is bound to its own scope and never falls
// back to the default; a prism session is not a fractal; and a caller that
// declared no scope (X-Bifract-Scope: none) is refused instead of handed the
// default fractal's data.
func TestGetSelectedIndex(t *testing.T) {
	// A manager with no database: every case that reaches GetDefaultFractal
	// carries WithNoScope, which refuses before touching storage.
	mgr := fractals.NewManager(nil, nil)
	cases := []struct {
		name    string
		mgr     *fractals.Manager
		vals    ctxVals
		noScope bool
		want    string
		wantErr bool
	}{
		{name: "no fractal manager", mgr: nil, vals: ctxVals{"selected_fractal": "f1"}, want: ""},
		{name: "fractal API key", mgr: mgr, vals: ctxVals{"auth_type": "api_key", "selected_fractal": "f1"}, want: "f1"},
		{name: "prism API key leaves scope to the prism path", mgr: mgr, vals: ctxVals{"auth_type": "api_key", "selected_prism": "p1"}, want: ""},
		{name: "unscoped API key is an error, not the default", mgr: mgr, vals: ctxVals{"auth_type": "api_key"}, wantErr: true},
		{name: "session fractal", mgr: mgr, vals: ctxVals{"auth_type": "session", "selected_fractal": "f1"}, want: "f1"},
		{name: "prism session is not the default fractal", mgr: mgr, vals: ctxVals{"auth_type": "session", "selected_prism": "p1"}, want: ""},
		{name: "session that declared no scope", mgr: mgr, vals: ctxVals{"auth_type": "session"}, noScope: true, wantErr: true},
		{name: "no auth type and no scope", mgr: mgr, vals: ctxVals{}, noScope: true, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &QueryHandler{fractalManager: tc.mgr}
			r := testRequest(http.MethodGet, "", tc.vals)
			if tc.noScope {
				r = r.WithContext(fractals.WithNoScope(r.Context()))
			}
			got, err := h.getSelectedIndex(r)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// A request that declared no scope must not be answered from the default
// fractal: prepareQuery surfaces the refusal as a scope failure.
func TestPrepareQueryNoScopeIsRefused(t *testing.T) {
	h := NewQueryHandlerFull(&storage.ClickHouseClient{}, 1000, fractals.NewManager(nil, nil), nil, nil)
	r := testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: "*"}), ctxVals{"user": testAdmin, "auth_type": "session"})
	r = r.WithContext(fractals.WithNoScope(r.Context()))
	rec := httptest.NewRecorder()
	if prep := h.prepareQuery(rec, r, purposeSearch); prep != nil {
		t.Fatalf("a scopeless request was prepared against %q", prep.selectedIndex)
	}
	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "Failed to determine fractal context") {
		t.Errorf("got %d %s", rec.Code, rec.Body.String())
	}
}

// buildFractalCondition scopes the landing page's recent-logs reads.
func TestBuildFractalCondition(t *testing.T) {
	h := &QueryHandler{}
	r := testRequest(http.MethodGet, "", ctxVals{})
	cond, noData, err := h.buildFractalCondition(r, "f'1")
	if err != nil || noData || cond != `fractal_id = 'f\'1'` {
		t.Errorf("single fractal: %q noData=%v err=%v", cond, noData, err)
	}
	cond, noData, err = h.buildFractalCondition(r, "")
	if err != nil || noData || cond != "" {
		t.Errorf("no scope: %q noData=%v err=%v", cond, noData, err)
	}

	// The default-fractal lookup failing (here: declared no scope) must not widen
	// the filter to legacy empty-fractal rows.
	h = &QueryHandler{fractalManager: fractals.NewManager(nil, nil)}
	r = r.WithContext(fractals.WithNoScope(r.Context()))
	cond, _, _ = h.buildFractalCondition(r, "f1")
	if cond != "fractal_id = 'f1'" {
		t.Errorf("unresolvable default widened the filter: %q", cond)
	}
}

func TestRecentLogsHandlersRejectWrongMethodAndScopeFailure(t *testing.T) {
	h := NewQueryHandlerFull(&storage.ClickHouseClient{}, 1000, fractals.NewManager(nil, nil), nil, nil)
	for name, handle := range map[string]http.HandlerFunc{
		"recent logs":      h.HandleGetRecentLogs,
		"recent histogram": h.HandleGetRecentHistogram,
	} {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			handle(rec, testRequest(http.MethodPost, "", adminCtx()))
			if rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("POST: status %d, want 405", rec.Code)
			}

			r := testRequest(http.MethodGet, "", ctxVals{"user": testAdmin, "auth_type": "session"})
			r = r.WithContext(fractals.WithNoScope(r.Context()))
			rec = httptest.NewRecorder()
			handle(rec, r)
			if rec.Code != http.StatusInternalServerError {
				t.Errorf("scopeless GET: status %d, want 500 (%s)", rec.Code, rec.Body.String())
			}
		})
	}
}
