package query

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"bifract/pkg/api"
	"bifract/pkg/parser"
)

func decodeValidate(t *testing.T, rec *httptest.ResponseRecorder) ValidateResponse {
	t.Helper()
	var resp ValidateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return resp
}

// HandleValidate runs as the user types, so its contract differs from search:
// a bad query is a 200 with valid=false (the editor underlines it), and only
// auth and malformed requests are HTTP errors.
func TestHandleValidate(t *testing.T) {
	cases := []struct {
		name      string
		method    string
		body      string
		vals      ctxVals
		wantCode  int
		wantValid bool
		wantErr   string
		wantType  string
		wantSpan  bool
	}{
		{name: "GET", method: http.MethodGet, body: "{}", vals: viewerCtx(), wantCode: http.StatusMethodNotAllowed},
		{name: "anonymous", body: `{"query":"*"}`, vals: ctxVals{}, wantCode: http.StatusUnauthorized, wantErr: "Authentication required"},
		{name: "no grant", body: `{"query":"*"}`, vals: ctxVals{"user": testViewer}, wantCode: http.StatusForbidden, wantErr: "Insufficient permissions"},
		{name: "malformed body", body: `{`, vals: viewerCtx(), wantCode: http.StatusBadRequest, wantErr: "Invalid request body"},
		{name: "empty while typing is fine", body: `{"query":""}`, vals: viewerCtx(), wantCode: http.StatusOK, wantValid: true},
		{name: "comment-only is fine", body: jsonBody(t, QueryRequest{Query: "// todo"}), vals: viewerCtx(), wantCode: http.StatusOK, wantValid: true},
		{name: "valid filter", body: `{"query":"level=error | groupby(host)"}`, vals: viewerCtx(), wantCode: http.StatusOK, wantValid: true},
		{name: "prism session", body: `{"query":"level=error"}`, vals: ctxVals{"user": testViewer, "prism_role": "viewer", "selected_prism": "p1"}, wantCode: http.StatusOK, wantValid: true},
		{name: "bad time range still validates", body: `{"query":"level=error","start":"nonsense"}`, vals: viewerCtx(), wantCode: http.StatusOK, wantValid: true},
		{
			name: "variables are substituted, not flagged",
			body: jsonBody(t, map[string]interface{}{
				"query": "level=@lvl", "variables": []map[string]string{{"name": "lvl", "value": "error"}},
			}),
			vals: viewerCtx(), wantCode: http.StatusOK, wantValid: true,
		},
		{name: "too long", body: jsonBody(t, QueryRequest{Query: "a=" + strings.Repeat("b", 10000)}), vals: viewerCtx(), wantCode: http.StatusOK, wantErr: "Query too long", wantType: "parse"},
		{
			name: "too long after expansion",
			body: jsonBody(t, map[string]interface{}{
				"query": "a=@v", "variables": []map[string]string{{"name": "v", "value": strings.Repeat("b", 10000)}},
			}),
			vals: viewerCtx(), wantCode: http.StatusOK, wantErr: "after variable expansion", wantType: "parse",
		},
		{name: "parse error underlines", body: `{"query":"level=error | eval(x:=)"}`, vals: viewerCtx(), wantCode: http.StatusOK, wantErr: "Parse error: ", wantType: "parse", wantSpan: true},
		{name: "unknown command", body: `{"query":"* | nosuchcommand()"}`, vals: viewerCtx(), wantCode: http.StatusOK, wantErr: "Query error: unsupported command", wantType: "translate"},
		{name: "tlsh() arguments are checked", body: `{"query":"tlsh(field=hash)"}`, vals: viewerCtx(), wantCode: http.StatusOK, wantErr: "tlsh(): needs hash="},

		// pgr() resolves its source from the database, which validation must not
		// touch; it is checked against a stand-in of the same shape instead.
		{name: "pgr() alone", body: `{"query":"pgr(start=\"g1\")"}`, vals: viewerCtx(), wantCode: http.StatusOK, wantValid: true},
		{name: "pgr() composed downstream", body: `{"query":"pgr(start=\"g1\") | anomaly_score>0.5 | pgraph()"}`, vals: viewerCtx(), wantCode: http.StatusOK, wantValid: true},
		{name: "pgr() without start", body: `{"query":"pgr()"}`, vals: viewerCtx(), wantCode: http.StatusOK, wantErr: "pgr() requires start=", wantType: "translate"},
		{name: "pgr() after a filter", body: `{"query":"level=error | pgr(start=\"g1\")"}`, vals: viewerCtx(), wantCode: http.StatusOK, wantErr: "must be the first command", wantType: "translate"},
	}
	h := newTestQueryHandler(1000)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			method := tc.method
			if method == "" {
				method = http.MethodPost
			}
			rec := httptest.NewRecorder()
			h.HandleValidate(rec, testRequest(method, tc.body, tc.vals))
			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tc.wantCode, rec.Body.String())
			}
			if method != http.MethodPost {
				return
			}
			resp := decodeValidate(t, rec)
			if resp.Valid != tc.wantValid {
				t.Fatalf("valid = %v, want %v (%+v)", resp.Valid, tc.wantValid, resp)
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

// Every source command needs a validation stand-in, or the editor flags each
// query that uses it as an unsupported command.
func TestEverySourceCommandHasAValidationStub(t *testing.T) {
	for _, name := range parser.SourceCommandNames() {
		if _, ok := sourceResolvers[name]; !ok {
			t.Errorf("source command %s() has no resolver", name)
		}
		if _, ok := sourceStubs[name]; !ok {
			t.Errorf("source command %s() has no validation stub", name)
		}
	}
}

// The search, stream, and field-stats endpoints share prepareQuery: a refusal it
// writes must be the whole response, never followed by the handler's own output.
func TestSearchEndpointsStopAtPrepareRefusal(t *testing.T) {
	h := newTestQueryHandler(1000)
	for name, handle := range map[string]http.HandlerFunc{
		"query":       h.HandleQuery,
		"stream":      h.HandleQueryStream,
		"field stats": h.HandleFieldStats,
		"export":      h.HandleExport,
	} {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			handle(rec, testRequest(http.MethodPost, `{"query":"level=error | nosuchcommand()"}`, adminCtx()))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
			var body map[string]interface{}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("response is not one JSON document: %q", rec.Body.String())
			}
			if body["success"] != false {
				t.Errorf("refusal body: %v", body)
			}
		})
	}
}

// An empty prism answers every search endpoint with an empty success, in the
// buffered response shape, before anything executes.
func TestSearchEndpointsAnswerEmptyPrism(t *testing.T) {
	h := newTestQueryHandler(1000)
	vals := ctxVals{"user": testAdmin, "selected_prism": "p1"}
	for name, handle := range map[string]http.HandlerFunc{
		"query":       h.HandleQuery,
		"stream":      h.HandleQueryStream,
		"field stats": h.HandleFieldStats,
	} {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			handle(rec, testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: "*"}), vals))
			resp := decodeQueryResponse(t, rec)
			if rec.Code != http.StatusOK || !resp.Success || resp.Count != 0 {
				t.Errorf("got %d %+v", rec.Code, resp)
			}
		})
	}
}

// Export streams a file, so a query that is not a plain filter is refused up
// front: a sorted, aggregated, or capped result is not "every matching log".
func TestHandleExportRefusesNonFilters(t *testing.T) {
	h := newTestQueryHandler(1000)
	for _, q := range []string{
		"level=error | groupby(host)",
		"level=error | sort(host)",
		"level=error | head(10)",
	} {
		t.Run(q, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.HandleExport(rec, testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: q, FractalID: "f1"}), adminCtx()))
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "Only filter queries can be exported") {
				t.Errorf("got %d %s", rec.Code, rec.Body.String())
			}
			if rec.Header().Get("Content-Disposition") != "" {
				t.Error("a refused export started a download")
			}
		})
	}
}

// Exporting an empty prism is a complete, empty file: the download starts, the
// body has no rows, and the trailer says so, rather than an error or an
// unscoped scan.
func TestHandleExportEmptyPrism(t *testing.T) {
	h := newTestQueryHandler(1000)
	vals := ctxVals{"user": testAdmin, "selected_prism": "p1"}

	t.Run("plain", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.HandleExport(rec, testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: "*"}), vals))
		res := rec.Result()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("status %d: %s", res.StatusCode, rec.Body.String())
		}
		if ct := res.Header.Get("Content-Type"); ct != "application/x-ndjson" {
			t.Errorf("Content-Type = %q", ct)
		}
		if cd := res.Header.Get("Content-Disposition"); !strings.HasPrefix(cd, `attachment; filename="bifract-export-`) || !strings.HasSuffix(cd, `.jsonl"`) {
			t.Errorf("Content-Disposition = %q", cd)
		}
		if res.Header.Get("Content-Encoding") != "" {
			t.Error("gzip applied without being accepted")
		}
		if rec.Body.Len() != 0 {
			t.Errorf("body = %q, want empty", rec.Body.String())
		}
		if got := res.Trailer.Get(api.ExportRowsTrailer); got != "0" {
			t.Errorf("rows trailer = %q, want 0", got)
		}
	})

	t.Run("gzip", func(t *testing.T) {
		r := testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: "*"}), vals)
		r.Header.Set("Accept-Encoding", "gzip")
		rec := httptest.NewRecorder()
		h.HandleExport(rec, r)
		if rec.Header().Get("Content-Encoding") != "gzip" {
			t.Fatalf("Content-Encoding = %q", rec.Header().Get("Content-Encoding"))
		}
		// A complete gzip stream (footer written) is how a client tells a finished
		// empty export from one cut off before its first row.
		zr, err := gzip.NewReader(rec.Body)
		if err != nil {
			t.Fatal(err)
		}
		if b, err := io.ReadAll(zr); err != nil || len(b) != 0 {
			t.Errorf("gzip body = %q, %v; want a complete empty stream", b, err)
		}
	})
}

func TestHandleGetLogByTimestampValidation(t *testing.T) {
	h := newTestQueryHandler(1000)
	cases := []struct {
		name     string
		method   string
		body     string
		vals     ctxVals
		wantCode int
		wantErr  string
	}{
		{name: "GET", method: http.MethodGet, body: "{}", vals: adminCtx(), wantCode: http.StatusMethodNotAllowed},
		{name: "malformed body", body: "{", vals: adminCtx(), wantCode: http.StatusBadRequest, wantErr: "Invalid request body"},
		{name: "timestamp not RFC3339", body: `{"timestamp":"2026-10-01 00:00:00","log_id":"a"}`, vals: adminCtx(), wantCode: http.StatusBadRequest, wantErr: "Invalid timestamp format"},
		// A non-admin with nothing in scope cannot read any log: answer "not found"
		// rather than reveal whether the id exists elsewhere.
		{name: "nothing accessible", body: `{"timestamp":"2026-10-01T00:00:00Z","log_id":"a"}`, vals: ctxVals{"user": testViewer}, wantCode: http.StatusNotFound, wantErr: "Log not found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			method := tc.method
			if method == "" {
				method = http.MethodPost
			}
			rec := httptest.NewRecorder()
			h.HandleGetLogByTimestamp(rec, testRequest(method, tc.body, tc.vals))
			if rec.Code != tc.wantCode || !strings.Contains(rec.Body.String(), tc.wantErr) {
				t.Errorf("got %d %s, want %d containing %q", rec.Code, rec.Body.String(), tc.wantCode, tc.wantErr)
			}
		})
	}
}

// HandleGetLogFields is a pinpoint read: it requires the exact timestamp and
// fractal from the search result, and never lets the client choose a fractal
// outside the session's scope.
func TestHandleGetLogFieldsValidation(t *testing.T) {
	h := newTestQueryHandler(1000)
	const ts = "2026-10-01 12:00:00.123"
	cases := []struct {
		name     string
		query    string
		vals     ctxVals
		wantCode int
		wantErr  string
	}{
		{name: "no log_id", query: "timestamp=" + ts, vals: adminCtx(), wantCode: http.StatusBadRequest, wantErr: "log_id is required"},
		{name: "no timestamp", query: "log_id=a", vals: adminCtx(), wantCode: http.StatusBadRequest, wantErr: "valid timestamp is required"},
		{name: "malformed timestamp", query: "log_id=a&timestamp=soon", vals: adminCtx(), wantCode: http.StatusBadRequest, wantErr: "valid timestamp is required"},
		{name: "nothing accessible", query: "log_id=a&timestamp=" + ts + "&fractal_id=f1", vals: ctxVals{"user": testViewer}, wantCode: http.StatusNotFound, wantErr: "Log not found"},
		{name: "no fractal_id", query: "log_id=a&timestamp=" + ts, vals: ctxVals{"user": testViewer, "selected_fractal": "f1"}, wantCode: http.StatusBadRequest, wantErr: "fractal_id is required"},
		{name: "fractal outside scope", query: "log_id=a&timestamp=" + ts + "&fractal_id=f2", vals: ctxVals{"user": testViewer, "selected_fractal": "f1"}, wantCode: http.StatusNotFound, wantErr: "Log not found"},
		{name: "prism with no members", query: "log_id=a&timestamp=" + ts + "&fractal_id=f2", vals: ctxVals{"user": testViewer, "selected_prism": "p1"}, wantCode: http.StatusNotFound, wantErr: "Log not found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := testRequest(http.MethodGet, "", tc.vals)
			r.URL.RawQuery = strings.ReplaceAll(tc.query, " ", "%20")
			rec := httptest.NewRecorder()
			h.HandleGetLogFields(rec, r)
			if rec.Code != tc.wantCode || !strings.Contains(rec.Body.String(), tc.wantErr) {
				t.Errorf("got %d %s, want %d containing %q", rec.Code, rec.Body.String(), tc.wantCode, tc.wantErr)
			}
		})
	}
}

func TestScopedFractalFilter(t *testing.T) {
	accessible := []string{"f1", "f2"}
	cases := []struct {
		requested string
		admin     bool
		want      string
	}{
		{"f1", false, "f1"},
		{"f3", false, ""}, // not trusted: falls back to an unscoped lookup plus the post-fetch check
		{"f3", true, "f3"},
		{"", true, ""},
		{"", false, ""},
	}
	for _, tc := range cases {
		if got := scopedFractalFilter(tc.requested, accessible, tc.admin); got != tc.want {
			t.Errorf("scopedFractalFilter(%q, admin=%v) = %q, want %q", tc.requested, tc.admin, got, tc.want)
		}
	}
}

func TestHandleReference(t *testing.T) {
	rec := httptest.NewRecorder()
	(&QueryHandler{}).HandleReference(rec, httptest.NewRequest(http.MethodGet, "/api/v1/query/reference", nil))
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	var resp ReferenceResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Functions) != len(bqlFunctionDocs) {
		t.Errorf("served %d functions, reference has %d", len(resp.Functions), len(bqlFunctionDocs))
	}
	ops := map[string]bool{}
	for _, op := range resp.Operators {
		ops[op.Operator] = true
		if op.Description == "" || len(op.Examples) == 0 {
			t.Errorf("operator %q is undocumented", op.Operator)
		}
	}
	for _, want := range []string{"=", "!=", "AND", "OR", "NOT", "|", "=~", "=^", "=$"} {
		if !ops[want] {
			t.Errorf("operator %q missing from the reference", want)
		}
	}
}
