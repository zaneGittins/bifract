package normalizers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"gopkg.in/yaml.v3"

	"bifract/internal/fakedb"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// Access control for these routes lives in the router (tenant admin for every
// write and for preview/samples); the handlers themselves only validate input and
// shape responses, which is what is pinned here.

type envelope struct {
	Success bool            `json:"success"`
	Error   string          `json:"error"`
	Data    json.RawMessage `json:"data"`
}

func newTestHandler(ch *storage.ClickHouseClient) (*Handler, *fakedb.PG) {
	m, fake := newManager()
	return NewHandler(m, ch), fake
}

func request(method, target, body, id string) *http.Request {
	r := httptest.NewRequest(method, target, strings.NewReader(body))
	ctx := context.WithValue(r.Context(), "user", &storage.User{Username: "alice"})
	if id != "" {
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("id", id)
		ctx = context.WithValue(ctx, chi.RouteCtxKey, rctx)
	}
	return r.WithContext(ctx)
}

func serve(t *testing.T, h http.HandlerFunc, r *http.Request) (int, envelope) {
	t.Helper()
	w := httptest.NewRecorder()
	h(w, r)
	var env envelope
	if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil {
		t.Fatalf("response is not a JSON envelope: %v\n%s", err, w.Body.String())
	}
	return w.Code, env
}

func TestHandleListEmptyIsArray(t *testing.T) {
	h, _ := newTestHandler(nil)
	code, env := serve(t, h.HandleList, request("GET", "/normalizers", "", ""))
	if code != http.StatusOK || string(env.Data) != "[]" {
		t.Fatalf("code=%d data=%s: an empty list must serialize as [] for the UI", code, env.Data)
	}

	h, fake := newTestHandler(nil)
	fake.On("FROM normalizers", fakedb.PGResult{Err: errors.New("down")})
	if code, env := serve(t, h.HandleList, request("GET", "/normalizers", "", "")); code != http.StatusInternalServerError || env.Success {
		t.Fatalf("list error: code=%d env=%+v", code, env)
	}
}

func TestHandleGet(t *testing.T) {
	h, fake := newTestHandler(nil)
	fake.On("WHERE id = $1", fakedb.Row(sysmonRow()...))
	code, env := serve(t, h.HandleGet, request("GET", "/normalizers/n-1", "", "n-1"))
	if code != http.StatusOK {
		t.Fatalf("code = %d", code)
	}
	var n Normalizer
	if err := json.Unmarshal(env.Data, &n); err != nil || n.ID != "n-1" || n.Version != 3 {
		t.Fatalf("data = %s (%v)", env.Data, err)
	}
	if fake.Calls()[0].Args[0] != "n-1" {
		t.Fatal("the URL id must be the one looked up")
	}

	h, _ = newTestHandler(nil)
	if code, _ := serve(t, h.HandleGet, request("GET", "/normalizers/x", "", "x")); code != http.StatusNotFound {
		t.Fatalf("missing: code = %d", code)
	}
}

func TestHandleCreate(t *testing.T) {
	h, fake := newTestHandler(nil)
	fake.On("INSERT INTO normalizers", fakedb.Row(sysmonRow()...))
	body := `{"name":"sysmon","transforms":["snake_case"],"field_mappings":[{"sources":["computer"],"target":"host"}]}`
	code, _ := serve(t, h.HandleCreate, request("POST", "/normalizers", body, ""))
	if code != http.StatusOK {
		t.Fatalf("code = %d", code)
	}
	ins := fake.Matching("INSERT INTO normalizers")
	if len(ins) != 1 || ins[0].Args[6] != "alice" {
		t.Fatalf("created_by must be the requesting user: %+v", ins)
	}

	for name, body := range map[string]string{
		"malformed JSON":    `{"name":`,
		"invalid transform": `{"name":"x","transforms":["shout"]}`,
		"blank name":        `{"name":"  "}`,
	} {
		h, fake := newTestHandler(nil)
		code, env := serve(t, h.HandleCreate, request("POST", "/normalizers", body, ""))
		if code != http.StatusBadRequest || env.Success {
			t.Errorf("%s: code=%d env=%+v", name, code, env)
		}
		if len(fake.Matching("INSERT")) != 0 {
			t.Errorf("%s: reached the database", name)
		}
	}
}

func TestHandleUpdateDeleteSetDefault(t *testing.T) {
	h, fake := newTestHandler(nil)
	fake.On("UPDATE normalizers", fakedb.Row(sysmonRow()...))
	if code, _ := serve(t, h.HandleUpdate, request("PUT", "/normalizers/n-1", `{"name":"sysmon"}`, "n-1")); code != http.StatusOK {
		t.Fatalf("update: code = %d", code)
	}
	if args := fake.Matching("UPDATE normalizers")[0].Args; args[len(args)-1] != "n-1" {
		t.Fatalf("update must target the URL id, args=%v", args)
	}
	if code, _ := serve(t, h.HandleUpdate, request("PUT", "/normalizers/n-1", `nope`, "n-1")); code != http.StatusBadRequest {
		t.Fatalf("update bad JSON: code = %d", code)
	}
	h, _ = newTestHandler(nil)
	if code, _ := serve(t, h.HandleUpdate, request("PUT", "/normalizers/x", `{"name":"x"}`, "x")); code != http.StatusBadRequest {
		t.Fatalf("update missing: code = %d", code)
	}

	h, fake = newTestHandler(nil)
	fake.On("SELECT is_default", fakedb.Row(true))
	if code, _ := serve(t, h.HandleDelete, request("DELETE", "/normalizers/n-0", "", "n-0")); code != http.StatusBadRequest {
		t.Fatalf("deleting the default: code = %d", code)
	}
	h, fake = newTestHandler(nil)
	fake.On("SELECT is_default", fakedb.Row(false))
	fake.On("DELETE FROM", fakedb.Affected(1))
	if code, _ := serve(t, h.HandleDelete, request("DELETE", "/normalizers/n-1", "", "n-1")); code != http.StatusOK {
		t.Fatalf("delete: code = %d", code)
	}

	h, _ = newTestHandler(nil)
	if code, _ := serve(t, h.HandleSetDefault, request("POST", "/normalizers/x/set-default", "", "x")); code != http.StatusBadRequest {
		t.Fatalf("set-default missing: code = %d", code)
	}
	h, fake = newTestHandler(nil)
	fake.On("SELECT EXISTS", fakedb.Row(true))
	if code, _ := serve(t, h.HandleSetDefault, request("POST", "/normalizers/n-1/set-default", "", "n-1")); code != http.StatusOK {
		t.Fatalf("set-default: code = %d", code)
	}
}

func TestHandleTokenUsageAndDuplicate(t *testing.T) {
	h, fake := newTestHandler(nil)
	fake.On("FROM ingest_tokens", fakedb.Row("t-1", "edr", "f-1", "Prod"))
	code, env := serve(t, h.HandleTokenUsage, request("GET", "/normalizers/n-1/tokens", "", "n-1"))
	if code != http.StatusOK || !strings.Contains(string(env.Data), `"token_name":"edr"`) {
		t.Fatalf("code=%d data=%s", code, env.Data)
	}
	h, fake = newTestHandler(nil)
	fake.On("FROM ingest_tokens", fakedb.PGResult{Err: errors.New("x")})
	if code, _ := serve(t, h.HandleTokenUsage, request("GET", "/", "", "n-1")); code != http.StatusInternalServerError {
		t.Fatalf("token usage error: code = %d", code)
	}

	h, fake = newTestHandler(nil)
	fake.On("WHERE id = $1", fakedb.Row(sysmonRow()...))
	fake.On("SELECT EXISTS", fakedb.Row(false))
	fake.On("INSERT INTO normalizers", fakedb.Row(sysmonRow()...))
	if code, _ := serve(t, h.HandleDuplicate, request("POST", "/", "", "n-1")); code != http.StatusOK {
		t.Fatalf("duplicate: code = %d", code)
	}
	if args := fake.Matching("INSERT")[0].Args; args[0] != "sysmon (Copy)" || args[6] != "alice" {
		t.Fatalf("duplicate args = %v", args)
	}
	h, _ = newTestHandler(nil)
	if code, _ := serve(t, h.HandleDuplicate, request("POST", "/", "", "x")); code != http.StatusBadRequest {
		t.Fatalf("duplicate missing: code = %d", code)
	}
}

func TestHandlePreviewRunsIngestPipeline(t *testing.T) {
	h, fake := newTestHandler(nil)
	req := PreviewRequest{
		Transforms:    []Transform{TransformFlattenLeaf, TransformSnakeCase},
		FieldMappings: []FieldMapping{{Sources: []string{"computer"}, Target: "host"}},
		ValueMappings: []ValueMapping{{FromField: "event_id", ToField: "category", Map: map[string]string{"1": "process_creation"}}},
		SampleJSON:    `{"Computer":"WS01","System":{"EventID":1},"Big":"` + strings.Repeat("x", 600) + `"}`,
	}
	body, _ := json.Marshal(req)
	code, env := serve(t, h.HandlePreview, request("POST", "/normalizers/preview", string(body), ""))
	if code != http.StatusOK {
		t.Fatalf("code=%d err=%s", code, env.Error)
	}
	if len(fake.Calls()) != 0 {
		t.Fatal("preview must not touch the database")
	}
	var tr TraceResult
	if err := json.Unmarshal(env.Data, &tr); err != nil {
		t.Fatal(err)
	}
	got := map[string]TracedField{}
	for _, f := range tr.Fields {
		got[f.Name] = f
	}
	if f := got["host"]; f.Value != "WS01" || f.Source != "computer" || f.MappingIndex != 0 {
		t.Errorf("host = %+v", f)
	}
	if f := got["event_id"]; f.Value != "1" {
		t.Errorf("flatten_leaf + snake_case must yield event_id=1, got %+v", f)
	}
	if f := got["category"]; f.Value != "process_creation" || !f.Derived {
		t.Errorf("category = %+v", f)
	}
	if v := got["big"].Value; len(v) != maxPreviewValueLen+3 || !strings.HasSuffix(v, "...") {
		t.Errorf("long values must be cut to %d chars plus an ellipsis, got len %d", maxPreviewValueLen, len(v))
	}
}

func TestHandlePreviewRejectsBadInput(t *testing.T) {
	h, _ := newTestHandler(nil)
	for name, tc := range map[string]struct{ body, want string }{
		"malformed body":    {`{`, "Invalid JSON"},
		"missing sample":    {`{"sample_json":"  "}`, "sample_json is required"},
		"sample not JSON":   {`{"sample_json":"not json"}`, "Invalid sample JSON"},
		"sample not object": {`{"sample_json":"[1,2]"}`, "Invalid sample JSON"},
	} {
		code, env := serve(t, h.HandlePreview, request("POST", "/", tc.body, ""))
		if code != http.StatusBadRequest || !strings.Contains(env.Error, tc.want) {
			t.Errorf("%s: code=%d error=%q", name, code, env.Error)
		}
	}

	code, env := serve(t, h.HandlePreview, request("POST", "/", `{"sample_json":"{}"}`, ""))
	if code != http.StatusOK || !strings.Contains(string(env.Data), `"fields":[]`) {
		t.Fatalf("an empty sample must give an empty field list: %d %s", code, env.Data)
	}
}

func TestHandleSamplesKeepsOneLogPerShape(t *testing.T) {
	ch, fake := storagetest.ClickHouse()
	h, _ := newTestHandler(ch)
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	fake.On("FROM logs_raw", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "raw_log", Type: "String"}, {Column: "ts", Type: "DateTime64(9)"}},
		Rows: [][]any{
			{`{"b":1,"a":2}`, now},
			{`{"a":5,"b":6}`, now.Add(-time.Second)}, // same shape, different key order
			{`plain text line`, now.Add(-2 * time.Second)},
			{`{"msg":"x"}`, now.Add(-3 * time.Second)},
			{`{"c":1}`, now.Add(-4 * time.Second)},
		},
	})
	code, env := serve(t, h.HandleSamples, request("GET", "/normalizers/samples?fractal_id=f-1&limit=2", "", ""))
	if code != http.StatusOK {
		t.Fatalf("code=%d err=%s", code, env.Error)
	}
	var samples []LogSample
	if err := json.Unmarshal(env.Data, &samples); err != nil {
		t.Fatal(err)
	}
	if len(samples) != 2 {
		t.Fatalf("limit=2 must cap the result, got %d: %+v", len(samples), samples)
	}
	if samples[0].Shape != "a,b" || samples[0].FieldsNum != 2 || samples[0].RawLog != `{"b":1,"a":2}` {
		t.Errorf("first sample = %+v", samples[0])
	}
	if samples[1].Shape != "msg" {
		t.Errorf("second sample must skip the duplicate shape and the non-JSON line, got %+v", samples[1])
	}

	q := fake.QueriedMatching("FROM logs_raw")
	if len(q) != 1 || !strings.Contains(q[0], "WHERE fractal_id = ?") || !strings.Contains(q[0], "ORDER BY timestamp DESC") {
		t.Fatalf("sample query = %v", q)
	}
}

func TestHandleSamplesGuards(t *testing.T) {
	h, _ := newTestHandler(nil)
	if code, _ := serve(t, h.HandleSamples, request("GET", "/?fractal_id=f-1", "", "")); code != http.StatusServiceUnavailable {
		t.Fatalf("no ClickHouse: code = %d", code)
	}

	ch, fake := storagetest.ClickHouse()
	h, _ = newTestHandler(ch)
	if code, _ := serve(t, h.HandleSamples, request("GET", "/?fractal_id=%20", "", "")); code != http.StatusBadRequest {
		t.Fatalf("blank fractal: code = %d", code)
	}
	if len(fake.Queried()) != 0 {
		t.Fatal("a rejected request must not query")
	}

	// Out-of-range limits fall back to the default rather than scanning more.
	rows := make([][]any, 30)
	for i := range rows {
		rows[i] = []any{`{"k` + strings.Repeat("x", i) + `":1}`, time.Unix(int64(i), 0)}
	}
	fake.On("FROM logs_raw", fakedb.CHResult{Cols: []fakedb.Col{{Column: "raw_log", Type: "String"}, {Column: "ts", Type: "DateTime"}}, Rows: rows})
	for _, limit := range []string{"0", "21", "abc"} {
		_, env := serve(t, h.HandleSamples, request("GET", "/?fractal_id=f-1&limit="+limit, "", ""))
		var samples []LogSample
		json.Unmarshal(env.Data, &samples)
		if len(samples) != 5 {
			t.Errorf("limit=%s: got %d samples, want the default 5", limit, len(samples))
		}
	}

	ch, fake = storagetest.ClickHouse()
	h, _ = newTestHandler(ch)
	fake.On("FROM logs_raw", fakedb.CHResult{Err: errors.New("too many parts")})
	if code, _ := serve(t, h.HandleSamples, request("GET", "/?fractal_id=f-1", "", "")); code != http.StatusInternalServerError {
		t.Fatalf("query error: code = %d", code)
	}
}

// Export then import must reproduce the same definition: the YAML file is how
// normalizers move between instances, so a field that does not survive the trip
// silently changes field names on the receiving side.
func TestExportImportYAMLRoundTrip(t *testing.T) {
	h, fake := newTestHandler(nil)
	row := normalizerRow("n-1", "Sysmon / EDR v2", false, 3,
		`["flatten_leaf","snake_case"]`,
		`[{"sources":["computer","$.System.Computer"],"target":"computer_name"}]`,
		`[{"from_field":"event_id","to_field":"category","map":{"1":"process_creation","5000000":"big"},"default":"other"}]`,
		`[{"field":"utc_time","format":"2006-01-02 15:04:05.000"}]`)
	fake.On("WHERE id = $1", fakedb.Row(row...))

	w := httptest.NewRecorder()
	h.HandleExportYAML(w, request("GET", "/normalizers/n-1/export", "", "n-1"))
	if w.Code != http.StatusOK {
		t.Fatalf("export code = %d: %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/yaml" {
		t.Errorf("Content-Type = %q", ct)
	}
	if cd := w.Header().Get("Content-Disposition"); cd != `attachment; filename="Sysmon___EDR_v2.yaml"` {
		t.Errorf("Content-Disposition = %q", cd)
	}
	exported := w.Body.String()
	for _, key := range []string{"from_field:", "to_field:", "field_mappings:", "timestamp_fields:"} {
		if !strings.Contains(exported, key) {
			t.Errorf("export is missing %q; yaml.v3 would otherwise lowercase multi-word keys:\n%s", key, exported)
		}
	}

	h2, fake2 := newTestHandler(nil)
	fake2.On("INSERT INTO normalizers", fakedb.Row(row...))
	code, env := serve(t, h2.HandleImportYAML, request("POST", "/normalizers/import", exported, ""))
	if code != http.StatusOK {
		t.Fatalf("import code=%d err=%s", code, env.Error)
	}
	args := fake2.Matching("INSERT INTO normalizers")[0].Args
	if args[0] != "Sysmon / EDR v2" || args[1] != "desc of Sysmon / EDR v2" || args[6] != "alice" {
		t.Errorf("name/description/created_by = %v", args[:2])
	}
	for i, col := range []int{3, 4, 5, 6} {
		if !jsonEqual(t, args[2+i].(string), row[col].(string)) {
			t.Errorf("column %d changed in the round trip:\n got  %s\n want %s", col, args[2+i], row[col])
		}
	}
}

func TestExportMissing(t *testing.T) {
	h, _ := newTestHandler(nil)
	w := httptest.NewRecorder()
	h.HandleExportYAML(w, request("GET", "/", "", "gone"))
	if w.Code != http.StatusNotFound {
		t.Fatalf("code = %d", w.Code)
	}
}

func TestImportYAMLRejectsBadInput(t *testing.T) {
	for name, tc := range map[string]struct{ body, want string }{
		"not YAML":          {"name: [unclosed", "Invalid YAML"},
		"no name":           {"description: d\ntransforms: [lowercase]\n", "name is required"},
		"invalid transform": {"name: x\ntransforms: [shout]\n", "Failed to import: invalid transform"},
		"bad value map":     {"name: x\nvalue_mappings:\n  - from_field: a\n    to_field: a\n    default: z\n", "must differ"},
	} {
		h, fake := newTestHandler(nil)
		code, env := serve(t, h.HandleImportYAML, request("POST", "/", tc.body, ""))
		if code != http.StatusBadRequest || !strings.Contains(env.Error, tc.want) {
			t.Errorf("%s: code=%d error=%q", name, code, env.Error)
		}
		if len(fake.Matching("INSERT")) != 0 {
			t.Errorf("%s: reached the database", name)
		}
	}
}

// The export type's YAML tags are the file format; a hand-written file using the
// documented snake_case keys must bind every field.
func TestNormalizerExportYAMLKeys(t *testing.T) {
	src := `
name: edr
description: endpoint
transforms: [flatten_full, dedot]
field_mappings:
  - sources: [a, b]
    target: c
value_mappings:
  - from_field: c
    to_field: d
    map: {"1": one}
    default: other
timestamp_fields:
  - field: ts
    format: rfc3339
`
	var got NormalizerExport
	if err := yaml.Unmarshal([]byte(src), &got); err != nil {
		t.Fatal(err)
	}
	want := NormalizerExport{
		Name: "edr", Description: "endpoint",
		Transforms:      []Transform{TransformFlattenFull, TransformDedot},
		FieldMappings:   []FieldMapping{{Sources: []string{"a", "b"}, Target: "c"}},
		ValueMappings:   []ValueMapping{{FromField: "c", ToField: "d", Map: map[string]string{"1": "one"}, Default: "other"}},
		TimestampFields: []TimestampField{{Field: "ts", Format: "rfc3339"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %+v\nwant %+v", got, want)
	}
}

func TestSanitizeFilename(t *testing.T) {
	for in, want := range map[string]string{
		"Windows Sysmon":    "Windows_Sysmon",
		`a"; filename=x.sh`: "a___filename_x_sh",
		"../../etc/passwd":  "______etc_passwd",
		"édr-1_ok":          "_dr-1_ok",
	} {
		if got := sanitizeFilename(in); got != want {
			t.Errorf("sanitizeFilename(%q) = %q, want %q", in, got, want)
		}
	}
}
