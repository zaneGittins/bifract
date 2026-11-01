package models

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"bifract/pkg/storage"
)

// The handler tests cover every path that must answer before the model store is
// consulted: permission, scope and request validation. The handler's manager has
// no database, so a path that reached one would panic rather than pass.

type scope map[string]interface{}

var (
	analystInFractal = scope{"user": &storage.User{Username: "ana"}, "fractal_role": "analyst", "selected_fractal": "f1"}
	viewerInFractal  = scope{"user": &storage.User{Username: "vic"}, "fractal_role": "viewer", "selected_fractal": "f1"}
	adminInFractal   = scope{"user": &storage.User{Username: "adm", IsAdmin: true}, "selected_fractal": "f1"}
	analystInPrism   = scope{"user": &storage.User{Username: "ana"}, "prism_role": "analyst", "selected_prism": "p1"}
	analystNoScope   = scope{"user": &storage.User{Username: "ana"}, "fractal_role": "analyst"}
)

type apiResponse struct {
	Success bool            `json:"success"`
	Data    json.RawMessage `json:"data"`
	Error   string          `json:"error"`
}

func call(t *testing.T, h http.HandlerFunc, body string, sc scope) (int, apiResponse) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/models", strings.NewReader(body))
	ctx := r.Context()
	for k, v := range sc {
		ctx = context.WithValue(ctx, k, v)
	}
	w := httptest.NewRecorder()
	h(w, r.WithContext(ctx))
	var resp apiResponse
	if w.Body.Len() > 0 {
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("response is not the JSON envelope: %v\n%s", err, w.Body.String())
		}
	}
	return w.Code, resp
}

func testHandler() *Handler { return NewHandler(testManager(), nil) }

// Every endpoint that changes a model, runs a backfill, toggles its alert or scans
// raw logs for a preview needs analyst. A viewer, or a request with no user, is
// refused before anything is read.
func TestMutatingEndpointsRequireAnalyst(t *testing.T) {
	h := testHandler()
	endpoints := map[string]http.HandlerFunc{
		"create": h.HandleCreate, "update": h.HandleUpdate, "delete": h.HandleDelete,
		"start backfill": h.HandleStartBackfill, "cancel backfill": h.HandleCancelBackfill,
		"enable alert": h.HandleEnableAlert, "disable alert": h.HandleDisableAlert,
		"preview": h.HandlePreview, "import": h.HandleImport,
	}
	for name, ep := range endpoints {
		for who, sc := range map[string]scope{"viewer": viewerInFractal, "anonymous": {"selected_fractal": "f1", "fractal_role": "admin"}} {
			code, resp := call(t, ep, `{}`, sc)
			if code != http.StatusForbidden || resp.Success {
				t.Errorf("%s as %s: %d %+v, want 403", name, who, code, resp)
			}
		}
	}
}

// Models belong to a fractal. A prism scope is refused by name, not resolved to
// some default fractal the caller may hold no role on.
func TestPrismScopeIsRefused(t *testing.T) {
	h := testHandler()
	for name, ep := range map[string]http.HandlerFunc{
		"list": h.HandleList, "create": h.HandleCreate, "preview": h.HandlePreview,
		"import": h.HandleImport, "test extraction": h.HandleTestExtraction,
	} {
		code, resp := call(t, ep, `{}`, analystInPrism)
		if code != http.StatusBadRequest || !strings.Contains(resp.Error, "Select a fractal rather than a prism") {
			t.Errorf("%s: %d %q", name, code, resp.Error)
		}
	}
	// With no fractal selected and nothing to fall back on, the request fails
	// rather than landing in an arbitrary fractal.
	code, resp := call(t, h.HandleList, ``, analystNoScope)
	if code != http.StatusBadRequest || !strings.Contains(resp.Error, "Failed to determine fractal context") {
		t.Errorf("no scope: %d %q", code, resp.Error)
	}
}

func TestMalformedBodiesAreBadRequests(t *testing.T) {
	h := testHandler()
	for name, ep := range map[string]http.HandlerFunc{
		"create": h.HandleCreate, "preview": h.HandlePreview, "test extraction": h.HandleTestExtraction,
		"generate query": h.HandleGenerateQuery, "parse query": h.HandleParseQuery,
	} {
		code, resp := call(t, ep, `{"name":`, analystInFractal)
		if code != http.StatusBadRequest || resp.Error != "invalid request body" {
			t.Errorf("%s: %d %q", name, code, resp.Error)
		}
	}
}

func TestCreateValidationIsABadRequest(t *testing.T) {
	code, resp := call(t, testHandler().HandleCreate, `{"name":"","model_type":"first_seen","definition":{"key_fields":["u"]}}`, adminInFractal)
	if code != http.StatusBadRequest || !strings.Contains(resp.Error, "name is required") {
		t.Errorf("%d %q", code, resp.Error)
	}
	code, resp = call(t, testHandler().HandleCreate, `{"name":"n","model_type":"rarity","definition":{"partition_key":"a b","value_key":"c"}}`, analystInFractal)
	if code != http.StatusBadRequest || !strings.Contains(resp.Error, "not a field name") {
		t.Errorf("%d %q", code, resp.Error)
	}
}

func TestPreviewValidation(t *testing.T) {
	h := testHandler()
	cases := []struct {
		body string
		want string
	}{
		{`{"model_type":"first_seen","definition":{"key_fields":["u"]},"window":"2d"}`, "invalid preview window"},
		{`{"model_type":"first_seen","definition":{"key_fields":["u"]},"start":"2026-01-01T00:00:00Z"}`, "needs both a start and an end"},
		{`{"model_type":"first_seen","definition":{"key_fields":["u"]},"start":"2026-01-02T00:00:00Z","end":"2026-01-01T00:00:00Z"}`, "must start before it ends"},
		{`{"model_type":"rarity","definition":{},"window":"1d"}`, "Preview failed: partition_key is required"},
	}
	for _, c := range cases {
		code, resp := call(t, h.HandlePreview, c.body, analystInFractal)
		if code != http.StatusBadRequest || !strings.Contains(resp.Error, c.want) {
			t.Errorf("%s: %d %q, want %q", c.body, code, resp.Error, c.want)
		}
	}
}

// The generate and parse endpoints are pure: they render or read BQL and touch no
// store, so any authenticated caller may use them.
func TestGenerateAndParseQuery(t *testing.T) {
	h := testHandler()
	code, resp := call(t, h.HandleGenerateQuery,
		`{"name":"rare","model_type":"volume_baseline","definition":{"key_fields":["host"],"alert":{"z_threshold":5}}}`, viewerInFractal)
	if code != http.StatusOK {
		t.Fatalf("generate: %d %s", code, resp.Error)
	}
	var gen map[string]string
	_ = json.Unmarshal(resp.Data, &gen)
	want := GenerateQuery("rare", ModelDefinition{KeyFields: []string{"host"}, Alert: &AlertConfig{ZThreshold: 5}}, ModelTypeVolumeBaseline)
	if gen["query"] != want {
		t.Errorf("generate: %q, want %q", gen["query"], want)
	}

	code, resp = call(t, h.HandleParseQuery, `{"query":"level = \"dns\" | cidr(src_ip, \"10.0.0.0/8\")"}`, viewerInFractal)
	if code != http.StatusOK {
		t.Fatalf("parse: %d %s", code, resp.Error)
	}
	var parsed ParsedSource
	_ = json.Unmarshal(resp.Data, &parsed)
	if len(parsed.Filter) != 2 || parsed.Filter[1].Op != "cidr" || !parsed.FilterComplete {
		t.Errorf("parse: %+v", parsed)
	}
	// A query the source cannot be is reported in the body, not as an HTTP error.
	code, resp = call(t, h.HandleParseQuery, `{"query":"level = \"a\" | groupby(host)"}`, viewerInFractal)
	_ = json.Unmarshal(resp.Data, &parsed)
	if code != http.StatusOK || len(parsed.Errors) == 0 {
		t.Errorf("rejected source: %d %+v", code, parsed)
	}
}

func TestImportValidation(t *testing.T) {
	h := testHandler()
	cases := []struct {
		body string
		code int
		want string
	}{
		{"name: [unclosed", http.StatusBadRequest, "Invalid YAML"},
		{"model_type: rarity\n", http.StatusBadRequest, "Model name is required"},
		{"name: n\nmodel_type: rarity\ndefinition:\n  value_key: v\n", http.StatusInternalServerError, "partition_key is required"},
	}
	for _, c := range cases {
		code, resp := call(t, h.HandleImport, c.body, analystInFractal)
		if code != c.code || !strings.Contains(resp.Error, c.want) {
			t.Errorf("%q: %d %q, want %d %q", c.body, code, resp.Error, c.code, c.want)
		}
	}
}

func TestTestExtractionRejectsAnEmptyRequest(t *testing.T) {
	code, resp := call(t, testHandler().HandleTestExtraction, `{"filter":[],"extractions":[]}`, viewerInFractal)
	if code != http.StatusInternalServerError || !strings.Contains(resp.Error, "no extractions provided") {
		t.Errorf("%d %q", code, resp.Error)
	}
}
