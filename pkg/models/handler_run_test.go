package models

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
	"gopkg.in/yaml.v3"
)

// Handler paths past permission and scope checks, over a loaded model.

var analystA = scope{"user": &storage.User{Username: "ana"}, "fractal_role": "analyst", "selected_fractal": fracA}

func callModel(t *testing.T, h http.HandlerFunc, target, body, id string, sc scope) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	rc := chi.NewRouteContext()
	rc.URLParams.Add("id", id)
	ctx := context.WithValue(r.Context(), chi.RouteCtxKey, rc)
	for k, v := range sc {
		ctx = context.WithValue(ctx, k, v)
	}
	w := httptest.NewRecorder()
	h(w, r.WithContext(ctx))
	return w
}

func decode(t *testing.T, w *httptest.ResponseRecorder, into any) {
	t.Helper()
	var env struct {
		Success bool            `json:"success"`
		Data    json.RawMessage `json:"data"`
		Error   string          `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil {
		t.Fatalf("not the envelope: %v\n%s", err, w.Body.String())
	}
	if !env.Success {
		t.Fatalf("%d %s", w.Code, env.Error)
	}
	if into != nil {
		if err := json.Unmarshal(env.Data, into); err != nil {
			t.Fatalf("data: %v\n%s", err, env.Data)
		}
	}
}

func handlerEnv(t *testing.T) (*fakeEnv, *Handler, *modelRow) {
	env := newEnv(t)
	wm := time.Now().UTC().Add(-time.Minute)
	r := seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.name, r.watermark = "rare procs", &wm })
	return env, NewHandler(env.mgr, nil), r
}

// A model in another fractal does not exist as far as this scope is concerned.
func TestHandlersHideOtherFractalsModels(t *testing.T) {
	env, h, _ := handlerEnv(t)
	other := seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.fractalID = fracB })
	for name, ep := range map[string]http.HandlerFunc{
		"get": h.HandleGet, "data": h.HandleGetData, "stats": h.HandleGetStats, "histogram": h.HandleGetHistogram,
		"export": h.HandleExport, "update": h.HandleUpdate, "delete": h.HandleDelete, "backfill": h.HandleStartBackfill,
		"enable": h.HandleEnableAlert,
	} {
		if w := callModel(t, ep, "/", `{}`, other.id, analystA); w.Code != http.StatusNotFound {
			t.Errorf("%s: %d", name, w.Code)
		}
		if w := callModel(t, ep, "/", `{}`, "missing", analystA); w.Code != http.StatusNotFound {
			t.Errorf("%s missing: %d", name, w.Code)
		}
	}
	if _, ok := env.db.get(other.id); !ok {
		t.Error("another fractal's model was deleted")
	}
}

func TestHandleGetAndList(t *testing.T) {
	env, h, r := handlerEnv(t)
	env.ch.On("SELECT count() FROM `"+r.chTable+"`", fakedb.Count(17))
	var detail struct {
		ID              string `json:"id"`
		RowCount        uint64 `json:"row_count"`
		StateLagSeconds *int64 `json:"state_lag_seconds"`
	}
	decode(t, callModel(t, h.HandleGet, "/", "", r.id, analystA), &detail)
	if detail.ID != r.id || detail.RowCount != 17 || detail.StateLagSeconds == nil {
		t.Errorf("detail: %+v", detail)
	}

	var list []Model
	decode(t, callModel(t, h.HandleList, "/", "", "", analystA), &list)
	if len(list) != 1 || list[0].Health == nil || list[0].StateLagSeconds == nil {
		t.Fatalf("list: %+v", list)
	}
	env.db.failOn("ORDER BY name", errors.New("pg down"))
	if w := callModel(t, h.HandleList, "/", "", "", analystA); w.Code != http.StatusInternalServerError {
		t.Errorf("list failure: %d", w.Code)
	}
}

func TestHandleGetData(t *testing.T) {
	env, h, _ := handlerEnv(t)
	r := seedModel(env, ModelTypeRarity, rarityDef, nil)
	env.ch.On("SELECT count() FROM (", fakedb.Count(120))
	env.ch.On(" ORDER BY ", fakedb.CHResult{Cols: cols("value_val", "String"), Rows: [][]any{{"a"}}})

	w := callModel(t, h.HandleGetData, "/?view=findings&limit=20&offset=-3&sort=percent&order=asc", "", r.id, analystA)
	var page struct {
		Data []map[string]any                   `json:"data"`
		Page struct{ Total, Limit, Offset int } `json:"page"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil || w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	if page.Page.Total != 120 || page.Page.Limit != 20 || page.Page.Offset != 0 || len(page.Data) != 1 {
		t.Errorf("page: %+v", page)
	}
	if q := env.ch.QueriedMatching(" ORDER BY "); len(q) != 1 || !strings.HasSuffix(q[0], "ORDER BY percent ASC LIMIT 20 OFFSET 0") {
		t.Errorf("query: %v", q)
	}
	if w := callModel(t, h.HandleGetData, "/?view=everything", "", r.id, analystA); w.Code != http.StatusBadRequest {
		t.Errorf("bad view: %d", w.Code)
	}
	env.ch.ClearRules()
	env.ch.On("SELECT count()", fakedb.CHResult{Err: errors.New("boom")})
	if w := callModel(t, h.HandleGetData, "/", "", r.id, analystA); w.Code != http.StatusInternalServerError {
		t.Errorf("read failure: %d", w.Code)
	}
	// A findings view the model has no rule for is an empty page, not an error.
	plain := seedModel(env, ModelTypeRarity, ModelDefinition{PartitionKey: "h", ValueKey: "v"}, nil)
	if w := callModel(t, h.HandleGetData, "/?view=findings", "", plain.id, analystA); w.Code != 200 || !strings.Contains(w.Body.String(), `"data":[]`) {
		t.Errorf("no rule: %d %s", w.Code, w.Body.String())
	}
}

func TestHandleStatsAndHistogram(t *testing.T) {
	env, h, r := handlerEnv(t)
	env.ch.On("AS flagged FROM (", fakedb.CHResult{Cols: cols("total", "UInt64", "flagged", "UInt64"), Rows: [][]any{{uint64(5), uint64(1)}}})
	var hist map[string]any
	decode(t, callModel(t, h.HandleGetHistogram, "/", "", r.id, analystA), &hist)
	if hist["flagged"] != float64(1) {
		t.Errorf("histogram: %v", hist)
	}
	var stats map[string]any
	decode(t, callModel(t, h.HandleGetStats, "/", "", r.id, analystA), &stats)
	if stats["findings_rule"] != "coverage > 0.9" {
		t.Errorf("stats: %v", stats)
	}
	env.ch.ClearRules()
	env.ch.On("SELECT", fakedb.CHResult{Err: errors.New("boom")})
	for name, ep := range map[string]http.HandlerFunc{"stats": h.HandleGetStats, "histogram": h.HandleGetHistogram} {
		if w := callModel(t, ep, "/", "", r.id, analystA); w.Code != http.StatusInternalServerError {
			t.Errorf("%s failure: %d", name, w.Code)
		}
	}
}

// An export re-imports as the same model.
func TestHandleExportImportRoundTrip(t *testing.T) {
	env, h, r := handlerEnv(t)
	w := callModel(t, h.HandleExport, "/", "", r.id, analystA)
	if w.Code != 200 || w.Header().Get("Content-Disposition") != `attachment; filename="rare_procs.yaml"` {
		t.Fatalf("%d %v", w.Code, w.Header())
	}
	var exp modelExport
	if err := yaml.Unmarshal(w.Body.Bytes(), &exp); err != nil || exp.Name != "rare procs" || exp.Definition.PartitionKey != "host" {
		t.Fatalf("export: %+v %v", exp, err)
	}

	imp := callModel(t, h.HandleImport, "/", w.Body.String(), "", analystA)
	var mo Model
	decode(t, imp, &mo)
	if mo.Name != "rare procs" || mo.ModelType != ModelTypeRarity || mo.Definition.Alert.CoverageThreshold != 0.9 || mo.FractalID != fracA {
		t.Errorf("imported: %+v", mo)
	}
	waitFor(t, "the imported model to build", func() bool { g, _ := env.db.get(mo.ID); return g.status == "active" })
}

func TestHandleCreateUpdateDelete(t *testing.T) {
	env, h, _ := handlerEnv(t)
	body := `{"name":"firsts","model_type":"first_seen","definition":{"key_fields":["user"]}}`
	w := callModel(t, h.HandleCreate, "/", body, "", analystA)
	if w.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	var mo Model
	decode(t, w, &mo)
	if mo.CreatedBy != "ana" || mo.AlertMode != "none" {
		t.Errorf("created: %+v", mo)
	}
	waitFor(t, "the build", func() bool { g, _ := env.db.get(mo.ID); return g.status == "active" })

	upd := `{"name":"firsts v2","definition":{"key_fields":["user"]}}`
	var updated Model
	decode(t, callModel(t, h.HandleUpdate, "/", upd, mo.ID, analystA), &updated)
	if updated.Name != "firsts v2" {
		t.Errorf("updated: %+v", updated)
	}
	bad := `{"name":"x","definition":{"key_fields":[]}}`
	if w := callModel(t, h.HandleUpdate, "/", bad, mo.ID, analystA); w.Code == http.StatusOK {
		t.Error("invalid update accepted")
	}
	if g, _ := env.db.get(mo.ID); g.name != "firsts v2" {
		t.Errorf("a refused update wrote: %q", g.name)
	}

	decode(t, callModel(t, h.HandleDelete, "/", "", mo.ID, analystA), nil)
	if _, ok := env.db.get(mo.ID); ok {
		t.Error("not deleted")
	}
	waitFor(t, "the drops", func() bool { return len(env.ch.ExecutedMatching("DROP TABLE IF EXISTS `"+mo.CHTableName+"`")) == 1 })
}

func TestHandleBackfillAndAlerts(t *testing.T) {
	env, h, r := handlerEnv(t)
	alerts := newRecordingAlerts()
	env.mgr.SetAlertManager(alerts)
	env.mgr.bfSem <- struct{}{} // keep the run queued so the response sees it running

	if w := callModel(t, h.HandleStartBackfill, "/", `{"window":"365d"}`, r.id, analystA); w.Code != http.StatusBadRequest {
		t.Errorf("bad window: %d", w.Code)
	}
	var mo Model
	decode(t, callModel(t, h.HandleStartBackfill, "/", `{"window":"24h"}`, r.id, analystA), &mo)
	if mo.BackfillStatus != "running" || mo.BackfillWindow != "24h" {
		t.Errorf("started: %+v", mo)
	}
	decode(t, callModel(t, h.HandleCancelBackfill, "/", "", r.id, analystA), nil)
	<-env.mgr.bfSem
	waitBackfill(t, env, r.id)
	if g, _ := env.db.get(r.id); g.bfStatus != "cancelled" {
		t.Errorf("after cancel: %q", g.bfStatus)
	}

	ranged := seedModel(env, ModelTypeRarity, rarityDef, nil)
	for name, body := range map[string]string{
		"reversed":   `{"from":"2026-05-08T00:00:00Z","to":"2026-05-01T00:00:00Z"}`,
		"too long":   `{"from":"2026-01-01T00:00:00Z","to":"2026-06-01T00:00:00Z"}`,
		"both kinds": `{"window":"7d","from":"2026-05-01T00:00:00Z","to":"2026-05-02T00:00:00Z"}`,
	} {
		if w := callModel(t, h.HandleStartBackfill, "/", body, ranged.id, analystA); w.Code != http.StatusBadRequest {
			t.Errorf("%s range: %d", name, w.Code)
		}
	}
	env.mgr.bfSem <- struct{}{}
	decode(t, callModel(t, h.HandleStartBackfill, "/", `{"from":"2026-05-01T00:00:00Z","to":"2026-05-08T00:00:00Z"}`, ranged.id, analystA), &mo)
	if mo.BackfillStatus != "running" || mo.BackfillWindow != "range" || mo.BackfillFrom == nil || mo.BackfillTo == nil ||
		!mo.BackfillFrom.Equal(time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)) || mo.BackfillTotal != 7 {
		t.Errorf("range started: %+v", mo)
	}
	env.mgr.CancelBackfill(ranged.id)
	<-env.mgr.bfSem
	waitBackfill(t, env, ranged.id)

	decode(t, callModel(t, h.HandleEnableAlert, "/", "", r.id, analystA), nil)
	decode(t, callModel(t, h.HandleDisableAlert, "/", "", r.id, analystA), nil)
	if on, ok := alerts.enabled["alert-1"]; !ok || on {
		t.Errorf("alert state: %v", alerts.enabled)
	}
	alerts.setErr = errors.New("nope")
	for name, ep := range map[string]http.HandlerFunc{"enable": h.HandleEnableAlert, "disable": h.HandleDisableAlert} {
		if w := callModel(t, ep, "/", "", r.id, analystA); w.Code != http.StatusInternalServerError {
			t.Errorf("%s failure: %d", name, w.Code)
		}
	}
}

func TestHandlePreviewAndTestExtraction(t *testing.T) {
	env, h, _ := handlerEnv(t)
	env.ch.On("AS network_size", fakedb.One("network_size", "UInt64", uint64(0)))
	var res PreviewResult
	decode(t, callModel(t, h.HandlePreview, "/", `{"model_type":"beacon","window":"1d","definition":{}}`, "", analystA), &res)
	if res.ModelType != ModelTypeBeacon || res.Window != "1d" {
		t.Errorf("preview: %+v", res)
	}
	env.ch.ClearRules()
	env.ch.On("AS network_size", fakedb.CHResult{Err: errors.New("boom")})
	if w := callModel(t, h.HandlePreview, "/", `{"model_type":"beacon","window":"1d","definition":{}}`, "", analystA); w.Code != http.StatusBadRequest {
		t.Errorf("preview failure: %d", w.Code)
	}

	env.ch.On("LIMIT 50", fakedb.CHResult{Cols: cols("d", "String", "cnt", "UInt64"), Rows: [][]any{{"x", uint64(1)}}})
	var ext ExtractionTest
	decode(t, callModel(t, h.HandleTestExtraction, "/",
		`{"extractions":[{"from_field":"url","output_field":"d","pattern":"//([^/]+)"}]}`, "", analystA), &ext)
	if ext.Count != 1 || ext.SQL == "" {
		t.Errorf("extraction: %+v", ext)
	}
}
