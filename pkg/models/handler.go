package models

import (
	"bifract/pkg/api"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"bifract/pkg/auth"
	"bifract/pkg/fractals"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"
	"github.com/go-chi/chi/v5"
	"gopkg.in/yaml.v3"
)

type modelExport struct {
	Name        string          `yaml:"name"`
	Description string          `yaml:"description,omitempty"`
	ModelType   ModelType       `yaml:"model_type"`
	Definition  ModelDefinition `yaml:"definition"`
}

// Handler provides HTTP endpoints for analytics model management.
type Handler struct {
	manager        *Manager
	fractalManager *fractals.Manager
}

// NewHandler creates a new models handler.
func NewHandler(manager *Manager, fractalManager *fractals.Manager) *Handler {
	return &Handler{manager: manager, fractalManager: fractalManager}
}

// HandleList lists all models for the current fractal.
func (h *Handler) HandleList(w http.ResponseWriter, r *http.Request) {
	fractalID, err := h.getFractalID(r)
	if err != nil {
		h.respondScopeError(w, err)
		return
	}
	models, err := h.manager.List(r.Context(), fractalID)
	if err != nil {
		log.Printf("[Models] list: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load models")
		return
	}
	for _, mo := range models {
		mo.SourceQuery = GenerateSourceQuery(mo.Definition)
	}
	api.WriteList(w, models)
}

// ModelDetail is a model together with the row count of its data table. The
// model's own fields are inlined, so the payload reads as the model itself.
type ModelDetail struct {
	*Model
	RowCount uint64 `json:"row_count"`
}

// HandleGet returns a single model by ID.
func (h *Handler) HandleGet(w http.ResponseWriter, r *http.Request) {
	model := h.getModelScoped(w, r)
	if model == nil {
		return
	}
	model.SourceQuery = GenerateSourceQuery(model.Definition)
	rowCount, _ := h.manager.RowCount(r.Context(), h.manager.readTableName(model))
	h.respondSuccess(w, ModelDetail{Model: model, RowCount: rowCount})
}

// HandleCreate creates a new model.
func (h *Handler) HandleCreate(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	fractalID, err := h.getFractalID(r)
	if err != nil {
		h.respondScopeError(w, err)
		return
	}

	var req CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.AlertMode == "" {
		req.AlertMode = "none"
	}

	createdBy := auth.AttributionUsername(r.Context())
	model, err := h.manager.Create(r.Context(), fractalID, req, createdBy)
	if err != nil {
		log.Printf("[Models] create: %v", err)
		h.respondError(w, http.StatusBadRequest, fmt.Sprintf("Failed to create model: %v", err))
		return
	}
	w.WriteHeader(http.StatusCreated)
	h.respondSuccess(w, model)
}

// HandleUpdate updates a model's definition. Rebuilds CH objects (data resets forward-only).
func (h *Handler) HandleUpdate(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	model := h.getModelScoped(w, r)
	if model == nil {
		return
	}

	var req UpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	updated, err := h.manager.Update(r.Context(), model.ID, req)
	if err != nil {
		log.Printf("[Models] update %s: %v", model.ID, err)
		h.respondError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to update model: %v", err))
		return
	}
	h.respondSuccess(w, updated)
}

// HandleDelete deletes a model and drops its ClickHouse objects.
func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	model := h.getModelScoped(w, r)
	if model == nil {
		return
	}
	if err := h.manager.Delete(r.Context(), model.ID); err != nil {
		log.Printf("[Models] delete %s: %v", model.ID, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to delete model")
		return
	}
	h.respondSuccess(w, map[string]bool{"deleted": true})
}

// HandleGetData returns paginated model data (with computed scores for rarity).
func (h *Handler) HandleGetData(w http.ResponseWriter, r *http.Request) {
	model := h.getModelScoped(w, r)
	if model == nil {
		return
	}
	fractalID, err := h.getFractalID(r)
	if err != nil {
		h.respondScopeError(w, err)
		return
	}

	q := r.URL.Query()
	search := q.Get("search")
	sortCol := q.Get("sort")
	sortDir := q.Get("order")
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	if limit <= 0 {
		limit = 50
	}

	rows, total, err := h.manager.GetData(r.Context(), model, fractalID, search, sortCol, sortDir, limit, offset)
	if err != nil {
		log.Printf("[Models] get data %s: %v", model.ID, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to fetch model data")
		return
	}
	if rows == nil {
		rows = []map[string]interface{}{}
	}
	api.WritePage(w, rows, api.Page{Total: int(total), Limit: limit, Offset: offset})
}

// HandleStartBackfill kicks off a one-time historical backfill for a model over
// the requested window (24h/7d/30d/90d). Returns the model with running state.
func (h *Handler) HandleStartBackfill(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	model := h.getModelScoped(w, r)
	if model == nil {
		return
	}
	var req BackfillRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.manager.StartBackfill(r.Context(), model, req.Window); err != nil {
		h.respondError(w, http.StatusBadRequest, fmt.Sprintf("Failed to start backfill: %v", err))
		return
	}
	updated, err := h.manager.Get(r.Context(), model.ID)
	if err != nil {
		h.respondError(w, http.StatusInternalServerError, "Failed to load model")
		return
	}
	h.respondSuccess(w, updated)
}

// HandleCancelBackfill stops an in-flight backfill. Partial data is kept.
func (h *Handler) HandleCancelBackfill(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	model := h.getModelScoped(w, r)
	if model == nil {
		return
	}
	h.manager.CancelBackfill(model.ID)
	h.respondSuccess(w, map[string]bool{"cancelled": true})
}

// TestExtractionRequest carries a filter and extraction steps to try against recent logs.
type TestExtractionRequest struct {
	Filter      []FilterCondition `json:"filter"`
	Extractions []ExtractionStep  `json:"extractions"`
}

// ExtractionTest is a sample extraction run: the rows it produced and the SQL
// that produced them.
type ExtractionTest struct {
	Results []map[string]interface{} `json:"results"`
	Count   int                      `json:"count"`
	SQL     string                   `json:"sql"`
}

// HandleTestExtraction runs a sample extraction against recent logs and returns matched values.
func (h *Handler) HandleTestExtraction(w http.ResponseWriter, r *http.Request) {
	fractalID, err := h.getFractalID(r)
	if err != nil {
		h.respondScopeError(w, err)
		return
	}

	var req TestExtractionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	results, sql, err := h.manager.TestExtraction(r.Context(), fractalID, req.Filter, req.Extractions)
	if err != nil {
		log.Printf("[Models] test extraction: %v", err)
		h.respondError(w, http.StatusInternalServerError, fmt.Sprintf("Extraction failed: %v", err))
		return
	}
	if results == nil {
		results = []map[string]interface{}{}
	}
	h.respondSuccess(w, ExtractionTest{Results: results, Count: len(results), SQL: sql})
}

// HandleEnableAlert enables the linked alert for a model.
func (h *Handler) HandleEnableAlert(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	model := h.getModelScoped(w, r)
	if model == nil {
		return
	}
	if err := h.manager.SetAlertEnabled(r.Context(), model.ID, true); err != nil {
		h.respondError(w, http.StatusInternalServerError, "Failed to enable alert")
		return
	}
	h.respondSuccess(w, map[string]bool{"enabled": true})
}

// HandleDisableAlert pauses the linked alert for a model.
func (h *Handler) HandleDisableAlert(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	model := h.getModelScoped(w, r)
	if model == nil {
		return
	}
	if err := h.manager.SetAlertEnabled(r.Context(), model.ID, false); err != nil {
		h.respondError(w, http.StatusInternalServerError, "Failed to disable alert")
		return
	}
	h.respondSuccess(w, map[string]bool{"disabled": true})
}

// GenerateQueryRequest carries a model definition to lower into BQL.
type GenerateQueryRequest struct {
	Name       string          `json:"name"`
	ModelType  ModelType       `json:"model_type"`
	Definition ModelDefinition `json:"definition"`
}

// HandleGenerateQuery returns the auto-generated BQL query for a model definition.
// Useful for previewing what the alert query would look like before committing.
func (h *Handler) HandleGenerateQuery(w http.ResponseWriter, r *http.Request) {
	var req GenerateQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	query := GenerateQuery(req.Name, req.Definition, req.ModelType)
	h.respondSuccess(w, map[string]string{"query": query})
}

// ParseQueryRequest carries BQL to raise into a structured model definition.
type ParseQueryRequest struct {
	Query     string    `json:"query"`
	ModelType ModelType `json:"model_type"`
}

// HandleParseQuery lowers a BQL source query into the structured filter +
// extraction half of a model definition, returning candidate fields for shaping
// and any validation messages. Validation problems are returned in the body
// (HTTP 200); only a malformed request body is a 400.
func (h *Handler) HandleParseQuery(w http.ResponseWriter, r *http.Request) {
	var req ParseQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	parsed := ParseSourceQuery(req.Query, req.ModelType)
	h.respondSuccess(w, parsed)
}

// PreviewRequest estimates a model definition over a recent window.
type PreviewRequest struct {
	ModelType  ModelType       `json:"model_type"`
	Definition ModelDefinition `json:"definition"`
	Window     string          `json:"window"`
}

// HandlePreview estimates a model's output over a recent window BEFORE the model
// is created, so an author can tune the definition and alert thresholds and see
// whether it surfaces the right results first. It creates no ClickHouse objects.
func (h *Handler) HandlePreview(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	fractalID, err := h.getFractalID(r)
	if err != nil {
		h.respondScopeError(w, err)
		return
	}
	var req PreviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Window == "" {
		req.Window = "7d"
	}
	result, err := h.manager.Preview(r.Context(), fractalID, req.ModelType, req.Definition, req.Window)
	if err != nil {
		log.Printf("[Models] preview: %v", err)
		h.respondError(w, http.StatusBadRequest, fmt.Sprintf("Preview failed: %v", err))
		return
	}
	h.respondSuccess(w, result)
}

// HandleGetStats returns aggregate statistics for a model's data table.
func (h *Handler) HandleGetStats(w http.ResponseWriter, r *http.Request) {
	model := h.getModelScoped(w, r)
	if model == nil {
		return
	}
	fractalID, err := h.getFractalID(r)
	if err != nil {
		h.respondScopeError(w, err)
		return
	}
	stats, err := h.manager.GetStats(r.Context(), model, fractalID)
	if err != nil {
		log.Printf("[Models] get stats %s: %v", model.ID, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to fetch model stats")
		return
	}
	h.respondSuccess(w, stats)
}

// HandleGetHistogram returns a type-aware score distribution for a model so
// creators can see the shape of its output.
func (h *Handler) HandleGetHistogram(w http.ResponseWriter, r *http.Request) {
	model := h.getModelScoped(w, r)
	if model == nil {
		return
	}
	fractalID, err := h.getFractalID(r)
	if err != nil {
		h.respondScopeError(w, err)
		return
	}
	hist, err := h.manager.GetHistogram(r.Context(), model, fractalID)
	if err != nil {
		log.Printf("[Models] get histogram %s: %v", model.ID, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to fetch model histogram")
		return
	}
	h.respondSuccess(w, hist)
}

// HandleExport returns the model definition as a downloadable YAML file.
func (h *Handler) HandleExport(w http.ResponseWriter, r *http.Request) {
	model := h.getModelScoped(w, r)
	if model == nil {
		return
	}
	exp := modelExport{
		Name:        model.Name,
		Description: model.Description,
		ModelType:   model.ModelType,
		Definition:  model.Definition,
	}
	out, err := yaml.Marshal(exp)
	if err != nil {
		h.respondError(w, http.StatusInternalServerError, "Failed to serialize model")
		return
	}
	filename := strings.ReplaceAll(model.Name, " ", "_") + ".yaml"
	w.Header().Set("Content-Type", "application/yaml")
	w.Header().Set("Content-Disposition", `attachment; filename="`+filename+`"`)
	w.Write(out)
}

// HandleImport creates a model from an uploaded YAML definition.
func (h *Handler) HandleImport(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	fractalID, err := h.getFractalID(r)
	if err != nil {
		h.respondScopeError(w, err)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "Failed to read request body")
		return
	}
	var exp modelExport
	if err := yaml.Unmarshal(body, &exp); err != nil {
		h.respondError(w, http.StatusBadRequest, fmt.Sprintf("Invalid YAML: %v", err))
		return
	}
	if exp.Name == "" {
		h.respondError(w, http.StatusBadRequest, "Model name is required")
		return
	}
	req := CreateRequest{
		Name:        exp.Name,
		Description: exp.Description,
		ModelType:   exp.ModelType,
		Definition:  exp.Definition,
		AlertMode:   "none",
	}
	model, err := h.manager.Create(r.Context(), fractalID, req, auth.AttributionUsername(r.Context()))
	if err != nil {
		log.Printf("[Models] import: %v", err)
		h.respondError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to import model: %v", err))
		return
	}
	h.respondSuccess(w, model)
}

// ---- Helpers ----

func (h *Handler) getModelScoped(w http.ResponseWriter, r *http.Request) *Model {
	id := chi.URLParam(r, "id")
	model, err := h.manager.Get(r.Context(), id)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Model not found")
		return nil
	}
	fractalID, err := h.getFractalID(r)
	if err != nil {
		h.respondScopeError(w, err)
		return nil
	}
	if model.FractalID != fractalID {
		h.respondError(w, http.StatusNotFound, "Model not found")
		return nil
	}
	return model
}

// errPrismScope reports a prism scope on a fractal-only endpoint.
var errPrismScope = errors.New("analytics models are scoped to a fractal, not a prism")

// respondScopeError turns a getFractalID failure into a response, naming the
// prism case so an API caller knows to select a fractal instead of guessing.
func (h *Handler) respondScopeError(w http.ResponseWriter, err error) {
	if errors.Is(err, errPrismScope) {
		h.respondError(w, http.StatusBadRequest, "Analytics models are scoped to a fractal. Select a fractal rather than a prism.")
		return
	}
	h.respondError(w, http.StatusBadRequest, "Failed to determine fractal context")
}

// getFractalID resolves the fractal an analytics model belongs to. Models are
// fractal-scoped (there is no prism_id on them), so a prism scope is rejected
// rather than resolved: falling through to the default fractal let a caller with
// a role on the prism alone read and write models in a fractal they may have no
// access to at all.
func (h *Handler) getFractalID(r *http.Request) (string, error) {
	if pid, ok := r.Context().Value("selected_prism").(string); ok && pid != "" {
		return "", errPrismScope
	}
	if fid, ok := r.Context().Value("selected_fractal").(string); ok && fid != "" {
		return fid, nil
	}
	if h.fractalManager == nil {
		return "", fmt.Errorf("no fractal context available")
	}
	defaultFractal, err := h.fractalManager.GetDefaultFractal(r.Context())
	if err != nil {
		return "", fmt.Errorf("failed to get default fractal: %w", err)
	}
	return defaultFractal.ID, nil
}

func (h *Handler) requireAnalyst(w http.ResponseWriter, r *http.Request) bool {
	user, _ := r.Context().Value("user").(*storage.User)
	fractalRole := rbac.RoleFromContext(r.Context())
	prismRole := rbac.PrismRoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) && !rbac.HasAccess(user, prismRole, rbac.RoleAnalyst) {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return false
	}
	return true
}

func (h *Handler) respondSuccess(w http.ResponseWriter, data interface{}) {
	api.WriteSuccess(w, data)
}

func (h *Handler) respondError(w http.ResponseWriter, status int, msg string) {
	api.WriteError(w, status, msg)
}
