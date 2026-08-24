package normalizers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"gopkg.in/yaml.v3"

	"bifract/pkg/storage"
)

type Handler struct {
	manager *Manager
	ch      *storage.ClickHouseClient // sample capture only; nil disables it
}

type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// NewHandler builds the normalizer API handler. ch may be nil, which disables
// sample capture but leaves every other endpoint working.
func NewHandler(manager *Manager, ch *storage.ClickHouseClient) *Handler {
	return &Handler{manager: manager, ch: ch}
}

func (h *Handler) HandleList(w http.ResponseWriter, r *http.Request) {
	normalizers, err := h.manager.List(r.Context())
	if err != nil {
		log.Printf("[Normalizers] Failed to list normalizers: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load normalizers")
		return
	}
	if normalizers == nil {
		normalizers = []Normalizer{}
	}
	h.respondSuccess(w, map[string]interface{}{"normalizers": normalizers, "count": len(normalizers)})
}

func (h *Handler) HandleGet(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	id := chi.URLParam(r, "id")
	n, err := h.manager.Get(r.Context(), id)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Normalizer not found")
		return
	}
	h.respondSuccess(w, n)
}

func (h *Handler) HandleCreate(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	username := h.getCurrentUser(r)

	var req CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}

	n, err := h.manager.Create(r.Context(), req, username)
	if err != nil {
		log.Printf("[Normalizers] Failed to create normalizer: %v", err)
		h.respondError(w, http.StatusBadRequest, "Failed to create normalizer")
		return
	}
	h.respondSuccess(w, n)
}

func (h *Handler) HandleUpdate(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	id := chi.URLParam(r, "id")

	var req UpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}

	n, err := h.manager.Update(r.Context(), id, req)
	if err != nil {
		log.Printf("[Normalizers] Failed to update normalizer %s: %v", id, err)
		h.respondError(w, http.StatusBadRequest, "Failed to update normalizer")
		return
	}
	h.respondSuccess(w, n)
}

func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	id := chi.URLParam(r, "id")
	if err := h.manager.Delete(r.Context(), id); err != nil {
		log.Printf("[Normalizers] Failed to delete normalizer %s: %v", id, err)
		h.respondError(w, http.StatusBadRequest, "Failed to delete normalizer")
		return
	}
	h.respondSuccess(w, map[string]string{"message": "Normalizer deleted"})
}

func (h *Handler) HandleSetDefault(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	id := chi.URLParam(r, "id")
	if err := h.manager.SetDefault(r.Context(), id); err != nil {
		log.Printf("[Normalizers] Failed to set default normalizer %s: %v", id, err)
		h.respondError(w, http.StatusBadRequest, "Failed to update default normalizer")
		return
	}
	h.respondSuccess(w, map[string]string{"message": "Default normalizer updated"})
}

// maxPreviewValueLen caps preview values. Logs legitimately carry multi-KB
// fields; the editor only needs enough to recognise the value.
const maxPreviewValueLen = 512

// HandlePreview applies the normalizer config from the request body to sample
// JSON and returns the resulting fields with provenance. Runs the same pipeline
// as ingestion (see Normalizer.Trace), so what the editor shows is what the
// ingest path will do. No DB interaction needed.
func (h *Handler) HandlePreview(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	var req struct {
		Transforms    []Transform    `json:"transforms"`
		FieldMappings []FieldMapping `json:"field_mappings"`
		ValueMappings []ValueMapping `json:"value_mappings"`
		SampleJSON    string         `json:"sample_json"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	if strings.TrimSpace(req.SampleJSON) == "" {
		h.respondError(w, http.StatusBadRequest, "sample_json is required")
		return
	}

	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(req.SampleJSON), &obj); err != nil {
		h.respondError(w, http.StatusBadRequest, fmt.Sprintf("Invalid sample JSON: %v", err))
		return
	}

	n := &Normalizer{
		Transforms:    req.Transforms,
		FieldMappings: req.FieldMappings,
		ValueMappings: req.ValueMappings,
	}
	trace := n.Trace(obj)

	for i := range trace.Fields {
		if len(trace.Fields[i].Value) > maxPreviewValueLen {
			trace.Fields[i].Value = trace.Fields[i].Value[:maxPreviewValueLen] + "..."
		}
	}
	if trace.Fields == nil {
		trace.Fields = []TracedField{}
	}

	h.respondSuccess(w, map[string]interface{}{
		"fields":     trace.Fields,
		"collisions": trace.Collisions,
	})
}

// HandleSamples returns recent raw logs from a fractal so the editor can preview
// against real data instead of hand-pasted JSON.
//
// Reads logs_raw, whose 7-day TTL already bounds the scan to at most a week of
// ingest partitions for the fractal, ordered by its own (timestamp, log_id) sort
// key so the read is in-order rather than a sort.
func (h *Handler) HandleSamples(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	if h.ch == nil {
		h.respondError(w, http.StatusServiceUnavailable, "Log storage unavailable")
		return
	}

	fractalID := strings.TrimSpace(r.URL.Query().Get("fractal_id"))
	if fractalID == "" {
		h.respondError(w, http.StatusBadRequest, "fractal_id is required")
		return
	}

	limit := 5
	if v := r.URL.Query().Get("limit"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 && parsed <= 20 {
			limit = parsed
		}
	}

	// Scan a bounded number of recent rows and keep one per distinct field shape,
	// so the tabs show genuinely different log formats rather than 5 near-copies.
	const scanRows = 300

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	// raw_log lives in logs_raw, which already holds only the last 7 days; order by
	// timestamp to surface the most recent samples.
	samples, err := h.collectSamples(ctx, h.ch.RawReadTable(), "timestamp", "", fractalID, scanRows, limit)
	if err != nil {
		log.Printf("[Normalizers] Sample capture failed: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to capture samples")
		return
	}

	if samples == nil {
		samples = []LogSample{}
	}
	h.respondSuccess(w, map[string]interface{}{"samples": samples, "count": len(samples)})
}

// LogSample is one captured raw log offered as preview input.
type LogSample struct {
	RawLog    string    `json:"raw_log"`
	Timestamp time.Time `json:"timestamp"`
	Shape     string    `json:"shape"`       // sorted top-level key signature
	FieldsNum int       `json:"fields_num"`  // top-level key count
}

// collectSamples reads recent raw logs from table and returns up to limit of
// them, deduplicated by top-level key shape.
func (h *Handler) collectSamples(ctx context.Context, table, tsColumn, extraWhere, fractalID string, scanRows, limit int) ([]LogSample, error) {
	query := fmt.Sprintf(`
		SELECT raw_log, %s AS ts
		FROM %s
		WHERE fractal_id = ? AND raw_log != '' %s
		ORDER BY %s DESC
		LIMIT %d`, tsColumn, table, extraWhere, tsColumn, scanRows)

	rows, err := h.ch.QueryRows(ctx, query, fractalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []LogSample
	seen := make(map[string]bool, limit)

	for rows.Next() {
		if len(out) >= limit {
			break
		}
		var raw string
		var ts time.Time
		if err := rows.Scan(&raw, &ts); err != nil {
			return nil, err
		}

		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(raw), &obj); err != nil {
			continue // non-JSON lines cannot drive a field-mapping preview
		}

		keys := make([]string, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		shape := strings.Join(keys, ",")
		if seen[shape] {
			continue
		}
		seen[shape] = true

		out = append(out, LogSample{
			RawLog:    raw,
			Timestamp: ts,
			Shape:     shape,
			FieldsNum: len(keys),
		})
	}
	return out, rows.Err()
}

// HandleTokenUsage returns tokens using a given normalizer, with fractal names.
func (h *Handler) HandleTokenUsage(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	id := chi.URLParam(r, "id")
	tokens, err := h.manager.GetTokenUsage(r.Context(), id)
	if err != nil {
		log.Printf("[Normalizers] Failed to get token usage for %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load token usage")
		return
	}
	h.respondSuccess(w, map[string]interface{}{"tokens": tokens})
}

// HandleDuplicate creates a copy of an existing normalizer with a new name.
func (h *Handler) HandleDuplicate(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	id := chi.URLParam(r, "id")
	username := h.getCurrentUser(r)

	n, err := h.manager.Duplicate(r.Context(), id, username)
	if err != nil {
		log.Printf("[Normalizers] Failed to duplicate normalizer %s: %v", id, err)
		h.respondError(w, http.StatusBadRequest, fmt.Sprintf("Failed to duplicate: %v", err))
		return
	}
	h.respondSuccess(w, n)
}

// HandleExportYAML exports a normalizer as YAML.
func (h *Handler) HandleExportYAML(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	id := chi.URLParam(r, "id")
	n, err := h.manager.Get(r.Context(), id)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Normalizer not found")
		return
	}

	export := NormalizerExport{
		Name:            n.Name,
		Description:     n.Description,
		Transforms:      n.Transforms,
		FieldMappings:   n.FieldMappings,
		ValueMappings:   n.ValueMappings,
		TimestampFields: n.TimestampFields,
	}

	out, err := yaml.Marshal(export)
	if err != nil {
		h.respondError(w, http.StatusInternalServerError, "Failed to marshal YAML")
		return
	}

	w.Header().Set("Content-Type", "text/yaml")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.yaml"`, sanitizeFilename(n.Name)))
	w.Write(out)
}

// HandleImportYAML imports a normalizer from YAML.
func (h *Handler) HandleImportYAML(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	username := h.getCurrentUser(r)

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1MB limit
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "Failed to read body")
		return
	}

	var export NormalizerExport
	if err := yaml.Unmarshal(body, &export); err != nil {
		h.respondError(w, http.StatusBadRequest, fmt.Sprintf("Invalid YAML: %v", err))
		return
	}

	if strings.TrimSpace(export.Name) == "" {
		h.respondError(w, http.StatusBadRequest, "Normalizer name is required in YAML")
		return
	}

	req := CreateRequest{
		Name:            export.Name,
		Description:     export.Description,
		Transforms:      export.Transforms,
		FieldMappings:   export.FieldMappings,
		ValueMappings:   export.ValueMappings,
		TimestampFields: export.TimestampFields,
	}

	n, err := h.manager.Create(r.Context(), req, username)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, fmt.Sprintf("Failed to import: %v", err))
		return
	}
	h.respondSuccess(w, n)
}

// sanitizeFilename makes a string safe for use in Content-Disposition.
func sanitizeFilename(s string) string {
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			return r
		}
		return '_'
	}, s)
	return s
}

func (h *Handler) requireAdmin(w http.ResponseWriter, r *http.Request) bool {
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil || !user.IsAdmin {
		h.respondError(w, http.StatusForbidden, "Admin access required")
		return false
	}
	return true
}

func (h *Handler) getCurrentUser(r *http.Request) string {
	if user, ok := r.Context().Value("user").(*storage.User); ok {
		return user.Username
	}
	return ""
}


func (h *Handler) respondSuccess(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(APIResponse{Success: true, Data: data})
}

func (h *Handler) respondError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(APIResponse{Success: false, Error: msg})
}
