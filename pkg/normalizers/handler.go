package normalizers

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"gopkg.in/yaml.v3"

	"bifract/pkg/storage"
)

type Handler struct {
	manager *Manager
}

type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

func NewHandler(manager *Manager) *Handler {
	return &Handler{manager: manager}
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

// HandlePreview applies the normalizer config from the request body to sample JSON
// and returns the normalized field names. No DB interaction needed.
func (h *Handler) HandlePreview(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Transforms      []Transform      `json:"transforms"`
		FieldMappings   []FieldMapping   `json:"field_mappings"`
		SampleJSON      string           `json:"sample_json"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	if strings.TrimSpace(req.SampleJSON) == "" {
		h.respondError(w, http.StatusBadRequest, "sample_json is required")
		return
	}

	// Build a temporary normalizer and compile it
	n := &Normalizer{
		Transforms:    req.Transforms,
		FieldMappings: req.FieldMappings,
	}
	compiled := n.Compile()

	// Parse the sample JSON
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(req.SampleJSON), &obj); err != nil {
		h.respondError(w, http.StatusBadRequest, fmt.Sprintf("Invalid sample JSON: %v", err))
		return
	}

	// Flatten and normalize, tracking collisions
	fields := make(map[string]string)
	collisions := make(map[string][]string) // normalized key -> list of original paths
	originalPaths := make(map[string]string) // normalized key -> first original path
	flattenPreview(obj, "", fields, compiled, collisions, originalPaths)

	// Build result entries
	var results []previewFieldResult
	seen := make(map[string]bool)
	buildPreviewResults(obj, "", compiled, &results, seen, collisions)

	h.respondSuccess(w, map[string]interface{}{
		"fields":     results,
		"collisions": collisions,
	})
}

// flattenPreview flattens JSON and tracks field name collisions.
func flattenPreview(obj map[string]interface{}, prefix string, fields map[string]string, norm *CompiledNormalizer, collisions map[string][]string, originalPaths map[string]string) {
	hasFlatten := norm != nil && norm.HasFlatten
	for key, value := range obj {
		fullPath := key
		if prefix != "" {
			fullPath = prefix + "_" + key
		}
		switch v := value.(type) {
		case map[string]interface{}:
			flattenPreview(v, fullPath, fields, norm, collisions, originalPaths)
		default:
			outKey := fullPath
			if hasFlatten {
				outKey = key
			}
			normalized := outKey
			if norm != nil {
				normalized = norm.ApplyFieldName(outKey)
			}
			valStr := fmt.Sprintf("%v", v)
			if prev, exists := originalPaths[normalized]; exists {
				if _, tracked := collisions[normalized]; !tracked {
					collisions[normalized] = []string{prev}
				}
				collisions[normalized] = append(collisions[normalized], fullPath)
			} else {
				originalPaths[normalized] = fullPath
			}
			fields[normalized] = valStr
		}
	}
}

// previewFieldResult is a single field in the preview output.
type previewFieldResult struct {
	Original   string `json:"original"`
	Normalized string `json:"normalized"`
	Value      string `json:"value"`
	Collision  bool   `json:"collision,omitempty"`
}

// buildPreviewResults walks the object for preview output.
func buildPreviewResults(obj map[string]interface{}, prefix string, norm *CompiledNormalizer, results *[]previewFieldResult, seen map[string]bool, collisions map[string][]string) {
	hasFlatten := norm != nil && norm.HasFlatten
	for key, value := range obj {
		fullPath := key
		if prefix != "" {
			fullPath = prefix + "_" + key
		}
		switch v := value.(type) {
		case map[string]interface{}:
			buildPreviewResults(v, fullPath, norm, results, seen, collisions)
		default:
			outKey := fullPath
			if hasFlatten {
				outKey = key
			}
			normalized := outKey
			if norm != nil {
				normalized = norm.ApplyFieldName(outKey)
			}
			_, hasCollision := collisions[normalized]
			if !seen[fullPath] {
				seen[fullPath] = true
				valStr := fmt.Sprintf("%v", v)
				*results = append(*results, previewFieldResult{
					Original:   fullPath,
					Normalized: normalized,
					Value:      valStr,
					Collision:  hasCollision,
				})
			}
		}
	}
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
