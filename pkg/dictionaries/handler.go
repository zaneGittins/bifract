package dictionaries

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"bifract/pkg/fractals"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"
)

// Handler provides HTTP endpoints for dictionary management.
type Handler struct {
	manager        *Manager
	fractalManager *fractals.Manager
}

// APIResponse is a standard API response envelope.
type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// NewHandler creates a new dictionary handler.
func NewHandler(manager *Manager, fractalManager *fractals.Manager) *Handler {
	return &Handler{manager: manager, fractalManager: fractalManager}
}

// ---- Dictionary CRUD ----

func (h *Handler) HandleListDictionaries(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[Dictionaries] Failed to get scope: %v", err)
		h.respondError(w, http.StatusBadRequest, "Failed to determine fractal context")
		return
	}

	dicts, err := h.manager.ListDictionaries(r.Context(), fractalID, prismID)
	if err != nil {
		log.Printf("[Dictionaries] Failed to list dictionaries: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load dictionaries")
		return
	}
	if dicts == nil {
		dicts = []*Dictionary{}
	}
	h.respondSuccess(w, map[string]interface{}{"dictionaries": dicts, "count": len(dicts)})
}

func (h *Handler) HandleCreateDictionary(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[Dictionaries] Failed to get scope: %v", err)
		h.respondError(w, http.StatusBadRequest, "Failed to determine fractal context")
		return
	}

	var req struct {
		Name        string             `json:"name"`
		Description string             `json:"description"`
		KeyColumn   string             `json:"key_column"`
		Columns     []DictionaryColumn `json:"columns"`
		IsGlobal    bool               `json:"is_global"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Name == "" {
		h.respondError(w, http.StatusBadRequest, "name is required")
		return
	}

	username := h.getCurrentUser(r)
	dict, err := h.manager.CreateDictionary(r.Context(), fractalID, prismID, req.Name, req.Description, req.KeyColumn, req.Columns, username, req.IsGlobal)
	if err != nil {
		log.Printf("[Dictionaries] Failed to create dictionary: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to create dictionary")
		return
	}
	h.respondSuccess(w, dict)
}

func (h *Handler) HandleGetDictionary(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	dict, err := h.manager.GetDictionary(r.Context(), id)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Dictionary not found")
		return
	}
	h.respondSuccess(w, dict)
}

func (h *Handler) HandleUpdateDictionary(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	id := chi.URLParam(r, "id")

	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		IsGlobal    bool   `json:"is_global"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	dict, err := h.manager.UpdateDictionary(r.Context(), id, req.Name, req.Description, req.IsGlobal)
	if err != nil {
		log.Printf("[Dictionaries] Failed to update dictionary %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to update dictionary")
		return
	}
	h.respondSuccess(w, dict)
}

func (h *Handler) HandleDeleteDictionary(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	id := chi.URLParam(r, "id")
	if err := h.manager.DeleteDictionary(r.Context(), id); err != nil {
		log.Printf("[Dictionaries] Failed to delete dictionary %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to delete dictionary")
		return
	}
	h.respondSuccess(w, map[string]bool{"deleted": true})
}

// ---- Columns ----

func (h *Handler) HandleAddColumn(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	id := chi.URLParam(r, "id")

	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Name == "" {
		h.respondError(w, http.StatusBadRequest, "name is required")
		return
	}

	dict, err := h.manager.AddColumn(r.Context(), id, req.Name)
	if err != nil {
		log.Printf("[Dictionaries] Failed to add column to dictionary %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to add column")
		return
	}
	h.respondSuccess(w, dict)
}

func (h *Handler) HandleRemoveColumn(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	id := chi.URLParam(r, "id")
	colName := chi.URLParam(r, "name")

	dict, err := h.manager.RemoveColumn(r.Context(), id, colName)
	if err != nil {
		log.Printf("[Dictionaries] Failed to remove column from dictionary %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to remove column")
		return
	}
	h.respondSuccess(w, dict)
}

func (h *Handler) HandleSetColumnKey(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	id := chi.URLParam(r, "id")
	colName := chi.URLParam(r, "name")

	dict, err := h.manager.SetColumnKey(r.Context(), id, colName)
	if err != nil {
		log.Printf("[Dictionaries] Failed to set column key on dictionary %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to set column key")
		return
	}
	h.respondSuccess(w, dict)
}

func (h *Handler) HandleUnsetColumnKey(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	id := chi.URLParam(r, "id")
	colName := chi.URLParam(r, "name")

	dict, err := h.manager.UnsetColumnKey(r.Context(), id, colName)
	if err != nil {
		log.Printf("[Dictionaries] Failed to unset column key on dictionary %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to unset column key")
		return
	}
	h.respondSuccess(w, dict)
}

// ---- Data (rows) ----

func (h *Handler) HandleGetRows(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	search := r.URL.Query().Get("search")

	limit := 100
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if n, err := strconv.Atoi(o); err == nil && n >= 0 {
			offset = n
		}
	}

	rows, total, err := h.manager.GetRows(r.Context(), id, search, limit, offset)
	if err != nil {
		log.Printf("[Dictionaries] Failed to get rows for dictionary %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load rows")
		return
	}
	if rows == nil {
		rows = []DictionaryRow{}
	}
	h.respondSuccess(w, map[string]interface{}{
		"rows":   rows,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

func (h *Handler) HandleUpsertRows(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	id := chi.URLParam(r, "id")

	var req struct {
		Rows []DictionaryRow `json:"rows"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if err := h.manager.UpsertRows(r.Context(), id, req.Rows); err != nil {
		log.Printf("[Dictionaries] Failed to upsert rows in dictionary %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to save rows")
		return
	}
	h.respondSuccess(w, map[string]int{"upserted": len(req.Rows)})
}

func (h *Handler) HandleDeleteRow(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	id := chi.URLParam(r, "id")
	key := chi.URLParam(r, "key")

	if err := h.manager.DeleteRow(r.Context(), id, key); err != nil {
		log.Printf("[Dictionaries] Failed to delete row from dictionary %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to delete row")
		return
	}
	h.respondSuccess(w, map[string]bool{"deleted": true})
}

// HandleImportCSV imports dictionary data from a CSV file.
// Accepts multipart/form-data with a "file" field or raw CSV body.
func (h *Handler) HandleImportCSV(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	id := chi.URLParam(r, "id")

	var csvReader io.Reader

	contentType := r.Header.Get("Content-Type")
	if len(contentType) >= 19 && contentType[:19] == "multipart/form-data" {
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			h.respondError(w, http.StatusBadRequest, "failed to parse multipart form")
			return
		}
		file, _, err := r.FormFile("file")
		if err != nil {
			h.respondError(w, http.StatusBadRequest, "file field required")
			return
		}
		defer file.Close()
		csvReader = file
	} else {
		csvReader = r.Body
	}

	count, err := h.manager.ImportCSV(r.Context(), id, csvReader)
	if err != nil {
		log.Printf("[Dictionaries] CSV import failed for dictionary %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "CSV import failed")
		return
	}
	h.respondSuccess(w, map[string]int{"imported": count})
}

// HandleReloadDictionary forces a ClickHouse dictionary reload.
func (h *Handler) HandleReloadDictionary(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	id := chi.URLParam(r, "id")
	if err := h.manager.ReloadDictionary(r.Context(), id); err != nil {
		log.Printf("[Dictionaries] Failed to reload dictionary %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to reload dictionary")
		return
	}
	h.respondSuccess(w, map[string]bool{"reloaded": true})
}

// HandleListDictionaryNames returns a map of dict name -> ClickHouse lookup name for the
// selected fractal or prism. Used by the query translator to resolve match() calls.
func (h *Handler) HandleListDictionaryNames(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[Dictionaries] Failed to get scope: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to determine fractal context")
		return
	}

	mappings, err := h.manager.ListDictionaryMappings(r.Context(), fractalID, prismID)
	if err != nil {
		log.Printf("[Dictionaries] Failed to list dictionary names: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load dictionary names")
		return
	}
	if mappings == nil {
		mappings = map[string]map[string]string{}
	}
	h.respondSuccess(w, mappings)
}

// ---- Dictionary Actions ----

func (h *Handler) HandleListDictionaryActions(w http.ResponseWriter, r *http.Request) {
	actions, err := h.manager.ListDictionaryActions(r.Context())
	if err != nil {
		log.Printf("[Dictionaries] Failed to list dictionary actions: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load dictionary actions")
		return
	}
	if actions == nil {
		actions = []*DictionaryAction{}
	}
	h.respondSuccess(w, map[string]interface{}{"actions": actions, "count": len(actions)})
}

func (h *Handler) HandleCreateDictionaryAction(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	var req struct {
		Name              string `json:"name"`
		Description       string `json:"description"`
		DictionaryName    string `json:"dictionary_name"`
		MaxLogsPerTrigger int    `json:"max_logs_per_trigger"`
		Enabled           bool   `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Name == "" || req.DictionaryName == "" {
		h.respondError(w, http.StatusBadRequest, "name and dictionary_name are required")
		return
	}

	username := h.getCurrentUser(r)
	action, err := h.manager.CreateDictionaryAction(r.Context(), req.Name, req.Description,
		req.DictionaryName, req.MaxLogsPerTrigger, req.Enabled, username)
	if err != nil {
		log.Printf("[Dictionaries] Failed to create dictionary action: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to create dictionary action")
		return
	}
	h.respondSuccess(w, action)
}

func (h *Handler) HandleGetDictionaryAction(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	action, err := h.manager.GetDictionaryAction(r.Context(), id)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Dictionary action not found")
		return
	}
	h.respondSuccess(w, action)
}

func (h *Handler) HandleUpdateDictionaryAction(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	id := chi.URLParam(r, "id")

	var req struct {
		Name              string `json:"name"`
		Description       string `json:"description"`
		DictionaryName    string `json:"dictionary_name"`
		MaxLogsPerTrigger int    `json:"max_logs_per_trigger"`
		Enabled           bool   `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	action, err := h.manager.UpdateDictionaryAction(r.Context(), id, req.Name, req.Description,
		req.DictionaryName, req.MaxLogsPerTrigger, req.Enabled)
	if err != nil {
		log.Printf("[Dictionaries] Failed to update dictionary action %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to update dictionary action")
		return
	}
	h.respondSuccess(w, action)
}

func (h *Handler) HandleDeleteDictionaryAction(w http.ResponseWriter, r *http.Request) {
	if !h.requireAnalyst(w, r) {
		return
	}
	id := chi.URLParam(r, "id")
	if err := h.manager.DeleteDictionaryAction(r.Context(), id); err != nil {
		log.Printf("[Dictionaries] Failed to delete dictionary action %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to delete dictionary action")
		return
	}
	h.respondSuccess(w, map[string]bool{"deleted": true})
}

// ---- Helpers ----

func (h *Handler) getScope(r *http.Request) (fractalID, prismID string, err error) {
	if pid, ok := r.Context().Value("selected_prism").(string); ok && pid != "" {
		return "", pid, nil
	}
	if fid, ok := r.Context().Value("selected_fractal").(string); ok && fid != "" {
		return fid, "", nil
	}
	if h.fractalManager == nil {
		return "", "", fmt.Errorf("no fractal context available")
	}
	defaultFractal, err := h.fractalManager.GetDefaultFractal(r.Context())
	if err != nil {
		return "", "", fmt.Errorf("failed to get default fractal: %w", err)
	}
	return defaultFractal.ID, "", nil
}

func (h *Handler) getCurrentUser(r *http.Request) string {
	if user := r.Context().Value("user"); user != nil {
		if userObj, ok := user.(*storage.User); ok {
			return userObj.Username
		}
	}
	return ""
}

// requireAnalyst checks that the current user has at least analyst role on the session fractal.
func (h *Handler) requireAnalyst(w http.ResponseWriter, r *http.Request) bool {
	user, _ := r.Context().Value("user").(*storage.User)
	fractalRole := rbac.RoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return false
	}
	return true
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
