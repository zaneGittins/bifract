package schemafields

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"

	"bifract/pkg/storage"
)

type Handler struct {
	manager       *Manager
	ch            *storage.ClickHouseClient
	onFieldChange func(map[string]bool) // called after create/reset so the parser can reload
}

type apiResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// NewHandler creates a handler for the schema fields admin API.
// onFieldChange is called with the new complete type-hinted field map after
// any write operation so the caller (main) can update parser.SetCustomTypeHintedFields.
func NewHandler(manager *Manager, ch *storage.ClickHouseClient, onFieldChange func(map[string]bool)) *Handler {
	return &Handler{manager: manager, ch: ch, onFieldChange: onFieldChange}
}

// HandleList returns project defaults and user-defined custom fields.
func (h *Handler) HandleList(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	custom, err := h.manager.List(r.Context())
	if err != nil {
		log.Printf("[SchemaFields] list: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load schema fields")
		return
	}
	if custom == nil {
		custom = []SchemaField{}
	}
	h.respondSuccess(w, map[string]interface{}{
		"defaults": ProjectDefaultFields,
		"custom":   custom,
	})
}

// HandleCreate adds a custom field, syncs ClickHouse schema, and notifies the parser.
func (h *Handler) HandleCreate(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	var req CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}

	f, err := h.manager.Create(r.Context(), req, h.getCurrentUser(r))
	if err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Sync ClickHouse schema with the new full field set.
	custom, _ := h.manager.List(r.Context())
	all := append(ProjectDefaultFields, custom...)
	if err := h.ch.ReconcileSchemaFields(r.Context(), ToSpecs(all)); err != nil {
		log.Printf("[SchemaFields] reconcile after create %q: %v", f.FieldName, err)
	}

	h.notifyFieldChange(custom)
	h.respondSuccess(w, f)
}

// HandleDelete removes a custom field from Postgres (soft: ClickHouse schema unchanged until reset).
func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	name := chi.URLParam(r, "name")
	if err := h.manager.Delete(r.Context(), name); err != nil {
		h.respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	custom, _ := h.manager.List(r.Context())
	h.notifyFieldChange(custom)
	h.respondSuccess(w, map[string]string{"message": fmt.Sprintf("Field %q removed", name)})
}

// HandleReset truncates all log data, rebuilds ClickHouse schema from current config,
// and reloads the parser type-hint map.
func (h *Handler) HandleReset(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	var body struct {
		Confirm string `json:"confirm"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Confirm != "DELETE ALL LOG DATA" {
		h.respondError(w, http.StatusBadRequest, `confirmation required: {"confirm": "DELETE ALL LOG DATA"}`)
		return
	}

	custom, err := h.manager.List(r.Context())
	if err != nil {
		log.Printf("[SchemaFields] list before reset: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load schema fields")
		return
	}
	all := append(ProjectDefaultFields, custom...)

	if err := h.ch.TruncateAndReschema(r.Context(), ToSpecs(all)); err != nil {
		log.Printf("[SchemaFields] reset: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Schema reset failed: "+err.Error())
		return
	}

	h.notifyFieldChange(custom)
	h.respondSuccess(w, map[string]string{"message": "Schema reset complete. All log data has been deleted."})
}


func (h *Handler) notifyFieldChange(custom []SchemaField) {
	if h.onFieldChange == nil {
		return
	}
	m := make(map[string]bool, len(custom))
	for _, f := range custom {
		m[f.FieldName] = true
	}
	h.onFieldChange(m)
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
	json.NewEncoder(w).Encode(apiResponse{Success: true, Data: data})
}

func (h *Handler) respondError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(apiResponse{Success: false, Error: msg})
}
