package contextlinks

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
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

func (h *Handler) HandleListEnabled(w http.ResponseWriter, r *http.Request) {
	links, err := h.manager.ListEnabled(r.Context())
	if err != nil {
		log.Printf("[ContextLinks] Failed to list enabled context links: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load context links")
		return
	}
	h.respondSuccess(w, map[string]interface{}{"context_links": links, "count": len(links)})
}

func (h *Handler) HandleList(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	links, err := h.manager.List(r.Context())
	if err != nil {
		log.Printf("[ContextLinks] Failed to list context links: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load context links")
		return
	}
	h.respondSuccess(w, map[string]interface{}{"context_links": links, "count": len(links)})
}

func (h *Handler) HandleGet(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	id := chi.URLParam(r, "id")
	cl, err := h.manager.Get(r.Context(), id)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Context link not found")
		return
	}
	h.respondSuccess(w, cl)
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
	if req.ShortName == "" {
		h.respondError(w, http.StatusBadRequest, "short_name is required")
		return
	}
	if req.ContextLink == "" {
		h.respondError(w, http.StatusBadRequest, "context_link URL is required")
		return
	}
	if len(req.MatchFields) == 0 {
		h.respondError(w, http.StatusBadRequest, "At least one match field is required")
		return
	}

	cl, err := h.manager.Create(r.Context(), req, username)
	if err != nil {
		log.Printf("[ContextLinks] Failed to create context link: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to create context link")
		return
	}
	h.respondSuccess(w, cl)
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
	if req.ShortName == "" {
		h.respondError(w, http.StatusBadRequest, "short_name is required")
		return
	}
	if req.ContextLink == "" {
		h.respondError(w, http.StatusBadRequest, "context_link URL is required")
		return
	}

	cl, err := h.manager.Update(r.Context(), id, req)
	if err != nil {
		log.Printf("[ContextLinks] Failed to update context link %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to update context link")
		return
	}
	h.respondSuccess(w, cl)
}

func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdmin(w, r) {
		return
	}
	id := chi.URLParam(r, "id")
	if err := h.manager.Delete(r.Context(), id); err != nil {
		log.Printf("[ContextLinks] Failed to delete context link %s: %v", id, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to delete context link")
		return
	}
	h.respondSuccess(w, map[string]string{"message": "Context link deleted"})
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
