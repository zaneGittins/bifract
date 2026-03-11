package normalizers

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
