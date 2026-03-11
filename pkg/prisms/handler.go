package prisms

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"bifract/pkg/storage"
)

// SessionManager allows the prism handler to update session selection.
type SessionManager interface {
	SetSelectedPrismInSessionFromRequest(r *http.Request, prismID string) error
	SetSelectedFractalInSessionFromRequest(r *http.Request, fractalID string) error
}

// Handler provides HTTP endpoints for prism management.
type Handler struct {
	manager        *Manager
	sessionManager SessionManager
}

// NewHandler creates a new prism handler.
func NewHandler(manager *Manager, sessionManager SessionManager) *Handler {
	return &Handler{manager: manager, sessionManager: sessionManager}
}

func (h *Handler) HandleListPrisms(w http.ResponseWriter, r *http.Request) {
	prisms, err := h.manager.ListPrisms(r.Context())
	if err != nil {
		log.Printf("[Prisms] Failed to list prisms: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to load prisms")
		return
	}
	if prisms == nil {
		prisms = []*Prism{}
	}
	respondSuccess(w, map[string]interface{}{"prisms": prisms, "count": len(prisms)})
}

func (h *Handler) HandleCreatePrism(w http.ResponseWriter, r *http.Request) {
	user := getCurrentUser(r)
	if user == nil || !user.IsAdmin {
		respondError(w, http.StatusForbidden, "only administrators can create prisms")
		return
	}

	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Name == "" {
		respondError(w, http.StatusBadRequest, "name is required")
		return
	}

	prism, err := h.manager.CreatePrism(r.Context(), req.Name, req.Description, user.Username)
	if err != nil {
		log.Printf("[Prisms] Failed to create prism: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to create prism")
		return
	}
	respondSuccess(w, prism)
}

func (h *Handler) HandleGetPrism(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	prism, err := h.manager.GetPrism(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusNotFound, "Prism not found")
		return
	}
	respondSuccess(w, prism)
}

func (h *Handler) HandleUpdatePrism(w http.ResponseWriter, r *http.Request) {
	user := getCurrentUser(r)
	if user == nil || !user.IsAdmin {
		respondError(w, http.StatusForbidden, "only administrators can update prisms")
		return
	}

	id := chi.URLParam(r, "id")
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	prism, err := h.manager.UpdatePrism(r.Context(), id, req.Name, req.Description)
	if err != nil {
		log.Printf("[Prisms] Failed to update prism %s: %v", id, err)
		respondError(w, http.StatusInternalServerError, "Failed to update prism")
		return
	}
	respondSuccess(w, prism)
}

func (h *Handler) HandleDeletePrism(w http.ResponseWriter, r *http.Request) {
	user := getCurrentUser(r)
	if user == nil || !user.IsAdmin {
		respondError(w, http.StatusForbidden, "only administrators can delete prisms")
		return
	}

	id := chi.URLParam(r, "id")
	if err := h.manager.DeletePrism(r.Context(), id); err != nil {
		log.Printf("[Prisms] Failed to delete prism %s: %v", id, err)
		respondError(w, http.StatusInternalServerError, "Failed to delete prism")
		return
	}
	respondSuccess(w, map[string]bool{"deleted": true})
}

func (h *Handler) HandleAddMember(w http.ResponseWriter, r *http.Request) {
	user := getCurrentUser(r)
	if user == nil || !user.IsAdmin {
		respondError(w, http.StatusForbidden, "only administrators can manage prism members")
		return
	}

	id := chi.URLParam(r, "id")
	var req struct {
		FractalID string `json:"fractal_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.FractalID == "" {
		respondError(w, http.StatusBadRequest, "fractal_id is required")
		return
	}

	if err := h.manager.AddMember(r.Context(), id, req.FractalID); err != nil {
		log.Printf("[Prisms] Failed to add member to prism %s: %v", id, err)
		respondError(w, http.StatusInternalServerError, "Failed to add member")
		return
	}

	prism, err := h.manager.GetPrism(r.Context(), id)
	if err != nil {
		log.Printf("[Prisms] Failed to get prism %s after adding member: %v", id, err)
		respondError(w, http.StatusInternalServerError, "Failed to load prism")
		return
	}
	respondSuccess(w, prism)
}

func (h *Handler) HandleRemoveMember(w http.ResponseWriter, r *http.Request) {
	user := getCurrentUser(r)
	if user == nil || !user.IsAdmin {
		respondError(w, http.StatusForbidden, "only administrators can manage prism members")
		return
	}

	id := chi.URLParam(r, "id")
	fractalID := chi.URLParam(r, "fractalID")

	if err := h.manager.RemoveMember(r.Context(), id, fractalID); err != nil {
		log.Printf("[Prisms] Failed to remove member from prism %s: %v", id, err)
		respondError(w, http.StatusInternalServerError, "Failed to remove member")
		return
	}

	prism, err := h.manager.GetPrism(r.Context(), id)
	if err != nil {
		log.Printf("[Prisms] Failed to get prism %s after removing member: %v", id, err)
		respondError(w, http.StatusInternalServerError, "Failed to load prism")
		return
	}
	respondSuccess(w, prism)
}

func (h *Handler) HandleSelectPrism(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	// Validate prism exists
	prism, err := h.manager.GetPrism(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusNotFound, "Prism not found")
		return
	}

	if err := h.sessionManager.SetSelectedPrismInSessionFromRequest(r, id); err != nil {
		log.Printf("[Prisms] Failed to select prism %s: %v", id, err)
		respondError(w, http.StatusInternalServerError, "Failed to select prism")
		return
	}
	respondSuccess(w, map[string]interface{}{"selected": true, "prism": prism})
}

// ---- helpers ----

func getCurrentUser(r *http.Request) *storage.User {
	if user := r.Context().Value("user"); user != nil {
		if u, ok := user.(*storage.User); ok {
			return u
		}
	}
	return nil
}

func respondSuccess(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	type resp struct {
		Success bool        `json:"success"`
		Data    interface{} `json:"data,omitempty"`
	}
	json.NewEncoder(w).Encode(resp{Success: true, Data: data})
}

func respondError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	type resp struct {
		Success bool   `json:"success"`
		Error   string `json:"error,omitempty"`
	}
	json.NewEncoder(w).Encode(resp{Success: false, Error: msg})
}
