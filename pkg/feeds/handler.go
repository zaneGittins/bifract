package feeds

import (
	"encoding/json"
	"log"
	"net/http"

	"bifract/pkg/alerts"
	"bifract/pkg/fractals"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
)

// Handler provides HTTP endpoints for alert feed management.
type Handler struct {
	manager        *Manager
	alertManager   *alerts.Manager
	fractalManager *fractals.Manager
	syncer         *Syncer
}

// NewHandler creates a new feed handler.
func NewHandler(manager *Manager, alertManager *alerts.Manager, fractalManager *fractals.Manager, syncer *Syncer) *Handler {
	return &Handler{
		manager:        manager,
		alertManager:   alertManager,
		fractalManager: fractalManager,
		syncer:         syncer,
	}
}

type apiResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

func (h *Handler) respond(w http.ResponseWriter, status int, data interface{}, errMsg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	resp := apiResponse{Success: errMsg == "", Data: data, Error: errMsg}
	json.NewEncoder(w).Encode(resp)
}

func (h *Handler) getCurrentUser(r *http.Request) *storage.User {
	if u, ok := r.Context().Value("user").(*storage.User); ok {
		return u
	}
	return nil
}

func (h *Handler) requireAdmin(w http.ResponseWriter, r *http.Request) *storage.User {
	user := h.getCurrentUser(r)
	if user == nil {
		h.respond(w, http.StatusUnauthorized, nil, "authentication required")
		return nil
	}
	if !user.IsAdmin {
		h.respond(w, http.StatusForbidden, nil, "admin access required")
		return nil
	}
	return user
}

func (h *Handler) getScope(r *http.Request) (string, string) {
	if prismID, ok := r.Context().Value("selected_prism").(string); ok && prismID != "" {
		return "", prismID
	}
	if fractalID, ok := r.Context().Value("selected_fractal").(string); ok && fractalID != "" {
		return fractalID, ""
	}
	f, err := h.fractalManager.GetDefaultFractal(r.Context())
	if err != nil {
		return "", ""
	}
	return f.ID, ""
}

// HandleListFeeds returns all feeds for the current fractal or prism.
func (h *Handler) HandleListFeeds(w http.ResponseWriter, r *http.Request) {
	fractalID, prismID := h.getScope(r)
	if fractalID == "" && prismID == "" {
		h.respond(w, http.StatusBadRequest, nil, "no fractal or prism selected")
		return
	}

	feeds, err := h.manager.List(r.Context(), fractalID, prismID)
	if err != nil {
		log.Printf("[Feeds] Failed to list feeds: %v", err)
		h.respond(w, http.StatusInternalServerError, nil, "Failed to load feeds")
		return
	}
	h.respond(w, http.StatusOK, feeds, "")
}

// HandleGetFeed returns a single feed.
func (h *Handler) HandleGetFeed(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	feed, err := h.manager.Get(r.Context(), id)
	if err != nil {
		log.Printf("[Feeds] Failed to get feed %s: %v", id, err)
		h.respond(w, http.StatusNotFound, nil, "Feed not found")
		return
	}
	h.respond(w, http.StatusOK, feed, "")
}

// HandleCreateFeed creates a new feed (admin only).
func (h *Handler) HandleCreateFeed(w http.ResponseWriter, r *http.Request) {
	user := h.requireAdmin(w, r)
	if user == nil {
		return
	}

	fractalID, prismID := h.getScope(r)
	if fractalID == "" && prismID == "" {
		h.respond(w, http.StatusBadRequest, nil, "no fractal or prism selected")
		return
	}

	var req CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respond(w, http.StatusBadRequest, nil, "invalid request body")
		return
	}

	feed, err := h.manager.Create(r.Context(), req, fractalID, prismID, user.Username)
	if err != nil {
		log.Printf("[Feeds] Failed to create feed: %v", err)
		h.respond(w, http.StatusBadRequest, nil, "Failed to create feed")
		return
	}
	h.respond(w, http.StatusCreated, feed, "")
}

// HandleUpdateFeed updates an existing feed (admin only).
func (h *Handler) HandleUpdateFeed(w http.ResponseWriter, r *http.Request) {
	user := h.requireAdmin(w, r)
	if user == nil {
		return
	}

	id := chi.URLParam(r, "id")
	var req UpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respond(w, http.StatusBadRequest, nil, "invalid request body")
		return
	}

	feed, err := h.manager.Update(r.Context(), id, req)
	if err != nil {
		log.Printf("[Feeds] Failed to update feed %s: %v", id, err)
		h.respond(w, http.StatusBadRequest, nil, "Failed to update feed")
		return
	}
	h.respond(w, http.StatusOK, feed, "")
}

// HandleDeleteFeed deletes a feed and all its alerts (admin only).
func (h *Handler) HandleDeleteFeed(w http.ResponseWriter, r *http.Request) {
	user := h.requireAdmin(w, r)
	if user == nil {
		return
	}

	id := chi.URLParam(r, "id")
	if err := h.manager.Delete(r.Context(), id); err != nil {
		log.Printf("[Feeds] Failed to delete feed %s: %v", id, err)
		h.respond(w, http.StatusNotFound, nil, "Failed to delete feed")
		return
	}
	h.respond(w, http.StatusOK, nil, "")
}

// HandleSyncFeed triggers an immediate sync (admin only).
func (h *Handler) HandleSyncFeed(w http.ResponseWriter, r *http.Request) {
	user := h.requireAdmin(w, r)
	if user == nil {
		return
	}

	id := chi.URLParam(r, "id")
	feed, err := h.manager.Get(r.Context(), id)
	if err != nil {
		log.Printf("[Feeds] Failed to get feed %s for sync: %v", id, err)
		h.respond(w, http.StatusNotFound, nil, "Feed not found")
		return
	}

	result, err := h.syncer.SyncFeed(r.Context(), feed)
	if err != nil {
		log.Printf("[Feeds] Failed to sync feed %s: %v", id, err)
		h.manager.UpdateSyncStatus(r.Context(), id, "error: "+err.Error(), 0)
		h.respond(w, http.StatusInternalServerError, nil, "Feed sync failed")
		return
	}

	h.manager.UpdateSyncStatus(r.Context(), id, "success", result.Added+result.Updated+result.Skipped)
	h.respond(w, http.StatusOK, result, "")
}

// HandleGetFeedAlerts returns all alerts for a specific feed (authenticated).
func (h *Handler) HandleGetFeedAlerts(w http.ResponseWriter, r *http.Request) {
	if h.getCurrentUser(r) == nil {
		h.respond(w, http.StatusUnauthorized, nil, "authentication required")
		return
	}

	id := chi.URLParam(r, "id")
	alertsList, err := h.alertManager.ListFeedAlerts(r.Context(), id)
	if err != nil {
		log.Printf("[Feeds] Failed to list alerts for feed %s: %v", id, err)
		h.respond(w, http.StatusInternalServerError, nil, "Failed to load feed alerts")
		return
	}
	h.respond(w, http.StatusOK, alertsList, "")
}

// HandleEnableAllAlerts enables all alerts for a feed (admin only).
func (h *Handler) HandleEnableAllAlerts(w http.ResponseWriter, r *http.Request) {
	user := h.requireAdmin(w, r)
	if user == nil {
		return
	}

	id := chi.URLParam(r, "id")
	if err := h.alertManager.EnableFeedAlerts(r.Context(), id, true, user.Username); err != nil {
		log.Printf("[Feeds] Failed to enable alerts for feed %s: %v", id, err)
		h.respond(w, http.StatusInternalServerError, nil, "Failed to enable feed alerts")
		return
	}
	h.respond(w, http.StatusOK, nil, "")
}

// HandleDisableAllAlerts disables all alerts for a feed (admin only).
func (h *Handler) HandleDisableAllAlerts(w http.ResponseWriter, r *http.Request) {
	user := h.requireAdmin(w, r)
	if user == nil {
		return
	}

	id := chi.URLParam(r, "id")
	if err := h.alertManager.EnableFeedAlerts(r.Context(), id, false, user.Username); err != nil {
		log.Printf("[Feeds] Failed to disable alerts for feed %s: %v", id, err)
		h.respond(w, http.StatusInternalServerError, nil, "Failed to disable feed alerts")
		return
	}
	h.respond(w, http.StatusOK, nil, "")
}

// HandleListAllFeedAlerts returns all feed alerts for the current fractal or prism (authenticated).
func (h *Handler) HandleListAllFeedAlerts(w http.ResponseWriter, r *http.Request) {
	if h.getCurrentUser(r) == nil {
		h.respond(w, http.StatusUnauthorized, nil, "authentication required")
		return
	}

	fractalID, prismID := h.getScope(r)
	if fractalID == "" && prismID == "" {
		h.respond(w, http.StatusBadRequest, nil, "no fractal or prism selected")
		return
	}

	alertsList, err := h.alertManager.ListAllFeedAlerts(r.Context(), fractalID, prismID)
	if err != nil {
		log.Printf("[Feeds] Failed to list all feed alerts: %v", err)
		h.respond(w, http.StatusInternalServerError, nil, "Failed to load feed alerts")
		return
	}
	h.respond(w, http.StatusOK, alertsList, "")
}
