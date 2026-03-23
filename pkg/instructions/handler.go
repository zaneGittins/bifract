package instructions

import (
	"encoding/json"
	"log"
	"net/http"

	"bifract/pkg/fractals"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
)

// Handler provides HTTP endpoints for instruction library management.
type Handler struct {
	manager        *Manager
	fractalManager *fractals.Manager
	rbacResolver   *rbac.Resolver
	syncer         *Syncer
}

// NewHandler creates a new instruction library handler.
func NewHandler(manager *Manager, fractalManager *fractals.Manager, syncer *Syncer) *Handler {
	return &Handler{
		manager:        manager,
		fractalManager: fractalManager,
		syncer:         syncer,
	}
}

// SetRBACResolver sets the RBAC resolver for permission checks.
func (h *Handler) SetRBACResolver(resolver *rbac.Resolver) {
	h.rbacResolver = resolver
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

func (h *Handler) requireRole(w http.ResponseWriter, r *http.Request, required rbac.Role) bool {
	user := h.getCurrentUser(r)
	if user == nil {
		h.respond(w, http.StatusUnauthorized, nil, "authentication required")
		return false
	}
	if user.IsAdmin {
		return true
	}
	fractalRole := rbac.RoleFromContext(r.Context())
	prismRole := rbac.PrismRoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, required) && !rbac.HasAccess(user, prismRole, required) {
		h.respond(w, http.StatusForbidden, nil, "insufficient permissions")
		return false
	}
	return true
}

// --- Library endpoints ---

// HandleListLibraries returns all libraries for the current scope.
func (h *Handler) HandleListLibraries(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleViewer) {
		return
	}
	fractalID, prismID := h.getScope(r)
	if fractalID == "" && prismID == "" {
		h.respond(w, http.StatusBadRequest, nil, "no fractal or prism selected")
		return
	}

	libs, err := h.manager.ListLibraries(r.Context(), fractalID, prismID)
	if err != nil {
		log.Printf("[Instructions] Failed to list libraries: %v", err)
		h.respond(w, http.StatusInternalServerError, nil, "failed to load libraries")
		return
	}
	h.respond(w, http.StatusOK, libs, "")
}

// HandleGetLibrary returns a single library with its pages.
func (h *Handler) HandleGetLibrary(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleViewer) {
		return
	}
	id := chi.URLParam(r, "id")
	lib, err := h.manager.GetLibrary(r.Context(), id)
	if err != nil {
		h.respond(w, http.StatusNotFound, nil, "library not found")
		return
	}

	pages, err := h.manager.ListPages(r.Context(), id)
	if err != nil {
		log.Printf("[Instructions] Failed to list pages for library %s: %v", id, err)
		h.respond(w, http.StatusInternalServerError, nil, "failed to load pages")
		return
	}

	h.respond(w, http.StatusOK, map[string]interface{}{
		"library": lib,
		"pages":   pages,
	}, "")
}

// HandleCreateLibrary creates a new library.
func (h *Handler) HandleCreateLibrary(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleAnalyst) {
		return
	}
	user := h.getCurrentUser(r)
	fractalID, prismID := h.getScope(r)
	if fractalID == "" && prismID == "" {
		h.respond(w, http.StatusBadRequest, nil, "no fractal or prism selected")
		return
	}

	// Repo-source libraries require admin
	var req CreateLibraryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respond(w, http.StatusBadRequest, nil, "invalid request body")
		return
	}
	if req.Source == SourceRepo {
		if !h.requireRole(w, r, rbac.RoleAdmin) {
			return
		}
	}

	lib, err := h.manager.CreateLibrary(r.Context(), req, fractalID, prismID, user.Username)
	if err != nil {
		log.Printf("[Instructions] Failed to create library: %v", err)
		h.respond(w, http.StatusBadRequest, nil, err.Error())
		return
	}
	h.respond(w, http.StatusCreated, lib, "")
}

// HandleUpdateLibrary updates a library.
func (h *Handler) HandleUpdateLibrary(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleAnalyst) {
		return
	}
	id := chi.URLParam(r, "id")

	var req UpdateLibraryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respond(w, http.StatusBadRequest, nil, "invalid request body")
		return
	}

	// Repo sync fields require admin
	if req.Source == SourceRepo || req.RepoURL != "" || req.AuthToken != "" || req.ClearToken {
		if !h.requireRole(w, r, rbac.RoleAdmin) {
			return
		}
	}

	lib, err := h.manager.UpdateLibrary(r.Context(), id, req)
	if err != nil {
		log.Printf("[Instructions] Failed to update library %s: %v", id, err)
		h.respond(w, http.StatusBadRequest, nil, err.Error())
		return
	}
	h.respond(w, http.StatusOK, lib, "")
}

// HandleDeleteLibrary deletes a library.
func (h *Handler) HandleDeleteLibrary(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleAdmin) {
		return
	}
	id := chi.URLParam(r, "id")
	if err := h.manager.DeleteLibrary(r.Context(), id); err != nil {
		log.Printf("[Instructions] Failed to delete library %s: %v", id, err)
		h.respond(w, http.StatusInternalServerError, nil, "failed to delete library")
		return
	}
	h.respond(w, http.StatusOK, nil, "")
}

// --- Page endpoints ---

// HandleListPages returns all pages for a library.
func (h *Handler) HandleListPages(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleViewer) {
		return
	}
	libraryID := chi.URLParam(r, "id")
	pages, err := h.manager.ListPages(r.Context(), libraryID)
	if err != nil {
		log.Printf("[Instructions] Failed to list pages for library %s: %v", libraryID, err)
		h.respond(w, http.StatusInternalServerError, nil, "failed to load pages")
		return
	}
	h.respond(w, http.StatusOK, pages, "")
}

// HandleGetPage returns a single page.
func (h *Handler) HandleGetPage(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleViewer) {
		return
	}
	pageID := chi.URLParam(r, "pageId")
	page, err := h.manager.GetPage(r.Context(), pageID)
	if err != nil {
		h.respond(w, http.StatusNotFound, nil, "page not found")
		return
	}
	h.respond(w, http.StatusOK, page, "")
}

// HandleCreatePage creates a new page in a library.
func (h *Handler) HandleCreatePage(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleAnalyst) {
		return
	}
	user := h.getCurrentUser(r)
	libraryID := chi.URLParam(r, "id")

	var req CreatePageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respond(w, http.StatusBadRequest, nil, "invalid request body")
		return
	}

	page, err := h.manager.CreatePage(r.Context(), libraryID, req, user.Username)
	if err != nil {
		log.Printf("[Instructions] Failed to create page in library %s: %v", libraryID, err)
		h.respond(w, http.StatusBadRequest, nil, err.Error())
		return
	}
	h.respond(w, http.StatusCreated, page, "")
}

// HandleUpdatePage updates a page.
func (h *Handler) HandleUpdatePage(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleAnalyst) {
		return
	}
	pageID := chi.URLParam(r, "pageId")

	var req UpdatePageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respond(w, http.StatusBadRequest, nil, "invalid request body")
		return
	}

	page, err := h.manager.UpdatePage(r.Context(), pageID, req)
	if err != nil {
		log.Printf("[Instructions] Failed to update page %s: %v", pageID, err)
		h.respond(w, http.StatusBadRequest, nil, err.Error())
		return
	}
	h.respond(w, http.StatusOK, page, "")
}

// HandleDeletePage deletes a page.
func (h *Handler) HandleDeletePage(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleAdmin) {
		return
	}
	pageID := chi.URLParam(r, "pageId")
	if err := h.manager.DeletePage(r.Context(), pageID); err != nil {
		log.Printf("[Instructions] Failed to delete page %s: %v", pageID, err)
		h.respond(w, http.StatusInternalServerError, nil, "failed to delete page")
		return
	}
	h.respond(w, http.StatusOK, nil, "")
}

// --- Sync endpoint ---

// HandleSyncLibrary triggers an immediate sync for a repo-source library.
func (h *Handler) HandleSyncLibrary(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleAdmin) {
		return
	}
	id := chi.URLParam(r, "id")

	lib, err := h.manager.GetLibrary(r.Context(), id)
	if err != nil {
		h.respond(w, http.StatusNotFound, nil, "library not found")
		return
	}
	if lib.Source != SourceRepo {
		h.respond(w, http.StatusBadRequest, nil, "library is not a repo source")
		return
	}

	if h.syncer == nil {
		h.respond(w, http.StatusServiceUnavailable, nil, "sync not available")
		return
	}

	result, err := h.syncer.SyncLibrary(r.Context(), lib)
	if err != nil {
		log.Printf("[Instructions] Failed to sync library %s: %v", id, err)
		h.respond(w, http.StatusInternalServerError, nil, "sync failed: "+err.Error())
		return
	}
	h.respond(w, http.StatusOK, result, "")
}
