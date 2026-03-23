package archives

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"bifract/pkg/fractals"
	"bifract/pkg/ingesttokens"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"
)

// Handler provides HTTP endpoints for archive operations.
type Handler struct {
	manager        *Manager
	fractalManager *fractals.Manager
	rbacResolver   *rbac.Resolver
	tokenStorage   *ingesttokens.Storage
}

// NewHandler creates a new archive handler.
func NewHandler(manager *Manager, fractalManager *fractals.Manager, rbacResolver *rbac.Resolver, tokenStorage *ingesttokens.Storage) *Handler {
	return &Handler{
		manager:        manager,
		fractalManager: fractalManager,
		rbacResolver:   rbacResolver,
		tokenStorage:   tokenStorage,
	}
}

// HandleCreateArchive starts an archive operation for a fractal.
func (h *Handler) HandleCreateArchive(w http.ResponseWriter, r *http.Request) {
	if h.manager == nil {
		h.sendError(w, http.StatusServiceUnavailable, "Archive system not available")
		return
	}

	user := h.getCurrentUser(r)
	if user == nil {
		h.sendError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	fractalID := chi.URLParam(r, "id")
	if fractalID == "" {
		h.sendError(w, http.StatusBadRequest, "Fractal ID is required")
		return
	}

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	// Get fractal to check retention
	fractal, err := h.fractalManager.GetFractal(r.Context(), fractalID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Fractal not found")
		return
	}

	archiveID, err := h.manager.CreateArchive(r.Context(), fractalID, user.Username, fractal.RetentionDays, ArchiveTypeAdhoc)
	if err != nil {
		log.Printf("[Archives] Failed to create archive for fractal %s: %v", fractalID, err)
		status := http.StatusInternalServerError
		if strings.Contains(err.Error(), "already in progress") {
			status = http.StatusConflict
		} else if strings.Contains(err.Error(), "not configured") {
			status = http.StatusServiceUnavailable
		}
		h.sendError(w, status, err.Error())
		return
	}

	h.sendSuccess(w, "Archive creation started", map[string]string{
		"archive_id": archiveID,
	})
}

// HandleListArchives returns all archives for a fractal.
func (h *Handler) HandleListArchives(w http.ResponseWriter, r *http.Request) {
	if h.manager == nil {
		h.sendError(w, http.StatusServiceUnavailable, "Archive system not available")
		return
	}

	user := h.getCurrentUser(r)
	if user == nil {
		h.sendError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	fractalID := chi.URLParam(r, "id")
	if fractalID == "" {
		h.sendError(w, http.StatusBadRequest, "Fractal ID is required")
		return
	}

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	archives, err := h.manager.ListArchives(r.Context(), fractalID)
	if err != nil {
		log.Printf("[Archives] Failed to list archives for fractal %s: %v", fractalID, err)
		h.sendError(w, http.StatusInternalServerError, "Failed to list archives")
		return
	}

	h.sendSuccess(w, "Archives retrieved", map[string]interface{}{
		"archives": archives,
	})
}

// HandleGetArchive returns a specific archive's details and status.
func (h *Handler) HandleGetArchive(w http.ResponseWriter, r *http.Request) {
	if h.manager == nil {
		h.sendError(w, http.StatusServiceUnavailable, "Archive system not available")
		return
	}

	user := h.getCurrentUser(r)
	if user == nil {
		h.sendError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	fractalID := chi.URLParam(r, "id")
	archiveID := chi.URLParam(r, "archiveId")

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	archive, err := h.manager.GetArchive(r.Context(), archiveID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Archive not found")
		return
	}

	if archive.FractalID != fractalID {
		h.sendError(w, http.StatusNotFound, "Archive not found")
		return
	}

	h.sendSuccess(w, "Archive retrieved", archive)
}

// HandleRestoreArchive starts a restore operation from an archive.
// The target fractal is derived from the provided ingest token, which is
// validated server-side. Cross-fractal restore is supported by providing
// a token scoped to the desired target fractal.
func (h *Handler) HandleRestoreArchive(w http.ResponseWriter, r *http.Request) {
	if h.manager == nil {
		h.sendError(w, http.StatusServiceUnavailable, "Archive system not available")
		return
	}

	user := h.getCurrentUser(r)
	if user == nil {
		h.sendError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	fractalID := chi.URLParam(r, "id")
	archiveID := chi.URLParam(r, "archiveId")

	// Must be admin on the source fractal (owns the archive)
	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	// Verify archive belongs to the source fractal
	archive, err := h.manager.GetArchive(r.Context(), archiveID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Archive not found")
		return
	}
	if archive.FractalID != fractalID {
		h.sendError(w, http.StatusNotFound, "Archive not found")
		return
	}

	var req RestoreArchiveRequest
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&req)
	}

	if req.IngestToken == "" {
		h.sendError(w, http.StatusBadRequest, "ingest_token is required for restore")
		return
	}

	// Validate the ingest token and derive the target fractal from it.
	validated, err := h.tokenStorage.ValidateToken(r.Context(), req.IngestToken)
	if err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid ingest token")
		return
	}
	targetFractalID := validated.FractalID

	// Must also be admin on the target fractal
	if targetFractalID != fractalID {
		targetRole := h.resolveFractalRole(r, targetFractalID)
		if !rbac.HasAccess(user, targetRole, rbac.RoleAdmin) {
			h.sendError(w, http.StatusForbidden, "Insufficient permissions on target fractal")
			return
		}
	}

	if err := h.manager.RestoreArchive(r.Context(), archiveID, targetFractalID, req.IngestToken); err != nil {
		log.Printf("[Archives] Failed to start restore for archive %s: %v", archiveID, err)
		status := http.StatusInternalServerError
		if strings.Contains(err.Error(), "already in progress") || strings.Contains(err.Error(), "can only restore completed") {
			status = http.StatusConflict
		} else if strings.Contains(err.Error(), "not configured") {
			status = http.StatusServiceUnavailable
		}
		h.sendError(w, status, err.Error())
		return
	}

	h.sendSuccess(w, "Restore started", map[string]string{
		"archive_id":        archiveID,
		"target_fractal_id": targetFractalID,
	})
}

// HandleCancelOperation cancels a running archive or restore operation.
func (h *Handler) HandleCancelOperation(w http.ResponseWriter, r *http.Request) {
	if h.manager == nil {
		h.sendError(w, http.StatusServiceUnavailable, "Archive system not available")
		return
	}

	user := h.getCurrentUser(r)
	if user == nil {
		h.sendError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	fractalID := chi.URLParam(r, "id")
	archiveID := chi.URLParam(r, "archiveId")

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	archive, err := h.manager.GetArchive(r.Context(), archiveID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Archive not found")
		return
	}
	if archive.FractalID != fractalID {
		h.sendError(w, http.StatusNotFound, "Archive not found")
		return
	}

	if err := h.manager.CancelOperation(r.Context(), archiveID); err != nil {
		log.Printf("[Archives] Failed to cancel operation %s: %v", archiveID, err)
		h.sendError(w, http.StatusInternalServerError, "Failed to cancel operation")
		return
	}

	h.sendSuccess(w, "Operation cancelled", nil)
}

// HandleDeleteArchive deletes an archive and its storage file.
func (h *Handler) HandleDeleteArchive(w http.ResponseWriter, r *http.Request) {
	if h.manager == nil {
		h.sendError(w, http.StatusServiceUnavailable, "Archive system not available")
		return
	}

	user := h.getCurrentUser(r)
	if user == nil {
		h.sendError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	fractalID := chi.URLParam(r, "id")
	archiveID := chi.URLParam(r, "archiveId")

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	// Verify archive belongs to this fractal
	archive, err := h.manager.GetArchive(r.Context(), archiveID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Archive not found")
		return
	}
	if archive.FractalID != fractalID {
		h.sendError(w, http.StatusNotFound, "Archive not found")
		return
	}

	if err := h.manager.DeleteArchive(r.Context(), archiveID); err != nil {
		log.Printf("[Archives] Failed to delete archive %s: %v", archiveID, err)
		h.sendError(w, http.StatusInternalServerError, "Failed to delete archive")
		return
	}

	h.sendSuccess(w, "Archive deleted", nil)
}

// Helper methods

func (h *Handler) getCurrentUser(r *http.Request) *storage.User {
	if user := r.Context().Value("user"); user != nil {
		if userObj, ok := user.(*storage.User); ok {
			return userObj
		}
	}
	return nil
}

func (h *Handler) resolveFractalRole(r *http.Request, fractalID string) rbac.Role {
	user := h.getCurrentUser(r)
	if user == nil {
		return rbac.RoleNone
	}
	if user.IsAdmin {
		return rbac.RoleAdmin
	}
	if h.rbacResolver == nil || fractalID == "" {
		return rbac.RoleNone
	}
	return h.rbacResolver.ResolveRole(r.Context(), user, fractalID)
}

func (h *Handler) sendSuccess(w http.ResponseWriter, message string, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(APIResponse{
		Success: true,
		Message: message,
		Data:    data,
	})
}

func (h *Handler) sendError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(APIResponse{
		Success: false,
		Error:   message,
	})
}
