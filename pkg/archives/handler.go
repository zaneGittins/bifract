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

	var req CreateArchiveRequest
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&req)
	}

	split := req.Split
	if split == "" {
		split = SplitNone
	}
	if !ValidSplitGranularity(split) {
		h.sendError(w, http.StatusBadRequest, "Invalid split granularity")
		return
	}

	fractal, err := h.fractalManager.GetFractal(r.Context(), fractalID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Fractal not found")
		return
	}

	groupID, err := h.manager.CreateArchiveGroup(r.Context(), fractalID, user.Username, fractal.RetentionDays, ArchiveTypeAdhoc, split)
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
		"group_id": groupID,
	})
}

// HandleListArchives returns all archives for a fractal, grouped by archive group.
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

	items, err := h.manager.ListArchiveItems(r.Context(), fractalID)
	if err != nil {
		log.Printf("[Archives] Failed to list archives for fractal %s: %v", fractalID, err)
		h.sendError(w, http.StatusInternalServerError, "Failed to list archives")
		return
	}

	h.sendSuccess(w, "Archives retrieved", map[string]interface{}{
		"items": items,
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

// HandleRestoreArchive starts a restore operation from a single archive.
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

	var req RestoreArchiveRequest
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&req)
	}

	if req.IngestToken == "" {
		h.sendError(w, http.StatusBadRequest, "ingest_token is required for restore")
		return
	}

	validated, err := h.tokenStorage.ValidateToken(r.Context(), req.IngestToken)
	if err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid ingest token")
		return
	}
	targetFractalID := validated.FractalID

	if targetFractalID != fractalID {
		targetRole := h.resolveFractalRole(r, targetFractalID)
		if !rbac.HasAccess(user, targetRole, rbac.RoleAdmin) {
			h.sendError(w, http.StatusForbidden, "Insufficient permissions on target fractal")
			return
		}
	}

	if err := h.manager.RestoreArchive(r.Context(), archiveID, targetFractalID, req.IngestToken, req.ClearExisting); err != nil {
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

// ========================================================================
// Archive Group Handlers
// ========================================================================

// HandleRestoreGroup starts a sequential restore of all archives in a group.
func (h *Handler) HandleRestoreGroup(w http.ResponseWriter, r *http.Request) {
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
	groupID := chi.URLParam(r, "groupId")

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	group, err := h.manager.GetArchiveGroup(r.Context(), groupID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Archive group not found")
		return
	}
	if group.FractalID != fractalID {
		h.sendError(w, http.StatusNotFound, "Archive group not found")
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

	validated, err := h.tokenStorage.ValidateToken(r.Context(), req.IngestToken)
	if err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid ingest token")
		return
	}
	targetFractalID := validated.FractalID

	if targetFractalID != fractalID {
		targetRole := h.resolveFractalRole(r, targetFractalID)
		if !rbac.HasAccess(user, targetRole, rbac.RoleAdmin) {
			h.sendError(w, http.StatusForbidden, "Insufficient permissions on target fractal")
			return
		}
	}

	if err := h.manager.RestoreGroup(r.Context(), groupID, targetFractalID, req.IngestToken, req.ClearExisting); err != nil {
		log.Printf("[Archives] Failed to start group restore %s: %v", groupID, err)
		status := http.StatusInternalServerError
		if strings.Contains(err.Error(), "already in progress") || strings.Contains(err.Error(), "active operation") {
			status = http.StatusConflict
		} else if strings.Contains(err.Error(), "not configured") {
			status = http.StatusServiceUnavailable
		}
		h.sendError(w, status, err.Error())
		return
	}

	h.sendSuccess(w, "Group restore started", map[string]string{
		"group_id":          groupID,
		"target_fractal_id": targetFractalID,
	})
}

// HandleCancelGroup cancels a running archive group operation.
func (h *Handler) HandleCancelGroup(w http.ResponseWriter, r *http.Request) {
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
	groupID := chi.URLParam(r, "groupId")

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	group, err := h.manager.GetArchiveGroup(r.Context(), groupID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Archive group not found")
		return
	}
	if group.FractalID != fractalID {
		h.sendError(w, http.StatusNotFound, "Archive group not found")
		return
	}

	if err := h.manager.CancelGroup(r.Context(), groupID); err != nil {
		log.Printf("[Archives] Failed to cancel group %s: %v", groupID, err)
		h.sendError(w, http.StatusInternalServerError, "Failed to cancel group operation")
		return
	}

	h.sendSuccess(w, "Group operation cancelled", nil)
}

// HandleDeleteGroup deletes an archive group and all its child archives.
func (h *Handler) HandleDeleteGroup(w http.ResponseWriter, r *http.Request) {
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
	groupID := chi.URLParam(r, "groupId")

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	group, err := h.manager.GetArchiveGroup(r.Context(), groupID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Archive group not found")
		return
	}
	if group.FractalID != fractalID {
		h.sendError(w, http.StatusNotFound, "Archive group not found")
		return
	}

	if err := h.manager.DeleteGroup(r.Context(), groupID); err != nil {
		log.Printf("[Archives] Failed to delete group %s: %v", groupID, err)
		h.sendError(w, http.StatusInternalServerError, "Failed to delete archive group")
		return
	}

	h.sendSuccess(w, "Archive group deleted", nil)
}

// ========================================================================
// Helper methods
// ========================================================================

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
