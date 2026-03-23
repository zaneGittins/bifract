package fractals

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"bifract/pkg/prisms"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"
)

// SessionManager interface for updating user sessions
type SessionManager interface {
	SetSelectedFractalInSessionFromRequest(r *http.Request, fractalID string) error
	SetSelectedPrismInSessionFromRequest(r *http.Request, prismID string) error
}

// Handler provides HTTP endpoints for index management
type Handler struct {
	manager        *Manager
	sessionManager SessionManager
	prismManager   *prisms.Manager
	pg             *storage.PostgresClient
	rbacResolver   *rbac.Resolver
}

// NewHandler creates a new index handler
func NewHandler(manager *Manager, sessionManager SessionManager, prismManager *prisms.Manager) *Handler {
	return &Handler{
		manager:        manager,
		sessionManager: sessionManager,
		prismManager:   prismManager,
	}
}

// SetRBAC injects the RBAC resolver and postgres client for permission management.
func (h *Handler) SetRBAC(pg *storage.PostgresClient, resolver *rbac.Resolver) {
	h.pg = pg
	h.rbacResolver = resolver
}

// HandleListFractals lists fractals and prisms filtered by user access.
func (h *Handler) HandleListFractals(w http.ResponseWriter, r *http.Request) {
	user := h.getCurrentUser(r)

	allFractals, err := h.manager.ListFractals(r.Context())
	if err != nil {
		log.Printf("[Fractals] Failed to list fractals: %v", err)
		h.sendError(w, http.StatusInternalServerError, "Failed to list fractals")
		return
	}

	// Filter fractals by access and annotate with user_role
	var visibleFractals []*Fractal
	if user != nil && user.IsAdmin {
		// Tenant admins see everything with admin role
		for _, f := range allFractals {
			f.UserRole = string(rbac.RoleAdmin)
		}
		visibleFractals = allFractals
	} else if user != nil && h.rbacResolver != nil {
		accessList, err := h.rbacResolver.GetAccessibleFractals(r.Context(), user.Username)
		if err != nil {
			log.Printf("[Fractals] Failed to resolve accessible fractals: %v", err)
			h.sendError(w, http.StatusInternalServerError, "Failed to list fractals")
			return
		}
		accessMap := make(map[string]rbac.Role, len(accessList))
		for _, a := range accessList {
			accessMap[a.FractalID] = a.Role
		}
		for _, f := range allFractals {
			if role, ok := accessMap[f.ID]; ok {
				f.UserRole = string(role)
				visibleFractals = append(visibleFractals, f)
			}
		}
	}
	if visibleFractals == nil {
		visibleFractals = []*Fractal{}
	}

	// Filter prisms: user must have an explicit prism_permissions grant
	var prismList []*prisms.Prism
	if h.prismManager != nil {
		allPrisms, err := h.prismManager.ListPrisms(r.Context())
		if err != nil {
			log.Printf("[Fractals] Failed to list prisms: %v", err)
			h.sendError(w, http.StatusInternalServerError, "Failed to list prisms")
			return
		}
		if user != nil && user.IsAdmin {
			prismList = allPrisms
		} else if user != nil && h.rbacResolver != nil {
			accessiblePrisms, err := h.rbacResolver.GetAccessiblePrisms(r.Context(), user.Username)
			if err == nil {
				prismAccessSet := make(map[string]bool, len(accessiblePrisms))
				for _, pa := range accessiblePrisms {
					prismAccessSet[pa.PrismID] = true
				}
				for _, p := range allPrisms {
					if prismAccessSet[p.ID] {
						prismList = append(prismList, p)
					}
				}
			}
		}
	}
	if prismList == nil {
		prismList = []*prisms.Prism{}
	}

	response := FractalListResponse{
		Fractals: visibleFractals,
		Prisms:   prismList,
		Total:    len(visibleFractals) + len(prismList),
	}

	h.sendSuccess(w, "Fractals retrieved successfully", response)
}

// HandleCreateFractal creates a new index (admin only)
func (h *Handler) HandleCreateFractal(w http.ResponseWriter, r *http.Request) {
	// Check if user is admin
	user := h.getCurrentUser(r)
	if user == nil || !user.IsAdmin {
		h.sendError(w, http.StatusForbidden, "Only administrators can create fractals")
		return
	}

	var req CreateFractalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	fractal, err := h.manager.CreateFractal(r.Context(), req, user.Username)
	if err != nil {
		log.Printf("[Fractals] Failed to create fractal: %v", err)
		h.sendError(w, http.StatusBadRequest, "Failed to create fractal")
		return
	}

	h.sendSuccess(w, "Fractal created successfully", map[string]interface{}{
		"index": fractal,
	})
}

// HandleGetFractal retrieves a specific index (viewer+)
func (h *Handler) HandleGetFractal(w http.ResponseWriter, r *http.Request) {
	user := h.getCurrentUser(r)
	fractalID := chi.URLParam(r, "id")
	if fractalID == "" {
		h.sendError(w, http.StatusBadRequest, "Fractal ID is required")
		return
	}

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleViewer) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	fractal, err := h.manager.GetFractal(r.Context(), fractalID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Fractal not found")
		return
	}

	h.sendSuccess(w, "Fractal retrieved successfully", map[string]interface{}{
		"index": fractal,
	})
}

// HandleUpdateFractal updates an existing index (fractal admin+)
func (h *Handler) HandleUpdateFractal(w http.ResponseWriter, r *http.Request) {
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

	var req UpdateFractalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	fractal, err := h.manager.UpdateFractal(r.Context(), fractalID, req)
	if err != nil {
		log.Printf("[Fractals] Failed to update fractal %s: %v", fractalID, err)
		h.sendError(w, http.StatusBadRequest, "Failed to update fractal")
		return
	}

	h.sendSuccess(w, "Fractal updated successfully", map[string]interface{}{
		"index": fractal,
	})
}

// HandleDeleteFractal deletes an index (admin only)
func (h *Handler) HandleDeleteFractal(w http.ResponseWriter, r *http.Request) {
	// Check if user is admin
	user := h.getCurrentUser(r)
	if user == nil || !user.IsAdmin {
		h.sendError(w, http.StatusForbidden, "Only administrators can delete fractals")
		return
	}

	fractalID := chi.URLParam(r, "id")
	if fractalID == "" {
		h.sendError(w, http.StatusBadRequest, "Fractal ID is required")
		return
	}

	if err := h.manager.DeleteFractal(r.Context(), fractalID); err != nil {
		log.Printf("[Fractals] Failed to delete fractal %s: %v", fractalID, err)
		h.sendError(w, http.StatusBadRequest, "Failed to delete fractal")
		return
	}

	h.sendSuccess(w, "Fractal deleted successfully", nil)
}

// HandleSelectFractal sets the selected index for the user's session (viewer+)
func (h *Handler) HandleSelectFractal(w http.ResponseWriter, r *http.Request) {
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

	// Validate that the index exists
	fractal, err := h.manager.GetFractal(r.Context(), fractalID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Fractal not found")
		return
	}

	// Verify user has at least viewer access to this fractal
	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleViewer) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	// Update session with selected index
	// Note: This will be implemented when we add session support to auth
	if err := h.setSelectedFractalInSession(w, r, fractalID); err != nil {
		log.Printf("[Fractals] Failed to update session: %v", err)
		h.sendError(w, http.StatusInternalServerError, "Failed to update session")
		return
	}

	h.sendSuccess(w, "Fractal selected successfully", map[string]interface{}{
		"selected_fractal": fractal,
	})
}

// HandleGetStats retrieves statistics for a specific index
func (h *Handler) HandleGetStats(w http.ResponseWriter, r *http.Request) {
	user := h.getCurrentUser(r)
	fractalID := chi.URLParam(r, "id")
	if fractalID == "" {
		h.sendError(w, http.StatusBadRequest, "Fractal ID is required")
		return
	}

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleViewer) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	stats, err := h.manager.GetFractalStats(r.Context(), fractalID)
	if err != nil {
		log.Printf("[Fractals] Failed to get stats for %s: %v", fractalID, err)
		h.sendError(w, http.StatusInternalServerError, "Failed to retrieve statistics")
		return
	}

	h.sendSuccess(w, "Fractal statistics retrieved successfully", map[string]interface{}{
		"stats": stats,
	})
}

// HandleSetRetention sets the retention period for a fractal (fractal admin+)
func (h *Handler) HandleSetRetention(w http.ResponseWriter, r *http.Request) {
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

	var req UpdateRetentionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if err := h.manager.SetRetention(r.Context(), fractalID, req.RetentionDays); err != nil {
		log.Printf("[Fractals] Failed to set retention for %s: %v", fractalID, err)
		h.sendError(w, http.StatusBadRequest, "Failed to update retention policy")
		return
	}

	h.sendSuccess(w, "Retention updated successfully", nil)
}

// HandleSetArchiveSchedule sets the archive schedule for a fractal (fractal admin+).
func (h *Handler) HandleSetArchiveSchedule(w http.ResponseWriter, r *http.Request) {
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

	var req UpdateArchiveScheduleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if err := h.manager.SetArchiveSchedule(r.Context(), fractalID, req.ArchiveSchedule, req.MaxArchives); err != nil {
		log.Printf("[Fractals] Failed to set archive schedule for %s: %v", fractalID, err)
		h.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	h.sendSuccess(w, "Archive schedule updated successfully", nil)
}

// HandleSetDiskQuota sets the disk quota and enforcement action for a fractal (fractal admin+).
func (h *Handler) HandleSetDiskQuota(w http.ResponseWriter, r *http.Request) {
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

	var req UpdateDiskQuotaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Action == "" {
		req.Action = "reject"
	}

	if err := h.manager.SetDiskQuota(r.Context(), fractalID, req.QuotaBytes, req.Action); err != nil {
		log.Printf("[Fractals] Failed to set disk quota for %s: %v", fractalID, err)
		h.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	h.sendSuccess(w, "Disk quota updated successfully", nil)
}

// HandleRefreshStats refreshes statistics for all fractals
func (h *Handler) HandleRefreshStats(w http.ResponseWriter, r *http.Request) {
	// Check if user is admin
	user := h.getCurrentUser(r)
	if user == nil || !user.IsAdmin {
		h.sendError(w, http.StatusForbidden, "Only administrators can refresh index statistics")
		return
	}

	if err := h.manager.RefreshFractalStats(r.Context()); err != nil {
		log.Printf("[Fractals] Failed to refresh stats: %v", err)
		h.sendError(w, http.StatusInternalServerError, "Failed to refresh statistics")
		return
	}

	h.sendSuccess(w, "Fractal statistics refreshed successfully", nil)
}

// Helper methods

// getCurrentUser extracts the current user from the request context
func (h *Handler) getCurrentUser(r *http.Request) *storage.User {
	if user := r.Context().Value("user"); user != nil {
		if userObj, ok := user.(*storage.User); ok {
			return userObj
		}
	}
	return nil
}

// getSelectedFractalFromSession gets the selected index from the user's session
func (h *Handler) getSelectedFractalFromSession(r *http.Request) (string, error) {
	// This will be implemented when we add session support
	// For now, return the default index
	defaultFractal, err := h.manager.GetDefaultFractal(r.Context())
	if err != nil {
		return "", err
	}
	return defaultFractal.ID, nil
}

// setSelectedFractalInSession stores the selected index in the user's session
func (h *Handler) setSelectedFractalInSession(w http.ResponseWriter, r *http.Request, fractalID string) error {
	// Check authentication type - only session-based requests can update fractal selection
	authType := r.Context().Value("auth_type")
	if authType != "session" {
		return fmt.Errorf("session authentication required for fractal selection")
	}

	// Use the session manager to properly update the session
	if h.sessionManager == nil {
		return fmt.Errorf("session manager not available")
	}

	err := h.sessionManager.SetSelectedFractalInSessionFromRequest(r, fractalID)
	if err != nil {
		return fmt.Errorf("failed to update session: %w", err)
	}

	return nil
}

// sendSuccess sends a successful JSON response
func (h *Handler) sendSuccess(w http.ResponseWriter, message string, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(APIResponse{
		Success: true,
		Message: message,
		Data:    data,
	})
}

// sendError sends an error JSON response
func (h *Handler) sendError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(APIResponse{
		Success: false,
		Error:   message,
	})
}

// resolveFractalRole resolves the calling user's role on a specific fractal.
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
	role, err := h.rbacResolver.ResolveFractalRole(r.Context(), user.Username, fractalID)
	if err != nil {
		return rbac.RoleNone
	}
	return role
}

// ============================
// Fractal Permission Endpoints
// ============================

// HandleListPermissions lists permissions for a fractal (fractal admin or tenant admin).
func (h *Handler) HandleListPermissions(w http.ResponseWriter, r *http.Request) {
	user := h.getCurrentUser(r)
	fractalID := chi.URLParam(r, "id")

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	perms, err := h.pg.ListFractalPermissions(r.Context(), fractalID)
	if err != nil {
		log.Printf("[Fractals] Failed to list permissions for %s: %v", fractalID, err)
		h.sendError(w, http.StatusInternalServerError, "Failed to list permissions")
		return
	}
	if perms == nil {
		perms = []storage.FractalPermission{}
	}
	h.sendSuccess(w, "Permissions retrieved", perms)
}

// HandleGrantPermission grants a user or group access to a fractal (fractal admin or tenant admin).
func (h *Handler) HandleGrantPermission(w http.ResponseWriter, r *http.Request) {
	user := h.getCurrentUser(r)
	fractalID := chi.URLParam(r, "id")

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	var req struct {
		Username *string `json:"username"`
		GroupID  *string `json:"group_id"`
		Role     string  `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Role != "viewer" && req.Role != "analyst" && req.Role != "admin" {
		h.sendError(w, http.StatusBadRequest, "Role must be 'viewer', 'analyst', or 'admin'")
		return
	}

	// Exactly one of username/group_id must be provided
	hasUser := req.Username != nil && *req.Username != ""
	hasGroup := req.GroupID != nil && *req.GroupID != ""
	if hasUser == hasGroup {
		h.sendError(w, http.StatusBadRequest, "Provide exactly one of username or group_id")
		return
	}

	// Non-tenant-admins (fractal admins) cannot grant admin role
	if !user.IsAdmin && req.Role == "admin" {
		h.sendError(w, http.StatusForbidden, "Only tenant administrators can grant admin role")
		return
	}

	var username *string
	var groupID *string
	if hasUser {
		username = req.Username
	}
	if hasGroup {
		groupID = req.GroupID
	}

	perm, err := h.pg.GrantFractalPermission(r.Context(), fractalID, username, groupID, req.Role, user.Username)
	if err != nil {
		log.Printf("[Fractals] Failed to grant permission on %s: %v", fractalID, err)
		h.sendError(w, http.StatusBadRequest, "Failed to grant permission")
		return
	}
	h.sendSuccess(w, "Permission granted", perm)
}

// HandleUpdatePermission updates a permission's role (fractal admin or tenant admin).
func (h *Handler) HandleUpdatePermission(w http.ResponseWriter, r *http.Request) {
	user := h.getCurrentUser(r)
	fractalID := chi.URLParam(r, "id")

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	permID := chi.URLParam(r, "permId")

	// Verify the permission belongs to this fractal (prevents cross-fractal IDOR)
	perm, err := h.pg.GetFractalPermission(r.Context(), permID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Permission not found")
		return
	}
	if perm.FractalID != fractalID {
		h.sendError(w, http.StatusForbidden, "Permission does not belong to this fractal")
		return
	}

	var req struct {
		Role string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	if req.Role != "viewer" && req.Role != "analyst" && req.Role != "admin" {
		h.sendError(w, http.StatusBadRequest, "Role must be 'viewer', 'analyst', or 'admin'")
		return
	}

	if !user.IsAdmin && req.Role == "admin" {
		h.sendError(w, http.StatusForbidden, "Only tenant administrators can grant admin role")
		return
	}

	if err := h.pg.UpdateFractalPermissionRole(r.Context(), permID, req.Role); err != nil {
		h.sendError(w, http.StatusBadRequest, "Failed to update permission")
		return
	}
	h.sendSuccess(w, "Permission updated", nil)
}

// HandleRevokePermission revokes a permission (fractal admin or tenant admin).
func (h *Handler) HandleRevokePermission(w http.ResponseWriter, r *http.Request) {
	user := h.getCurrentUser(r)
	fractalID := chi.URLParam(r, "id")

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	permID := chi.URLParam(r, "permId")

	// Verify the permission belongs to this fractal (prevents cross-fractal IDOR)
	perm, err := h.pg.GetFractalPermission(r.Context(), permID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Permission not found")
		return
	}
	if perm.FractalID != fractalID {
		h.sendError(w, http.StatusForbidden, "Permission does not belong to this fractal")
		return
	}

	// Prevent revoking your own permission (fractal admins could lock themselves out)
	if perm.Username != nil && *perm.Username == user.Username && !user.IsAdmin {
		h.sendError(w, http.StatusBadRequest, "Cannot revoke your own permission")
		return
	}

	if err := h.pg.RevokeFractalPermission(r.Context(), permID); err != nil {
		h.sendError(w, http.StatusBadRequest, "Failed to revoke permission")
		return
	}
	h.sendSuccess(w, "Permission revoked", nil)
}