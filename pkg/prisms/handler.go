package prisms

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"

	"bifract/pkg/rbac"
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
	pg             *storage.PostgresClient
	sessionManager SessionManager
	rbacResolver   *rbac.Resolver
}

// NewHandler creates a new prism handler.
func NewHandler(manager *Manager, pg *storage.PostgresClient, sessionManager SessionManager) *Handler {
	return &Handler{manager: manager, pg: pg, sessionManager: sessionManager}
}

// SetRBACResolver sets the RBAC resolver for prism-level access checks.
func (h *Handler) SetRBACResolver(resolver *rbac.Resolver) {
	h.rbacResolver = resolver
}

// resolvePrismRole returns the user's effective role on a prism.
func (h *Handler) resolvePrismRole(r *http.Request, prismID string) rbac.Role {
	user := getCurrentUser(r)
	if user == nil {
		return rbac.RoleNone
	}
	if user.IsAdmin {
		return rbac.RoleAdmin
	}
	if h.rbacResolver == nil {
		return rbac.PrismRoleFromContext(r.Context())
	}
	return h.rbacResolver.ResolvePrismRoleWithAdmin(r.Context(), user, prismID)
}

// requirePrismRole checks the user has the required role on a prism.
func (h *Handler) requirePrismRole(w http.ResponseWriter, r *http.Request, prismID string, required rbac.Role) bool {
	user := getCurrentUser(r)
	role := h.resolvePrismRole(r, prismID)
	if !rbac.HasAccess(user, role, required) {
		respondError(w, http.StatusForbidden, "Insufficient permissions")
		return false
	}
	return true
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

	// Auto-grant the creator admin role on the new prism
	username := user.Username
	if _, err := h.pg.GrantPrismPermission(r.Context(), prism.ID, &username, nil, "admin", user.Username); err != nil {
		log.Printf("[Prisms] Failed to auto-grant admin permission on prism %s: %v", prism.ID, err)
	}

	respondSuccess(w, prism)
}

func (h *Handler) HandleGetPrism(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	if !h.requirePrismRole(w, r, id, rbac.RoleViewer) {
		return
	}

	prism, err := h.manager.GetPrism(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusNotFound, "Prism not found")
		return
	}
	respondSuccess(w, prism)
}

func (h *Handler) HandleUpdatePrism(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if !h.requirePrismRole(w, r, id, rbac.RoleAdmin) {
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
	id := chi.URLParam(r, "id")
	if !h.requirePrismRole(w, r, id, rbac.RoleAdmin) {
		return
	}

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
	id := chi.URLParam(r, "id")
	if !h.requirePrismRole(w, r, id, rbac.RoleAdmin) {
		return
	}

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

	// Verify the user has at least viewer access on this prism
	if !h.requirePrismRole(w, r, id, rbac.RoleViewer) {
		return
	}

	if err := h.sessionManager.SetSelectedPrismInSessionFromRequest(r, id); err != nil {
		log.Printf("[Prisms] Failed to select prism %s: %v", id, err)
		respondError(w, http.StatusInternalServerError, "Failed to select prism")
		return
	}
	respondSuccess(w, map[string]interface{}{"selected": true, "prism": prism})
}

// ---- Permission Management ----

func (h *Handler) HandleListPrismPermissions(w http.ResponseWriter, r *http.Request) {
	prismID := chi.URLParam(r, "id")
	if !h.requirePrismRole(w, r, prismID, rbac.RoleAdmin) {
		return
	}

	perms, err := h.pg.ListPrismPermissions(r.Context(), prismID)
	if err != nil {
		log.Printf("[Prisms] Failed to list permissions for %s: %v", prismID, err)
		respondError(w, http.StatusInternalServerError, "Failed to list permissions")
		return
	}
	if perms == nil {
		perms = []storage.PrismPermission{}
	}
	respondSuccess(w, map[string]interface{}{"permissions": perms})
}

func (h *Handler) HandleGrantPrismPermission(w http.ResponseWriter, r *http.Request) {
	user := getCurrentUser(r)
	prismID := chi.URLParam(r, "id")
	if !h.requirePrismRole(w, r, prismID, rbac.RoleAdmin) {
		return
	}

	var req struct {
		Username *string `json:"username"`
		GroupID  *string `json:"group_id"`
		Role     string  `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Role != "viewer" && req.Role != "analyst" && req.Role != "admin" {
		respondError(w, http.StatusBadRequest, "role must be 'viewer', 'analyst', or 'admin'")
		return
	}

	hasUser := req.Username != nil && *req.Username != ""
	hasGroup := req.GroupID != nil && *req.GroupID != ""
	if hasUser == hasGroup {
		respondError(w, http.StatusBadRequest, "provide exactly one of username or group_id")
		return
	}

	if !user.IsAdmin && req.Role == "admin" {
		respondError(w, http.StatusForbidden, "only tenant administrators can grant admin role")
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

	perm, err := h.pg.GrantPrismPermission(r.Context(), prismID, username, groupID, req.Role, user.Username)
	if err != nil {
		log.Printf("[Prisms] Failed to grant permission on %s: %v", prismID, err)
		respondError(w, http.StatusBadRequest, "Failed to grant permission")
		return
	}
	respondSuccess(w, perm)
}

func (h *Handler) HandleUpdatePrismPermission(w http.ResponseWriter, r *http.Request) {
	user := getCurrentUser(r)
	prismID := chi.URLParam(r, "id")
	if !h.requirePrismRole(w, r, prismID, rbac.RoleAdmin) {
		return
	}

	permID := chi.URLParam(r, "permId")

	// IDOR check: verify the permission belongs to this prism
	perm, err := h.pg.GetPrismPermission(r.Context(), permID)
	if err != nil {
		respondError(w, http.StatusNotFound, "Permission not found")
		return
	}
	if perm.PrismID != prismID {
		respondError(w, http.StatusForbidden, "Permission does not belong to this prism")
		return
	}

	var req struct {
		Role string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Role != "viewer" && req.Role != "analyst" && req.Role != "admin" {
		respondError(w, http.StatusBadRequest, "role must be 'viewer', 'analyst', or 'admin'")
		return
	}

	if !user.IsAdmin && req.Role == "admin" {
		respondError(w, http.StatusForbidden, "only tenant administrators can grant admin role")
		return
	}

	if err := h.pg.UpdatePrismPermissionRole(r.Context(), permID, req.Role); err != nil {
		respondError(w, http.StatusBadRequest, "Failed to update permission")
		return
	}
	respondSuccess(w, map[string]string{"message": "Permission updated"})
}

func (h *Handler) HandleRevokePrismPermission(w http.ResponseWriter, r *http.Request) {
	user := getCurrentUser(r)
	prismID := chi.URLParam(r, "id")
	if !h.requirePrismRole(w, r, prismID, rbac.RoleAdmin) {
		return
	}

	permID := chi.URLParam(r, "permId")

	// IDOR check: verify the permission belongs to this prism
	perm, err := h.pg.GetPrismPermission(r.Context(), permID)
	if err != nil {
		respondError(w, http.StatusNotFound, "Permission not found")
		return
	}
	if perm.PrismID != prismID {
		respondError(w, http.StatusForbidden, "Permission does not belong to this prism")
		return
	}

	// Prevent revoking your own permission
	if perm.Username != nil && *perm.Username == user.Username && !user.IsAdmin {
		respondError(w, http.StatusBadRequest, "Cannot revoke your own permission")
		return
	}

	if err := h.pg.RevokePrismPermission(r.Context(), permID); err != nil {
		respondError(w, http.StatusBadRequest, "Failed to revoke permission")
		return
	}
	respondSuccess(w, map[string]string{"message": "Permission revoked"})
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
