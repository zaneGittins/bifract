package apikeys

import (
	"bifract/pkg/api"
	"bifract/pkg/auth"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"

	"bifract/pkg/rbac"
	"bifract/pkg/storage"
)

// PrismResolver provides prism lookup for API key management.
type PrismResolver interface {
	GetPrismInfo(ctx context.Context, prismID string) (id, name, description string, err error)
}

// Handler provides HTTP endpoints for API key management
type Handler struct {
	storage       *Storage
	rbacResolver  *rbac.Resolver
	prismResolver PrismResolver
}

// NewHandler creates a new API key handler
func NewHandler(pg *storage.PostgresClient) *Handler {
	return &Handler{
		storage: NewStorage(pg),
	}
}

// SetRBAC injects the RBAC resolver for permission checks.
func (h *Handler) SetRBAC(resolver *rbac.Resolver) {
	h.rbacResolver = resolver
}

// SetPrismResolver injects the prism resolver for prism API key management.
func (h *Handler) SetPrismResolver(pr PrismResolver) {
	h.prismResolver = pr
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

// resolvePrismRole resolves the calling user's role on a specific prism.
func (h *Handler) resolvePrismRole(r *http.Request, prismID string) rbac.Role {
	user := h.getCurrentUser(r)
	if user == nil {
		return rbac.RoleNone
	}
	if user.IsAdmin {
		return rbac.RoleAdmin
	}
	if h.rbacResolver == nil || prismID == "" {
		return rbac.RoleNone
	}
	return h.rbacResolver.ResolvePrismRoleWithAdmin(r.Context(), user, prismID)
}

// ---- Fractal-scoped handlers ----

// HandleListAPIKeys lists all API keys for a specific fractal (fractal admin+)
func (h *Handler) HandleListAPIKeys(w http.ResponseWriter, r *http.Request) {
	fractalID := chi.URLParam(r, "id")
	if fractalID == "" {
		h.sendError(w, http.StatusBadRequest, "Fractal ID is required")
		return
	}

	user := h.getCurrentUser(r)
	if user == nil {
		h.sendError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	keys, err := h.storage.ListAPIKeysByFractal(r.Context(), fractalID)
	if err != nil {
		log.Printf("[APIKeys] Failed to list API keys for fractal %s: %v", fractalID, err)
		h.sendError(w, http.StatusInternalServerError, "Failed to list API keys")
		return
	}

	api.WriteList(w, keys)
}

// HandleListAllAPIKeys lists every API key in the instance (tenant admin).
func (h *Handler) HandleListAllAPIKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := h.storage.ListAllAPIKeys(r.Context())
	if err != nil {
		log.Printf("[APIKeys] Failed to list all API keys: %v", err)
		h.sendError(w, http.StatusInternalServerError, "Failed to list API keys")
		return
	}
	api.WriteList(w, keys)
}

// HandleCreateAPIKey creates a new API key for a fractal (fractal admin+)
func (h *Handler) HandleCreateAPIKey(w http.ResponseWriter, r *http.Request) {
	fractalID := chi.URLParam(r, "id")
	if fractalID == "" {
		h.sendError(w, http.StatusBadRequest, "Fractal ID is required")
		return
	}

	user := h.getCurrentUser(r)
	if user == nil {
		h.sendError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	var req CreateAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Name == "" {
		h.sendError(w, http.StatusBadRequest, "API key name is required")
		return
	}

	if err := req.Validate(); err != nil {
		h.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	// The instance-wide grant belongs to no scope, so it is never issued here.
	if req.TenantAdmin {
		h.sendError(w, http.StatusBadRequest, "A tenant admin key is instance-wide: create it at /api/v1/api-keys")
		return
	}

	// Resolve fractal name for key format
	fractalName := fractalID
	if h.rbacResolver != nil {
		if name, err := h.resolveFractalName(r.Context(), fractalID); err == nil && name != "" {
			fractalName = name
		}
	}

	fullKey, keyID, err := h.storage.GenerateAPIKey(r.Context(), fractalName)
	if err != nil {
		log.Printf("[APIKeys] Failed to generate API key: %v", err)
		h.sendError(w, http.StatusInternalServerError, "Failed to generate API key")
		return
	}

	apiKey, err := h.storage.CreateFractalAPIKey(r.Context(), req, fractalID, auth.AttributionUsername(r.Context()), fullKey, keyID)
	if err != nil {
		log.Printf("[APIKeys] Failed to create API key: %v", err)
		h.sendError(w, http.StatusInternalServerError, "Failed to create API key")
		return
	}

	response := CreateAPIKeyResponse{
		Key:    fullKey,
		KeyID:  keyID,
		APIKey: *apiKey,
	}

	h.sendSuccess(w, "API key created successfully", response)
}

// HandleGetAPIKey retrieves a specific API key (fractal admin+)
func (h *Handler) HandleGetAPIKey(w http.ResponseWriter, r *http.Request) {
	fractalID := chi.URLParam(r, "id")
	keyID := chi.URLParam(r, "keyId")

	if fractalID == "" || keyID == "" {
		h.sendError(w, http.StatusBadRequest, "Fractal ID and Key ID are required")
		return
	}

	user := h.getCurrentUser(r)
	if user == nil {
		h.sendError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	apiKey, err := h.storage.GetFractalAPIKey(r.Context(), keyID, fractalID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "API key not found")
		return
	}

	h.sendSuccess(w, "API key retrieved successfully", apiKey)
}

// HandleUpdateAPIKey updates an existing API key (fractal admin+)
func (h *Handler) HandleUpdateAPIKey(w http.ResponseWriter, r *http.Request) {
	fractalID := chi.URLParam(r, "id")
	keyID := chi.URLParam(r, "keyId")

	if fractalID == "" || keyID == "" {
		h.sendError(w, http.StatusBadRequest, "Fractal ID and Key ID are required")
		return
	}

	user := h.getCurrentUser(r)
	if user == nil {
		h.sendError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	var req UpdateAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	current, err := h.storage.GetFractalAPIKey(r.Context(), keyID, fractalID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "API key not found")
		return
	}
	if err := req.Validate(current); err != nil {
		h.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	apiKey, err := h.storage.UpdateFractalAPIKey(r.Context(), keyID, fractalID, req)
	if err != nil {
		log.Printf("[APIKeys] Failed to update API key %s: %v", keyID, err)
		h.sendError(w, http.StatusBadRequest, "Failed to update API key")
		return
	}

	h.sendSuccess(w, "API key updated successfully", apiKey)
}

// HandleDeleteAPIKey deletes an API key (fractal admin+)
func (h *Handler) HandleDeleteAPIKey(w http.ResponseWriter, r *http.Request) {
	fractalID := chi.URLParam(r, "id")
	keyID := chi.URLParam(r, "keyId")

	if fractalID == "" || keyID == "" {
		h.sendError(w, http.StatusBadRequest, "Fractal ID and Key ID are required")
		return
	}

	user := h.getCurrentUser(r)
	if user == nil {
		h.sendError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	if err := h.storage.DeleteFractalAPIKey(r.Context(), keyID, fractalID); err != nil {
		log.Printf("[APIKeys] Failed to delete API key %s: %v", keyID, err)
		h.sendError(w, http.StatusBadRequest, "Failed to delete API key")
		return
	}

	h.sendSuccess(w, "API key deleted successfully", nil)
}

// HandleToggleAPIKey toggles the active status of an API key (fractal admin+)
func (h *Handler) HandleToggleAPIKey(w http.ResponseWriter, r *http.Request) {
	fractalID := chi.URLParam(r, "id")
	keyID := chi.URLParam(r, "keyId")

	if fractalID == "" || keyID == "" {
		h.sendError(w, http.StatusBadRequest, "Fractal ID and Key ID are required")
		return
	}

	user := h.getCurrentUser(r)
	if user == nil {
		h.sendError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	role := h.resolveFractalRole(r, fractalID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	apiKey, err := h.storage.ToggleFractalAPIKey(r.Context(), keyID, fractalID)
	if errors.Is(err, ErrKeyNotFound) {
		h.sendError(w, http.StatusNotFound, "API key not found")
		return
	}
	if err != nil {
		log.Printf("[APIKeys] Failed to toggle API key %s: %v", keyID, err)
		h.sendError(w, http.StatusBadRequest, "Failed to toggle API key")
		return
	}

	action := "deactivated"
	if apiKey.IsActive {
		action = "activated"
	}

	h.sendSuccess(w, fmt.Sprintf("API key %s successfully", action), map[string]interface{}{
		"api_key": apiKey,
	})
}

// ---- Prism-scoped handlers ----

// HandleListPrismAPIKeys lists all API keys for a specific prism (prism admin+)
func (h *Handler) HandleListPrismAPIKeys(w http.ResponseWriter, r *http.Request) {
	prismID := chi.URLParam(r, "id")
	if prismID == "" {
		h.sendError(w, http.StatusBadRequest, "Prism ID is required")
		return
	}

	user := h.getCurrentUser(r)
	if user == nil {
		h.sendError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	role := h.resolvePrismRole(r, prismID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	keys, err := h.storage.ListAPIKeysByPrism(r.Context(), prismID)
	if err != nil {
		log.Printf("[APIKeys] Failed to list API keys for prism %s: %v", prismID, err)
		h.sendError(w, http.StatusInternalServerError, "Failed to list API keys")
		return
	}

	api.WriteList(w, keys)
}

// HandleCreatePrismAPIKey creates a new API key for a prism (prism admin+)
func (h *Handler) HandleCreatePrismAPIKey(w http.ResponseWriter, r *http.Request) {
	prismID := chi.URLParam(r, "id")
	if prismID == "" {
		h.sendError(w, http.StatusBadRequest, "Prism ID is required")
		return
	}

	user := h.getCurrentUser(r)
	if user == nil {
		h.sendError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	role := h.resolvePrismRole(r, prismID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	// Verify prism exists and get name
	if h.prismResolver == nil {
		h.sendError(w, http.StatusInternalServerError, "Prism resolver not configured")
		return
	}
	_, prismName, _, err := h.prismResolver.GetPrismInfo(r.Context(), prismID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Prism not found")
		return
	}

	var req CreateAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Name == "" {
		h.sendError(w, http.StatusBadRequest, "API key name is required")
		return
	}

	if err := req.Validate(); err != nil {
		h.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	// The instance-wide grant belongs to no scope, so it is never issued here.
	if req.TenantAdmin {
		h.sendError(w, http.StatusBadRequest, "A tenant admin key is instance-wide: create it at /api/v1/api-keys")
		return
	}

	fullKey, keyID, err := h.storage.GenerateAPIKey(r.Context(), prismName)
	if err != nil {
		log.Printf("[APIKeys] Failed to generate API key: %v", err)
		h.sendError(w, http.StatusInternalServerError, "Failed to generate API key")
		return
	}

	apiKey, err := h.storage.CreatePrismAPIKey(r.Context(), req, prismID, auth.AttributionUsername(r.Context()), fullKey, keyID)
	if err != nil {
		log.Printf("[APIKeys] Failed to create prism API key: %v", err)
		h.sendError(w, http.StatusInternalServerError, "Failed to create API key")
		return
	}

	response := CreateAPIKeyResponse{
		Key:    fullKey,
		KeyID:  keyID,
		APIKey: *apiKey,
	}

	h.sendSuccess(w, "API key created successfully", response)
}

// HandleGetPrismAPIKey retrieves a specific prism-scoped API key (prism admin+)
func (h *Handler) HandleGetPrismAPIKey(w http.ResponseWriter, r *http.Request) {
	prismID := chi.URLParam(r, "id")
	keyID := chi.URLParam(r, "keyId")

	if prismID == "" || keyID == "" {
		h.sendError(w, http.StatusBadRequest, "Prism ID and Key ID are required")
		return
	}

	user := h.getCurrentUser(r)
	if user == nil {
		h.sendError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	role := h.resolvePrismRole(r, prismID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	apiKey, err := h.storage.GetPrismAPIKey(r.Context(), keyID, prismID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "API key not found")
		return
	}

	h.sendSuccess(w, "API key retrieved successfully", apiKey)
}

// HandleUpdatePrismAPIKey updates a prism-scoped API key (prism admin+)
func (h *Handler) HandleUpdatePrismAPIKey(w http.ResponseWriter, r *http.Request) {
	prismID := chi.URLParam(r, "id")
	keyID := chi.URLParam(r, "keyId")

	if prismID == "" || keyID == "" {
		h.sendError(w, http.StatusBadRequest, "Prism ID and Key ID are required")
		return
	}

	user := h.getCurrentUser(r)
	if user == nil {
		h.sendError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	role := h.resolvePrismRole(r, prismID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	var req UpdateAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	current, err := h.storage.GetPrismAPIKey(r.Context(), keyID, prismID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "API key not found")
		return
	}
	if err := req.Validate(current); err != nil {
		h.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	apiKey, err := h.storage.UpdatePrismAPIKey(r.Context(), keyID, prismID, req)
	if err != nil {
		log.Printf("[APIKeys] Failed to update prism API key %s: %v", keyID, err)
		h.sendError(w, http.StatusBadRequest, "Failed to update API key")
		return
	}

	h.sendSuccess(w, "API key updated successfully", apiKey)
}

// HandleDeletePrismAPIKey deletes a prism-scoped API key (prism admin+)
func (h *Handler) HandleDeletePrismAPIKey(w http.ResponseWriter, r *http.Request) {
	prismID := chi.URLParam(r, "id")
	keyID := chi.URLParam(r, "keyId")

	if prismID == "" || keyID == "" {
		h.sendError(w, http.StatusBadRequest, "Prism ID and Key ID are required")
		return
	}

	user := h.getCurrentUser(r)
	if user == nil {
		h.sendError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	role := h.resolvePrismRole(r, prismID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	if err := h.storage.DeletePrismAPIKey(r.Context(), keyID, prismID); err != nil {
		log.Printf("[APIKeys] Failed to delete prism API key %s: %v", keyID, err)
		h.sendError(w, http.StatusBadRequest, "Failed to delete API key")
		return
	}

	h.sendSuccess(w, "API key deleted successfully", nil)
}

// HandleTogglePrismAPIKey toggles a prism-scoped API key (prism admin+)
func (h *Handler) HandleTogglePrismAPIKey(w http.ResponseWriter, r *http.Request) {
	prismID := chi.URLParam(r, "id")
	keyID := chi.URLParam(r, "keyId")

	if prismID == "" || keyID == "" {
		h.sendError(w, http.StatusBadRequest, "Prism ID and Key ID are required")
		return
	}

	user := h.getCurrentUser(r)
	if user == nil {
		h.sendError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	role := h.resolvePrismRole(r, prismID)
	if !rbac.HasAccess(user, role, rbac.RoleAdmin) {
		h.sendError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	apiKey, err := h.storage.TogglePrismAPIKey(r.Context(), keyID, prismID)
	if errors.Is(err, ErrKeyNotFound) {
		h.sendError(w, http.StatusNotFound, "API key not found")
		return
	}
	if err != nil {
		log.Printf("[APIKeys] Failed to toggle prism API key %s: %v", keyID, err)
		h.sendError(w, http.StatusBadRequest, "Failed to toggle API key")
		return
	}

	action := "deactivated"
	if apiKey.IsActive {
		action = "activated"
	}

	h.sendSuccess(w, fmt.Sprintf("API key %s successfully", action), map[string]interface{}{
		"api_key": apiKey,
	})
}

// ---- Instance-wide handlers ----
//
// An instance-wide key holds no fractal and no prism, so it is managed here
// rather than under a scope. The routes are tenant-admin only, which the router
// enforces before the handler runs.

// HandleCreateTenantAPIKey issues an instance-wide API key.
func (h *Handler) HandleCreateTenantAPIKey(w http.ResponseWriter, r *http.Request) {
	var req CreateAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Name == "" {
		h.sendError(w, http.StatusBadRequest, "API key name is required")
		return
	}

	// The grant is what this route issues, so it is set here rather than trusted
	// from the body; a scope role would contradict it.
	req.TenantAdmin = true
	req.Role = ""
	if err := req.Validate(); err != nil {
		h.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	fullKey, keyID, err := h.storage.GenerateAPIKey(r.Context(), TenantKeyPrefix)
	if err != nil {
		log.Printf("[APIKeys] Failed to generate instance-wide API key: %v", err)
		h.sendError(w, http.StatusInternalServerError, "Failed to generate API key")
		return
	}

	apiKey, err := h.storage.CreateTenantAPIKey(r.Context(), req, auth.AttributionUsername(r.Context()), fullKey, keyID)
	if err != nil {
		log.Printf("[APIKeys] Failed to create instance-wide API key: %v", err)
		h.sendError(w, http.StatusInternalServerError, "Failed to create API key")
		return
	}

	h.sendSuccess(w, "API key created successfully", CreateAPIKeyResponse{
		Key:    fullKey,
		KeyID:  keyID,
		APIKey: *apiKey,
	})
}

// HandleGetTenantAPIKey reads one instance-wide API key.
func (h *Handler) HandleGetTenantAPIKey(w http.ResponseWriter, r *http.Request) {
	keyID := chi.URLParam(r, "keyId")
	if keyID == "" {
		h.sendError(w, http.StatusBadRequest, "Key ID is required")
		return
	}

	apiKey, err := h.storage.GetTenantAPIKey(r.Context(), keyID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "API key not found")
		return
	}

	h.sendSuccess(w, "API key retrieved successfully", apiKey)
}

// HandleUpdateTenantAPIKey updates an instance-wide API key.
func (h *Handler) HandleUpdateTenantAPIKey(w http.ResponseWriter, r *http.Request) {
	keyID := chi.URLParam(r, "keyId")
	if keyID == "" {
		h.sendError(w, http.StatusBadRequest, "Key ID is required")
		return
	}

	var req UpdateAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	current, err := h.storage.GetTenantAPIKey(r.Context(), keyID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "API key not found")
		return
	}
	if err := req.Validate(current); err != nil {
		h.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	apiKey, err := h.storage.UpdateTenantAPIKey(r.Context(), keyID, req)
	if err != nil {
		log.Printf("[APIKeys] Failed to update instance-wide API key %s: %v", keyID, err)
		h.sendError(w, http.StatusInternalServerError, "Failed to update API key")
		return
	}

	h.sendSuccess(w, "API key updated successfully", apiKey)
}

// HandleDeleteTenantAPIKey removes an instance-wide API key.
func (h *Handler) HandleDeleteTenantAPIKey(w http.ResponseWriter, r *http.Request) {
	keyID := chi.URLParam(r, "keyId")
	if keyID == "" {
		h.sendError(w, http.StatusBadRequest, "Key ID is required")
		return
	}

	if err := h.storage.DeleteTenantAPIKey(r.Context(), keyID); err != nil {
		log.Printf("[APIKeys] Failed to delete instance-wide API key %s: %v", keyID, err)
		h.sendError(w, http.StatusNotFound, "API key not found")
		return
	}

	h.sendSuccess(w, "API key deleted successfully", nil)
}

// HandleToggleTenantAPIKey activates or deactivates an instance-wide API key.
func (h *Handler) HandleToggleTenantAPIKey(w http.ResponseWriter, r *http.Request) {
	keyID := chi.URLParam(r, "keyId")
	if keyID == "" {
		h.sendError(w, http.StatusBadRequest, "Key ID is required")
		return
	}

	apiKey, err := h.storage.ToggleTenantAPIKey(r.Context(), keyID)
	if errors.Is(err, ErrKeyNotFound) {
		h.sendError(w, http.StatusNotFound, "API key not found")
		return
	}
	if err != nil {
		log.Printf("[APIKeys] Failed to toggle instance-wide API key %s: %v", keyID, err)
		h.sendError(w, http.StatusBadRequest, "Failed to toggle API key")
		return
	}

	action := "deactivated"
	if apiKey.IsActive {
		action = "activated"
	}

	h.sendSuccess(w, fmt.Sprintf("API key %s successfully", action), map[string]interface{}{
		"api_key": apiKey,
	})
}

// ---- Helpers ----

// getCurrentUser extracts the current user from the request context
func (h *Handler) getCurrentUser(r *http.Request) *storage.User {
	if user := r.Context().Value("user"); user != nil {
		if userObj, ok := user.(*storage.User); ok {
			return userObj
		}
	}
	return nil
}

// resolveFractalName looks up the fractal name by ID. Used for key generation.
func (h *Handler) resolveFractalName(ctx context.Context, fractalID string) (string, error) {
	return h.storage.GetFractalName(ctx, fractalID)
}

// sendSuccess sends a successful JSON response
func (h *Handler) sendSuccess(w http.ResponseWriter, message string, data interface{}) {
	api.WriteMessage(w, message, data)
}

// sendError sends an error JSON response
func (h *Handler) sendError(w http.ResponseWriter, statusCode int, message string) {
	api.WriteError(w, statusCode, message)
}
