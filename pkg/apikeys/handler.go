package apikeys

import (
	"context"
	"encoding/json"
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
	storage        *Storage
	rbacResolver   *rbac.Resolver
	prismResolver  PrismResolver
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

	h.sendSuccess(w, "API keys retrieved successfully", map[string]interface{}{
		"api_keys": keys,
		"total":    len(keys),
	})
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

	if req.Permissions != nil {
		if _, err := ValidatePermissions(req.Permissions); err != nil {
			h.sendError(w, http.StatusBadRequest, err.Error())
			return
		}
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

	apiKey, err := h.storage.CreateFractalAPIKey(r.Context(), req, fractalID, user.Username, fullKey, keyID)
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

	h.sendSuccess(w, "API key retrieved successfully", map[string]interface{}{
		"api_key": apiKey,
	})
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

	if req.Permissions != nil {
		if _, err := ValidatePermissions(req.Permissions); err != nil {
			h.sendError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	apiKey, err := h.storage.UpdateFractalAPIKey(r.Context(), keyID, fractalID, req)
	if err != nil {
		log.Printf("[APIKeys] Failed to update API key %s: %v", keyID, err)
		h.sendError(w, http.StatusBadRequest, "Failed to update API key")
		return
	}

	h.sendSuccess(w, "API key updated successfully", map[string]interface{}{
		"api_key": apiKey,
	})
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

	h.sendSuccess(w, "API keys retrieved successfully", map[string]interface{}{
		"api_keys": keys,
		"total":    len(keys),
	})
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

	if req.Permissions != nil {
		if _, err := ValidatePermissions(req.Permissions); err != nil {
			h.sendError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	fullKey, keyID, err := h.storage.GenerateAPIKey(r.Context(), prismName)
	if err != nil {
		log.Printf("[APIKeys] Failed to generate API key: %v", err)
		h.sendError(w, http.StatusInternalServerError, "Failed to generate API key")
		return
	}

	apiKey, err := h.storage.CreatePrismAPIKey(r.Context(), req, prismID, user.Username, fullKey, keyID)
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

	h.sendSuccess(w, "API key retrieved successfully", map[string]interface{}{
		"api_key": apiKey,
	})
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

	if req.Permissions != nil {
		if _, err := ValidatePermissions(req.Permissions); err != nil {
			h.sendError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	apiKey, err := h.storage.UpdatePrismAPIKey(r.Context(), keyID, prismID, req)
	if err != nil {
		log.Printf("[APIKeys] Failed to update prism API key %s: %v", keyID, err)
		h.sendError(w, http.StatusBadRequest, "Failed to update API key")
		return
	}

	h.sendSuccess(w, "API key updated successfully", map[string]interface{}{
		"api_key": apiKey,
	})
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

// HandleValidateAPIKey validates an API key (internal, not exposed via routes)
func (h *Handler) HandleValidateAPIKey(ctx context.Context, apiKey string) (*ValidatedAPIKey, error) {
	return h.storage.ValidateAPIKey(ctx, apiKey)
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
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"success": true,
		"message": message,
	}

	if data != nil {
		response["data"] = data
	}

	json.NewEncoder(w).Encode(response)
}

// sendError sends an error JSON response
func (h *Handler) sendError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"error":   message,
	})
}
