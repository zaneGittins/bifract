package apikeys

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"bifract/pkg/fractals"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"
)

// Handler provides HTTP endpoints for API key management
type Handler struct {
	storage        *Storage
	fractalManager *fractals.Manager
	rbacResolver   *rbac.Resolver
}

// SetRBAC injects the RBAC resolver for permission checks.
func (h *Handler) SetRBAC(resolver *rbac.Resolver) {
	h.rbacResolver = resolver
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

// NewHandler creates a new API key handler
func NewHandler(pg *storage.PostgresClient, fractalManager *fractals.Manager) *Handler {
	return &Handler{
		storage:        NewStorage(pg),
		fractalManager: fractalManager,
	}
}

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

	// Verify fractal exists
	if _, err := h.fractalManager.GetFractal(r.Context(), fractalID); err != nil {
		h.sendError(w, http.StatusNotFound, "Fractal not found")
		return
	}

	// List API keys for this fractal
	keys, err := h.storage.ListAPIKeys(r.Context(), fractalID)
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

	// Verify fractal exists and get its details
	fractal, err := h.fractalManager.GetFractal(r.Context(), fractalID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, "Fractal not found")
		return
	}

	// Parse request body
	var req CreateAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Validate request
	if req.Name == "" {
		h.sendError(w, http.StatusBadRequest, "API key name is required")
		return
	}

	// Validate permissions upfront so we return 400 for bad input
	if req.Permissions != nil {
		if _, err := ValidatePermissions(req.Permissions); err != nil {
			h.sendError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	// Generate API key
	fullKey, keyID, err := h.storage.GenerateAPIKey(r.Context(), fractal.Name)
	if err != nil {
		log.Printf("[APIKeys] Failed to generate API key: %v", err)
		h.sendError(w, http.StatusInternalServerError, "Failed to generate API key")
		return
	}

	// Create in database
	apiKey, err := h.storage.CreateAPIKey(r.Context(), req, fractalID, user.Username, fullKey, keyID)
	if err != nil {
		log.Printf("[APIKeys] Failed to create API key: %v", err)
		h.sendError(w, http.StatusInternalServerError, "Failed to create API key")
		return
	}

	// Add fractal name to response
	apiKey.FractalName = fractal.Name

	// Create response with full key (only shown once)
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

	// Get API key
	apiKey, err := h.storage.GetAPIKey(r.Context(), keyID, fractalID)
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

	// Parse request body
	var req UpdateAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Validate permissions if provided
	if req.Permissions != nil {
		if _, err := ValidatePermissions(req.Permissions); err != nil {
			h.sendError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	// Update API key
	apiKey, err := h.storage.UpdateAPIKey(r.Context(), keyID, fractalID, req)
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

	// Delete API key
	if err := h.storage.DeleteAPIKey(r.Context(), keyID, fractalID); err != nil {
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

	// Toggle API key
	apiKey, err := h.storage.ToggleAPIKey(r.Context(), keyID, fractalID)
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

// HandleValidateAPIKey validates an API key (internal endpoint, not exposed via routes)
func (h *Handler) HandleValidateAPIKey(ctx context.Context, apiKey string) (*ValidatedAPIKey, error) {
	return h.storage.ValidateAPIKey(ctx, apiKey)
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