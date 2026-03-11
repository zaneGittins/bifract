package ingesttokens

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"bifract/pkg/fractals"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"
)

type Handler struct {
	storage        *Storage
	fractalManager *fractals.Manager
	cache          *TokenCache
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

func NewHandler(s *Storage, fractalManager *fractals.Manager, cache *TokenCache) *Handler {
	return &Handler{
		storage:        s,
		fractalManager: fractalManager,
		cache:          cache,
	}
}

func (h *Handler) HandleListTokens(w http.ResponseWriter, r *http.Request) {
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

	if _, err := h.fractalManager.GetFractal(r.Context(), fractalID); err != nil {
		h.sendError(w, http.StatusNotFound, "Fractal not found")
		return
	}

	tokens, err := h.storage.ListTokens(r.Context(), fractalID)
	if err != nil {
		log.Printf("[IngestTokens] Failed to list tokens: %v", err)
		h.sendError(w, http.StatusInternalServerError, "Failed to list ingest tokens")
		return
	}

	h.sendSuccess(w, "Ingest tokens retrieved successfully", map[string]interface{}{
		"ingest_tokens": tokens,
		"total":         len(tokens),
	})
}

func (h *Handler) HandleCreateToken(w http.ResponseWriter, r *http.Request) {
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

	if _, err := h.fractalManager.GetFractal(r.Context(), fractalID); err != nil {
		h.sendError(w, http.StatusNotFound, "Fractal not found")
		return
	}

	var req CreateTokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Name == "" {
		h.sendError(w, http.StatusBadRequest, "Token name is required")
		return
	}

	token, fullToken, err := h.storage.CreateToken(r.Context(), req, fractalID, user.Username)
	if err != nil {
		log.Printf("[IngestTokens] Failed to create token: %v", err)
		h.sendError(w, http.StatusInternalServerError, "Failed to create ingest token")
		return
	}

	h.sendSuccess(w, "Ingest token created successfully", CreateTokenResponse{
		Token:       fullToken,
		TokenPrefix: token.TokenPrefix,
		IngestToken: *token,
	})
}

func (h *Handler) HandleGetToken(w http.ResponseWriter, r *http.Request) {
	fractalID := chi.URLParam(r, "id")
	tokenID := chi.URLParam(r, "tokenId")

	if fractalID == "" || tokenID == "" {
		h.sendError(w, http.StatusBadRequest, "Fractal ID and Token ID are required")
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

	token, err := h.storage.GetToken(r.Context(), tokenID, fractalID)
	if err != nil {
		h.sendError(w, http.StatusNotFound, fmt.Sprintf("Ingest token not found: %v", err))
		return
	}

	h.sendSuccess(w, "Ingest token retrieved successfully", map[string]interface{}{
		"ingest_token": token,
	})
}

func (h *Handler) HandleUpdateToken(w http.ResponseWriter, r *http.Request) {
	fractalID := chi.URLParam(r, "id")
	tokenID := chi.URLParam(r, "tokenId")

	if fractalID == "" || tokenID == "" {
		h.sendError(w, http.StatusBadRequest, "Fractal ID and Token ID are required")
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

	var req UpdateTokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	token, err := h.storage.UpdateToken(r.Context(), tokenID, fractalID, req)
	if err != nil {
		log.Printf("[IngestTokens] Failed to update token: %v", err)
		h.sendError(w, http.StatusBadRequest, "Failed to update ingest token")
		return
	}

	if h.cache != nil {
		h.cache.InvalidateAll()
	}

	h.sendSuccess(w, "Ingest token updated successfully", map[string]interface{}{
		"ingest_token": token,
	})
}

func (h *Handler) HandleDeleteToken(w http.ResponseWriter, r *http.Request) {
	fractalID := chi.URLParam(r, "id")
	tokenID := chi.URLParam(r, "tokenId")

	if fractalID == "" || tokenID == "" {
		h.sendError(w, http.StatusBadRequest, "Fractal ID and Token ID are required")
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

	if err := h.storage.DeleteToken(r.Context(), tokenID, fractalID); err != nil {
		log.Printf("[IngestTokens] Failed to delete token: %v", err)
		h.sendError(w, http.StatusBadRequest, "Failed to delete ingest token")
		return
	}

	if h.cache != nil {
		h.cache.InvalidateAll()
	}

	h.sendSuccess(w, "Ingest token deleted successfully", nil)
}

func (h *Handler) HandleToggleToken(w http.ResponseWriter, r *http.Request) {
	fractalID := chi.URLParam(r, "id")
	tokenID := chi.URLParam(r, "tokenId")

	if fractalID == "" || tokenID == "" {
		h.sendError(w, http.StatusBadRequest, "Fractal ID and Token ID are required")
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

	token, err := h.storage.ToggleToken(r.Context(), tokenID, fractalID)
	if err != nil {
		log.Printf("[IngestTokens] Failed to toggle token: %v", err)
		h.sendError(w, http.StatusBadRequest, "Failed to toggle ingest token")
		return
	}

	if h.cache != nil {
		h.cache.InvalidateAll()
	}

	action := "deactivated"
	if token.IsActive {
		action = "activated"
	}

	h.sendSuccess(w, fmt.Sprintf("Ingest token %s successfully", action), map[string]interface{}{
		"ingest_token": token,
	})
}

func (h *Handler) getCurrentUser(r *http.Request) *storage.User {
	if user := r.Context().Value("user"); user != nil {
		if userObj, ok := user.(*storage.User); ok {
			return userObj
		}
	}
	return nil
}

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

func (h *Handler) sendError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"error":   message,
	})
}
