package savedqueries

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/lib/pq"

	"bifract/pkg/fractals"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"
)

// RBACResolver checks whether a user has any access to a fractal.
type RBACResolver interface {
	HasFractalAccess(ctx context.Context, user *storage.User, fractalID string) bool
}

type Handler struct {
	pg             *storage.PostgresClient
	fractalManager *fractals.Manager
	rbacResolver   RBACResolver
	rbacFull       *rbac.Resolver
}

type SavedQuery struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	QueryText string    `json:"query_text"`
	Tags      []string  `json:"tags"`
	FractalID string    `json:"fractal_id,omitempty"`
	PrismID   string    `json:"prism_id,omitempty"`
	CreatedBy string    `json:"created_by"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

func NewHandler(pg *storage.PostgresClient, fractalManager *fractals.Manager) *Handler {
	return &Handler{pg: pg, fractalManager: fractalManager}
}

// SetRBACResolver sets the RBAC resolver (for deferred initialization).
func (h *Handler) SetRBACResolver(resolver RBACResolver) {
	h.rbacResolver = resolver
}

// SetRBACFull sets the full RBAC resolver for prism access checks.
func (h *Handler) SetRBACFull(resolver *rbac.Resolver) {
	h.rbacFull = resolver
}

// verifyAccess checks the user has access to the given fractal or prism scope.
func (h *Handler) verifyAccess(w http.ResponseWriter, r *http.Request, fractalID, prismID string) bool {
	user, _ := r.Context().Value("user").(*storage.User)
	if user == nil {
		h.respondError(w, http.StatusUnauthorized, "authentication required")
		return false
	}
	if user.IsAdmin {
		return true
	}
	if fractalID != "" && h.rbacResolver != nil {
		if !h.rbacResolver.HasFractalAccess(r.Context(), user, fractalID) {
			h.respondError(w, http.StatusForbidden, "access denied")
			return false
		}
		return true
	}
	if prismID != "" && h.rbacFull != nil {
		role := h.rbacFull.ResolvePrismRoleWithAdmin(r.Context(), user, prismID)
		if !rbac.HasAccess(user, role, rbac.RoleViewer) {
			h.respondError(w, http.StatusForbidden, "access denied")
			return false
		}
		return true
	}
	return true
}

// getScope returns the fractalID and prismID from context. Exactly one will be non-empty.
func (h *Handler) getScope(r *http.Request) (fractalID, prismID string, err error) {
	if pid, _ := r.Context().Value("selected_prism").(string); pid != "" {
		return "", pid, nil
	}
	if fid, _ := r.Context().Value("selected_fractal").(string); fid != "" {
		return fid, "", nil
	}
	if h.fractalManager != nil {
		df, err := h.fractalManager.GetDefaultFractal(r.Context())
		if err != nil {
			return "", "", fmt.Errorf("failed to get default fractal: %w", err)
		}
		return df.ID, "", nil
	}
	return "", "", fmt.Errorf("no fractal or prism context available")
}

func (h *Handler) HandleList(w http.ResponseWriter, r *http.Request) {
	// Prefer explicit fractal_id/prism_id from query param
	fractalID := strings.TrimSpace(r.URL.Query().Get("fractal_id"))
	prismID := strings.TrimSpace(r.URL.Query().Get("prism_id"))
	if fractalID == "" && prismID == "" {
		var err error
		fractalID, prismID, err = h.getScope(r)
		if err != nil {
			log.Printf("[SavedQueries] Failed to get scope: %v", err)
			h.respondError(w, http.StatusBadRequest, "Failed to determine context")
			return
		}
	}

	if !h.verifyAccess(w, r, fractalID, prismID) {
		return
	}

	search := strings.TrimSpace(r.URL.Query().Get("search"))
	tag := strings.TrimSpace(r.URL.Query().Get("tag"))

	var query string
	var args []interface{}
	argIdx := 1

	if prismID != "" {
		query = fmt.Sprintf(`SELECT id, name, query_text, tags, COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, ''), COALESCE(created_by, ''), created_at, updated_at
			FROM saved_queries WHERE prism_id = $%d`, argIdx)
		args = append(args, prismID)
	} else {
		query = fmt.Sprintf(`SELECT id, name, query_text, tags, COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, ''), COALESCE(created_by, ''), created_at, updated_at
			FROM saved_queries WHERE fractal_id = $%d`, argIdx)
		args = append(args, fractalID)
	}
	argIdx++

	if search != "" {
		query += fmt.Sprintf(" AND name ILIKE '%%' || $%d || '%%'", argIdx)
		args = append(args, search)
		argIdx++
	}
	if tag != "" {
		query += fmt.Sprintf(" AND $%d = ANY(tags)", argIdx)
		args = append(args, tag)
		argIdx++
	}
	query += " ORDER BY name ASC"

	rows, err := h.pg.Query(r.Context(), query, args...)
	if err != nil {
		log.Printf("[SavedQueries] Failed to list saved queries: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load saved queries")
		return
	}
	defer rows.Close()

	var queries []SavedQuery
	for rows.Next() {
		var sq SavedQuery
		if err := rows.Scan(&sq.ID, &sq.Name, &sq.QueryText, pq.Array(&sq.Tags),
			&sq.FractalID, &sq.PrismID, &sq.CreatedBy, &sq.CreatedAt, &sq.UpdatedAt); err != nil {
			log.Printf("[SavedQueries] Failed to scan row: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to load saved queries")
			return
		}
		if sq.Tags == nil {
			sq.Tags = []string{}
		}
		queries = append(queries, sq)
	}
	if queries == nil {
		queries = []SavedQuery{}
	}

	h.respondSuccess(w, map[string]interface{}{"saved_queries": queries, "count": len(queries)})
}

func (h *Handler) HandleCreate(w http.ResponseWriter, r *http.Request) {
	username := h.getCurrentUser(r)
	if username == "" {
		h.respondError(w, http.StatusUnauthorized, "authentication required")
		return
	}

	var req struct {
		Name      string   `json:"name"`
		QueryText string   `json:"query_text"`
		Tags      []string `json:"tags"`
		FractalID string   `json:"fractal_id"`
		PrismID   string   `json:"prism_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	req.Name = strings.TrimSpace(req.Name)
	req.QueryText = strings.TrimSpace(req.QueryText)

	if req.Name == "" {
		h.respondError(w, http.StatusBadRequest, "name is required")
		return
	}
	if len(req.Name) > 255 {
		h.respondError(w, http.StatusBadRequest, "name must be 255 characters or fewer")
		return
	}
	if req.QueryText == "" {
		h.respondError(w, http.StatusBadRequest, "query_text is required")
		return
	}

	// Prefer explicit IDs from request body, fall back to session context
	fractalID := strings.TrimSpace(req.FractalID)
	prismID := strings.TrimSpace(req.PrismID)
	if fractalID == "" && prismID == "" {
		var err error
		fractalID, prismID, err = h.getScope(r)
		if err != nil {
			log.Printf("[SavedQueries] Failed to get scope: %v", err)
			h.respondError(w, http.StatusBadRequest, "Failed to determine context")
			return
		}
	}

	if !h.verifyAccess(w, r, fractalID, prismID) {
		return
	}

	var cleanTags []string
	for _, t := range req.Tags {
		t = strings.TrimSpace(t)
		if t != "" {
			cleanTags = append(cleanTags, t)
		}
	}
	if cleanTags == nil {
		cleanTags = []string{}
	}

	var fractalIDPtr, prismIDPtr interface{}
	if fractalID != "" {
		fractalIDPtr = fractalID
	}
	if prismID != "" {
		prismIDPtr = prismID
	}

	var sq SavedQuery
	err := h.pg.QueryRow(r.Context(), `
		INSERT INTO saved_queries (name, query_text, tags, fractal_id, prism_id, created_by)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, name, query_text, tags, COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, ''), COALESCE(created_by, ''), created_at, updated_at`,
		req.Name, req.QueryText, pq.Array(cleanTags), fractalIDPtr, prismIDPtr, username,
	).Scan(&sq.ID, &sq.Name, &sq.QueryText, pq.Array(&sq.Tags),
		&sq.FractalID, &sq.PrismID, &sq.CreatedBy, &sq.CreatedAt, &sq.UpdatedAt)

	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "unique constraint") {
			h.respondError(w, http.StatusConflict, "a saved query with this name already exists")
			return
		}
		log.Printf("[SavedQueries] Failed to create saved query: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to save query")
		return
	}
	if sq.Tags == nil {
		sq.Tags = []string{}
	}

	h.respondSuccess(w, map[string]interface{}{"saved_query": sq})
}

func (h *Handler) HandleUpdate(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[SavedQueries] Failed to get scope: %v", err)
		h.respondError(w, http.StatusBadRequest, "Failed to determine context")
		return
	}

	if !h.verifyAccess(w, r, fractalID, prismID) {
		return
	}

	var req struct {
		Name      string   `json:"name"`
		QueryText string   `json:"query_text"`
		Tags      []string `json:"tags"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	req.Name = strings.TrimSpace(req.Name)
	req.QueryText = strings.TrimSpace(req.QueryText)

	if req.Name == "" {
		h.respondError(w, http.StatusBadRequest, "name is required")
		return
	}
	if req.QueryText == "" {
		h.respondError(w, http.StatusBadRequest, "query_text is required")
		return
	}

	var cleanTags []string
	for _, t := range req.Tags {
		t = strings.TrimSpace(t)
		if t != "" {
			cleanTags = append(cleanTags, t)
		}
	}
	if cleanTags == nil {
		cleanTags = []string{}
	}

	// Build WHERE clause based on scope
	var whereScope string
	var scopeArg interface{}
	if prismID != "" {
		whereScope = "prism_id = $5"
		scopeArg = prismID
	} else {
		whereScope = "fractal_id = $5"
		scopeArg = fractalID
	}

	var sq SavedQuery
	err = h.pg.QueryRow(r.Context(), fmt.Sprintf(`
		UPDATE saved_queries SET name = $1, query_text = $2, tags = $3
		WHERE id = $4 AND %s
		RETURNING id, name, query_text, tags, COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, ''), COALESCE(created_by, ''), created_at, updated_at`, whereScope),
		req.Name, req.QueryText, pq.Array(cleanTags), id, scopeArg,
	).Scan(&sq.ID, &sq.Name, &sq.QueryText, pq.Array(&sq.Tags),
		&sq.FractalID, &sq.PrismID, &sq.CreatedBy, &sq.CreatedAt, &sq.UpdatedAt)

	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			h.respondError(w, http.StatusNotFound, "saved query not found")
			return
		}
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "unique constraint") {
			h.respondError(w, http.StatusConflict, "a saved query with this name already exists")
			return
		}
		log.Printf("[SavedQueries] Failed to update saved query: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to update saved query")
		return
	}
	if sq.Tags == nil {
		sq.Tags = []string{}
	}

	h.respondSuccess(w, map[string]interface{}{"saved_query": sq})
}

func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[SavedQueries] Failed to get scope: %v", err)
		h.respondError(w, http.StatusBadRequest, "Failed to determine context")
		return
	}

	if !h.verifyAccess(w, r, fractalID, prismID) {
		return
	}

	var query string
	var scopeArg interface{}
	if prismID != "" {
		query = "DELETE FROM saved_queries WHERE id = $1 AND prism_id = $2"
		scopeArg = prismID
	} else {
		query = "DELETE FROM saved_queries WHERE id = $1 AND fractal_id = $2"
		scopeArg = fractalID
	}

	result, err := h.pg.Exec(r.Context(), query, id, scopeArg)
	if err != nil {
		log.Printf("[SavedQueries] Failed to delete saved query: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to delete saved query")
		return
	}
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		h.respondError(w, http.StatusNotFound, "saved query not found")
		return
	}

	h.respondSuccess(w, map[string]bool{"deleted": true})
}

// -- Helpers --

func (h *Handler) getCurrentUser(r *http.Request) string {
	if user, ok := r.Context().Value("user").(*storage.User); ok {
		return user.Username
	}
	return ""
}

func (h *Handler) respondSuccess(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(APIResponse{Success: true, Data: data})
}

func (h *Handler) respondError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(APIResponse{Success: false, Error: msg})
}
