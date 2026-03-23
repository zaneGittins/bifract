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
}

type SavedQuery struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	QueryText string    `json:"query_text"`
	Tags      []string  `json:"tags"`
	FractalID string    `json:"fractal_id"`
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

// verifyFractalAccess checks the user has access to the given fractal.
// Returns true if access is allowed.
func (h *Handler) verifyFractalAccess(w http.ResponseWriter, r *http.Request, fractalID string) bool {
	if h.rbacResolver == nil {
		return true
	}
	user, _ := r.Context().Value("user").(*storage.User)
	if user == nil {
		h.respondError(w, http.StatusUnauthorized, "authentication required")
		return false
	}
	if !h.rbacResolver.HasFractalAccess(r.Context(), user, fractalID) {
		h.respondError(w, http.StatusForbidden, "access denied to this fractal")
		return false
	}
	return true
}

func (h *Handler) HandleList(w http.ResponseWriter, r *http.Request) {
	// Prefer explicit fractal_id from query param to avoid session race conditions
	fractalID := strings.TrimSpace(r.URL.Query().Get("fractal_id"))
	if fractalID == "" {
		var err error
		fractalID, err = h.getSelectedFractal(r)
		if err != nil {
			log.Printf("[SavedQueries] Failed to get fractal: %v", err)
			h.respondError(w, http.StatusBadRequest, "Failed to determine fractal context")
			return
		}
	}

	if !h.verifyFractalAccess(w, r, fractalID) {
		return
	}

	search := strings.TrimSpace(r.URL.Query().Get("search"))
	tag := strings.TrimSpace(r.URL.Query().Get("tag"))

	query := `SELECT id, name, query_text, tags, fractal_id, created_by, created_at, updated_at
		FROM saved_queries WHERE fractal_id = $1`
	args := []interface{}{fractalID}
	argIdx := 2

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
			&sq.FractalID, &sq.CreatedBy, &sq.CreatedAt, &sq.UpdatedAt); err != nil {
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

	// Prefer explicit fractal_id from request body to avoid session race conditions
	fractalID := strings.TrimSpace(req.FractalID)
	if fractalID == "" {
		var err error
		fractalID, err = h.getSelectedFractal(r)
		if err != nil {
			log.Printf("[SavedQueries] Failed to get fractal: %v", err)
			h.respondError(w, http.StatusBadRequest, "Failed to determine fractal context")
			return
		}
	}

	if !h.verifyFractalAccess(w, r, fractalID) {
		return
	}

	// Clean tags
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

	var sq SavedQuery
	err := h.pg.QueryRow(r.Context(), `
		INSERT INTO saved_queries (name, query_text, tags, fractal_id, created_by)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, name, query_text, tags, fractal_id, created_by, created_at, updated_at`,
		req.Name, req.QueryText, pq.Array(cleanTags), fractalID, username,
	).Scan(&sq.ID, &sq.Name, &sq.QueryText, pq.Array(&sq.Tags),
		&sq.FractalID, &sq.CreatedBy, &sq.CreatedAt, &sq.UpdatedAt)

	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "unique constraint") {
			h.respondError(w, http.StatusConflict, "a saved query with this name already exists in this fractal")
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
	username := h.getCurrentUser(r)
	if username == "" {
		h.respondError(w, http.StatusUnauthorized, "authentication required")
		return
	}

	id := chi.URLParam(r, "id")
	fractalID, err := h.getSelectedFractal(r)
	if err != nil {
		log.Printf("[SavedQueries] Failed to get fractal: %v", err)
		h.respondError(w, http.StatusBadRequest, "Failed to determine fractal context")
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

	var sq SavedQuery
	err = h.pg.QueryRow(r.Context(), `
		UPDATE saved_queries SET name = $1, query_text = $2, tags = $3
		WHERE id = $4 AND fractal_id = $5 AND created_by = $6
		RETURNING id, name, query_text, tags, fractal_id, created_by, created_at, updated_at`,
		req.Name, req.QueryText, pq.Array(cleanTags), id, fractalID, username,
	).Scan(&sq.ID, &sq.Name, &sq.QueryText, pq.Array(&sq.Tags),
		&sq.FractalID, &sq.CreatedBy, &sq.CreatedAt, &sq.UpdatedAt)

	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			h.respondError(w, http.StatusNotFound, "saved query not found")
			return
		}
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "unique constraint") {
			h.respondError(w, http.StatusConflict, "a saved query with this name already exists in this fractal")
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
	username := h.getCurrentUser(r)
	if username == "" {
		h.respondError(w, http.StatusUnauthorized, "authentication required")
		return
	}

	id := chi.URLParam(r, "id")
	fractalID, err := h.getSelectedFractal(r)
	if err != nil {
		log.Printf("[SavedQueries] Failed to get fractal: %v", err)
		h.respondError(w, http.StatusBadRequest, "Failed to determine fractal context")
		return
	}

	result, err := h.pg.Exec(r.Context(),
		"DELETE FROM saved_queries WHERE id = $1 AND fractal_id = $2 AND created_by = $3", id, fractalID, username)
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

func (h *Handler) getSelectedFractal(r *http.Request) (string, error) {
	if fid, ok := r.Context().Value("selected_fractal").(string); ok && fid != "" {
		return fid, nil
	}
	if h.fractalManager == nil {
		return "", fmt.Errorf("no fractal context available")
	}
	defaultFractal, err := h.fractalManager.GetDefaultFractal(r.Context())
	if err != nil {
		return "", fmt.Errorf("failed to get default fractal: %w", err)
	}
	return defaultFractal.ID, nil
}

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
