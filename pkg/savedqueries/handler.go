package savedqueries

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/lib/pq"

	"bifract/pkg/bqlvars"
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
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	QueryText   string          `json:"query_text"`
	Description string          `json:"description"`
	Tags        []string        `json:"tags"`
	Variables   json.RawMessage `json:"variables"`
	Visibility  string          `json:"visibility"`
	Favorited   bool            `json:"favorited"`
	UseCount    int64           `json:"use_count"`
	LastUsedAt  *time.Time      `json:"last_used_at,omitempty"`
	FractalID   string          `json:"fractal_id,omitempty"`
	PrismID     string          `json:"prism_id,omitempty"`
	CreatedBy   string          `json:"created_by"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// normalizeVariables validates and canonicalizes the variable bindings for
// storage: it drops malformed/empty-named entries and always returns a JSON
// array string (never null), so an empty set persists as "[]".
func normalizeVariables(raw json.RawMessage) string {
	parsed := bqlvars.ParseVariables(raw)
	clean := make([]bqlvars.Variable, 0, len(parsed))
	for _, v := range parsed {
		name := strings.TrimSpace(v.Name)
		if name == "" {
			continue
		}
		clean = append(clean, bqlvars.Variable{Name: name, Value: v.Value})
	}
	b, err := json.Marshal(clean)
	if err != nil {
		return "[]"
	}
	return string(b)
}

// normalizeVisibility constrains visibility to the supported values, defaulting
// to "shared" (the historical behavior where saved queries are fractal-wide).
func normalizeVisibility(v string) string {
	if strings.ToLower(strings.TrimSpace(v)) == "personal" {
		return "personal"
	}
	return "shared"
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
	// Scope is ALWAYS session - never trust request params. Accepting
	// fractal_id/prism_id in the URL let a caller enumerate saved queries
	// in any scope whose ID they guessed, bypassing the session boundary.
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[SavedQueries] Failed to get scope: %v", err)
		h.respondError(w, http.StatusBadRequest, "Failed to determine context")
		return
	}

	if !h.verifyAccess(w, r, fractalID, prismID) {
		return
	}

	username := h.getCurrentUser(r)
	search := strings.TrimSpace(r.URL.Query().Get("search"))
	tag := strings.TrimSpace(r.URL.Query().Get("tag"))

	// $1 = username (favorites join + personal visibility), $2 = scope id.
	var scopeCol string
	if prismID != "" {
		scopeCol = "sq.prism_id"
	} else {
		scopeCol = "sq.fractal_id"
	}
	var scopeArg interface{}
	if prismID != "" {
		scopeArg = prismID
	} else {
		scopeArg = fractalID
	}

	query := fmt.Sprintf(`
		SELECT sq.id, sq.name, sq.query_text, COALESCE(sq.description, ''), sq.tags,
			COALESCE(sq.variables, '[]'),
			COALESCE(sq.visibility, 'shared'), (f.username IS NOT NULL) AS favorited,
			COALESCE(sq.use_count, 0), sq.last_used_at,
			COALESCE(sq.fractal_id::text, ''), COALESCE(sq.prism_id::text, ''),
			COALESCE(sq.created_by, ''), sq.created_at, sq.updated_at
		FROM saved_queries sq
		LEFT JOIN saved_query_favorites f ON f.saved_query_id = sq.id AND f.username = $1
		WHERE %s = $2
		  AND (COALESCE(sq.visibility, 'shared') = 'shared' OR sq.created_by = $1)`, scopeCol)
	args := []interface{}{username, scopeArg}
	argIdx := 3

	if search != "" {
		query += fmt.Sprintf(" AND (sq.name ILIKE '%%' || $%d || '%%' OR sq.query_text ILIKE '%%' || $%d || '%%')", argIdx, argIdx)
		args = append(args, search)
		argIdx++
	}
	if tag != "" {
		query += fmt.Sprintf(" AND $%d = ANY(sq.tags)", argIdx)
		args = append(args, tag)
		argIdx++
	}
	// Favorites first, then most recently used, then alphabetical.
	query += " ORDER BY favorited DESC, sq.last_used_at DESC NULLS LAST, sq.name ASC"

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
		var lastUsed sql.NullTime
		if err := rows.Scan(&sq.ID, &sq.Name, &sq.QueryText, &sq.Description, pq.Array(&sq.Tags),
			&sq.Variables,
			&sq.Visibility, &sq.Favorited, &sq.UseCount, &lastUsed,
			&sq.FractalID, &sq.PrismID, &sq.CreatedBy, &sq.CreatedAt, &sq.UpdatedAt); err != nil {
			log.Printf("[SavedQueries] Failed to scan row: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to load saved queries")
			return
		}
		if lastUsed.Valid {
			t := lastUsed.Time.UTC()
			sq.LastUsedAt = &t
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
		Name        string          `json:"name"`
		QueryText   string          `json:"query_text"`
		Description string          `json:"description"`
		Tags        []string        `json:"tags"`
		Variables   json.RawMessage `json:"variables"`
		Visibility  string          `json:"visibility"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	req.Name = strings.TrimSpace(req.Name)
	req.QueryText = strings.TrimSpace(req.QueryText)
	req.Description = strings.TrimSpace(req.Description)
	visibility := normalizeVisibility(req.Visibility)
	variables := normalizeVariables(req.Variables)

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

	// Scope comes from the session exclusively. Accepting it in the request
	// body let callers create saved queries in whatever scope's UUID they
	// knew, bypassing the session boundary.
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[SavedQueries] Failed to get scope: %v", err)
		h.respondError(w, http.StatusBadRequest, "Failed to determine context")
		return
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
	err = h.pg.QueryRow(r.Context(), `
		INSERT INTO saved_queries (name, query_text, description, tags, variables, visibility, fractal_id, prism_id, created_by)
		VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9)
		RETURNING id, name, query_text, COALESCE(description, ''), tags, COALESCE(variables, '[]'), COALESCE(visibility, 'shared'), COALESCE(use_count, 0), COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, ''), COALESCE(created_by, ''), created_at, updated_at`,
		req.Name, req.QueryText, req.Description, pq.Array(cleanTags), variables, visibility, fractalIDPtr, prismIDPtr, username,
	).Scan(&sq.ID, &sq.Name, &sq.QueryText, &sq.Description, pq.Array(&sq.Tags), &sq.Variables, &sq.Visibility, &sq.UseCount,
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
		Name        string          `json:"name"`
		QueryText   string          `json:"query_text"`
		Description string          `json:"description"`
		Tags        []string        `json:"tags"`
		Variables   json.RawMessage `json:"variables"`
		Visibility  string          `json:"visibility"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	req.Name = strings.TrimSpace(req.Name)
	req.QueryText = strings.TrimSpace(req.QueryText)
	req.Description = strings.TrimSpace(req.Description)
	visibility := normalizeVisibility(req.Visibility)
	variables := normalizeVariables(req.Variables)

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

	// Build WHERE clause based on scope.
	var whereScope string
	var scopeArg interface{}
	if prismID != "" {
		whereScope = "prism_id = $6"
		scopeArg = prismID
	} else {
		whereScope = "fractal_id = $6"
		scopeArg = fractalID
	}

	username := h.getCurrentUser(r)

	var sq SavedQuery
	var lastUsed sql.NullTime
	err = h.pg.QueryRow(r.Context(), fmt.Sprintf(`
		UPDATE saved_queries SET name = $1, query_text = $2, description = $3, tags = $4, visibility = $5, variables = $9::jsonb
		WHERE id = $7 AND %s
		RETURNING id, name, query_text, COALESCE(description, ''), tags, COALESCE(variables, '[]'), COALESCE(visibility, 'shared'),
			COALESCE(use_count, 0), last_used_at,
			EXISTS (SELECT 1 FROM saved_query_favorites f WHERE f.saved_query_id = saved_queries.id AND f.username = $8),
			COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, ''), COALESCE(created_by, ''), created_at, updated_at`, whereScope),
		req.Name, req.QueryText, req.Description, pq.Array(cleanTags), visibility, scopeArg, id, username, variables,
	).Scan(&sq.ID, &sq.Name, &sq.QueryText, &sq.Description, pq.Array(&sq.Tags), &sq.Variables, &sq.Visibility,
		&sq.UseCount, &lastUsed, &sq.Favorited,
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
	if lastUsed.Valid {
		t := lastUsed.Time.UTC()
		sq.LastUsedAt = &t
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

// scopePredicate returns the scope column and arg for the active session scope.
func scopePredicate(fractalID, prismID string) (col string, arg interface{}) {
	if prismID != "" {
		return "prism_id", prismID
	}
	return "fractal_id", fractalID
}

// HandleMarkUsed records that a saved query was run: bumps use_count and
// last_used_at so popular team queries float to the top.
func (h *Handler) HandleMarkUsed(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "Failed to determine context")
		return
	}
	if !h.verifyAccess(w, r, fractalID, prismID) {
		return
	}
	col, arg := scopePredicate(fractalID, prismID)
	_, err = h.pg.Exec(r.Context(),
		fmt.Sprintf("UPDATE saved_queries SET use_count = COALESCE(use_count, 0) + 1, last_used_at = NOW() WHERE id = $1 AND %s = $2", col),
		id, arg)
	if err != nil {
		log.Printf("[SavedQueries] Failed to mark used: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to record usage")
		return
	}
	h.respondSuccess(w, map[string]bool{"ok": true})
}

// HandleFavorite pins a saved query for the current user (per-user).
func (h *Handler) HandleFavorite(w http.ResponseWriter, r *http.Request) {
	username := h.getCurrentUser(r)
	if username == "" {
		h.respondError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	id := chi.URLParam(r, "id")
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "Failed to determine context")
		return
	}
	if !h.verifyAccess(w, r, fractalID, prismID) {
		return
	}

	// Only allow favoriting a query that exists in this scope and is visible to
	// the user, so a guessed id in another scope cannot be pinned.
	col, arg := scopePredicate(fractalID, prismID)
	var exists bool
	err = h.pg.QueryRow(r.Context(),
		fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM saved_queries WHERE id = $1 AND %s = $2 AND (COALESCE(visibility, 'shared') = 'shared' OR created_by = $3))", col),
		id, arg, username).Scan(&exists)
	if err != nil {
		log.Printf("[SavedQueries] Failed to check favorite target: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to favorite query")
		return
	}
	if !exists {
		h.respondError(w, http.StatusNotFound, "saved query not found")
		return
	}

	if _, err := h.pg.Exec(r.Context(),
		"INSERT INTO saved_query_favorites (username, saved_query_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
		username, id); err != nil {
		log.Printf("[SavedQueries] Failed to favorite: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to favorite query")
		return
	}
	h.respondSuccess(w, map[string]bool{"favorited": true})
}

// HandleUnfavorite removes the current user's pin on a saved query.
func (h *Handler) HandleUnfavorite(w http.ResponseWriter, r *http.Request) {
	username := h.getCurrentUser(r)
	if username == "" {
		h.respondError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	id := chi.URLParam(r, "id")
	if _, err := h.pg.Exec(r.Context(),
		"DELETE FROM saved_query_favorites WHERE username = $1 AND saved_query_id = $2",
		username, id); err != nil {
		log.Printf("[SavedQueries] Failed to unfavorite: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to unfavorite query")
		return
	}
	h.respondSuccess(w, map[string]bool{"favorited": false})
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
