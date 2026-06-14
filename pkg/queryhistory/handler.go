package queryhistory

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

	"bifract/pkg/fractals"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"
)

// historyCap bounds how many distinct queries we retain per user + scope.
// Query history is tiny relative to log data, but we prune so it cannot grow
// unbounded for a power user who runs thousands of distinct queries.
const historyCap = 50

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

type QueryHistory struct {
	ID          string    `json:"id"`
	QueryText   string    `json:"query_text"`
	TimeRange   string    `json:"time_range,omitempty"`
	CustomStart string    `json:"custom_start,omitempty"`
	CustomEnd   string    `json:"custom_end,omitempty"`
	ResultCount *int64    `json:"result_count,omitempty"`
	DurationMs  *int64    `json:"duration_ms,omitempty"`
	Status      string    `json:"status,omitempty"`
	RunCount    int64     `json:"run_count"`
	FirstRunAt  time.Time `json:"first_run_at"`
	LastRunAt   time.Time `json:"last_run_at"`
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

// getScope returns the fractalID and prismID from context. Exactly one will be
// non-empty. Scope is read from the session exclusively, never from request
// params, mirroring saved_queries so callers cannot enumerate other scopes.
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

func (h *Handler) getCurrentUser(r *http.Request) string {
	if user, ok := r.Context().Value("user").(*storage.User); ok {
		return user.Username
	}
	return ""
}

// scopeArgs returns the fractal/prism values as nullable interface{} args for
// SQL, and the COALESCE'd scope id used for list/prune predicates.
func scopeArgs(fractalID, prismID string) (fractalIDPtr, prismIDPtr interface{}) {
	if fractalID != "" {
		fractalIDPtr = fractalID
	}
	if prismID != "" {
		prismIDPtr = prismID
	}
	return
}

func (h *Handler) HandleList(w http.ResponseWriter, r *http.Request) {
	username := h.getCurrentUser(r)
	if username == "" {
		h.respondError(w, http.StatusUnauthorized, "authentication required")
		return
	}

	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[QueryHistory] Failed to get scope: %v", err)
		h.respondError(w, http.StatusBadRequest, "Failed to determine context")
		return
	}
	if !h.verifyAccess(w, r, fractalID, prismID) {
		return
	}

	search := strings.TrimSpace(r.URL.Query().Get("search"))
	fractalIDPtr, prismIDPtr := scopeArgs(fractalID, prismID)

	query := `SELECT id, query_text, COALESCE(time_range, ''), custom_start, custom_end,
			result_count, duration_ms, COALESCE(status, ''), run_count, first_run_at, last_run_at
		FROM query_history
		WHERE username = $1 AND COALESCE(fractal_id, prism_id) = COALESCE($2::uuid, $3::uuid)`
	args := []interface{}{username, fractalIDPtr, prismIDPtr}
	if search != "" {
		query += " AND query_text ILIKE '%' || $4 || '%'"
		args = append(args, search)
	}
	query += fmt.Sprintf(" ORDER BY last_run_at DESC LIMIT %d", historyCap)

	rows, err := h.pg.Query(r.Context(), query, args...)
	if err != nil {
		log.Printf("[QueryHistory] Failed to list: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load query history")
		return
	}
	defer rows.Close()

	history := []QueryHistory{}
	for rows.Next() {
		var (
			qh          QueryHistory
			customStart sql.NullTime
			customEnd   sql.NullTime
			resultCount sql.NullInt64
			durationMs  sql.NullInt64
		)
		if err := rows.Scan(&qh.ID, &qh.QueryText, &qh.TimeRange, &customStart, &customEnd,
			&resultCount, &durationMs, &qh.Status, &qh.RunCount, &qh.FirstRunAt, &qh.LastRunAt); err != nil {
			log.Printf("[QueryHistory] Failed to scan row: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to load query history")
			return
		}
		if customStart.Valid {
			qh.CustomStart = customStart.Time.UTC().Format(time.RFC3339)
		}
		if customEnd.Valid {
			qh.CustomEnd = customEnd.Time.UTC().Format(time.RFC3339)
		}
		if resultCount.Valid {
			qh.ResultCount = &resultCount.Int64
		}
		if durationMs.Valid {
			qh.DurationMs = &durationMs.Int64
		}
		history = append(history, qh)
	}

	h.respondSuccess(w, map[string]interface{}{"history": history, "count": len(history)})
}

func (h *Handler) HandleRecord(w http.ResponseWriter, r *http.Request) {
	username := h.getCurrentUser(r)
	if username == "" {
		h.respondError(w, http.StatusUnauthorized, "authentication required")
		return
	}

	var req struct {
		QueryText   string `json:"query_text"`
		TimeRange   string `json:"time_range"`
		CustomStart string `json:"custom_start"`
		CustomEnd   string `json:"custom_end"`
		ResultCount *int64 `json:"result_count"`
		DurationMs  *int64 `json:"duration_ms"`
		Status      string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	req.QueryText = strings.TrimSpace(req.QueryText)
	// Skip noise: empty queries and bare wildcards are not worth recording.
	if req.QueryText == "" || req.QueryText == "*" {
		h.respondSuccess(w, map[string]bool{"recorded": false})
		return
	}
	if len(req.QueryText) > 8000 {
		h.respondError(w, http.StatusBadRequest, "query is too long to record")
		return
	}

	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[QueryHistory] Failed to get scope: %v", err)
		h.respondError(w, http.StatusBadRequest, "Failed to determine context")
		return
	}
	if !h.verifyAccess(w, r, fractalID, prismID) {
		return
	}

	fractalIDPtr, prismIDPtr := scopeArgs(fractalID, prismID)

	upsert := `
		INSERT INTO query_history
			(username, query_text, time_range, custom_start, custom_end, result_count, duration_ms, status, fractal_id, prism_id)
		VALUES ($1, $2, NULLIF($3,''), $4, $5, $6, $7, NULLIF($8,''), $9, $10)
		ON CONFLICT (username, md5(query_text), COALESCE(fractal_id, prism_id))
		DO UPDATE SET
			run_count    = query_history.run_count + 1,
			last_run_at  = NOW(),
			time_range   = EXCLUDED.time_range,
			custom_start = EXCLUDED.custom_start,
			custom_end   = EXCLUDED.custom_end,
			result_count = EXCLUDED.result_count,
			duration_ms  = EXCLUDED.duration_ms,
			status       = EXCLUDED.status`

	_, err = h.pg.Exec(r.Context(), upsert,
		username, req.QueryText, req.TimeRange, parseTime(req.CustomStart), parseTime(req.CustomEnd),
		req.ResultCount, req.DurationMs, req.Status, fractalIDPtr, prismIDPtr)
	if err != nil {
		log.Printf("[QueryHistory] Failed to record: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to record query")
		return
	}

	// Prune beyond the cap, keeping the most recently run distinct queries.
	prune := fmt.Sprintf(`
		DELETE FROM query_history
		WHERE username = $1 AND COALESCE(fractal_id, prism_id) = COALESCE($2::uuid, $3::uuid)
		  AND id NOT IN (
			SELECT id FROM query_history
			WHERE username = $1 AND COALESCE(fractal_id, prism_id) = COALESCE($2::uuid, $3::uuid)
			ORDER BY last_run_at DESC LIMIT %d
		  )`, historyCap)
	if _, err := h.pg.Exec(r.Context(), prune, username, fractalIDPtr, prismIDPtr); err != nil {
		// Pruning is best-effort; the row was already recorded.
		log.Printf("[QueryHistory] Failed to prune: %v", err)
	}

	h.respondSuccess(w, map[string]bool{"recorded": true})
}

// HandleClear removes all query history for the current user + scope.
func (h *Handler) HandleClear(w http.ResponseWriter, r *http.Request) {
	username := h.getCurrentUser(r)
	if username == "" {
		h.respondError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "Failed to determine context")
		return
	}
	if !h.verifyAccess(w, r, fractalID, prismID) {
		return
	}
	fractalIDPtr, prismIDPtr := scopeArgs(fractalID, prismID)
	_, err = h.pg.Exec(r.Context(),
		`DELETE FROM query_history WHERE username = $1 AND COALESCE(fractal_id, prism_id) = COALESCE($2::uuid, $3::uuid)`,
		username, fractalIDPtr, prismIDPtr)
	if err != nil {
		log.Printf("[QueryHistory] Failed to clear: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to clear query history")
		return
	}
	h.respondSuccess(w, map[string]bool{"cleared": true})
}

// HandleDelete removes a single query history entry owned by the current user.
func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	username := h.getCurrentUser(r)
	if username == "" {
		h.respondError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	id := chi.URLParam(r, "id")
	// Ownership is enforced by the username predicate; scope is not needed since
	// the row id plus username already uniquely identifies the entry.
	result, err := h.pg.Exec(r.Context(),
		`DELETE FROM query_history WHERE id = $1 AND username = $2`, id, username)
	if err != nil {
		log.Printf("[QueryHistory] Failed to delete: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to delete query history entry")
		return
	}
	if n, _ := result.RowsAffected(); n == 0 {
		h.respondError(w, http.StatusNotFound, "history entry not found")
		return
	}
	h.respondSuccess(w, map[string]bool{"deleted": true})
}

// parseTime converts an RFC3339 string to a nullable timestamp arg. Empty or
// unparseable input becomes SQL NULL.
func parseTime(s string) interface{} {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC()
	}
	return nil
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
