package comments

import (
	"bifract/pkg/api"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"bifract/pkg/auth"
	"bifract/pkg/fractals"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
)

type CommentHandler struct {
	pg             *storage.PostgresClient
	ch             *storage.ClickHouseClient
	fractalManager *fractals.Manager
}

type CreateCommentRequest struct {
	LogID        string   `json:"log_id"`
	LogTimestamp string   `json:"log_timestamp"`
	Text         string   `json:"text"`
	Tags         []string `json:"tags,omitempty"`
	Query        string   `json:"query,omitempty"`
	FractalID    string   `json:"fractal_id,omitempty"`
	PrismID      string   `json:"prism_id,omitempty"`
}

type UpdateCommentRequest struct {
	Text string   `json:"text"`
	Tags []string `json:"tags,omitempty"`
}

// Response is the shared API envelope. The alias keeps the package-local
// name while there is one type, and one schema, behind it.
type Response = api.Response[any]

func NewCommentHandlerWithFractals(pg *storage.PostgresClient, ch *storage.ClickHouseClient, fractalManager *fractals.Manager) *CommentHandler {
	return &CommentHandler{
		pg:             pg,
		ch:             ch,
		fractalManager: fractalManager,
	}
}

// HandleCreateComment creates a new comment (analyst+)
func (h *CommentHandler) HandleCreateComment(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(*storage.User)

	fractalRole := rbac.RoleFromContext(r.Context())
	prismRole := rbac.PrismRoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) && !rbac.HasAccess(user, prismRole, rbac.RoleAnalyst) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	var req CreateCommentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[HandleCreateComment] JSON decode error: %v", err)
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	log.Printf("[HandleCreateComment] Received request: LogID=%s, Timestamp=%s", req.LogID, req.LogTimestamp)

	// Validate input
	if req.LogID == "" || req.Text == "" {
		api.WriteError(w, http.StatusBadRequest, "log_id and text are required")
		return
	}

	// Input size limit
	const maxCommentLength = 5000
	if len(req.Text) > maxCommentLength {
		api.WriteError(w, http.StatusBadRequest, fmt.Sprintf("Comment too long (%d chars, max %d)", len(req.Text), maxCommentLength))
		return
	}

	// Enforce API key permissions
	if authType, _ := r.Context().Value("auth_type").(string); authType == "api_key" {
		perms, _ := r.Context().Value("api_key_permissions").(map[string]interface{})
		if canComment, ok := perms["comment"].(bool); !ok || !canComment {
			api.WriteError(w, http.StatusForbidden, "API key does not have comment permission")
			return
		}
	}

	// Scope is ALWAYS derived from the session - never from the request body.
	// Accepting request-body scope was a cross-fractal probe vector: an
	// attacker could set req.FractalID to a fractal they don't own and use
	// this endpoint's log lookup + comment create to confirm log existence
	// in that fractal, or create a comment in an unauthorized scope.
	sessionFractalID, sessionPrismID := h.getScope(r)

	// Resolve log timestamp: if provided, parse it; otherwise look it up
	// from ClickHouse. The log lookup is scoped so callers can only look
	// up logs within their current session scope.
	var logTimestamp time.Time
	if req.LogTimestamp != "" {
		var err error
		logTimestamp, err = time.Parse(time.RFC3339, req.LogTimestamp)
		if err != nil {
			api.WriteError(w, http.StatusBadRequest, "Invalid log_timestamp format (use RFC3339)")
			return
		}
	} else {
		// For prism sessions GetLogByTimestamp with an empty fractalID would
		// match anywhere, so we explicitly fall back to an empty string only
		// for admins. Non-admins on a prism must rely on the prism's members
		// but the single-string API can't express that - they'll get a 404
		// for logs not in their current fractal, which is the safe default.
		lookupFractal := sessionFractalID
		logEntry, err := h.ch.GetLogByTimestamp(r.Context(), time.Time{}, req.LogID, lookupFractal)
		if err != nil || logEntry == nil {
			api.WriteError(w, http.StatusBadRequest, "Could not find log entry; provide log_timestamp or verify log_id")
			return
		}
		switch ts := logEntry["timestamp"].(type) {
		case time.Time:
			logTimestamp = ts
		case string:
			parsed, err := time.Parse("2006-01-02 15:04:05.000", ts)
			if err != nil {
				parsed, err = time.Parse(time.RFC3339, ts)
			}
			if err != nil {
				api.WriteError(w, http.StatusBadRequest, "Could not parse timestamp for log entry")
				return
			}
			logTimestamp = parsed
		default:
			api.WriteError(w, http.StatusBadRequest, "Could not resolve timestamp for log entry")
			return
		}
	}

	// Scope comes from the session exclusively. The alerts_scope_check DB
	// constraint now enforces exactly-one-of, so any code path that tried to
	// set both (e.g. the legacy "store fractal_id AND prism_id for prism
	// comments" pattern) would fail at the DB layer anyway.
	fractalID := sessionFractalID
	prismID := sessionPrismID
	if fractalID == "" && prismID == "" {
		api.WriteError(w, http.StatusBadRequest, "No fractal or prism context")
		return
	}

	// comments.author is NOT NULL and references users(username), so an API key
	// posts as its creator. A key whose creator was deleted has nobody to
	// attribute to and is refused rather than left to fail on the constraint.
	author := auth.AttributionUsername(r.Context())
	if author == "" {
		api.WriteError(w, http.StatusForbidden, "This API key has no owning user to attribute the comment to")
		return
	}

	// Create comment scoped to fractal or prism
	comment := storage.Comment{
		LogID:        req.LogID,
		LogTimestamp: logTimestamp,
		Text:         req.Text,
		Author:       author,
		Tags:         req.Tags,
		Query:        req.Query,
		FractalID:    fractalID,
		PrismID:      prismID,
	}

	newComment, err := h.pg.InsertComment(r.Context(), comment)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to create comment")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Comment created successfully",
		Data:    newComment,
	})
}

// HandleGetComment gets a single comment by ID
func (h *CommentHandler) HandleGetComment(w http.ResponseWriter, r *http.Request) {
	commentID := chi.URLParam(r, "id")

	comment, err := h.pg.GetComment(r.Context(), commentID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Comment not found")
		return
	}

	// Verify the caller's scope matches the comment's scope
	scopeFractal, scopePrism := h.getScope(r)
	scopeMatch := (comment.FractalID != "" && comment.FractalID == scopeFractal) ||
		(comment.PrismID != "" && comment.PrismID == scopePrism)
	if !scopeMatch {
		api.WriteError(w, http.StatusForbidden, "Comment not found")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Data:    comment,
	})
}

// HandleUpdateComment updates a comment (author only)
func (h *CommentHandler) HandleUpdateComment(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(*storage.User)
	commentID := chi.URLParam(r, "id")

	var req UpdateCommentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Validate input
	if req.Text == "" {
		api.WriteError(w, http.StatusBadRequest, "text is required")
		return
	}

	const maxCommentLength = 5000
	if len(req.Text) > maxCommentLength {
		api.WriteError(w, http.StatusBadRequest, fmt.Sprintf("Comment too long (%d chars, max %d)", len(req.Text), maxCommentLength))
		return
	}

	// Update comment
	err := h.pg.UpdateComment(r.Context(), commentID, user.Username, req.Text, req.Tags)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Failed to update comment (not found or unauthorized)")
		return
	}

	// Fetch updated comment
	updatedComment, err := h.pg.GetComment(r.Context(), commentID)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to fetch updated comment")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Comment updated successfully",
		Data:    updatedComment,
	})
}

// HandleDeleteComment deletes a comment (author only)
func (h *CommentHandler) HandleDeleteComment(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(*storage.User)
	commentID := chi.URLParam(r, "id")

	err := h.pg.DeleteComment(r.Context(), commentID, user.Username)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Failed to delete comment (not found or unauthorized)")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Comment deleted successfully",
	})
}

// HandleGetLogComments gets all comments for a specific log
func (h *CommentHandler) HandleGetLogComments(w http.ResponseWriter, r *http.Request) {
	logID := chi.URLParam(r, "log_id")

	scopeFractal, scopePrism := h.getScope(r)

	var comments []storage.Comment
	var err error
	if scopePrism != "" {
		comments, err = h.pg.GetCommentsByLogIDAndPrism(r.Context(), logID, scopePrism)
	} else {
		comments, err = h.pg.GetCommentsByLogIDAndFractal(r.Context(), logID, scopeFractal)
	}
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to fetch comments")
		return
	}

	// Return empty array if no comments
	if comments == nil {
		comments = []storage.Comment{}
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Data:    comments,
	})
}

// HandleGetCommentedLogs gets all logs that have comments
func (h *CommentHandler) HandleGetCommentedLogs(w http.ResponseWriter, r *http.Request) {
	scopeFractal, scopePrism := h.getScope(r)

	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")

	limit := 50
	offset := 0

	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 100 {
			limit = l
		}
	}

	if offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
			offset = o
		}
	}

	var logs []map[string]interface{}
	var total int
	var err error
	if scopePrism != "" {
		logs, total, err = h.pg.GetAllCommentedLogsByPrism(r.Context(), scopePrism, limit, offset)
	} else {
		logs, total, err = h.pg.GetAllCommentedLogsByFractal(r.Context(), scopeFractal, limit, offset)
	}
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to fetch commented logs")
		return
	}

	// Return empty array if no logs
	if logs == nil {
		logs = []map[string]interface{}{}
	}

	api.WritePage(w, logs, api.Page{Total: total, Limit: limit, Offset: offset})
}

// HandleGetFlatComments returns individual comments (not grouped by log) for the current fractal or prism.
func (h *CommentHandler) HandleGetFlatComments(w http.ResponseWriter, r *http.Request) {
	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")

	limit := 2000
	offset := 0

	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 5000 {
			limit = l
		}
	}
	if offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
			offset = o
		}
	}

	scopeFractal, scopePrism := h.getScope(r)

	var comments []storage.Comment
	var total int
	var err error
	if scopePrism != "" {
		comments, total, err = h.pg.GetAllCommentsByPrism(r.Context(), scopePrism, limit, offset)
	} else {
		comments, total, err = h.pg.GetAllCommentsByFractal(r.Context(), scopeFractal, limit, offset)
	}
	if err != nil {
		log.Printf("[Comments] Failed to get flat comments: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to fetch comments")
		return
	}

	if comments == nil {
		comments = []storage.Comment{}
	}

	api.WritePage(w, comments, api.Page{Total: total, Limit: limit, Offset: offset})
}

// BulkTagRequest applies or removes one tag across several comments.
type BulkTagRequest struct {
	CommentIDs []string `json:"comment_ids"`
	Tag        string   `json:"tag"`
}

// HandleBulkAddTag adds a tag to multiple comments. Requires Analyst+ role.
func (h *CommentHandler) HandleBulkAddTag(w http.ResponseWriter, r *http.Request) {
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil {
		api.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	fractalRole := rbac.RoleFromContext(r.Context())
	prismRole := rbac.PrismRoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) && !rbac.HasAccess(user, prismRole, rbac.RoleAnalyst) {
		api.WriteError(w, http.StatusForbidden, "Analyst access required")
		return
	}

	var req BulkTagRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	req.Tag = strings.TrimSpace(req.Tag)
	if req.Tag == "" || len(req.Tag) > 100 {
		api.WriteError(w, http.StatusBadRequest, "Tag must be 1-100 characters")
		return
	}
	if len(req.CommentIDs) == 0 || len(req.CommentIDs) > 500 {
		api.WriteError(w, http.StatusBadRequest, "Must provide 1-500 comment IDs")
		return
	}

	scopeFractal, scopePrism := h.getScope(r)
	count, err := h.pg.BulkAddTagToComments(r.Context(), req.CommentIDs, req.Tag, user.Username, user.IsAdmin, scopeFractal, scopePrism)
	if err != nil {
		log.Printf("[Comments] Bulk add tag failed: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to add tag")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"data":    map[string]interface{}{"updated": count},
	})
}

// HandleBulkRemoveTag removes a tag from multiple comments. Requires Analyst+ role.
func (h *CommentHandler) HandleBulkRemoveTag(w http.ResponseWriter, r *http.Request) {
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil {
		api.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	fractalRole := rbac.RoleFromContext(r.Context())
	prismRole := rbac.PrismRoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) && !rbac.HasAccess(user, prismRole, rbac.RoleAnalyst) {
		api.WriteError(w, http.StatusForbidden, "Analyst access required")
		return
	}

	var req BulkTagRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	req.Tag = strings.TrimSpace(req.Tag)
	if req.Tag == "" || len(req.Tag) > 100 {
		api.WriteError(w, http.StatusBadRequest, "Tag must be 1-100 characters")
		return
	}
	if len(req.CommentIDs) == 0 || len(req.CommentIDs) > 500 {
		api.WriteError(w, http.StatusBadRequest, "Must provide 1-500 comment IDs")
		return
	}

	scopeFractal, scopePrism := h.getScope(r)
	count, err := h.pg.BulkRemoveTagFromComments(r.Context(), req.CommentIDs, req.Tag, user.Username, user.IsAdmin, scopeFractal, scopePrism)
	if err != nil {
		log.Printf("[Comments] Bulk remove tag failed: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to remove tag")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"data":    map[string]interface{}{"updated": count},
	})
}

// BulkDeleteRequest names the comments to delete.
type BulkDeleteRequest struct {
	CommentIDs []string `json:"comment_ids"`
}

// HandleBulkDeleteComments deletes multiple comments by ID. Requires Analyst+ role.
// Non-admin users can only delete comments they authored.
func (h *CommentHandler) HandleBulkDeleteComments(w http.ResponseWriter, r *http.Request) {
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil {
		api.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	fractalRole := rbac.RoleFromContext(r.Context())
	prismRole := rbac.PrismRoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) && !rbac.HasAccess(user, prismRole, rbac.RoleAnalyst) {
		api.WriteError(w, http.StatusForbidden, "Analyst access required")
		return
	}

	var req BulkDeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if len(req.CommentIDs) == 0 || len(req.CommentIDs) > 500 {
		api.WriteError(w, http.StatusBadRequest, "Must provide 1-500 comment IDs")
		return
	}

	scopeFractal, scopePrism := h.getScope(r)
	count, err := h.pg.BulkDeleteComments(r.Context(), req.CommentIDs, user.Username, user.IsAdmin, scopeFractal, scopePrism)
	if err != nil {
		log.Printf("[Comments] Bulk delete failed: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to delete comments")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"data":    map[string]interface{}{"deleted": count},
	})
}

// HandleGetTags returns distinct tags used in comments for the current scope.
func (h *CommentHandler) HandleGetTags(w http.ResponseWriter, r *http.Request) {
	scopeFractal, scopePrism := h.getScope(r)

	var tags []string
	var err error
	if scopePrism != "" {
		tags, err = h.pg.GetDistinctTagsByPrism(r.Context(), scopePrism)
	} else {
		tags, err = h.pg.GetDistinctTagsByFractal(r.Context(), scopeFractal)
	}
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to fetch tags")
		return
	}

	if tags == nil {
		tags = []string{}
	}

	api.WriteJSON(w, http.StatusOK, Response{Success: true, Data: tags})
}

// HandleDeleteCommentsByLogID deletes all comments for a specific log_id
// This is used for cascading deletes when logs are removed (admin only)
func (h *CommentHandler) HandleDeleteCommentsByLogID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		api.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil {
		api.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	fractalRole := rbac.RoleFromContext(r.Context())
	prismRole := rbac.PrismRoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAdmin) && !rbac.HasAccess(user, prismRole, rbac.RoleAdmin) {
		api.WriteError(w, http.StatusForbidden, "Admin access required")
		return
	}

	logID := chi.URLParam(r, "log_id")
	if logID == "" {
		api.WriteError(w, http.StatusBadRequest, "log_id parameter is required")
		return
	}

	err := h.pg.DeleteCommentsByLogID(r.Context(), logID)
	if err != nil {
		log.Printf("[Comments] Failed to delete comments for log %s: %v", logID, err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to delete comments")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: fmt.Sprintf("All comments for log_id %s deleted successfully", logID),
	})
}

// getScope returns the fractalID and prismID from context. Exactly one will be non-empty.
func (h *CommentHandler) getScope(r *http.Request) (fractalID, prismID string) {
	if pid, _ := r.Context().Value("selected_prism").(string); pid != "" {
		return "", pid
	}
	if fid, _ := r.Context().Value("selected_fractal").(string); fid != "" {
		return fid, ""
	}
	if h.fractalManager != nil {
		if df, err := h.fractalManager.GetDefaultFractal(r.Context()); err == nil {
			return df.ID, ""
		}
	}
	return "", ""
}

// LogFieldsRequest names the logs whose parsed fields to fetch.
type LogFieldsRequest struct {
	LogIDs []string `json:"log_ids"`
}

// HandleGetLogFields batch-fetches parsed field data for multiple logs.
func (h *CommentHandler) HandleGetLogFields(w http.ResponseWriter, r *http.Request) {
	selectedFractal, _ := h.getScope(r)

	var req LogFieldsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if len(req.LogIDs) == 0 {
		api.WriteJSON(w, http.StatusOK, Response{Success: true, Data: []interface{}{}})
		return
	}
	if len(req.LogIDs) > 500 {
		api.WriteError(w, http.StatusBadRequest, "Too many log IDs (max 500)")
		return
	}

	logs, err := h.ch.GetLogFieldsByIDs(r.Context(), req.LogIDs, selectedFractal)
	if err != nil {
		log.Printf("[Comments] Failed to get log fields: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to fetch log fields")
		return
	}
	if logs == nil {
		logs = []map[string]interface{}{}
	}

	api.WriteJSON(w, http.StatusOK, Response{Success: true, Data: logs})
}
