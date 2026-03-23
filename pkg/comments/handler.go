package comments

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

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
}

type UpdateCommentRequest struct {
	Text string   `json:"text"`
	Tags []string `json:"tags,omitempty"`
}

type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Error   string      `json:"error,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

func NewCommentHandler(pg *storage.PostgresClient, ch *storage.ClickHouseClient) *CommentHandler {
	return &CommentHandler{
		pg: pg,
		ch: ch,
	}
}

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
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Insufficient permissions"})
		return
	}

	var req CreateCommentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[HandleCreateComment] JSON decode error: %v", err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Invalid request body",
		})
		return
	}

	log.Printf("[HandleCreateComment] Received request: LogID=%s, Timestamp=%s", req.LogID, req.LogTimestamp)

	// Validate input
	if req.LogID == "" || req.Text == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "log_id and text are required",
		})
		return
	}

	// Input size limit
	const maxCommentLength = 5000
	if len(req.Text) > maxCommentLength {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   fmt.Sprintf("Comment too long (%d chars, max %d)", len(req.Text), maxCommentLength),
		})
		return
	}

	// Enforce API key permissions
	if authType, _ := r.Context().Value("auth_type").(string); authType == "api_key" {
		perms, _ := r.Context().Value("api_key_permissions").(map[string]interface{})
		if canComment, ok := perms["comment"].(bool); !ok || !canComment {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(Response{
				Success: false,
				Error:   "API key does not have comment permission",
			})
			return
		}
	}

	// Resolve log timestamp: if provided, parse it; otherwise look it up
	// from ClickHouse so callers don't need to supply it.
	var logTimestamp time.Time
	if req.LogTimestamp != "" {
		var err error
		logTimestamp, err = time.Parse(time.RFC3339, req.LogTimestamp)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response{
				Success: false,
				Error:   "Invalid log_timestamp format (use RFC3339)",
			})
			return
		}
	} else {
		logEntry, err := h.ch.GetLogByTimestamp(r.Context(), time.Time{}, req.LogID, "")
		if err != nil || logEntry == nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response{
				Success: false,
				Error:   "Could not find log entry; provide log_timestamp or verify log_id",
			})
			return
		}
		if ts, ok := logEntry["timestamp"].(time.Time); ok {
			logTimestamp = ts
		} else {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response{
				Success: false,
				Error:   "Could not resolve timestamp for log entry",
			})
			return
		}
	}

	// Determine fractal: prefer the log's own fractal_id from the request
	// (sourced from ClickHouse log data), fall back to session fractal.
	fractalID := req.FractalID
	if fractalID == "" {
		var err error
		fractalID, err = h.getSelectedFractal(r)
		if err != nil {
			log.Printf("[Comments] Failed to get selected fractal: %v", err)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response{
				Success: false,
				Error:   "Failed to determine fractal context",
			})
			return
		}
	}

	// Create comment
	comment := storage.Comment{
		LogID:        req.LogID,
		LogTimestamp: logTimestamp,
		Text:         req.Text,
		Author:       user.Username,
		Tags:         req.Tags,
		Query:        req.Query,
		FractalID:    fractalID,
	}

	newComment, err := h.pg.InsertComment(r.Context(), comment)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to create comment",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Comment not found",
		})
		return
	}

	// Verify the caller has access to the comment's fractal
	selectedFractal, err := h.getSelectedFractal(r)
	if err != nil || (comment.FractalID != "" && selectedFractal != comment.FractalID) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Comment not found",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Invalid request body",
		})
		return
	}

	// Validate input
	if req.Text == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "text is required",
		})
		return
	}

	const maxCommentLength = 5000
	if len(req.Text) > maxCommentLength {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   fmt.Sprintf("Comment too long (%d chars, max %d)", len(req.Text), maxCommentLength),
		})
		return
	}

	// Update comment
	err := h.pg.UpdateComment(r.Context(), commentID, user.Username, req.Text, req.Tags)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to update comment (not found or unauthorized)",
		})
		return
	}

	// Fetch updated comment
	updatedComment, err := h.pg.GetComment(r.Context(), commentID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to fetch updated comment",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to delete comment (not found or unauthorized)",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Comment deleted successfully",
	})
}

// HandleGetLogComments gets all comments for a specific log
func (h *CommentHandler) HandleGetLogComments(w http.ResponseWriter, r *http.Request) {
	logID := chi.URLParam(r, "log_id")

	// Get selected fractal for comment isolation
	selectedFractal, err := h.getSelectedFractal(r)
	if err != nil {
		log.Printf("[Comments] Failed to get selected fractal: %v", err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to determine fractal context",
		})
		return
	}

	comments, err := h.pg.GetCommentsByLogIDAndFractal(r.Context(), logID, selectedFractal)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to fetch comments",
		})
		return
	}

	// Return empty array if no comments
	if comments == nil {
		comments = []storage.Comment{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Data:    comments,
	})
}

// HandleGetCommentedLogs gets all logs that have comments
func (h *CommentHandler) HandleGetCommentedLogs(w http.ResponseWriter, r *http.Request) {
	// Get selected fractal for comment isolation
	selectedFractal, err := h.getSelectedFractal(r)
	if err != nil {
		log.Printf("[Comments] Failed to get selected fractal: %v", err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to determine fractal context",
		})
		return
	}

	// Parse optional limit and offset for pagination
	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")

	limit := 50 // Default limit
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

	// Get commented logs filtered by fractal
	logs, total, err := h.pg.GetAllCommentedLogsByFractal(r.Context(), selectedFractal, limit, offset)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to fetch commented logs",
		})
		return
	}

	// Return empty array if no logs
	if logs == nil {
		logs = []map[string]interface{}{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"data":    logs,
		"total":   total,
		"limit":   limit,
		"offset":  offset,
	})
}

// HandleGetFlatComments returns individual comments (not grouped by log) for the current fractal.
func (h *CommentHandler) HandleGetFlatComments(w http.ResponseWriter, r *http.Request) {
	selectedFractal, err := h.getSelectedFractal(r)
	if err != nil {
		log.Printf("[Comments] Failed to get selected fractal: %v", err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to determine fractal context"})
		return
	}

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

	comments, total, err := h.pg.GetAllCommentsByFractal(r.Context(), selectedFractal, limit, offset)
	if err != nil {
		log.Printf("[Comments] Failed to get flat comments: %v", err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to fetch comments"})
		return
	}

	if comments == nil {
		comments = []storage.Comment{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"data":    comments,
		"total":   total,
		"limit":   limit,
		"offset":  offset,
	})
}

// HandleBulkAddTag adds a tag to multiple comments. Requires Analyst+ role.
func (h *CommentHandler) HandleBulkAddTag(w http.ResponseWriter, r *http.Request) {
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Authentication required"})
		return
	}

	fractalRole := rbac.RoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Analyst access required"})
		return
	}

	var req struct {
		CommentIDs []string `json:"comment_ids"`
		Tag        string   `json:"tag"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Invalid request body"})
		return
	}

	req.Tag = strings.TrimSpace(req.Tag)
	if req.Tag == "" || len(req.Tag) > 100 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Tag must be 1-100 characters"})
		return
	}
	if len(req.CommentIDs) == 0 || len(req.CommentIDs) > 500 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Must provide 1-500 comment IDs"})
		return
	}

	count, err := h.pg.BulkAddTagToComments(r.Context(), req.CommentIDs, req.Tag, user.Username, user.IsAdmin)
	if err != nil {
		log.Printf("[Comments] Bulk add tag failed: %v", err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to add tag"})
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
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Authentication required"})
		return
	}

	fractalRole := rbac.RoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Analyst access required"})
		return
	}

	var req struct {
		CommentIDs []string `json:"comment_ids"`
		Tag        string   `json:"tag"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Invalid request body"})
		return
	}

	req.Tag = strings.TrimSpace(req.Tag)
	if req.Tag == "" || len(req.Tag) > 100 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Tag must be 1-100 characters"})
		return
	}
	if len(req.CommentIDs) == 0 || len(req.CommentIDs) > 500 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Must provide 1-500 comment IDs"})
		return
	}

	count, err := h.pg.BulkRemoveTagFromComments(r.Context(), req.CommentIDs, req.Tag, user.Username, user.IsAdmin)
	if err != nil {
		log.Printf("[Comments] Bulk remove tag failed: %v", err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to remove tag"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"data":    map[string]interface{}{"updated": count},
	})
}

// HandleBulkDeleteComments deletes multiple comments by ID. Requires Analyst+ role.
// Non-admin users can only delete comments they authored.
func (h *CommentHandler) HandleBulkDeleteComments(w http.ResponseWriter, r *http.Request) {
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Authentication required"})
		return
	}

	fractalRole := rbac.RoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Analyst access required"})
		return
	}

	var req struct {
		CommentIDs []string `json:"comment_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Invalid request body"})
		return
	}

	if len(req.CommentIDs) == 0 || len(req.CommentIDs) > 500 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Must provide 1-500 comment IDs"})
		return
	}

	count, err := h.pg.BulkDeleteComments(r.Context(), req.CommentIDs, user.Username, user.IsAdmin)
	if err != nil {
		log.Printf("[Comments] Bulk delete failed: %v", err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to delete comments"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"data":    map[string]interface{}{"deleted": count},
	})
}

// HandleGetTags returns distinct tags used in comments for the current fractal.
func (h *CommentHandler) HandleGetTags(w http.ResponseWriter, r *http.Request) {
	selectedFractal, err := h.getSelectedFractal(r)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to determine fractal context"})
		return
	}

	tags, err := h.pg.GetDistinctTagsByFractal(r.Context(), selectedFractal)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to fetch tags"})
		return
	}

	if tags == nil {
		tags = []string{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Data: tags})
}

// HandleDeleteCommentsByLogID deletes all comments for a specific log_id
// This is used for cascading deletes when logs are removed (admin only)
func (h *CommentHandler) HandleDeleteCommentsByLogID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Authentication required"})
		return
	}

	fractalRole := rbac.RoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAdmin) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Admin access required"})
		return
	}

	logID := chi.URLParam(r, "log_id")
	if logID == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "log_id parameter is required",
		})
		return
	}

	err := h.pg.DeleteCommentsByLogID(r.Context(), logID)
	if err != nil {
		log.Printf("[Comments] Failed to delete comments for log %s: %v", logID, err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to delete comments",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: fmt.Sprintf("All comments for log_id %s deleted successfully", logID),
	})
}

// DeleteCommentsByLogID is a helper method for cascading deletes
// This can be called directly by other handlers when logs are deleted
func (h *CommentHandler) DeleteCommentsByLogID(ctx context.Context, logID string) error {
	return h.pg.DeleteCommentsByLogID(ctx, logID)
}

// DeleteCommentsByLogIDs is a helper method for batch cascading deletes
// This can be called directly by other handlers when multiple logs are deleted
func (h *CommentHandler) DeleteCommentsByLogIDs(ctx context.Context, logIDs []string) error {
	return h.pg.DeleteCommentsByLogIDs(ctx, logIDs)
}

// DeleteAllComments is a helper method for clearing all comments
// This can be called directly by other handlers when all logs are cleared
func (h *CommentHandler) DeleteAllComments(ctx context.Context) error {
	return h.pg.DeleteAllComments(ctx)
}

// getSelectedFractal retrieves the selected fractal for the current user session
func (h *CommentHandler) getSelectedFractal(r *http.Request) (string, error) {
	// First try to get the selected fractal from the request context (set by auth middleware)
	if selectedFractal := r.Context().Value("selected_fractal"); selectedFractal != nil {
		if fractalID, ok := selectedFractal.(string); ok && fractalID != "" {
			return fractalID, nil
		}
	}

	// If no fractal manager is available, use default behavior (backwards compatibility)
	if h.fractalManager == nil {
		return "", nil
	}

	// Fall back to default fractal if none selected in session
	defaultFractal, err := h.fractalManager.GetDefaultFractal(r.Context())
	if err != nil {
		return "", fmt.Errorf("failed to get default fractal: %w", err)
	}

	return defaultFractal.ID, nil
}

// HandleGetLogFields batch-fetches parsed field data for multiple logs.
func (h *CommentHandler) HandleGetLogFields(w http.ResponseWriter, r *http.Request) {
	selectedFractal, err := h.getSelectedFractal(r)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to determine fractal context"})
		return
	}

	var req struct {
		LogIDs []string `json:"log_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Invalid request body"})
		return
	}

	if len(req.LogIDs) == 0 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: true, Data: []interface{}{}})
		return
	}
	if len(req.LogIDs) > 500 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Too many log IDs (max 500)"})
		return
	}

	logs, err := h.ch.GetLogFieldsByIDs(r.Context(), req.LogIDs, selectedFractal)
	if err != nil {
		log.Printf("[Comments] Failed to get log fields: %v", err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to fetch log fields"})
		return
	}
	if logs == nil {
		logs = []map[string]interface{}{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Data: logs})
}
