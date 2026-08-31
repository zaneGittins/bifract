package comments

import (
	"bifract/pkg/api"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"bifract/pkg/auth"
	"bifract/pkg/fractals"
	"bifract/pkg/prisms"
	"bifract/pkg/rbac"
	"bifract/pkg/sse"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
)

type CommentHandler struct {
	pg             *storage.PostgresClient
	ch             *storage.ClickHouseClient
	fractalManager *fractals.Manager
	prismManager   *prisms.Manager
	sseHub         *sse.Hub
}

type CreateCommentRequest struct {
	LogID        string   `json:"log_id"`
	LogTimestamp string   `json:"log_timestamp"`
	Text         string   `json:"text"`
	Tags         []string `json:"tags,omitempty"`
	Query        string   `json:"query,omitempty"`
	FractalID    string   `json:"fractal_id,omitempty"`
	PrismID      string   `json:"prism_id,omitempty"`

	// NotebookID files the comment into a notebook as evidence. Starring a row
	// is this request with an empty Text: the comment records that the log
	// matters whether or not anyone has written about it yet.
	NotebookID string `json:"notebook_id,omitempty"`
	// Title is the evidence section's line in the notebook outline. Ignored
	// without NotebookID.
	Title string `json:"title,omitempty"`
}

type UpdateCommentRequest struct {
	// Text absent leaves the comment's words alone; empty clears them, which
	// returns the row to a bare star rather than deleting the evidence.
	Text *string  `json:"text,omitempty"`
	Tags []string `json:"tags,omitempty"`
}

// Response is the shared API envelope. The alias keeps the package-local
// name while there is one type, and one schema, behind it.
type Response = api.Response[any]

func NewCommentHandlerWithFractals(pg *storage.PostgresClient, ch *storage.ClickHouseClient, fractalManager *fractals.Manager, prismManager *prisms.Manager) *CommentHandler {
	return &CommentHandler{
		pg:             pg,
		ch:             ch,
		fractalManager: fractalManager,
		prismManager:   prismManager,
	}
}

// SetSSEHub wires live notebook updates so evidence filed from the search page
// reaches anyone reading that notebook.
func (h *CommentHandler) SetSSEHub(hub *sse.Hub) {
	h.sseHub = hub
}

// logScope resolves which fractals a lookup by log_id may read from. A prism
// session has no single fractal id, and both ClickHouse helpers below drop their
// fractal filter when given an empty one, which spans every fractal.
func (h *CommentHandler) logScope() fractals.LogScope {
	return fractals.LogScope{Fractals: h.fractalManager, Prisms: h.prismManager}
}

// answerScopeError answers the request when a logScope lookup failed. Returns
// true when it did, meaning the caller must stop.
func (h *CommentHandler) answerScopeError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, fractals.ErrNoScope) {
		api.WriteError(w, http.StatusBadRequest, "No fractal or prism selected")
		return true
	}
	log.Printf("[Comments] Failed to resolve accessible fractals: %v", err)
	api.WriteError(w, http.StatusInternalServerError, "Failed to determine scope")
	return true
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

	// Text is optional only when filing into a notebook: that is a star, and
	// the annotation may be written later.
	if req.LogID == "" || (req.Text == "" && req.NotebookID == "") {
		api.WriteError(w, http.StatusBadRequest, "log_id and text are required")
		return
	}

	// log_id is a storage.GenerateLogID hash and reaches ClickHouse lookups from
	// several call sites, so it is constrained to hex here rather than trusted to
	// be quoted correctly downstream.
	if !validLogID(req.LogID) {
		api.WriteError(w, http.StatusBadRequest, "log_id must be a hexadecimal log identifier")
		return
	}

	// Input size limit
	const maxCommentLength = 5000
	if len(req.Text) > maxCommentLength {
		api.WriteError(w, http.StatusBadRequest, fmt.Sprintf("Comment too long (%d chars, max %d)", len(req.Text), maxCommentLength))
		return
	}

	// Scope is ALWAYS derived from the session - never from the request body,
	// which was a cross-fractal probe vector: the log lookup below would confirm
	// a log's existence in a fractal the caller has no access to.
	sessionFractalID, sessionPrismID, ok := h.requireScope(w, r)
	if !ok {
		return
	}

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
		// sessionFractalID is empty in a prism, which makes the lookup span every
		// fractal, so the log's own fractal_id is checked against the scope after.
		accessible, err := h.logScope().AccessibleFractalIDs(r.Context())
		if h.answerScopeError(w, err) {
			return
		}
		logEntry, err := h.ch.GetLogByTimestamp(r.Context(), time.Time{}, req.LogID, sessionFractalID)
		if err != nil || logEntry == nil {
			api.WriteError(w, http.StatusBadRequest, "Could not find log entry; provide log_timestamp or verify log_id")
			return
		}
		logFractalID, _ := logEntry["fractal_id"].(string)
		if !h.logScope().Allows(r.Context(), logFractalID, accessible) {
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

	notebookID, ok := h.resolveEvidenceNotebook(w, r, req.NotebookID, fractalID, prismID)
	if !ok {
		return
	}

	var title *string
	if t := strings.TrimSpace(req.Title); t != "" && notebookID != "" {
		clamped := truncateRunes(t, maxEvidenceTitleChars)
		title = &clamped
	}

	// A star carries no text, so a repeat of one is a no-op: one event is one
	// row in a notebook's outline. A comment carrying text is always written,
	// even on an event the notebook already holds, because it is a new
	// annotation rather than a duplicate of the same click.
	write, err := h.pg.InsertCommentWithEvidence(r.Context(), comment, notebookID, title, req.Text == "")
	if err != nil {
		log.Printf("[Comments] Failed to create comment: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to create comment")
		return
	}

	message := "Comment created successfully"
	if write.Existing {
		message = "Log is already evidence in this notebook"
	}
	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: message,
		Data:    write.Comment,
	})

	if write.Section != nil && h.sseHub != nil {
		h.sseHub.Broadcast("notebook:"+notebookID, sse.Event{Type: sse.SectionAdded, Data: write.Section}, r.Header.Get("X-SSE-Client-ID"))
	}
}

// maxEvidenceTitleChars matches notebook_sections.title's column width.
const maxEvidenceTitleChars = 255

// truncateRunes cuts to at most n characters. Slicing bytes would split a
// multibyte rune and Postgres rejects the result as invalid UTF-8.
func truncateRunes(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n])
}

// resolveEvidenceNotebook validates the notebook a comment is being filed into.
// It must exist in the caller's own scope: accepting one from another fractal
// would let a comment leak into a notebook the author cannot read.
func (h *CommentHandler) resolveEvidenceNotebook(w http.ResponseWriter, r *http.Request, notebookID, fractalID, prismID string) (string, bool) {
	if notebookID == "" {
		return "", true
	}
	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return "", false
	}
	if nb.FractalID != fractalID || nb.PrismID != prismID {
		api.WriteError(w, http.StatusForbidden, "Notebook is not in the current scope")
		return "", false
	}
	if nb.IsLocked() {
		api.WriteError(w, http.StatusConflict, nb.LockedMessage())
		return "", false
	}
	return nb.ID, true
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
	scopeFractal, scopePrism, ok := h.requireScope(w, r)
	if !ok {
		return
	}
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

// UpdateCommentTagsRequest replaces a comment's tags.
type UpdateCommentTagsRequest struct {
	Tags []string `json:"tags"`
}

// maxCommentTags and maxCommentTagChars bound what one comment can carry, so a
// client cannot turn the tag array into unbounded storage.
const (
	maxCommentTags     = 32
	maxCommentTagChars = 64
)

// HandleUpdateCommentTags replaces a comment's tags. Unlike its text, which is
// one person's words and only they may change, tags are how a team organises
// shared evidence, so any analyst in the comment's scope may set them.
func (h *CommentHandler) HandleUpdateCommentTags(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(*storage.User)
	fractalRole := rbac.RoleFromContext(r.Context())
	prismRole := rbac.PrismRoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) && !rbac.HasAccess(user, prismRole, rbac.RoleAnalyst) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	commentID := chi.URLParam(r, "id")

	var req UpdateCommentTagsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	if len(req.Tags) > maxCommentTags {
		api.WriteError(w, http.StatusBadRequest, fmt.Sprintf("At most %d tags per comment", maxCommentTags))
		return
	}
	tags := make([]string, 0, len(req.Tags))
	seen := map[string]bool{}
	for _, tag := range req.Tags {
		tag = strings.TrimSpace(tag)
		if tag == "" || seen[tag] {
			continue
		}
		if len(tag) > maxCommentTagChars {
			api.WriteError(w, http.StatusBadRequest, fmt.Sprintf("Tag too long (max %d characters)", maxCommentTagChars))
			return
		}
		seen[tag] = true
		tags = append(tags, tag)
	}

	comment, err := h.pg.GetComment(r.Context(), commentID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Comment not found")
		return
	}
	scopeFractal, scopePrism, ok := h.requireScope(w, r)
	if !ok {
		return
	}
	if comment.FractalID != scopeFractal || comment.PrismID != scopePrism {
		api.WriteError(w, http.StatusForbidden, "Comment is not in the current scope")
		return
	}

	if err := h.pg.UpdateCommentTags(r.Context(), commentID, tags); err != nil {
		log.Printf("[Comments] Failed to update tags: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to update tags")
		return
	}

	updated, err := h.pg.GetComment(r.Context(), commentID)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to fetch updated comment")
		return
	}
	api.WriteJSON(w, http.StatusOK, Response{Success: true, Message: "Tags updated", Data: updated})
}

// HandleUpdateComment updates a comment (author only)
func (h *CommentHandler) HandleUpdateComment(w http.ResponseWriter, r *http.Request) {
	commentID := chi.URLParam(r, "id")

	var req UpdateCommentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	const maxCommentLength = 5000
	if req.Text != nil && len(*req.Text) > maxCommentLength {
		api.WriteError(w, http.StatusBadRequest, fmt.Sprintf("Comment too long (%d chars, max %d)", len(*req.Text), maxCommentLength))
		return
	}

	// Ownership matches on what was written as the author, which for an API key is
	// the person who created it, not the synthetic apikey_<id> principal. Matching
	// on the principal meant a key could not edit or delete its own comments.
	err := h.pg.UpdateComment(r.Context(), commentID, auth.AttributionUsername(r.Context()), req.Text, req.Tags)
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
	commentID := chi.URLParam(r, "id")

	err := h.pg.DeleteComment(r.Context(), commentID, auth.AttributionUsername(r.Context()))
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

	scopeFractal, scopePrism, ok := h.requireScope(w, r)
	if !ok {
		return
	}

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
	scopeFractal, scopePrism, ok := h.requireScope(w, r)
	if !ok {
		return
	}

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

	scopeFractal, scopePrism, ok := h.requireScope(w, r)
	if !ok {
		return
	}

	// filed=unfiled is the bucket of annotations nobody collected into a
	// notebook, which is the one view that finds work left half done.
	filing := storage.CommentFilingAny
	switch r.URL.Query().Get("filed") {
	case "filed":
		filing = storage.CommentFilingFiled
	case "unfiled":
		filing = storage.CommentFilingUnfiled
	}

	comments, total, err := h.pg.GetCommentsByScope(r.Context(), scopeFractal, scopePrism, filing, limit, offset)
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

	scopeFractal, scopePrism, ok := h.requireScope(w, r)
	if !ok {
		return
	}
	count, err := h.pg.BulkAddTagToComments(r.Context(), req.CommentIDs, req.Tag, auth.AttributionUsername(r.Context()), user.IsAdmin, scopeFractal, scopePrism)
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

	scopeFractal, scopePrism, ok := h.requireScope(w, r)
	if !ok {
		return
	}
	count, err := h.pg.BulkRemoveTagFromComments(r.Context(), req.CommentIDs, req.Tag, auth.AttributionUsername(r.Context()), user.IsAdmin, scopeFractal, scopePrism)
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

	scopeFractal, scopePrism, ok := h.requireScope(w, r)
	if !ok {
		return
	}
	count, err := h.pg.BulkDeleteComments(r.Context(), req.CommentIDs, auth.AttributionUsername(r.Context()), user.IsAdmin, scopeFractal, scopePrism)
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
	scopeFractal, scopePrism, ok := h.requireScope(w, r)
	if !ok {
		return
	}

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

// requireScope resolves the request scope and answers the request itself when
// there is none, so a scopeless lookup cannot reach the store as an empty id.
func (h *CommentHandler) requireScope(w http.ResponseWriter, r *http.Request) (fractalID, prismID string, ok bool) {
	fractalID, prismID = h.getScope(r)
	if fractalID == "" && prismID == "" {
		api.WriteError(w, http.StatusBadRequest, "No fractal or prism selected")
		return "", "", false
	}
	return fractalID, prismID, true
}

// getScope returns the fractalID and prismID from context. Exactly one will be
// non-empty, or both empty when the request declared no scope.
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
	var req LogFieldsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	if len(req.LogIDs) > 500 {
		api.WriteError(w, http.StatusBadRequest, "Too many log IDs (max 500)")
		return
	}

	accessible, err := h.logScope().ReadFilterIDs(r.Context())
	if h.answerScopeError(w, err) {
		return
	}
	if len(req.LogIDs) == 0 || len(accessible) == 0 {
		api.WriteJSON(w, http.StatusOK, Response{Success: true, Data: []interface{}{}})
		return
	}

	logs, err := h.ch.GetLogFieldsByIDs(r.Context(), req.LogIDs, accessible)
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

// logIDPattern matches a storage.GenerateLogID value. The length is a range
// rather than a fixed 32 so ids written by earlier hash widths still validate;
// the character class is the part that matters.
var logIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{8,64}$`)

// validLogID reports whether s is a well-formed log identifier.
func validLogID(s string) bool {
	return logIDPattern.MatchString(s)
}
