package notebooks

import (
	"bifract/pkg/api"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"bifract/pkg/auth"
	"bifract/pkg/fractals"
	"bifract/pkg/rbac"
	"bifract/pkg/sse"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
	"gopkg.in/yaml.v3"
)

type NotebookHandler struct {
	pg             *storage.PostgresClient
	ch             *storage.ClickHouseClient
	fractalManager *fractals.Manager
	rbacResolver   *rbac.Resolver
	sseHub         *sse.Hub
	litellmURL     string
	litellmKey     string
}

// SetRBACResolver sets the RBAC resolver for fractal-level access checks.
func (h *NotebookHandler) SetRBACResolver(resolver *rbac.Resolver) {
	h.rbacResolver = resolver
}

// SetSSEHub sets the SSE hub for live update broadcasting.
func (h *NotebookHandler) SetSSEHub(hub *sse.Hub) {
	h.sseHub = hub
}

// broadcastSSE sends an SSE event to all clients viewing this notebook,
// excluding the originator identified by X-SSE-Client-ID.
func (h *NotebookHandler) broadcastSSE(r *http.Request, notebookID string, event sse.Event) {
	if h.sseHub == nil {
		return
	}
	h.sseHub.Broadcast("notebook:"+notebookID, event, r.Header.Get("X-SSE-Client-ID"))
}

// HandleSSE establishes an SSE connection for live notebook updates.
func (h *NotebookHandler) HandleSSE(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")

	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}
	if (nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleViewer)) || (nb.PrismID != "" && !h.requireRoleOnPrism(r, nb.PrismID, rbac.RoleViewer)) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	user := r.Context().Value("user").(*storage.User)
	info := sse.ClientInfo{
		Username:        user.Username,
		DisplayName:     user.DisplayName,
		GravatarColor:   user.GravatarColor,
		GravatarInitial: user.GravatarInitial,
	}

	// Update presence in DB so polling-based clients also see this user.
	// notebook_presence.username is a foreign key, so machine principals (which
	// have no users row) stay out of it; SSE still works for them.
	if !auth.IsAPIKey(r.Context()) {
		_ = h.pg.UpdateNotebookPresence(r.Context(), notebookID, user.Username)
	}

	room := "notebook:" + notebookID

	// Notify existing clients that this user joined.
	h.sseHub.Broadcast(room, sse.Event{
		Type: sse.PresenceJoined,
		Data: info,
	}, "")

	// Blocks until disconnect.
	client := h.sseHub.ServeSSE(w, r, room, info)
	if client == nil {
		return
	}

	// Clean up presence in DB and notify remaining clients.
	if !auth.IsAPIKey(r.Context()) {
		_ = h.pg.DeleteNotebookPresence(context.Background(), notebookID, user.Username)
	}

	h.sseHub.Broadcast(room, sse.Event{
		Type: sse.PresenceLeft,
		Data: map[string]string{"username": user.Username},
	}, client.ID)
}

func forbidden(w http.ResponseWriter) {
	api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
}

// requireRoleOnFractal checks the user has the required role on a specific fractal.
func (h *NotebookHandler) requireRoleOnFractal(r *http.Request, fractalID string, required rbac.Role) bool {
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil {
		return false
	}
	if user.IsAdmin {
		return true
	}
	// API key users have their role pre-resolved by the auth middleware;
	// querying fractal_permissions would fail because the synthetic
	// "apikey_<id>" username has no DB entries.
	if authType, _ := r.Context().Value("auth_type").(string); authType == "api_key" {
		fractalRole := rbac.RoleFromContext(r.Context())
		return rbac.HasAccess(user, fractalRole, required)
	}
	if h.rbacResolver == nil {
		fractalRole := rbac.RoleFromContext(r.Context())
		return rbac.HasAccess(user, fractalRole, required)
	}
	role := h.rbacResolver.ResolveRole(r.Context(), user, fractalID)
	return rbac.HasAccess(user, role, required)
}

// requireRoleOnPrism checks the user has the required role on a specific prism.
func (h *NotebookHandler) requireRoleOnPrism(r *http.Request, prismID string, required rbac.Role) bool {
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil {
		return false
	}
	if user.IsAdmin {
		return true
	}
	if h.rbacResolver == nil {
		prismRole := rbac.PrismRoleFromContext(r.Context())
		return rbac.HasAccess(user, prismRole, required)
	}
	return rbac.HasAccess(user, h.rbacResolver.ResolvePrismRoleWithAdmin(r.Context(), user, prismID), required)
}

// Response is the shared API envelope. The alias keeps the package-local
// name while there is one type, and one schema, behind it.
type Response = api.Response[any]

// commentPrefetchTimeout bounds the detached log prefetch that follows a
// generate-from-comments call. The request has already returned, so nothing else
// would ever cancel it.
const commentPrefetchTimeout = 2 * time.Minute

func NewNotebookHandler(pg *storage.PostgresClient, ch *storage.ClickHouseClient, fractalManager *fractals.Manager, litellmURL, litellmKey string) *NotebookHandler {
	return &NotebookHandler{
		pg:             pg,
		ch:             ch,
		fractalManager: fractalManager,
		litellmURL:     litellmURL,
		litellmKey:     litellmKey,
	}
}

func (h *NotebookHandler) aiEnabled() bool {
	return h.litellmURL != "" && h.litellmKey != ""
}

// HandleListNotebooks retrieves all notebooks for the current fractal with search and pagination
func (h *NotebookHandler) HandleListNotebooks(w http.ResponseWriter, r *http.Request) {
	// Get selected fractal for notebook isolation
	selectedFractal, err := h.getSelectedFractal(r)
	if err != nil {
		log.Printf("[Notebooks] Failed to get selected fractal: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to determine fractal context")
		return
	}

	// Re-check access on the scope every request. The scope was authorized when
	// it was selected, but permissions can be revoked while the session lives on.
	selectedPrism, _ := r.Context().Value("selected_prism").(string)
	if selectedPrism != "" {
		if !h.requireRoleOnPrism(r, selectedPrism, rbac.RoleViewer) {
			forbidden(w)
			return
		}
	} else if selectedFractal != "" {
		if !h.requireRoleOnFractal(r, selectedFractal, rbac.RoleViewer) {
			forbidden(w)
			return
		}
	} else {
		// Neither scope is selected, so there is nothing to authorize and
		// nothing to list. Reported as a bad request, matching the other
		// handlers here, rather than as a scope lookup that fails downstream.
		api.WriteError(w, http.StatusBadRequest, "No fractal or prism selected")
		return
	}

	// Parse pagination parameters
	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")
	searchQuery := r.URL.Query().Get("search")

	limit := 20 // Default limit
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

	// Get notebooks with pagination, filtered by the search term when one is given
	notebooks, total, err := h.pg.GetNotebooksByScope(r.Context(), selectedFractal, selectedPrism, searchQuery, limit, offset)
	if err != nil {
		log.Printf("[Notebooks] Failed to fetch notebooks: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to fetch notebooks")
		return
	}

	// Return empty array if no notebooks
	if notebooks == nil {
		notebooks = []storage.Notebook{}
	}

	api.WritePage(w, notebooks, api.Page{Total: total, Limit: limit, Offset: offset})
}

// HandleCreateNotebook creates a new notebook (analyst+)
func (h *NotebookHandler) HandleCreateNotebook(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(*storage.User)

	fractalRole := rbac.RoleFromContext(r.Context())
	prismRole := rbac.PrismRoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) && !rbac.HasAccess(user, prismRole, rbac.RoleAnalyst) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	var req CreateNotebookRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Validate input
	if req.Name == "" {
		api.WriteError(w, http.StatusBadRequest, "name is required")
		return
	}

	if req.TimeRangeType == "" {
		api.WriteError(w, http.StatusBadRequest, "time_range_type is required")
		return
	}

	// Validate time range type
	validTimeRanges := []string{"1h", "24h", "7d", "30d", "all", "custom"}
	isValidTimeRange := false
	for _, validRange := range validTimeRanges {
		if req.TimeRangeType == validRange {
			isValidTimeRange = true
			break
		}
	}

	if !isValidTimeRange {
		api.WriteJSON(w, http.StatusOK, Response{
			Success: false,
			Error:   "time_range_type must be one of: 1h, 24h, 7d, 30d, custom",
		})
		return
	}

	// For custom time range, validate start and end times
	if req.TimeRangeType == "custom" {
		if req.TimeRangeStart == nil || req.TimeRangeEnd == nil {
			api.WriteError(w, http.StatusBadRequest, "time_range_start and time_range_end are required for custom time range")
			return
		}
	}

	// Set default max results if not provided
	if req.MaxResultsPerSection <= 0 {
		req.MaxResultsPerSection = 1000
	}

	req.Timezone = strings.TrimSpace(req.Timezone)
	if req.Timezone != "" && !storage.ValidTimezone(req.Timezone) {
		api.WriteError(w, http.StatusBadRequest, "Unknown timezone")
		return
	}

	// Get selected fractal for notebook isolation
	selectedFractal, err := h.getSelectedFractal(r)
	if err != nil {
		log.Printf("[Notebooks] Failed to get selected fractal: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to determine fractal context")
		return
	}

	// Create notebook scoped to prism or fractal
	notebook := storage.Notebook{
		Name:                 req.Name,
		Description:          req.Description,
		TimeRangeType:        req.TimeRangeType,
		TimeRangeStart:       req.TimeRangeStart,
		TimeRangeEnd:         req.TimeRangeEnd,
		MaxResultsPerSection: req.MaxResultsPerSection,
		Timezone:             req.Timezone,
		CreatedBy:            auth.AttributionUsername(r.Context()),
	}
	if prismID, ok := r.Context().Value("selected_prism").(string); ok && prismID != "" {
		notebook.PrismID = prismID
	} else {
		notebook.FractalID = selectedFractal
	}

	newNotebook, err := h.pg.InsertNotebook(r.Context(), notebook)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to create notebook")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Notebook created successfully",
		Data:    newNotebook,
	})
}

// HandleGetNotebook retrieves a specific notebook with its sections
func (h *NotebookHandler) HandleGetNotebook(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")

	notebook, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}

	// Verify user has access to the notebook's fractal
	if (notebook.FractalID != "" && !h.requireRoleOnFractal(r, notebook.FractalID, rbac.RoleViewer)) || (notebook.PrismID != "" && !h.requireRoleOnPrism(r, notebook.PrismID, rbac.RoleViewer)) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	// Get sections for the notebook
	sections, err := h.pg.GetNotebookSections(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to fetch notebook sections")
		return
	}

	// Convert storage types to response types
	notebookResponse := Notebook{
		ID:                    notebook.ID,
		Name:                  notebook.Name,
		Description:           notebook.Description,
		TimeRangeType:         notebook.TimeRangeType,
		TimeRangeStart:        notebook.TimeRangeStart,
		TimeRangeEnd:          notebook.TimeRangeEnd,
		MaxResultsPerSection:  notebook.MaxResultsPerSection,
		Timezone:              storage.SafeTimezone(notebook.Timezone),
		FractalID:             notebook.FractalID,
		Variables:             notebook.Variables,
		CreatedBy:             notebook.CreatedBy,
		AuthorDisplayName:     notebook.AuthorDisplayName,
		AuthorGravatarColor:   notebook.AuthorGravatarColor,
		AuthorGravatarInitial: notebook.AuthorGravatarInitial,
		CreatedAt:             notebook.CreatedAt,
		UpdatedAt:             notebook.UpdatedAt,
	}

	// Convert sections
	for _, section := range sections {
		notebookResponse.Sections = append(notebookResponse.Sections, NotebookSection{
			ID:              section.ID,
			NotebookID:      section.NotebookID,
			SectionType:     section.SectionType,
			Title:           section.Title,
			Content:         section.Content,
			RenderedContent: section.RenderedContent,
			OrderIndex:      section.OrderIndex,
			LastExecutedAt:  section.LastExecutedAt,
			LastResults:     section.LastResults,
			ChartType:       section.ChartType,
			ChartConfig:     section.ChartConfig,
			Tags:            section.Tags,
			EventTime:       section.EventTime,
			CreatedAt:       section.CreatedAt,
			UpdatedAt:       section.UpdatedAt,
		})
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Data:    notebookResponse,
	})
}

// HandleGetNotebookSummary returns a notebook's outline without any cached query
// results. The search page's notebook rail refreshes on every notebook switch and
// after every capture, so it must not pull the result blobs that HandleGetNotebook
// returns; on a notebook of any size those dominate the payload.
func (h *NotebookHandler) HandleGetNotebookSummary(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")

	notebook, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}
	if (notebook.FractalID != "" && !h.requireRoleOnFractal(r, notebook.FractalID, rbac.RoleViewer)) || (notebook.PrismID != "" && !h.requireRoleOnPrism(r, notebook.PrismID, rbac.RoleViewer)) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	sections, err := h.pg.GetNotebookSectionSummaries(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to fetch notebook sections")
		return
	}

	counts := map[string]int{}
	for _, sec := range sections {
		counts[sec.SectionType]++
	}

	canEdit := false
	if notebook.FractalID != "" {
		canEdit = h.requireRoleOnFractal(r, notebook.FractalID, rbac.RoleAnalyst)
	} else if notebook.PrismID != "" {
		canEdit = h.requireRoleOnPrism(r, notebook.PrismID, rbac.RoleAnalyst)
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Data: NotebookSummary{
			ID:            notebook.ID,
			Name:          notebook.Name,
			Description:   notebook.Description,
			TimeRangeType: notebook.TimeRangeType,
			Timezone:      storage.SafeTimezone(notebook.Timezone),
			FractalID:     notebook.FractalID,
			PrismID:       notebook.PrismID,
			CanEdit:       canEdit,
			SectionCount:  len(sections),
			Counts:        counts,
			Sections:      sections,
			UpdatedAt:     notebook.UpdatedAt,
		},
	})
}

// HandleUpdateNotebook updates notebook metadata. Authorization is via fractal role.
func (h *NotebookHandler) HandleUpdateNotebook(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")

	// Verify fractal access
	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}
	if (nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleAnalyst)) || (nb.PrismID != "" && !h.requireRoleOnPrism(r, nb.PrismID, rbac.RoleAnalyst)) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	var req UpdateNotebookRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Timezone != nil {
		tz := strings.TrimSpace(*req.Timezone)
		if !storage.ValidTimezone(tz) {
			api.WriteError(w, http.StatusBadRequest, "Unknown timezone")
			return
		}
		req.Timezone = &tz
	}

	// Update notebook
	err = h.pg.UpdateNotebook(r.Context(), notebookID, req.Name, req.Description, req.TimeRangeType, req.Timezone, req.TimeRangeStart, req.TimeRangeEnd, req.MaxResultsPerSection)
	if err != nil {
		if strings.Contains(err.Error(), "not found or unauthorized") {
			api.WriteError(w, http.StatusNotFound, "Notebook not found or unauthorized")
		} else {
			api.WriteError(w, http.StatusInternalServerError, "Failed to update notebook")
		}
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Notebook updated successfully",
	})
}

// HandleDeleteNotebook deletes a notebook. Authorization is via fractal role.
func (h *NotebookHandler) HandleDeleteNotebook(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")

	// Verify fractal access
	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}
	if (nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleAnalyst)) || (nb.PrismID != "" && !h.requireRoleOnPrism(r, nb.PrismID, rbac.RoleAnalyst)) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	err = h.pg.DeleteNotebook(r.Context(), notebookID)
	if err != nil {
		if strings.Contains(err.Error(), "not found or unauthorized") {
			api.WriteError(w, http.StatusNotFound, "Notebook not found or unauthorized")
		} else {
			api.WriteError(w, http.StatusInternalServerError, "Failed to delete notebook")
		}
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Notebook deleted successfully",
	})
}

// HandleCreateSection creates a new section in a notebook
// creatableSectionTypes are the section types a client may create directly.
// comment_context is included: it is the evidence section, and pinning a log
// from search is the same record the comment generator writes.
var creatableSectionTypes = map[string]bool{
	"markdown":        true,
	"query":           true,
	"comment_context": true,
	"ai_summary":      true,
	"ai_attack_chain": true,
}

var evidenceLogIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{8,64}$`)

// validateEvidenceContent checks that an evidence section's content is the JSON
// object its renderer expects. Without this the section renders as an error for
// everyone reading the notebook, with no way to tell why from the UI.
func validateEvidenceContent(content string) error {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(content), &data); err != nil {
		return fmt.Errorf("evidence content must be a JSON object")
	}
	logID, ok := data["log_id"].(string)
	if !ok || logID == "" {
		return fmt.Errorf("evidence content requires a log_id")
	}
	if !evidenceLogIDPattern.MatchString(logID) {
		return fmt.Errorf("evidence log_id is not a valid log identifier")
	}
	return nil
}

// maxSectionTitleChars matches notebook_sections.title's column width. Cutting
// here turns a client's long title into a short one instead of a 500 from
// Postgres, and cuts on runes so the result is still valid UTF-8.
const maxSectionTitleChars = 255

func clampSectionTitle(title *string) *string {
	if title == nil {
		return nil
	}
	clamped := truncateRunes(*title, maxSectionTitleChars)
	if clamped == *title {
		return title
	}
	return &clamped
}

// truncateRunes cuts to at most n characters. Slicing a Go string cuts bytes,
// which splits a multibyte rune whenever the cut lands mid-character: Postgres
// rejects the result as invalid UTF-8 and the whole section is dropped with
// only a log line to say why.
func truncateRunes(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n])
}

// normalizeEventTime stores event times in UTC and treats the zero value as
// absent, so a client that serializes an unset time does not pin the section
// to year one.
func normalizeEventTime(t *time.Time) *time.Time {
	if t == nil || t.IsZero() {
		return nil
	}
	utc := t.UTC()
	return &utc
}

func (h *NotebookHandler) HandleCreateSection(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")

	// Verify fractal access
	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}
	if (nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleAnalyst)) || (nb.PrismID != "" && !h.requireRoleOnPrism(r, nb.PrismID, rbac.RoleAnalyst)) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	var req CreateSectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Validate input
	if !creatableSectionTypes[req.SectionType] {
		api.WriteError(w, http.StatusBadRequest, "section_type must be 'markdown', 'query', 'comment_context', 'ai_summary', or 'ai_attack_chain'")
		return
	}

	if req.SectionType == "comment_context" {
		if err := validateEvidenceContent(req.Content); err != nil {
			api.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	if req.SectionType == "ai_summary" || req.SectionType == "ai_attack_chain" {
		if !h.aiEnabled() {
			api.WriteError(w, http.StatusServiceUnavailable, "AI is not configured")
			return
		}
		sections, err := h.pg.GetNotebookSectionSummaries(r.Context(), notebookID)
		if err == nil {
			for _, s := range sections {
				if s.SectionType == req.SectionType {
					api.WriteError(w, http.StatusConflict, "Only one section of this AI type is allowed per notebook")
					return
				}
			}
		}
		if req.Content == "" {
			req.Content = " "
		}
	}

	if req.Content == "" && req.SectionType != "ai_summary" && req.SectionType != "ai_attack_chain" {
		api.WriteError(w, http.StatusBadRequest, "content is required")
		return
	}

	orderIndex := req.OrderIndex
	if req.Append {
		next, err := h.pg.NextNotebookSectionOrderIndex(r.Context(), notebookID)
		if err != nil {
			api.WriteError(w, http.StatusInternalServerError, "Failed to place section")
			return
		}
		orderIndex = next
	}

	// Create section
	section := storage.NotebookSection{
		NotebookID:  notebookID,
		SectionType: req.SectionType,
		Title:       clampSectionTitle(req.Title),
		Content:     req.Content,
		OrderIndex:  orderIndex,
		Tags:        req.Tags,
		EventTime:   normalizeEventTime(req.EventTime),
	}

	newSection, err := h.pg.InsertNotebookSection(r.Context(), section)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to create section")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Section created successfully",
		Data:    newSection,
	})

	h.broadcastSSE(r, notebookID, sse.Event{Type: sse.SectionAdded, Data: newSection})
}

// HandleUpdateSection updates a notebook section
func (h *NotebookHandler) HandleUpdateSection(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")
	sectionID := chi.URLParam(r, "section_id")

	// Verify fractal access via parent notebook
	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}
	if (nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleAnalyst)) || (nb.PrismID != "" && !h.requireRoleOnPrism(r, nb.PrismID, rbac.RoleAnalyst)) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	var req UpdateSectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Update section
	var renderedContent *string
	var chartConfigJSON *string
	if req.ChartConfig != nil {
		b, err := json.Marshal(req.ChartConfig)
		if err == nil {
			s := string(b)
			chartConfigJSON = &s
		}
	}
	err = h.pg.UpdateNotebookSection(r.Context(), sectionID, storage.NotebookSectionUpdate{
		Title:           clampSectionTitle(req.Title),
		Content:         req.Content,
		RenderedContent: renderedContent,
		ChartConfig:     chartConfigJSON,
		Tags:            req.Tags,
		EventTime:       normalizeEventTime(req.EventTime),
		ClearEventTime:  req.ClearEventTime,
	})
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to update section")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Section updated successfully",
	})

	sseData := map[string]interface{}{"id": sectionID}
	if req.Title != nil {
		sseData["title"] = req.Title
	}
	if req.Content != nil {
		sseData["content"] = req.Content
	}
	if req.ChartConfig != nil {
		sseData["chart_config"] = req.ChartConfig
	}
	if req.Tags != nil {
		sseData["tags"] = *req.Tags
	}
	if req.ClearEventTime {
		sseData["event_time"] = nil
	} else if et := normalizeEventTime(req.EventTime); et != nil {
		sseData["event_time"] = et
	}
	h.broadcastSSE(r, notebookID, sse.Event{
		Type: sse.SectionUpdated,
		Data: sseData,
	})
}

// HandleDeleteSection deletes a notebook section
func (h *NotebookHandler) HandleDeleteSection(w http.ResponseWriter, r *http.Request) {
	sectionID := chi.URLParam(r, "section_id")

	// Fetch section to get parent notebook, then check fractal access
	section, err := h.pg.GetNotebookSection(r.Context(), sectionID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Section not found")
		return
	}
	nb, err := h.pg.GetNotebook(r.Context(), section.NotebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}
	if (nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleAnalyst)) || (nb.PrismID != "" && !h.requireRoleOnPrism(r, nb.PrismID, rbac.RoleAnalyst)) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	err = h.pg.DeleteNotebookSection(r.Context(), sectionID)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to delete section")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Section deleted successfully",
	})

	h.broadcastSSE(r, section.NotebookID, sse.Event{
		Type: sse.SectionRemoved,
		Data: map[string]string{"id": sectionID, "notebook_id": section.NotebookID},
	})
}

// HandleExecuteQuerySection executes a query section and updates cached results
func (h *NotebookHandler) HandleExecuteQuerySection(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")
	sectionID := chi.URLParam(r, "section_id")

	var req ExecuteQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// It's okay if no body is provided, we'll use notebook defaults
	}

	// Get the notebook to access time range settings
	notebook, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}

	if (notebook.FractalID != "" && !h.requireRoleOnFractal(r, notebook.FractalID, rbac.RoleAnalyst)) || (notebook.PrismID != "" && !h.requireRoleOnPrism(r, notebook.PrismID, rbac.RoleAnalyst)) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	// Get the specific section
	section, err := h.pg.GetNotebookSection(r.Context(), sectionID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Section not found")
		return
	}

	// Verify this section belongs to the notebook
	if section.NotebookID != notebookID || section.NotebookID != notebook.ID {
		api.WriteError(w, http.StatusBadRequest, "Section does not belong to notebook")
		return
	}

	// Verify this is a query section
	if section.SectionType != "query" {
		api.WriteError(w, http.StatusBadRequest, "Section is not a query section")
		return
	}

	// TODO: Implement full query execution integration with QueryHandler
	// Calculate time range: startTime, endTime := h.calculateTimeRange(notebook)
	// Create query request and execute it properly

	// Create a placeholder response for now - this would be replaced with actual query execution
	queryResults := map[string]interface{}{
		"success":      true,
		"results":      []map[string]interface{}{},
		"count":        0,
		"query":        section.Content,
		"execution_ms": 0,
		"error":        "Query execution integration pending - requires QueryHandler instance",
	}

	// Store the results in the section
	resultsJSON, _ := json.Marshal(queryResults)
	err = h.pg.UpdateSectionQueryResults(r.Context(), sectionID, string(resultsJSON), section.ChartType, section.ChartConfig)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to update section results")
		return
	}

	// Return the query results
	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Query executed (placeholder implementation)",
		Data:    queryResults,
	})
}

// HandleReorderSections reorders sections in a notebook
func (h *NotebookHandler) HandleReorderSections(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")

	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}
	if (nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleAnalyst)) || (nb.PrismID != "" && !h.requireRoleOnPrism(r, nb.PrismID, rbac.RoleAnalyst)) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	var req ReorderSectionsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Validate input
	if len(req.SectionOrder) == 0 {
		api.WriteError(w, http.StatusBadRequest, "section_order is required")
		return
	}

	// Reorder sections
	err = h.pg.ReorderNotebookSections(r.Context(), notebookID, req.SectionOrder)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to reorder sections")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Sections reordered successfully",
	})

	h.broadcastSSE(r, notebookID, sse.Event{
		Type: sse.SectionsReordered,
		Data: map[string]interface{}{"section_order": req.SectionOrder},
	})
}

// UpdateSectionResultsRequest replaces a query section cached results.
type UpdateSectionResultsRequest struct {
	LastExecutedAt string `json:"last_executed_at"`
	LastResults    string `json:"last_results"`
}

// HandleUpdateSectionResults updates query section results
func (h *NotebookHandler) HandleUpdateSectionResults(w http.ResponseWriter, r *http.Request) {
	sectionID := chi.URLParam(r, "section_id")

	section, err := h.pg.GetNotebookSection(r.Context(), sectionID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Section not found")
		return
	}
	nb, err := h.pg.GetNotebook(r.Context(), section.NotebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}
	if (nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleAnalyst)) || (nb.PrismID != "" && !h.requireRoleOnPrism(r, nb.PrismID, rbac.RoleAnalyst)) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	var req UpdateSectionResultsRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Parse timestamp
	var lastExecutedAt *time.Time
	if req.LastExecutedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, req.LastExecutedAt); err == nil {
			lastExecutedAt = &parsed
		}
	}

	// Update section results
	err = h.pg.UpdateSectionResults(r.Context(), sectionID, lastExecutedAt, req.LastResults)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to update section results")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Section results updated successfully",
	})

	h.broadcastSSE(r, section.NotebookID, sse.Event{
		Type: sse.SectionResultsUpdated,
		Data: map[string]interface{}{
			"id":               sectionID,
			"last_executed_at": req.LastExecutedAt,
			"last_results":     req.LastResults,
		},
	})
}

// HandleUpdatePresence updates user presence for a notebook
func (h *NotebookHandler) HandleUpdatePresence(w http.ResponseWriter, r *http.Request) {
	if auth.IsAPIKey(r.Context()) {
		api.WriteError(w, http.StatusForbidden, "presence is per-user and not available for API key authentication")
		return
	}
	user := r.Context().Value("user").(*storage.User)
	notebookID := chi.URLParam(r, "id")

	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}
	if (nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleViewer)) || (nb.PrismID != "" && !h.requireRoleOnPrism(r, nb.PrismID, rbac.RoleViewer)) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	err = h.pg.UpdateNotebookPresence(r.Context(), notebookID, user.Username)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to update presence")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
	})
}

// HandleGetPresence gets active users for a notebook
func (h *NotebookHandler) HandleGetPresence(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")

	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}
	if (nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleViewer)) || (nb.PrismID != "" && !h.requireRoleOnPrism(r, nb.PrismID, rbac.RoleViewer)) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	presence, err := h.pg.GetNotebookPresence(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to fetch presence")
		return
	}

	// Convert to response format
	var responsePresence []NotebookPresence
	for _, p := range presence {
		responsePresence = append(responsePresence, NotebookPresence{
			NotebookID:          p.NotebookID,
			Username:            p.Username,
			LastSeenAt:          p.LastSeenAt,
			UserDisplayName:     p.UserDisplayName,
			UserGravatarColor:   p.UserGravatarColor,
			UserGravatarInitial: p.UserGravatarInitial,
		})
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Data:    responsePresence,
	})
}

// getSelectedFractal retrieves the selected fractal for the current user session
func (h *NotebookHandler) getSelectedFractal(r *http.Request) (string, error) {
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

func (h *NotebookHandler) HandleUpdateVariables(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")

	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}
	if (nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleAnalyst)) || (nb.PrismID != "" && !h.requireRoleOnPrism(r, nb.PrismID, rbac.RoleAnalyst)) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	var req UpdateVariablesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Variables == nil {
		req.Variables = json.RawMessage("[]")
	}

	if err = h.pg.UpdateNotebookVariables(r.Context(), notebookID, req.Variables); err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to update variables")
		return
	}

	api.WriteJSON(w, http.StatusOK, Response{Success: true, Message: "Variables updated"})
}

// HandleAIStatus returns whether AI summary generation is available.
func (h *NotebookHandler) HandleAIStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":    true,
		"ai_enabled": h.aiEnabled(),
	})
}

// HandleGenerateAISummary generates an AI summary of all other sections in a notebook.
func (h *NotebookHandler) HandleGenerateAISummary(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")
	sectionID := chi.URLParam(r, "section_id")

	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}
	if (nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleAnalyst)) || (nb.PrismID != "" && !h.requireRoleOnPrism(r, nb.PrismID, rbac.RoleAnalyst)) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}
	if !h.aiEnabled() {
		api.WriteError(w, http.StatusServiceUnavailable, "AI is not configured")
		return
	}

	section, err := h.pg.GetNotebookSection(r.Context(), sectionID)
	if err != nil || section.NotebookID != notebookID {
		api.WriteError(w, http.StatusNotFound, "Section not found")
		return
	}
	if section.SectionType != "ai_summary" && section.SectionType != "ai_attack_chain" {
		api.WriteError(w, http.StatusBadRequest, "Section is not an AI Summary section")
		return
	}

	sections, err := h.pg.GetNotebookSections(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to load sections")
		return
	}

	var cellTexts []string
	sectionMap := make(map[string]string) // section_id -> description for attack chain
	for _, s := range sections {
		if s.SectionType == "ai_summary" || s.SectionType == "ai_attack_chain" {
			continue
		}
		label := "Markdown"
		if s.SectionType == "query" {
			label = "Query"
		} else if s.SectionType == "comment_context" {
			label = "Comment"
		}
		title := "Untitled"
		if s.Title != nil && *s.Title != "" {
			title = *s.Title
		}
		cellText := fmt.Sprintf("[%s: %s] (section_id=%s)\n%s", label, title, s.ID, s.Content)

		// For query sections, include execution results if available
		if s.SectionType == "query" && s.LastResults != nil {
			resultsJSON, err := json.Marshal(s.LastResults)
			if err == nil && string(resultsJSON) != "null" && len(resultsJSON) > 2 {
				resultsStr := string(resultsJSON)
				const maxResultsLen = 4000
				if len(resultsStr) > maxResultsLen {
					resultsStr = resultsStr[:maxResultsLen] + "...(truncated)"
				}
				cellText += "\n\nQuery Results:\n" + resultsStr
			}
		}

		cellTexts = append(cellTexts, cellText)
		if s.SectionType == "comment_context" {
			sectionMap[s.ID] = title
		}
	}

	if len(cellTexts) == 0 {
		api.WriteError(w, http.StatusBadRequest, "No sections to summarize")
		return
	}

	notebookContent := strings.Join(cellTexts, "\n\n---\n\n")

	if section.SectionType == "ai_attack_chain" {
		result, err := h.callLiteLLMWithTools(r.Context(), notebookContent, sectionMap)
		if err != nil {
			log.Printf("[Notebooks] AI attack chain generation failed: %v", err)
			api.WriteError(w, http.StatusInternalServerError, "Failed to generate attack chain summary")
			return
		}

		err = h.pg.UpdateNotebookSection(r.Context(), sectionID, storage.NotebookSectionUpdate{Title: section.Title, Content: &result})
		if err != nil {
			api.WriteError(w, http.StatusInternalServerError, "Failed to save attack chain summary")
			return
		}

		api.WriteJSON(w, http.StatusOK, Response{
			Success: true,
			Message: "Attack chain summary generated",
			Data:    map[string]string{"summary": result},
		})
	} else {
		summary, err := h.callLiteLLM(r.Context(), notebookContent)
		if err != nil {
			log.Printf("[Notebooks] AI summary generation failed: %v", err)
			api.WriteError(w, http.StatusInternalServerError, "Failed to generate summary")
			return
		}

		err = h.pg.UpdateNotebookSection(r.Context(), sectionID, storage.NotebookSectionUpdate{Title: section.Title, Content: &summary})
		if err != nil {
			api.WriteError(w, http.StatusInternalServerError, "Failed to save summary")
			return
		}

		api.WriteJSON(w, http.StatusOK, Response{
			Success: true,
			Message: "Summary generated",
			Data:    map[string]string{"summary": summary},
		})
	}
}

func (h *NotebookHandler) callLiteLLM(ctx context.Context, notebookContent string) (string, error) {
	type msg struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	type reqBody struct {
		Model    string `json:"model"`
		Messages []msg  `json:"messages"`
	}

	// Cap input to avoid exceeding context limits
	const maxContentLen = 12000
	if len(notebookContent) > maxContentLen {
		notebookContent = notebookContent[:maxContentLen] + "\n\n[Content truncated]"
	}

	prompt := "You are an assistant embedded in a log analysis notebook. " +
		"Summarize the following notebook cells into a single concise paragraph. " +
		"Focus on what the notebook is investigating, key findings from queries, " +
		"and any conclusions drawn in the markdown cells. " +
		"Do not use bullet points or headers. Keep it to 3-5 sentences.\n\n" +
		notebookContent

	body := reqBody{
		Model:    "bifract-chat",
		Messages: []msg{{Role: "user", Content: prompt}},
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("marshal error: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		h.litellmURL+"/chat/completions", bytes.NewReader(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("request error: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if h.litellmKey != "" {
		req.Header.Set("Authorization", "Bearer "+h.litellmKey)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("litellm call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("litellm error %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode error: %w", err)
	}
	if len(result.Choices) == 0 {
		return "", fmt.Errorf("no choices in response")
	}

	return strings.TrimSpace(result.Choices[0].Message.Content), nil
}

// callLiteLLMWithTools calls the LLM with function calling to get structured MITRE ATT&CK output.
// sectionMap maps section_id -> description string for LLM context.
func (h *NotebookHandler) callLiteLLMWithTools(ctx context.Context, notebookContent string, sectionMap map[string]string) (string, error) {
	const maxContentLen = 12000
	if len(notebookContent) > maxContentLen {
		notebookContent = notebookContent[:maxContentLen] + "\n\n[Content truncated]"
	}

	// Build section reference for the LLM
	sectionRef := "\n\nAvailable comment section IDs for reference:\n"
	for id, desc := range sectionMap {
		sectionRef += fmt.Sprintf("- %s: %s\n", id, desc)
	}

	prompt := "You are a security analyst embedded in a log investigation notebook. " +
		"Analyze the following notebook comments and their associated log data, then map findings to MITRE ATT&CK tactics. " +
		"Provide an executive summary of the attack chain and categorize each relevant finding under the appropriate tactic. " +
		"Reference the comment section IDs provided so findings can be linked back to the original comments. " +
		"Only include tactics that have findings. If no findings match a tactic, omit it from the output.\n\n" +
		notebookContent + sectionRef

	// Build function schema
	toolSchema := map[string]interface{}{
		"type": "function",
		"function": map[string]interface{}{
			"name":        "mitre_attack_summary",
			"description": "Map log investigation findings to MITRE ATT&CK tactics",
			"parameters": map[string]interface{}{
				"type":     "object",
				"required": []string{"executive_summary", "tactics"},
				"properties": map[string]interface{}{
					"executive_summary": map[string]interface{}{
						"type":        "string",
						"description": "2-4 sentence overview of the attack chain observed across all comments",
					},
					"tactics": map[string]interface{}{
						"type": "array",
						"items": map[string]interface{}{
							"type":     "object",
							"required": []string{"tactic", "findings"},
							"properties": map[string]interface{}{
								"tactic": map[string]interface{}{
									"type": "string",
									"enum": []string{
										"Reconnaissance", "Resource Development", "Initial Access",
										"Execution", "Persistence", "Privilege Escalation",
										"Defense Evasion", "Credential Access", "Discovery",
										"Lateral Movement", "Collection", "Command and Control",
										"Exfiltration", "Impact",
									},
								},
								"findings": map[string]interface{}{
									"type": "array",
									"items": map[string]interface{}{
										"type":     "object",
										"required": []string{"description", "section_id"},
										"properties": map[string]interface{}{
											"description": map[string]interface{}{
												"type":        "string",
												"description": "Brief description of the finding",
											},
											"section_id": map[string]interface{}{
												"type":        "string",
												"description": "ID of the comment section this finding relates to",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	type msg struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}

	body := map[string]interface{}{
		"model":    "bifract-chat",
		"messages": []msg{{Role: "user", Content: prompt}},
		"tools":    []interface{}{toolSchema},
		"tool_choice": map[string]interface{}{
			"type":     "function",
			"function": map[string]string{"name": "mitre_attack_summary"},
		},
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("marshal error: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		h.litellmURL+"/chat/completions", bytes.NewReader(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("request error: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if h.litellmKey != "" {
		req.Header.Set("Authorization", "Bearer "+h.litellmKey)
	}

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("litellm call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("litellm error %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Choices []struct {
			Message struct {
				Content   string `json:"content"`
				ToolCalls []struct {
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode error: %w", err)
	}
	if len(result.Choices) == 0 {
		return "", fmt.Errorf("no choices in response")
	}

	msg0 := result.Choices[0].Message
	if len(msg0.ToolCalls) > 0 && msg0.ToolCalls[0].Function.Arguments != "" {
		return msg0.ToolCalls[0].Function.Arguments, nil
	}

	// Fallback: if the model returned content instead of a tool call, wrap it
	if msg0.Content != "" {
		fallback := map[string]interface{}{
			"executive_summary": strings.TrimSpace(msg0.Content),
			"tactics":           []interface{}{},
		}
		fb, _ := json.Marshal(fallback)
		return string(fb), nil
	}

	return "", fmt.Errorf("no tool call or content in response")
}

// YAML export/import types

type nbVariableYAML struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

type notebookYAML struct {
	Kind        string           `yaml:"kind"`
	Name        string           `yaml:"name"`
	Description string           `yaml:"description,omitempty"`
	TimeRange   string           `yaml:"time_range_type,omitempty"`
	MaxResults  int              `yaml:"max_results_per_section,omitempty"`
	Timezone    string           `yaml:"timezone,omitempty"`
	Variables   []nbVariableYAML `yaml:"variables,omitempty"`
	Sections    []sectionYAML    `yaml:"sections"`
}

type sectionYAML struct {
	Type        string      `yaml:"type"`
	Title       string      `yaml:"title,omitempty"`
	Content     string      `yaml:"content"`
	ChartType   string      `yaml:"chart_type,omitempty"`
	ChartConfig interface{} `yaml:"chart_config,omitempty"`
	Tags        []string    `yaml:"tags,omitempty"`
	OrderIndex  int         `yaml:"order_index"`
	EventTime   *time.Time  `yaml:"event_time,omitempty"`
}

// HandleGetNotebookTags returns all distinct tags used across sections of a notebook.
func (h *NotebookHandler) HandleGetNotebookTags(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")

	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}
	if (nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleViewer)) || (nb.PrismID != "" && !h.requireRoleOnPrism(r, nb.PrismID, rbac.RoleViewer)) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	tags, err := h.pg.GetNotebookSectionTags(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to retrieve tags")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"tags": tags})
}

// HandleExportNotebook exports a notebook as YAML
func (h *NotebookHandler) HandleExportNotebook(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")
	notebook, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		api.WriteError(w, http.StatusNotFound, "Notebook not found")
		return
	}

	if (notebook.FractalID != "" && !h.requireRoleOnFractal(r, notebook.FractalID, rbac.RoleViewer)) || (notebook.PrismID != "" && !h.requireRoleOnPrism(r, notebook.PrismID, rbac.RoleViewer)) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	sections, err := h.pg.GetNotebookSections(r.Context(), notebookID)
	if err != nil {
		sections = []storage.NotebookSection{}
	}

	export := notebookYAML{
		Kind:        "Notebook",
		Name:        notebook.Name,
		Description: notebook.Description,
		TimeRange:   notebook.TimeRangeType,
		MaxResults:  notebook.MaxResultsPerSection,
		Timezone:    storage.SafeTimezone(notebook.Timezone),
	}

	if notebook.Variables != nil && len(notebook.Variables) > 0 {
		var vars []nbVariableYAML
		if err := json.Unmarshal(notebook.Variables, &vars); err == nil && len(vars) > 0 {
			export.Variables = vars
		}
	}

	for _, s := range sections {
		sec := sectionYAML{
			Type:       s.SectionType,
			Content:    s.Content,
			OrderIndex: s.OrderIndex,
		}
		if s.Title != nil {
			sec.Title = *s.Title
		}
		if s.ChartType != nil {
			sec.ChartType = *s.ChartType
		}
		if len(s.ChartConfig) > 0 {
			var cfg interface{}
			if err := json.Unmarshal(s.ChartConfig, &cfg); err == nil {
				sec.ChartConfig = cfg
			}
		}
		if len(s.Tags) > 0 {
			sec.Tags = s.Tags
		}
		sec.EventTime = s.EventTime
		export.Sections = append(export.Sections, sec)
	}

	out, err := yaml.Marshal(export)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "Failed to export")
		return
	}

	w.Header().Set("Content-Type", "text/yaml")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.yaml"`, notebook.Name))
	w.Write(out)
}

// HandleImportNotebook imports a notebook from YAML
func (h *NotebookHandler) HandleImportNotebook(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(*storage.User)
	fractalRole := rbac.RoleFromContext(r.Context())
	prismRole := rbac.PrismRoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) && !rbac.HasAccess(user, prismRole, rbac.RoleAnalyst) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	selectedFractal, _ := h.getSelectedFractal(r)
	selectedPrism, _ := r.Context().Value("selected_prism").(string)
	if selectedFractal == "" && selectedPrism == "" {
		api.WriteError(w, http.StatusBadRequest, "No fractal or prism selected")
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1MB limit
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "Failed to read request")
		return
	}

	var imported notebookYAML
	if err := yaml.Unmarshal(body, &imported); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid YAML format")
		return
	}

	if imported.Name == "" {
		api.WriteError(w, http.StatusBadRequest, "Notebook name is required")
		return
	}

	if imported.TimeRange == "" {
		imported.TimeRange = "24h"
	}
	if imported.MaxResults <= 0 {
		imported.MaxResults = 1000
	}

	var varsJSON json.RawMessage
	if len(imported.Variables) > 0 {
		varsJSON, _ = json.Marshal(imported.Variables)
	}

	nb := storage.Notebook{
		Name:                 imported.Name,
		Description:          imported.Description,
		TimeRangeType:        imported.TimeRange,
		MaxResultsPerSection: imported.MaxResults,
		Variables:            varsJSON,
		// An unknown or absent zone falls back to UTC rather than failing the
		// import, matching how a stale stored zone is handled on read.
		Timezone:  storage.SafeTimezone(imported.Timezone),
		CreatedBy: auth.AttributionUsername(r.Context()),
	}
	if selectedPrism != "" {
		nb.PrismID = selectedPrism
	} else {
		nb.FractalID = selectedFractal
	}

	created, err := h.pg.InsertNotebook(r.Context(), nb)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Printf("[Notebooks] Failed to create notebook from import: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to create notebook")
		return
	}

	hasAISummary := false
	hasAIAttackChain := false
	for i, sec := range imported.Sections {
		if sec.Type == "ai_summary" {
			if hasAISummary {
				continue
			}
			hasAISummary = true
		}
		if sec.Type == "ai_attack_chain" {
			if hasAIAttackChain {
				continue
			}
			hasAIAttackChain = true
		}
		section := storage.NotebookSection{
			NotebookID:  created.ID,
			SectionType: sec.Type,
			Content:     sec.Content,
			OrderIndex:  i,
		}
		if sec.Title != "" {
			title := sec.Title
			section.Title = &title
		}
		if sec.ChartType != "" {
			ct := sec.ChartType
			section.ChartType = &ct
		}
		if sec.ChartConfig != nil {
			if cfgJSON, err := json.Marshal(sec.ChartConfig); err == nil {
				section.ChartConfig = cfgJSON
			}
		}
		if len(sec.Tags) > 0 {
			section.Tags = sec.Tags
		}
		section.EventTime = normalizeEventTime(sec.EventTime)
		if _, err := h.pg.InsertNotebookSection(r.Context(), section); err != nil {
			fmt.Printf("[Notebooks] Failed to import section %d: %v\n", i, err)
		}
	}

	api.WriteJSON(w, http.StatusOK, Response{Success: true, Data: map[string]interface{}{"notebook": created}})
}

// HandleGenerateFromComments generates a notebook from all comments with a given tag.
func (h *NotebookHandler) HandleGenerateFromComments(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(*storage.User)

	fractalRole := rbac.RoleFromContext(r.Context())
	prismRole := rbac.PrismRoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) && !rbac.HasAccess(user, prismRole, rbac.RoleAnalyst) {
		api.WriteError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	var req GenerateFromCommentsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	req.Tag = strings.TrimSpace(req.Tag)
	if req.Tag == "" {
		api.WriteError(w, http.StatusBadRequest, "tag is required")
		return
	}

	selectedFractal, _ := h.getSelectedFractal(r)
	selectedPrism, _ := r.Context().Value("selected_prism").(string)
	if selectedFractal == "" && selectedPrism == "" {
		api.WriteError(w, http.StatusBadRequest, "No fractal or prism selected")
		return
	}

	// Comments are fractal-scoped; in prism context use default fractal for comment lookup
	commentFractal := selectedFractal
	if commentFractal == "" && h.fractalManager != nil {
		if df, err := h.fractalManager.GetDefaultFractal(r.Context()); err == nil {
			commentFractal = df.ID
		}
	}

	comments, err := h.pg.GetCommentsByTagAndFractal(r.Context(), commentFractal, req.Tag)
	if err != nil {
		log.Printf("[Notebooks] Failed to get comments by tag: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to fetch comments")
		return
	}

	if len(comments) == 0 {
		api.WriteJSON(w, http.StatusNotFound, Response{Success: false, Error: fmt.Sprintf("No comments found with tag: %s", req.Tag)})
		return
	}

	notebookName := fmt.Sprintf("Notebook: %s", req.Tag)

	// Generating replaces any notebook of the same name outright, discarding
	// whatever was added to it by hand since the last run. That is destructive
	// enough to require the caller to say so: without Overwrite this reports the
	// conflict and changes nothing.
	existing, err := h.pg.GetNotebookByNameAndScope(r.Context(), notebookName, selectedFractal, selectedPrism)
	if err != nil {
		log.Printf("[Notebooks] Failed to check existing notebook: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to check for existing notebook")
		return
	}
	if existing != nil {
		if !req.Overwrite {
			api.WriteJSON(w, http.StatusConflict, Response{
				Success: false,
				Error:   fmt.Sprintf("A notebook named %q already exists. Regenerating replaces it and discards any sections added to it since.", notebookName),
				Data: map[string]interface{}{
					"conflict":    true,
					"notebook_id": existing.ID,
					"name":        existing.Name,
				},
			})
			return
		}
		if err := h.pg.DeleteNotebook(r.Context(), existing.ID); err != nil {
			log.Printf("[Notebooks] Failed to delete existing notebook: %v", err)
			api.WriteError(w, http.StatusInternalServerError, "Failed to replace the existing notebook")
			return
		}
	}

	notebook := storage.Notebook{
		Name:                 notebookName,
		Description:          fmt.Sprintf("Auto-generated from comments tagged \"%s\"", req.Tag),
		TimeRangeType:        "all",
		MaxResultsPerSection: 1000,
		CreatedBy:            auth.AttributionUsername(r.Context()),
	}
	if selectedPrism != "" {
		notebook.PrismID = selectedPrism
	} else {
		notebook.FractalID = selectedFractal
	}

	newNotebook, err := h.pg.InsertNotebook(r.Context(), notebook)
	if err != nil {
		log.Printf("[Notebooks] Failed to create notebook: %v", err)
		api.WriteError(w, http.StatusInternalServerError, "Failed to create notebook")
		return
	}

	orderIdx := 0

	// If AI is enabled, add an AI summary or attack chain section first
	var aiSectionID string
	var aiSectionType string
	if h.aiEnabled() {
		if req.AttackChain {
			aiSectionType = "ai_attack_chain"
			aiTitle := "AI Attack Chain Summary"
			aiSection := storage.NotebookSection{
				NotebookID:  newNotebook.ID,
				SectionType: "ai_attack_chain",
				Title:       &aiTitle,
				Content:     "",
				OrderIndex:  orderIdx,
			}
			created, err := h.pg.InsertNotebookSection(r.Context(), aiSection)
			if err != nil {
				log.Printf("[Notebooks] Failed to create AI attack chain section: %v", err)
			} else {
				aiSectionID = created.ID
				orderIdx++
			}
		} else {
			aiSectionType = "ai_summary"
			aiTitle := "AI Summary"
			aiSection := storage.NotebookSection{
				NotebookID:  newNotebook.ID,
				SectionType: "ai_summary",
				Title:       &aiTitle,
				Content:     "",
				OrderIndex:  orderIdx,
			}
			created, err := h.pg.InsertNotebookSection(r.Context(), aiSection)
			if err != nil {
				log.Printf("[Notebooks] Failed to create AI summary section: %v", err)
			} else {
				aiSectionID = created.ID
				orderIdx++
			}
		}
	}

	// Create a comment_context section for each comment, collecting section/log_id
	// pairs plus the event-time span they cover so the prefetch can bound its scan.
	type sectionLogID struct {
		SectionID string
		LogID     string
	}
	var logIDSections []sectionLogID
	var logsFrom, logsTo time.Time

	for _, c := range comments {
		displayName := c.AuthorDisplayName
		if displayName == "" {
			displayName = c.Author
		}

		titleText := c.Text
		if trimmed := truncateRunes(titleText, 80); trimmed != titleText {
			titleText = trimmed + "..."
		}
		sectionTitle := truncateRunes(fmt.Sprintf("%s: %s", displayName, titleText), maxSectionTitleChars)

		gravatarInitial := c.AuthorGravatarInitial
		if gravatarInitial == "" {
			if runes := []rune(c.Author); len(runes) > 0 {
				gravatarInitial = strings.ToUpper(string(runes[0]))
			}
		}

		contextData := map[string]string{
			"author":                  c.Author,
			"author_display_name":     displayName,
			"author_gravatar_color":   c.AuthorGravatarColor,
			"author_gravatar_initial": gravatarInitial,
			"comment_text":            c.Text,
			"query":                   c.Query,
			"log_id":                  c.LogID,
			"commented_at":            c.CreatedAt.Format(time.RFC3339),
			// Event time, not comment time: pivoting back into search has to
			// centre on when the log happened, which can predate the comment by
			// days. Sections generated before this was stored fall back to the
			// prefetched row's timestamp.
			"log_timestamp": c.LogTimestamp.UTC().Format(time.RFC3339),
		}
		contentBytes, err := json.Marshal(contextData)
		if err != nil {
			log.Printf("[Notebooks] Failed to marshal comment context: %v", err)
			continue
		}

		section := storage.NotebookSection{
			NotebookID:  newNotebook.ID,
			SectionType: "comment_context",
			Title:       &sectionTitle,
			Content:     string(contentBytes),
			OrderIndex:  orderIdx,
			EventTime:   normalizeEventTime(&c.LogTimestamp),
		}
		created, err := h.pg.InsertNotebookSection(r.Context(), section)
		if err != nil {
			log.Printf("[Notebooks] Failed to create comment_context section: %v", err)
		} else if c.LogID != "" {
			logIDSections = append(logIDSections, sectionLogID{
				SectionID: created.ID,
				LogID:     c.LogID,
			})
			if logsFrom.IsZero() || c.LogTimestamp.Before(logsFrom) {
				logsFrom = c.LogTimestamp
			}
			if logsTo.IsZero() || c.LogTimestamp.After(logsTo) {
				logsTo = c.LogTimestamp
			}
		}
		orderIdx++
	}

	// Prefetch log data for all comment_context sections asynchronously. The
	// lookup is one bounded batch rather than a point query per comment: logs is
	// ordered by (timestamp, log_id), so the event-time span the comments cover
	// prunes granules instead of bloom-checking the fractal's whole history once
	// per section.
	if h.ch != nil && len(logIDSections) > 0 {
		go func(sections []sectionLogID, fractalID string, from, to time.Time) {
			ids := make([]string, 0, len(sections))
			for _, s := range sections {
				ids = append(ids, s.LogID)
			}

			readCtx, cancelRead := context.WithTimeout(context.Background(), commentPrefetchTimeout)
			defer cancelRead()

			rowsByID, err := h.ch.GetLogDisplayRowsByIDs(readCtx, fractalID, ids, from, to)
			if err != nil {
				log.Printf("[Notebooks] Failed to prefetch %d comment logs: %v", len(ids), err)
				return
			}

			// The writes get their own budget: a read that used most of its own
			// would otherwise cancel every save and discard the rows just fetched.
			ctx, cancel := context.WithTimeout(context.Background(), commentPrefetchTimeout)
			defer cancel()

			for _, s := range sections {
				row, ok := rowsByID[s.LogID]
				if !ok {
					continue
				}
				resultData := map[string]interface{}{
					"results":       []map[string]interface{}{row},
					"count":         1,
					"execution_ms":  0,
					"chart_type":    "table",
					"is_aggregated": false,
				}
				resultsJSON, err := json.Marshal(resultData)
				if err != nil {
					log.Printf("[Notebooks] Failed to marshal log results: %v", err)
					continue
				}
				if err := h.pg.UpdateSectionQueryResults(ctx, s.SectionID, string(resultsJSON), nil, nil); err != nil {
					log.Printf("[Notebooks] Failed to save prefetched log results: %v", err)
				}
			}
		}(logIDSections, commentFractal, logsFrom, logsTo)
	}

	// Generate AI summary or attack chain asynchronously if enabled
	if h.aiEnabled() && aiSectionID != "" {
		go func() {
			sections, err := h.pg.GetNotebookSections(context.Background(), newNotebook.ID)
			if err != nil {
				log.Printf("[Notebooks] Failed to load sections for AI summary: %v", err)
				return
			}

			var cellTexts []string
			sectionMap := make(map[string]string) // section_id -> description for attack chain
			for _, s := range sections {
				if s.SectionType == "ai_summary" || s.SectionType == "ai_attack_chain" {
					continue
				}
				label := "Markdown"
				if s.SectionType == "query" {
					label = "Query"
				} else if s.SectionType == "comment_context" {
					label = "Comment"
				}
				title := "Untitled"
				if s.Title != nil && *s.Title != "" {
					title = *s.Title
				}
				cellTexts = append(cellTexts, fmt.Sprintf("[%s: %s] (section_id=%s)\n%s", label, title, s.ID, s.Content))
				if s.SectionType == "comment_context" {
					sectionMap[s.ID] = title
				}
			}

			if len(cellTexts) == 0 {
				return
			}

			notebookContent := strings.Join(cellTexts, "\n\n---\n\n")

			if aiSectionType == "ai_attack_chain" {
				result, err := h.callLiteLLMWithTools(context.Background(), notebookContent, sectionMap)
				if err != nil {
					log.Printf("[Notebooks] AI attack chain generation failed: %v", err)
					return
				}
				aiTitle := "AI Attack Chain Summary"
				if err := h.pg.UpdateNotebookSection(context.Background(), aiSectionID, storage.NotebookSectionUpdate{Title: &aiTitle, Content: &result}); err != nil {
					log.Printf("[Notebooks] Failed to save AI attack chain: %v", err)
				}
			} else {
				summary, err := h.callLiteLLM(context.Background(), notebookContent)
				if err != nil {
					log.Printf("[Notebooks] AI summary generation failed: %v", err)
					return
				}
				aiTitle := "AI Summary"
				if err := h.pg.UpdateNotebookSection(context.Background(), aiSectionID, storage.NotebookSectionUpdate{Title: &aiTitle, Content: &summary}); err != nil {
					log.Printf("[Notebooks] Failed to save AI summary: %v", err)
				}
			}
		}()
	}

	api.WriteJSON(w, http.StatusOK, Response{
		Success: true,
		Message: "Notebook generated successfully",
		Data: map[string]interface{}{
			"notebook_id": newNotebook.ID,
			"name":        newNotebook.Name,
			"sections":    orderIdx,
		},
	})
}
