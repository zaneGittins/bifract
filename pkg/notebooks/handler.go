package notebooks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"bifract/pkg/fractals"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
	"gopkg.in/yaml.v3"
)

type NotebookHandler struct {
	pg             *storage.PostgresClient
	ch             *storage.ClickHouseClient
	fractalManager *fractals.Manager
	rbacResolver   *rbac.Resolver
	litellmURL     string
	litellmKey     string
}

// SetRBACResolver sets the RBAC resolver for fractal-level access checks.
func (h *NotebookHandler) SetRBACResolver(resolver *rbac.Resolver) {
	h.rbacResolver = resolver
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
	if h.rbacResolver == nil {
		fractalRole := rbac.RoleFromContext(r.Context())
		return rbac.HasAccess(user, fractalRole, required)
	}
	role := h.rbacResolver.ResolveRole(r.Context(), user, fractalID)
	return rbac.HasAccess(user, role, required)
}

type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Error   string      `json:"error,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to determine fractal context",
		})
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

	// Get notebooks with pagination
	var notebooks []storage.Notebook
	var total int
	if prismID, ok := r.Context().Value("selected_prism").(string); ok && prismID != "" {
		notebooks, total, err = h.pg.GetNotebooksByPrism(r.Context(), prismID, limit, offset)
	} else {
		notebooks, total, err = h.pg.GetNotebooksByFractal(r.Context(), selectedFractal, limit, offset)
	}
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to fetch notebooks",
		})
		return
	}

	// TODO: Implement search filtering if searchQuery is provided
	// For now, we'll return all notebooks
	_ = searchQuery

	// Return empty array if no notebooks
	if notebooks == nil {
		notebooks = []storage.Notebook{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":   true,
		"data":      notebooks,
		"total":     total,
		"limit":     limit,
		"offset":    offset,
	})
}

// HandleCreateNotebook creates a new notebook (analyst+)
func (h *NotebookHandler) HandleCreateNotebook(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(*storage.User)

	fractalRole := rbac.RoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Insufficient permissions"})
		return
	}

	var req CreateNotebookRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Invalid request body",
		})
		return
	}

	// Validate input
	if req.Name == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "name is required",
		})
		return
	}

	if req.TimeRangeType == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "time_range_type is required",
		})
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "time_range_type must be one of: 1h, 24h, 7d, 30d, custom",
		})
		return
	}

	// For custom time range, validate start and end times
	if req.TimeRangeType == "custom" {
		if req.TimeRangeStart == nil || req.TimeRangeEnd == nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response{
				Success: false,
				Error:   "time_range_start and time_range_end are required for custom time range",
			})
			return
		}
	}

	// Set default max results if not provided
	if req.MaxResultsPerSection <= 0 {
		req.MaxResultsPerSection = 1000
	}

	// Get selected fractal for notebook isolation
	selectedFractal, err := h.getSelectedFractal(r)
	if err != nil {
		log.Printf("[Notebooks] Failed to get selected fractal: %v", err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to determine fractal context",
		})
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
		CreatedBy:            user.Username,
	}
	if prismID, ok := r.Context().Value("selected_prism").(string); ok && prismID != "" {
		notebook.PrismID = prismID
	} else {
		notebook.FractalID = selectedFractal
	}

	newNotebook, err := h.pg.InsertNotebook(r.Context(), notebook)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to create notebook",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Notebook not found",
		})
		return
	}

	// Verify user has access to the notebook's fractal
	if notebook.FractalID != "" && !h.requireRoleOnFractal(r, notebook.FractalID, rbac.RoleViewer) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Insufficient permissions"})
		return
	}

	// Get sections for the notebook
	sections, err := h.pg.GetNotebookSections(r.Context(), notebookID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to fetch notebook sections",
		})
		return
	}

	// Convert storage types to response types
	notebookResponse := Notebook{
		ID:                   notebook.ID,
		Name:                 notebook.Name,
		Description:          notebook.Description,
		TimeRangeType:        notebook.TimeRangeType,
		TimeRangeStart:       notebook.TimeRangeStart,
		TimeRangeEnd:         notebook.TimeRangeEnd,
		MaxResultsPerSection: notebook.MaxResultsPerSection,
		FractalID:            notebook.FractalID,
		CreatedBy:            notebook.CreatedBy,
		AuthorDisplayName:    notebook.AuthorDisplayName,
		AuthorGravatarColor:  notebook.AuthorGravatarColor,
		AuthorGravatarInitial: notebook.AuthorGravatarInitial,
		CreatedAt:            notebook.CreatedAt,
		UpdatedAt:            notebook.UpdatedAt,
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
			CreatedAt:       section.CreatedAt,
			UpdatedAt:       section.UpdatedAt,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Data:    notebookResponse,
	})
}

// HandleUpdateNotebook updates notebook metadata (author only)
func (h *NotebookHandler) HandleUpdateNotebook(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(*storage.User)
	notebookID := chi.URLParam(r, "id")

	// Verify fractal access
	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Notebook not found"})
		return
	}
	if nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleAnalyst) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Insufficient permissions"})
		return
	}

	var req UpdateNotebookRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Invalid request body",
		})
		return
	}

	// Update notebook
	err = h.pg.UpdateNotebook(r.Context(), notebookID, user.Username, req.Name, req.Description, req.TimeRangeType, req.TimeRangeStart, req.TimeRangeEnd, req.MaxResultsPerSection)
	if err != nil {
		if strings.Contains(err.Error(), "not found or unauthorized") {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response{
				Success: false,
				Error:   "Notebook not found or unauthorized",
			})
		} else {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response{
				Success: false,
				Error:   "Failed to update notebook",
			})
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Notebook updated successfully",
	})
}

// HandleDeleteNotebook deletes a notebook (author only)
func (h *NotebookHandler) HandleDeleteNotebook(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(*storage.User)
	notebookID := chi.URLParam(r, "id")

	// Verify fractal access
	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Notebook not found"})
		return
	}
	if nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleAnalyst) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Insufficient permissions"})
		return
	}

	err = h.pg.DeleteNotebook(r.Context(), notebookID, user.Username)
	if err != nil {
		if strings.Contains(err.Error(), "not found or unauthorized") {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response{
				Success: false,
				Error:   "Notebook not found or unauthorized",
			})
		} else {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response{
				Success: false,
				Error:   "Failed to delete notebook",
			})
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Notebook deleted successfully",
	})
}

// HandleCreateSection creates a new section in a notebook
func (h *NotebookHandler) HandleCreateSection(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")

	// Verify fractal access
	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Notebook not found"})
		return
	}
	if nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleAnalyst) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Insufficient permissions"})
		return
	}

	var req CreateSectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Invalid request body",
		})
		return
	}

	// Validate input
	if req.SectionType != "markdown" && req.SectionType != "query" && req.SectionType != "ai_summary" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "section_type must be 'markdown', 'query', or 'ai_summary'",
		})
		return
	}

	if req.SectionType == "ai_summary" {
		if !h.aiEnabled() {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response{Success: false, Error: "AI is not configured"})
			return
		}
		sections, err := h.pg.GetNotebookSections(r.Context(), notebookID)
		if err == nil {
			for _, s := range sections {
				if s.SectionType == "ai_summary" {
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode(Response{Success: false, Error: "Only one AI Summary section is allowed per notebook"})
					return
				}
			}
		}
		if req.Content == "" {
			req.Content = " "
		}
	}

	if req.Content == "" && req.SectionType != "ai_summary" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "content is required",
		})
		return
	}

	// Create section
	section := storage.NotebookSection{
		NotebookID:  notebookID,
		SectionType: req.SectionType,
		Title:       req.Title,
		Content:     req.Content,
		OrderIndex:  req.OrderIndex,
	}

	newSection, err := h.pg.InsertNotebookSection(r.Context(), section)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to create section",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Section created successfully",
		Data:    newSection,
	})
}

// HandleUpdateSection updates a notebook section
func (h *NotebookHandler) HandleUpdateSection(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")
	sectionID := chi.URLParam(r, "section_id")

	// Verify fractal access via parent notebook
	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Notebook not found"})
		return
	}
	if nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleAnalyst) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Insufficient permissions"})
		return
	}

	var req UpdateSectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Invalid request body",
		})
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
	err = h.pg.UpdateNotebookSection(r.Context(), sectionID, req.Title, req.Content, renderedContent, chartConfigJSON)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to update section",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Section updated successfully",
	})
}

// HandleDeleteSection deletes a notebook section
func (h *NotebookHandler) HandleDeleteSection(w http.ResponseWriter, r *http.Request) {
	sectionID := chi.URLParam(r, "section_id")

	// Fetch section to get parent notebook, then check fractal access
	section, err := h.pg.GetNotebookSection(r.Context(), sectionID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Section not found"})
		return
	}
	nb, err := h.pg.GetNotebook(r.Context(), section.NotebookID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Notebook not found"})
		return
	}
	if nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleAnalyst) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Insufficient permissions"})
		return
	}

	err = h.pg.DeleteNotebookSection(r.Context(), sectionID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to delete section",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Section deleted successfully",
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Notebook not found",
		})
		return
	}

	if notebook.FractalID != "" && !h.requireRoleOnFractal(r, notebook.FractalID, rbac.RoleAnalyst) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Insufficient permissions"})
		return
	}

	// Get the specific section
	section, err := h.pg.GetNotebookSection(r.Context(), sectionID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Section not found",
		})
		return
	}

	// Verify this section belongs to the notebook
	if section.NotebookID != notebookID || section.NotebookID != notebook.ID {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Section does not belong to notebook",
		})
		return
	}

	// Verify this is a query section
	if section.SectionType != "query" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Section is not a query section",
		})
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to update section results",
		})
		return
	}

	// Return the query results
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Notebook not found"})
		return
	}
	if nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleAnalyst) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Insufficient permissions"})
		return
	}

	var req ReorderSectionsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Invalid request body",
		})
		return
	}

	// Validate input
	if len(req.SectionOrder) == 0 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "section_order is required",
		})
		return
	}

	// Reorder sections
	err = h.pg.ReorderNotebookSections(r.Context(), notebookID, req.SectionOrder)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to reorder sections",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Sections reordered successfully",
	})
}

// HandleUpdateSectionResults updates query section results
func (h *NotebookHandler) HandleUpdateSectionResults(w http.ResponseWriter, r *http.Request) {
	sectionID := chi.URLParam(r, "section_id")

	section, err := h.pg.GetNotebookSection(r.Context(), sectionID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Section not found"})
		return
	}
	nb, err := h.pg.GetNotebook(r.Context(), section.NotebookID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Notebook not found"})
		return
	}
	if nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleAnalyst) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Insufficient permissions"})
		return
	}

	var req struct {
		LastExecutedAt string `json:"last_executed_at"`
		LastResults    string `json:"last_results"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Invalid request body",
		})
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to update section results",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Section results updated successfully",
	})
}

// HandleUpdatePresence updates user presence for a notebook
func (h *NotebookHandler) HandleUpdatePresence(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(*storage.User)
	notebookID := chi.URLParam(r, "id")

	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Notebook not found"})
		return
	}
	if nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleViewer) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Insufficient permissions"})
		return
	}

	err = h.pg.UpdateNotebookPresence(r.Context(), notebookID, user.Username)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to update presence",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
	})
}

// HandleGetPresence gets active users for a notebook
func (h *NotebookHandler) HandleGetPresence(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")

	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Notebook not found"})
		return
	}
	if nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleViewer) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Insufficient permissions"})
		return
	}

	presence, err := h.pg.GetNotebookPresence(r.Context(), notebookID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: false,
			Error:   "Failed to fetch presence",
		})
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

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Data:    responsePresence,
	})
}

// calculateTimeRange determines the start and end times based on notebook settings
func (h *NotebookHandler) calculateTimeRange(notebook *storage.Notebook) (time.Time, time.Time) {
	now := time.Now()

	switch notebook.TimeRangeType {
	case "1h":
		return now.Add(-1 * time.Hour), now
	case "24h":
		return now.Add(-24 * time.Hour), now
	case "7d":
		return now.Add(-7 * 24 * time.Hour), now
	case "30d":
		return now.Add(-30 * 24 * time.Hour), now
	case "custom":
		if notebook.TimeRangeStart != nil && notebook.TimeRangeEnd != nil {
			return *notebook.TimeRangeStart, *notebook.TimeRangeEnd
		}
		// Fallback to 24h if custom range is invalid
		return now.Add(-24 * time.Hour), now
	default:
		// Default to 24h if invalid range type
		return now.Add(-24 * time.Hour), now
	}
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
	user := r.Context().Value("user").(*storage.User)

	nb, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Notebook not found"})
		return
	}
	if nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleAnalyst) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Insufficient permissions"})
		return
	}

	var req UpdateVariablesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Invalid request body"})
		return
	}

	if req.Variables == nil {
		req.Variables = json.RawMessage("[]")
	}

	if err = h.pg.UpdateNotebookVariables(r.Context(), notebookID, user.Username, req.Variables); err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to update variables"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Message: "Variables updated"})
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Notebook not found"})
		return
	}
	if nb.FractalID != "" && !h.requireRoleOnFractal(r, nb.FractalID, rbac.RoleAnalyst) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Insufficient permissions"})
		return
	}
	if !h.aiEnabled() {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "AI is not configured"})
		return
	}

	section, err := h.pg.GetNotebookSection(r.Context(), sectionID)
	if err != nil || section.NotebookID != notebookID {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Section not found"})
		return
	}
	if section.SectionType != "ai_summary" && section.SectionType != "ai_attack_chain" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Section is not an AI Summary section"})
		return
	}

	sections, err := h.pg.GetNotebookSections(r.Context(), notebookID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to load sections"})
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "No sections to summarize"})
		return
	}

	notebookContent := strings.Join(cellTexts, "\n\n---\n\n")

	if section.SectionType == "ai_attack_chain" {
		result, err := h.callLiteLLMWithTools(r.Context(), notebookContent, sectionMap)
		if err != nil {
			log.Printf("[Notebooks] AI attack chain generation failed: %v", err)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to generate attack chain summary"})
			return
		}

		err = h.pg.UpdateNotebookSection(r.Context(), sectionID, section.Title, &result, nil, nil)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to save attack chain summary"})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
			Success: true,
			Message: "Attack chain summary generated",
			Data:    map[string]string{"summary": result},
		})
	} else {
		summary, err := h.callLiteLLM(r.Context(), notebookContent)
		if err != nil {
			log.Printf("[Notebooks] AI summary generation failed: %v", err)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to generate summary"})
			return
		}

		err = h.pg.UpdateNotebookSection(r.Context(), sectionID, section.Title, &summary, nil, nil)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to save summary"})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{
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
	Variables   []nbVariableYAML `yaml:"variables,omitempty"`
	Sections    []sectionYAML    `yaml:"sections"`
}

type sectionYAML struct {
	Type        string      `yaml:"type"`
	Title       string      `yaml:"title,omitempty"`
	Content     string      `yaml:"content"`
	ChartType   string      `yaml:"chart_type,omitempty"`
	ChartConfig interface{} `yaml:"chart_config,omitempty"`
	OrderIndex  int         `yaml:"order_index"`
}

// HandleExportNotebook exports a notebook as YAML
func (h *NotebookHandler) HandleExportNotebook(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "id")
	notebook, err := h.pg.GetNotebook(r.Context(), notebookID)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Notebook not found"})
		return
	}

	if notebook.FractalID != "" && !h.requireRoleOnFractal(r, notebook.FractalID, rbac.RoleViewer) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Insufficient permissions"})
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
		export.Sections = append(export.Sections, sec)
	}

	out, err := yaml.Marshal(export)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to export"})
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
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Insufficient permissions"})
		return
	}

	selectedFractal, err := h.getSelectedFractal(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "No fractal selected"})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1MB limit
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to read request"})
		return
	}

	var imported notebookYAML
	if err := yaml.Unmarshal(body, &imported); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Invalid YAML format"})
		return
	}

	if imported.Name == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Notebook name is required"})
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
		FractalID:            selectedFractal,
		Variables:            varsJSON,
		CreatedBy:            user.Username,
	}

	created, err := h.pg.InsertNotebook(r.Context(), nb)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Printf("[Notebooks] Failed to create notebook from import: %v", err)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to create notebook"})
		return
	}

	hasAISummary := false
	for i, sec := range imported.Sections {
		if sec.Type == "ai_summary" {
			if hasAISummary {
				continue
			}
			hasAISummary = true
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
		if _, err := h.pg.InsertNotebookSection(r.Context(), section); err != nil {
			fmt.Printf("[Notebooks] Failed to import section %d: %v\n", i, err)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Data: map[string]interface{}{"notebook": created}})
}

// HandleGenerateFromComments generates a notebook from all comments with a given tag.
func (h *NotebookHandler) HandleGenerateFromComments(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(*storage.User)

	fractalRole := rbac.RoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Insufficient permissions"})
		return
	}

	var req GenerateFromCommentsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Invalid request body"})
		return
	}

	req.Tag = strings.TrimSpace(req.Tag)
	if req.Tag == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "tag is required"})
		return
	}

	selectedFractal, err := h.getSelectedFractal(r)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to determine fractal context"})
		return
	}

	comments, err := h.pg.GetCommentsByTagAndFractal(r.Context(), selectedFractal, req.Tag)
	if err != nil {
		log.Printf("[Notebooks] Failed to get comments by tag: %v", err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to fetch comments"})
		return
	}

	if len(comments) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(Response{Success: false, Error: fmt.Sprintf("No comments found with tag: %s", req.Tag)})
		return
	}

	notebookName := fmt.Sprintf("Notebook: %s", req.Tag)

	// Delete existing notebook with this name if it exists
	existing, err := h.pg.GetNotebookByNameAndFractal(r.Context(), notebookName, selectedFractal)
	if err != nil {
		log.Printf("[Notebooks] Failed to check existing notebook: %v", err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to check for existing notebook"})
		return
	}
	if existing != nil {
		if err := h.pg.DeleteNotebookByID(r.Context(), existing.ID); err != nil {
			log.Printf("[Notebooks] Failed to delete existing notebook: %v", err)
		}
	}

	notebook := storage.Notebook{
		Name:                 notebookName,
		Description:          fmt.Sprintf("Auto-generated from comments tagged \"%s\"", req.Tag),
		TimeRangeType:        "all",
		MaxResultsPerSection: 1000,
		FractalID:            selectedFractal,
		CreatedBy:            user.Username,
	}

	newNotebook, err := h.pg.InsertNotebook(r.Context(), notebook)
	if err != nil {
		log.Printf("[Notebooks] Failed to create notebook: %v", err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to create notebook"})
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

	// Create a comment_context section for each comment, collecting section/log_id pairs for prefetch
	type sectionLogID struct {
		SectionID string
		LogID     string
		FractalID string
	}
	var logIDSections []sectionLogID

	for _, c := range comments {
		displayName := c.AuthorDisplayName
		if displayName == "" {
			displayName = c.Author
		}

		titleText := c.Text
		if len(titleText) > 80 {
			titleText = titleText[:80] + "..."
		}
		sectionTitle := fmt.Sprintf("%s: %s", displayName, titleText)

		gravatarInitial := c.AuthorGravatarInitial
		if gravatarInitial == "" && len(c.Author) > 0 {
			gravatarInitial = strings.ToUpper(string(c.Author[0]))
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
		}
		created, err := h.pg.InsertNotebookSection(r.Context(), section)
		if err != nil {
			log.Printf("[Notebooks] Failed to create comment_context section: %v", err)
		} else if c.LogID != "" {
			logIDSections = append(logIDSections, sectionLogID{
				SectionID: created.ID,
				LogID:     c.LogID,
				FractalID: selectedFractal,
			})
		}
		orderIdx++
	}

	// Prefetch log data for all comment_context sections asynchronously
	if h.ch != nil && len(logIDSections) > 0 {
		go func(sections []sectionLogID) {
			for _, s := range sections {
				sql := fmt.Sprintf(
					"SELECT timestamp, raw_log, log_id, toString(fields) AS fields FROM %s WHERE fractal_id = '%s' AND log_id = '%s' LIMIT 1",
					h.ch.ReadTable(), s.FractalID, s.LogID,
				)
				rows, err := h.ch.Query(context.Background(), sql)
				if err != nil {
					log.Printf("[Notebooks] Failed to prefetch log_id %s: %v", s.LogID, err)
					continue
				}

				resultData := map[string]interface{}{
					"results":      rows,
					"count":        len(rows),
					"execution_ms": 0,
					"chart_type":   "table",
					"is_aggregated": false,
				}
				resultsJSON, err := json.Marshal(resultData)
				if err != nil {
					log.Printf("[Notebooks] Failed to marshal log results: %v", err)
					continue
				}

				if err := h.pg.UpdateSectionQueryResults(context.Background(), s.SectionID, string(resultsJSON), nil, nil); err != nil {
					log.Printf("[Notebooks] Failed to save prefetched log results: %v", err)
				}
			}
		}(logIDSections)
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
				if err := h.pg.UpdateNotebookSection(context.Background(), aiSectionID, &aiTitle, &result, nil, nil); err != nil {
					log.Printf("[Notebooks] Failed to save AI attack chain: %v", err)
				}
			} else {
				summary, err := h.callLiteLLM(context.Background(), notebookContent)
				if err != nil {
					log.Printf("[Notebooks] AI summary generation failed: %v", err)
					return
				}
				aiTitle := "AI Summary"
				if err := h.pg.UpdateNotebookSection(context.Background(), aiSectionID, &aiTitle, &summary, nil, nil); err != nil {
					log.Printf("[Notebooks] Failed to save AI summary: %v", err)
				}
			}
		}()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: "Notebook generated successfully",
		Data: map[string]interface{}{
			"notebook_id": newNotebook.ID,
			"name":        newNotebook.Name,
			"sections":    orderIdx,
		},
	})
}