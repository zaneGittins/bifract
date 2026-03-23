package dashboards

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"bifract/pkg/fractals"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"

	"github.com/go-chi/chi/v5"
	"gopkg.in/yaml.v3"
)

type DashboardHandler struct {
	pg             *storage.PostgresClient
	fractalManager *fractals.Manager
	rbacResolver   *rbac.Resolver
}

// SetRBACResolver sets the RBAC resolver for fractal-level access checks.
func (h *DashboardHandler) SetRBACResolver(resolver *rbac.Resolver) {
	h.rbacResolver = resolver
}

// requireRoleOnFractal checks the user has the required role on a specific fractal.
func (h *DashboardHandler) requireRoleOnFractal(r *http.Request, fractalID string, required rbac.Role) bool {
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
func (h *DashboardHandler) requireRoleOnPrism(r *http.Request, prismID string, required rbac.Role) bool {
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

// getDashboardScope fetches a dashboard and returns its fractal ID and prism ID.
func (h *DashboardHandler) getDashboardScope(ctx context.Context, dashboardID string) (fractalID, prismID string, err error) {
	d, err := h.pg.GetDashboard(ctx, dashboardID)
	if err != nil {
		return "", "", err
	}
	return d.FractalID, d.PrismID, nil
}

// requireDashboardRole checks the user has the required role on the dashboard's scope (fractal or prism).
func (h *DashboardHandler) requireDashboardRole(w http.ResponseWriter, r *http.Request, fractalID, prismID string, required rbac.Role) bool {
	if fractalID != "" {
		if !h.requireRoleOnFractal(r, fractalID, required) {
			jsonForbidden(w)
			return false
		}
	} else if prismID != "" {
		if !h.requireRoleOnPrism(r, prismID, required) {
			jsonForbidden(w)
			return false
		}
	}
	return true
}

func NewDashboardHandler(pg *storage.PostgresClient, fractalManager *fractals.Manager) *DashboardHandler {
	return &DashboardHandler{
		pg:             pg,
		fractalManager: fractalManager,
	}
}

func (h *DashboardHandler) HandleListDashboards(w http.ResponseWriter, r *http.Request) {
	selectedFractal, err := h.getSelectedFractal(r)
	if err != nil {
		log.Printf("[Dashboards] Failed to get selected fractal: %v", err)
		jsonError(w, "Failed to determine fractal context")
		return
	}

	limit := 20
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 100 {
			limit = v
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if v, err := strconv.Atoi(o); err == nil && v >= 0 {
			offset = v
		}
	}

	var dashboards []storage.Dashboard
	var total int
	if prismID, ok := r.Context().Value("selected_prism").(string); ok && prismID != "" {
		dashboards, total, err = h.pg.GetDashboardsByPrism(r.Context(), prismID, limit, offset)
	} else {
		dashboards, total, err = h.pg.GetDashboardsByFractal(r.Context(), selectedFractal, limit, offset)
	}
	if err != nil {
		jsonError(w, "Failed to fetch dashboards")
		return
	}
	if dashboards == nil {
		dashboards = []storage.Dashboard{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"data":    dashboards,
		"total":   total,
		"limit":   limit,
		"offset":  offset,
	})
}

func (h *DashboardHandler) HandleCreateDashboard(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(*storage.User)

	fractalRole := rbac.RoleFromContext(r.Context())
	prismRole := rbac.PrismRoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) && !rbac.HasAccess(user, prismRole, rbac.RoleAnalyst) {
		jsonError(w, "Insufficient permissions")
		return
	}

	var req CreateDashboardRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "Invalid request body")
		return
	}

	if req.Name == "" {
		jsonError(w, "name is required")
		return
	}
	if req.TimeRangeType == "" {
		req.TimeRangeType = "last1h"
	}

	validTimeRanges := map[string]bool{"last1h": true, "last24h": true, "last7d": true, "last30d": true, "all": true, "custom": true}
	if !validTimeRanges[req.TimeRangeType] {
		jsonError(w, "time_range_type must be one of: last1h, last24h, last7d, last30d, all, custom")
		return
	}
	if req.TimeRangeType == "custom" && (req.TimeRangeStart == nil || req.TimeRangeEnd == nil) {
		jsonError(w, "time_range_start and time_range_end are required for custom time range")
		return
	}

	selectedFractal, err := h.getSelectedFractal(r)
	if err != nil {
		log.Printf("[Dashboards] Failed to get selected fractal: %v", err)
		jsonError(w, "Failed to determine fractal context")
		return
	}

	d := storage.Dashboard{
		Name:           req.Name,
		Description:    req.Description,
		TimeRangeType:  req.TimeRangeType,
		TimeRangeStart: req.TimeRangeStart,
		TimeRangeEnd:   req.TimeRangeEnd,
		CreatedBy:      user.Username,
	}
	if prismID, ok := r.Context().Value("selected_prism").(string); ok && prismID != "" {
		d.PrismID = prismID
	} else {
		d.FractalID = selectedFractal
	}

	newDashboard, err := h.pg.InsertDashboard(r.Context(), d)
	if err != nil {
		jsonError(w, "Failed to create dashboard")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Message: "Dashboard created successfully", Data: newDashboard})
}

func (h *DashboardHandler) HandleGetDashboard(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	dashboard, err := h.pg.GetDashboard(r.Context(), id)
	if err != nil {
		jsonError(w, "Dashboard not found")
		return
	}

	if (dashboard.FractalID != "" && !h.requireRoleOnFractal(r, dashboard.FractalID, rbac.RoleViewer)) || (dashboard.PrismID != "" && !h.requireRoleOnPrism(r, dashboard.PrismID, rbac.RoleViewer)) {
		jsonForbidden(w)
		return
	}

	widgets, err := h.pg.GetDashboardWidgets(r.Context(), id)
	if err != nil {
		jsonError(w, "Failed to fetch dashboard widgets")
		return
	}
	if widgets == nil {
		widgets = []storage.DashboardWidget{}
	}
	dashboard.Widgets = widgets

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Data: dashboard})
}

func (h *DashboardHandler) HandleUpdateDashboard(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	fractalID, prismID, err := h.getDashboardScope(r.Context(), id)
	if err != nil {
		jsonError(w, "Dashboard not found")
		return
	}
	if !h.requireDashboardRole(w, r, fractalID, prismID, rbac.RoleAnalyst) {
		return
	}

	var req UpdateDashboardRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "Invalid request body")
		return
	}

	err = h.pg.UpdateDashboard(r.Context(), id, req.Name, req.Description, req.TimeRangeType, req.TimeRangeStart, req.TimeRangeEnd)
	if err != nil {
		jsonError(w, "Failed to update dashboard")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Message: "Dashboard updated successfully"})
}

func (h *DashboardHandler) HandleDeleteDashboard(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	fractalID, prismID, err := h.getDashboardScope(r.Context(), id)
	if err != nil {
		jsonError(w, "Dashboard not found")
		return
	}
	if !h.requireDashboardRole(w, r, fractalID, prismID, rbac.RoleAnalyst) {
		return
	}

	err = h.pg.DeleteDashboard(r.Context(), id)
	if err != nil {
		jsonError(w, "Failed to delete dashboard")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Message: "Dashboard deleted successfully"})
}

func (h *DashboardHandler) HandleCreateWidget(w http.ResponseWriter, r *http.Request) {
	dashboardID := chi.URLParam(r, "id")

	fractalID, prismID, err := h.getDashboardScope(r.Context(), dashboardID)
	if err != nil {
		jsonError(w, "Dashboard not found")
		return
	}
	if !h.requireDashboardRole(w, r, fractalID, prismID, rbac.RoleAnalyst) {
		return
	}

	var req CreateWidgetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "Invalid request body")
		return
	}

	if req.ChartType == "" {
		req.ChartType = "table"
	}
	if req.Width <= 0 {
		req.Width = 6
	}
	if req.Height <= 0 {
		req.Height = 4
	}

	widget := storage.DashboardWidget{
		DashboardID:  dashboardID,
		Title:        req.Title,
		QueryContent: req.QueryContent,
		ChartType:    req.ChartType,
		PosX:         req.PosX,
		PosY:         req.PosY,
		Width:        req.Width,
		Height:       req.Height,
	}

	newWidget, err := h.pg.InsertDashboardWidget(r.Context(), widget)
	if err != nil {
		jsonError(w, "Failed to create widget")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Message: "Widget created successfully", Data: newWidget})
}

func (h *DashboardHandler) HandleUpdateWidget(w http.ResponseWriter, r *http.Request) {
	dashboardID := chi.URLParam(r, "id")
	widgetID := chi.URLParam(r, "widget_id")

	fractalID, prismID, err := h.getDashboardScope(r.Context(), dashboardID)
	if err != nil {
		jsonError(w, "Dashboard not found")
		return
	}
	if !h.requireDashboardRole(w, r, fractalID, prismID, rbac.RoleAnalyst) {
		return
	}

	var req UpdateWidgetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "Invalid request body")
		return
	}

	var chartConfigJSON *string
	if req.ChartConfig != nil {
		b, err := json.Marshal(req.ChartConfig)
		if err == nil {
			s := string(b)
			chartConfigJSON = &s
		}
	}

	err = h.pg.UpdateDashboardWidget(r.Context(), widgetID, req.Title, req.QueryContent, req.ChartType, chartConfigJSON)
	if err != nil {
		jsonError(w, "Failed to update widget")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Message: "Widget updated successfully"})
}

func (h *DashboardHandler) HandleUpdateWidgetResults(w http.ResponseWriter, r *http.Request) {
	dashboardID := chi.URLParam(r, "id")
	widgetID := chi.URLParam(r, "widget_id")

	fractalID, prismID, err := h.getDashboardScope(r.Context(), dashboardID)
	if err != nil {
		jsonError(w, "Dashboard not found")
		return
	}
	if !h.requireDashboardRole(w, r, fractalID, prismID, rbac.RoleAnalyst) {
		return
	}

	var req UpdateWidgetResultsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "Invalid request body")
		return
	}

	err = h.pg.UpdateDashboardWidgetResults(r.Context(), widgetID, req.LastResults, req.ChartType)
	if err != nil {
		jsonError(w, "Failed to update widget results")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Message: "Widget results updated"})
}

func (h *DashboardHandler) HandleUpdateWidgetLayout(w http.ResponseWriter, r *http.Request) {
	dashboardID := chi.URLParam(r, "id")
	widgetID := chi.URLParam(r, "widget_id")

	fractalID, prismID, err := h.getDashboardScope(r.Context(), dashboardID)
	if err != nil {
		jsonError(w, "Dashboard not found")
		return
	}
	if !h.requireDashboardRole(w, r, fractalID, prismID, rbac.RoleAnalyst) {
		return
	}

	var req UpdateWidgetLayoutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "Invalid request body")
		return
	}

	err = h.pg.UpdateDashboardWidgetLayout(r.Context(), widgetID, req.PosX, req.PosY, req.Width, req.Height)
	if err != nil {
		jsonError(w, "Failed to update widget layout")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Message: "Widget layout updated"})
}

func (h *DashboardHandler) HandleDeleteWidget(w http.ResponseWriter, r *http.Request) {
	dashboardID := chi.URLParam(r, "id")
	widgetID := chi.URLParam(r, "widget_id")

	fractalID, prismID, err := h.getDashboardScope(r.Context(), dashboardID)
	if err != nil {
		jsonError(w, "Dashboard not found")
		return
	}
	if !h.requireDashboardRole(w, r, fractalID, prismID, rbac.RoleAnalyst) {
		return
	}

	err = h.pg.DeleteDashboardWidget(r.Context(), widgetID)
	if err != nil {
		jsonError(w, "Failed to delete widget")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Message: "Widget deleted successfully"})
}

func (h *DashboardHandler) getSelectedFractal(r *http.Request) (string, error) {
	if selectedFractal := r.Context().Value("selected_fractal"); selectedFractal != nil {
		if fractalID, ok := selectedFractal.(string); ok && fractalID != "" {
			return fractalID, nil
		}
	}
	if h.fractalManager == nil {
		return "", nil
	}
	defaultFractal, err := h.fractalManager.GetDefaultFractal(r.Context())
	if err != nil {
		return "", fmt.Errorf("failed to get default fractal: %w", err)
	}
	return defaultFractal.ID, nil
}

func (h *DashboardHandler) HandleUpdatePresence(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	user, _ := r.Context().Value("user").(*storage.User)
	if user == nil {
		jsonError(w, "unauthorized")
		return
	}

	fractalID, prismID, err := h.getDashboardScope(r.Context(), id)
	if err != nil {
		jsonError(w, "Dashboard not found")
		return
	}
	if !h.requireDashboardRole(w, r, fractalID, prismID, rbac.RoleViewer) {
		return
	}

	if err := h.pg.UpdateDashboardPresence(r.Context(), id, user.Username); err != nil {
		jsonError(w, "failed to update presence")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Data: map[string]bool{"ok": true}})
}

func (h *DashboardHandler) HandleGetPresence(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	fractalID, prismID, err := h.getDashboardScope(r.Context(), id)
	if err != nil {
		jsonError(w, "Dashboard not found")
		return
	}
	if !h.requireDashboardRole(w, r, fractalID, prismID, rbac.RoleViewer) {
		return
	}

	presence, err := h.pg.GetDashboardPresence(r.Context(), id)
	if err != nil {
		jsonError(w, "failed to get presence")
		return
	}
	if presence == nil {
		presence = []storage.ResourcePresence{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Data: presence})
}

func (h *DashboardHandler) HandleUpdateVariables(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	fractalID, prismID, err := h.getDashboardScope(r.Context(), id)
	if err != nil {
		jsonError(w, "Dashboard not found")
		return
	}
	if !h.requireDashboardRole(w, r, fractalID, prismID, rbac.RoleAnalyst) {
		return
	}

	var req UpdateVariablesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "Invalid request body")
		return
	}

	if req.Variables == nil {
		req.Variables = json.RawMessage("[]")
	}

	if err = h.pg.UpdateDashboardVariables(r.Context(), id, req.Variables); err != nil {
		jsonError(w, "Failed to update variables")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Message: "Variables updated"})
}

func jsonError(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: false, Error: msg})
}

func jsonForbidden(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusForbidden)
	json.NewEncoder(w).Encode(Response{Success: false, Error: "Insufficient permissions"})
}

// YAML export/import types

type variableYAML struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

type dashboardYAML struct {
	Kind        string         `yaml:"kind"`
	Name        string         `yaml:"name"`
	Description string         `yaml:"description,omitempty"`
	TimeRange   string         `yaml:"time_range_type,omitempty"`
	Variables   []variableYAML `yaml:"variables,omitempty"`
	Widgets     []widgetYAML   `yaml:"widgets"`
}

type widgetYAML struct {
	Title       string      `yaml:"title,omitempty"`
	Query       string      `yaml:"query"`
	ChartType   string      `yaml:"chart_type,omitempty"`
	ChartConfig interface{} `yaml:"chart_config,omitempty"`
	PosX        int         `yaml:"pos_x"`
	PosY        int         `yaml:"pos_y"`
	Width       int         `yaml:"width"`
	Height      int         `yaml:"height"`
}

// HandleExportDashboard exports a dashboard as YAML
func (h *DashboardHandler) HandleExportDashboard(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	dashboard, err := h.pg.GetDashboard(r.Context(), id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Dashboard not found"})
		return
	}

	if (dashboard.FractalID != "" && !h.requireRoleOnFractal(r, dashboard.FractalID, rbac.RoleViewer)) || (dashboard.PrismID != "" && !h.requireRoleOnPrism(r, dashboard.PrismID, rbac.RoleViewer)) {
		jsonForbidden(w)
		return
	}

	widgets, err := h.pg.GetDashboardWidgets(r.Context(), id)
	if err != nil {
		widgets = []storage.DashboardWidget{}
	}

	export := dashboardYAML{
		Kind:        "Dashboard",
		Name:        dashboard.Name,
		Description: dashboard.Description,
		TimeRange:   dashboard.TimeRangeType,
	}

	// Include variables in export
	if dashboard.Variables != nil && len(dashboard.Variables) > 0 {
		var vars []variableYAML
		if err := json.Unmarshal(dashboard.Variables, &vars); err == nil && len(vars) > 0 {
			export.Variables = vars
		}
	}

	for _, w := range widgets {
		wg := widgetYAML{
			Query:     w.QueryContent,
			ChartType: w.ChartType,
			PosX:      w.PosX,
			PosY:      w.PosY,
			Width:     w.Width,
			Height:    w.Height,
		}
		if w.Title != nil {
			wg.Title = *w.Title
		}
		if len(w.ChartConfig) > 0 {
			var cfg interface{}
			if err := json.Unmarshal(w.ChartConfig, &cfg); err == nil {
				wg.ChartConfig = cfg
			}
		}
		export.Widgets = append(export.Widgets, wg)
	}

	out, err := yaml.Marshal(export)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to export"})
		return
	}

	w.Header().Set("Content-Type", "text/yaml")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.yaml"`, dashboard.Name))
	w.Write(out)
}

// HandleImportDashboard imports a dashboard from YAML
func (h *DashboardHandler) HandleImportDashboard(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(*storage.User)
	fractalRole := rbac.RoleFromContext(r.Context())
	prismRole := rbac.PrismRoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) && !rbac.HasAccess(user, prismRole, rbac.RoleAnalyst) {
		jsonForbidden(w)
		return
	}

	selectedFractal, _ := h.getSelectedFractal(r)
	selectedPrism, _ := r.Context().Value("selected_prism").(string)
	if selectedFractal == "" && selectedPrism == "" {
		jsonError(w, "No fractal or prism selected")
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		jsonError(w, "Failed to read request")
		return
	}

	var imported dashboardYAML
	if err := yaml.Unmarshal(body, &imported); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Invalid YAML format"})
		return
	}

	if imported.Name == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Dashboard name is required"})
		return
	}

	if imported.TimeRange == "" {
		imported.TimeRange = "last24h"
	}

	var varsJSON json.RawMessage
	if len(imported.Variables) > 0 {
		varsJSON, _ = json.Marshal(imported.Variables)
	}

	d := storage.Dashboard{
		Name:          imported.Name,
		Description:   imported.Description,
		TimeRangeType: imported.TimeRange,
		Variables:     varsJSON,
		CreatedBy:     user.Username,
	}
	if selectedPrism != "" {
		d.PrismID = selectedPrism
	} else {
		d.FractalID = selectedFractal
	}

	created, err := h.pg.InsertDashboard(r.Context(), d)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Printf("[Dashboards] Failed to create dashboard from import: %v", err)
		json.NewEncoder(w).Encode(Response{Success: false, Error: "Failed to create dashboard"})
		return
	}

	for i, wg := range imported.Widgets {
		widget := storage.DashboardWidget{
			DashboardID:  created.ID,
			QueryContent: wg.Query,
			ChartType:    wg.ChartType,
			PosX:         wg.PosX,
			PosY:         wg.PosY,
			Width:        wg.Width,
			Height:       wg.Height,
		}
		if wg.Title != "" {
			title := wg.Title
			widget.Title = &title
		}
		if wg.ChartConfig != nil {
			if cfgJSON, err := json.Marshal(wg.ChartConfig); err == nil {
				widget.ChartConfig = cfgJSON
			}
		}
		if widget.ChartType == "" {
			widget.ChartType = "table"
		}
		if widget.Width < 2 {
			widget.Width = 4
		}
		if widget.Height < 2 {
			widget.Height = 3
		}
		if _, err := h.pg.InsertDashboardWidget(r.Context(), widget); err != nil {
			fmt.Printf("[Dashboards] Failed to import widget %d: %v\n", i, err)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Success: true, Data: map[string]interface{}{"dashboard": created}})
}
