package alerts

import (
	"bifract/pkg/api"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"bifract/pkg/auth"
	"bifract/pkg/fractals"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"
	"github.com/go-chi/chi/v5"
)

// Handler provides HTTP endpoints for alert and webhook management
type Handler struct {
	manager        *Manager
	fractalManager *fractals.Manager
	rbacResolver   *rbac.Resolver
	testRunner     *TestRunner
}

// APIResponse is the shared API envelope. The alias keeps the package-local
// name while there is one type, and one schema, behind it.
type APIResponse = api.Response[any]

// NewHandlerWithFractals creates a new alert API handler with fractal support
func NewHandlerWithFractals(manager *Manager, fractalManager *fractals.Manager) *Handler {
	return &Handler{
		manager:        manager,
		fractalManager: fractalManager,
	}
}

// SetRBACResolver sets the RBAC resolver for fractal-level access checks.
func (h *Handler) SetRBACResolver(resolver *rbac.Resolver) {
	h.rbacResolver = resolver
}

// ============================
// Alert Management Endpoints
// ============================

// HandleListAlerts retrieves all alerts with optional filtering (viewer+)
// Feed-imported rules are served by /alerts/feed, which pages separately, so
// this list is only hand-authored alerts and is small in practice. It is bounded
// anyway so the response can never be unbounded; the ceiling is generous because
// the alerts screen filters client-side over the whole set.
const (
	defaultAlertPageSize = 500
	maxAlertPageSize     = 2000
)

func (h *Handler) HandleListAlerts(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleViewer) {
		return
	}

	ctx := r.Context()

	fractalID, prismID, ok := h.requireScope(w, r, "alerts")
	if !ok {
		return
	}

	enabledOnly := r.URL.Query().Get("enabled") == "true"

	alerts, err := h.manager.ListAlerts(ctx, enabledOnly, fractalID, prismID)
	if err != nil {
		log.Printf("[Alerts] Failed to list alerts: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load alerts")
		return
	}

	limit, offset := api.PageParams(r, defaultAlertPageSize, maxAlertPageSize)
	window, page := api.Slice(alerts, limit, offset)
	api.WritePage(w, window, page)
}

// HandleCreateAlert creates a new alert (analyst+)
func (h *Handler) HandleCreateAlert(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get current user from context (set by auth middleware)
	username := h.getCurrentUser(r)
	if username == "" {
		h.respondError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	if !h.requireRole(w, r, rbac.RoleAnalyst) {
		return
	}

	// Parse request body
	var req AlertCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Validate required fields
	if strings.TrimSpace(req.Name) == "" {
		h.respondError(w, http.StatusBadRequest, "Alert name is required")
		return
	}
	if strings.TrimSpace(req.QueryString) == "" {
		h.respondError(w, http.StatusBadRequest, "Query string is required")
		return
	}

	fractalID, prismID, ok := h.requireScope(w, r, "alerts")
	if !ok {
		return
	}

	alert, err := h.manager.CreateAlert(ctx, req, h.attributionUser(r), fractalID, prismID)
	if err != nil {
		if h.gateRequiredResponse(w, err) || h.policyBlockedResponse(w, err) {
			return
		}
		if errors.Is(err, ErrInvalidAlert) ||
			strings.Contains(err.Error(), "invalid query syntax") || strings.Contains(err.Error(), "cannot use aggregate") {
			h.respondError(w, http.StatusBadRequest, err.Error())
		} else if strings.Contains(err.Error(), "duplicate key value") || strings.Contains(err.Error(), "already exists") {
			h.respondError(w, http.StatusConflict, "Alert name already exists")
		} else {
			log.Printf("[Alerts] Failed to create alert: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to create alert")
		}
		return
	}

	h.respondSuccess(w, alert)
}

// HandleGetAlert retrieves a specific alert by ID
func (h *Handler) HandleGetAlert(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	alertID := chi.URLParam(r, "id")

	if alertID == "" {
		h.respondError(w, http.StatusBadRequest, "Alert ID is required")
		return
	}

	alert, err := h.manager.GetAlert(ctx, alertID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			h.respondError(w, http.StatusNotFound, "Alert not found")
		} else {
			log.Printf("[Alerts] Failed to get alert: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to load alert")
		}
		return
	}

	// Verify user has access to the alert's fractal
	if alert.FractalID != "" {
		if !h.requireRoleOnFractal(w, r, alert.FractalID, rbac.RoleViewer) {
			return
		}
	} else if alert.PrismID != "" {
		if !h.requireRoleOnPrism(w, r, alert.PrismID, rbac.RoleViewer) {
			return
		}
	}

	h.respondSuccess(w, alert)
}

// HandleUpdateAlert updates an existing alert (analyst+)
func (h *Handler) HandleUpdateAlert(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	alertID := chi.URLParam(r, "id")

	if alertID == "" {
		h.respondError(w, http.StatusBadRequest, "Alert ID is required")
		return
	}

	// Fetch alert to verify fractal ownership
	existing, err := h.manager.GetAlert(ctx, alertID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			h.respondError(w, http.StatusNotFound, "Alert not found")
		} else {
			h.respondError(w, http.StatusInternalServerError, "Failed to load alert")
		}
		return
	}
	if existing.FractalID != "" {
		if !h.requireRoleOnFractal(w, r, existing.FractalID, rbac.RoleAnalyst) {
			return
		}
	} else if existing.PrismID != "" {
		if !h.requireRoleOnPrism(w, r, existing.PrismID, rbac.RoleAnalyst) {
			return
		}
	} else if !h.requireRole(w, r, rbac.RoleAnalyst) {
		return
	}

	// Parse request body
	var req AlertUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Validate required fields
	if strings.TrimSpace(req.Name) == "" {
		h.respondError(w, http.StatusBadRequest, "Alert name is required")
		return
	}
	if strings.TrimSpace(req.QueryString) == "" {
		h.respondError(w, http.StatusBadRequest, "Query string is required")
		return
	}

	// Update alert
	alert, err := h.manager.UpdateAlert(ctx, alertID, req, h.attributionUser(r))
	if err != nil {
		if h.gateRequiredResponse(w, err) || h.policyBlockedResponse(w, err) {
			return
		}
		if strings.Contains(err.Error(), "not found") {
			h.respondError(w, http.StatusNotFound, "Alert not found")
		} else if strings.Contains(err.Error(), "invalid query syntax") || strings.Contains(err.Error(), "cannot use aggregate") {
			h.respondError(w, http.StatusBadRequest, err.Error())
		} else {
			log.Printf("[Alerts] Failed to update alert: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to update alert")
		}
		return
	}

	h.respondSuccess(w, alert)
}

// HandleDeleteAlert removes an alert (analyst+)
func (h *Handler) HandleDeleteAlert(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	alertID := chi.URLParam(r, "id")

	if alertID == "" {
		h.respondError(w, http.StatusBadRequest, "Alert ID is required")
		return
	}

	// Fetch alert to verify fractal ownership
	existing, err := h.manager.GetAlert(ctx, alertID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			h.respondError(w, http.StatusNotFound, "Alert not found")
		} else {
			h.respondError(w, http.StatusInternalServerError, "Failed to load alert")
		}
		return
	}
	if existing.FractalID != "" {
		if !h.requireRoleOnFractal(w, r, existing.FractalID, rbac.RoleAnalyst) {
			return
		}
	} else if existing.PrismID != "" {
		if !h.requireRoleOnPrism(w, r, existing.PrismID, rbac.RoleAnalyst) {
			return
		}
	} else if !h.requireRole(w, r, rbac.RoleAnalyst) {
		return
	}

	if err := h.manager.DeleteAlert(ctx, alertID); err != nil {
		if h.gateRequiredResponse(w, err) {
			return
		}
		if strings.Contains(err.Error(), "not found") {
			h.respondError(w, http.StatusNotFound, "Alert not found")
		} else {
			log.Printf("[Alerts] Failed to delete alert: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to delete alert")
		}
		return
	}

	api.WriteOK(w, "Alert deleted successfully")
}

// ImportYAMLRequest carries an alert definition to import as JSON. The same
// endpoint also accepts a raw YAML body.
type ImportYAMLRequest struct {
	YAMLContent  string `json:"yaml_content"`
	NormalizerID string `json:"normalizer_id"`
}

// HandleImportYAML imports an alert from YAML content.
// Accepts either:
//   - application/json: {"yaml_content": "...", "normalizer_id": "..."} (for Sigma with normalizer)
//   - text/plain or other: raw YAML body (backward compatible)
func (h *Handler) HandleImportYAML(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get current user from context
	username := h.getCurrentUser(r)
	if username == "" {
		h.respondError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	user := h.getUserObj(r)
	fractalRole := rbac.RoleFromContext(r.Context())
	prismRole := rbac.PrismRoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, rbac.RoleAnalyst) && !rbac.HasAccess(user, prismRole, rbac.RoleAnalyst) {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	var yamlContent string
	var normalizerID string

	contentType := r.Header.Get("Content-Type")
	if strings.HasPrefix(contentType, "application/json") {
		var req ImportYAMLRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			h.respondError(w, http.StatusBadRequest, "Invalid request body")
			return
		}
		yamlContent = strings.TrimSpace(req.YAMLContent)
		normalizerID = req.NormalizerID
	} else {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			h.respondError(w, http.StatusBadRequest, "Failed to read request body")
			return
		}
		yamlContent = strings.TrimSpace(string(body))
	}

	if yamlContent == "" {
		h.respondError(w, http.StatusBadRequest, "YAML content is required")
		return
	}

	fractalID, prismID, ok := h.requireScope(w, r, "alerts")
	if !ok {
		return
	}

	alert, err := h.manager.ImportFromYAML(ctx, yamlContent, h.attributionUser(r), fractalID, prismID, normalizerID)
	if err != nil {
		if h.gateRequiredResponse(w, err) || h.policyBlockedResponse(w, err) {
			return
		}
		errMsg := err.Error()
		if strings.Contains(errMsg, "failed to parse YAML") ||
			strings.Contains(errMsg, "invalid query syntax") ||
			strings.Contains(errMsg, "failed to parse Sigma") ||
			strings.Contains(errMsg, "failed to translate Sigma") ||
			strings.Contains(errMsg, "generated BQL query is invalid") {
			h.respondError(w, http.StatusBadRequest, errMsg)
		} else {
			log.Printf("[Alerts] Failed to import alert: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to import alert")
		}
		return
	}

	api.WriteMessage(w, "Alert imported successfully", alert)
}

// HandleGetExecutions retrieves execution history for an alert
func (h *Handler) HandleGetExecutions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	alertID := chi.URLParam(r, "id")

	if alertID == "" {
		h.respondError(w, http.StatusBadRequest, "Alert ID is required")
		return
	}

	// Verify user has access to the alert's fractal
	alert, err := h.manager.GetAlert(ctx, alertID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			h.respondError(w, http.StatusNotFound, "Alert not found")
		} else {
			h.respondError(w, http.StatusInternalServerError, "Failed to load alert")
		}
		return
	}
	if alert.FractalID != "" {
		if !h.requireRoleOnFractal(w, r, alert.FractalID, rbac.RoleViewer) {
			return
		}
	} else if alert.PrismID != "" {
		if !h.requireRoleOnPrism(w, r, alert.PrismID, rbac.RoleViewer) {
			return
		}
	}

	// Parse pagination parameters
	limit := 50 // Default limit
	offset := 0 // Default offset

	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if parsedLimit, err := parseIntParam(limitStr); err == nil && parsedLimit > 0 && parsedLimit <= 1000 {
			limit = parsedLimit
		}
	}

	if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
		if parsedOffset, err := parseIntParam(offsetStr); err == nil && parsedOffset >= 0 {
			offset = parsedOffset
		}
	}

	// Query executions
	query := `
		SELECT id, triggered_at, log_count, throttled, throttle_key, execution_time_ms, webhook_results, fractal_results
		FROM alert_executions
		WHERE alert_id = $1
		ORDER BY triggered_at DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := h.manager.pg.Query(ctx, query, alertID, limit, offset)
	if err != nil {
		log.Printf("[Alerts] Failed to get executions: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load execution history")
		return
	}
	defer rows.Close()

	var executions []map[string]interface{}
	for rows.Next() {
		var execution struct {
			ID              string `json:"id"`
			TriggeredAt     string `json:"triggered_at"`
			LogCount        int    `json:"log_count"`
			Throttled       bool   `json:"throttled"`
			ThrottleKey     string `json:"throttle_key"`
			ExecutionTimeMs int    `json:"execution_time_ms"`
			WebhookResults  string `json:"webhook_results"`
			FractalResults  string `json:"fractal_results"`
		}

		err := rows.Scan(
			&execution.ID, &execution.TriggeredAt, &execution.LogCount,
			&execution.Throttled, &execution.ThrottleKey, &execution.ExecutionTimeMs,
			&execution.WebhookResults, &execution.FractalResults,
		)
		if err != nil {
			log.Printf("[Alerts] Failed to scan execution: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to load execution details")
			return
		}

		// Parse webhook results JSON
		var webhookResults []WebhookResult
		if execution.WebhookResults != "" && execution.WebhookResults != "null" {
			json.Unmarshal([]byte(execution.WebhookResults), &webhookResults)
		}

		// Parse fractal results JSON
		var fractalResults []FractalResult
		if execution.FractalResults != "" && execution.FractalResults != "null" {
			json.Unmarshal([]byte(execution.FractalResults), &fractalResults)
		}

		executions = append(executions, map[string]interface{}{
			"id":                execution.ID,
			"triggered_at":      execution.TriggeredAt,
			"log_count":         execution.LogCount,
			"throttled":         execution.Throttled,
			"throttle_key":      execution.ThrottleKey,
			"execution_time_ms": execution.ExecutionTimeMs,
			"webhook_results":   webhookResults,
			"fractal_results":   fractalResults,
		})
	}

	api.WritePage(w, executions, api.Page{Total: len(executions), Limit: limit, Offset: offset})
}

// ============================
// Webhook Management Endpoints
// ============================

// HandleListWebhooks retrieves webhook actions in the current fractal/prism scope
func (h *Handler) HandleListWebhooks(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()

	fractalID, prismID, ok := h.requireScope(w, r, "webhooks")
	if !ok {
		return
	}

	enabledOnly := r.URL.Query().Get("enabled") == "true"

	webhooks, err := h.manager.ListWebhookActions(ctx, enabledOnly, fractalID, prismID)
	if err != nil {
		log.Printf("[Alerts] Failed to list webhooks: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load webhooks")
		return
	}

	api.WriteList(w, webhooks)
}

// HandleCreateWebhook creates a new webhook action
func (h *Handler) HandleCreateWebhook(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()

	var req WebhookCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Validate required fields
	if strings.TrimSpace(req.Name) == "" {
		h.respondError(w, http.StatusBadRequest, "Webhook name is required")
		return
	}
	if strings.TrimSpace(req.URL) == "" {
		h.respondError(w, http.StatusBadRequest, "Webhook URL is required")
		return
	}

	fractalID, prismID, ok := h.requireScope(w, r, "webhook create")
	if !ok {
		return
	}

	webhook, err := h.manager.CreateWebhookAction(ctx, req, h.attributionUser(r), fractalID, prismID)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key value") || strings.Contains(err.Error(), "already exists") {
			h.respondError(w, http.StatusConflict, "Webhook name already exists in this scope")
		} else {
			log.Printf("[Alerts] Failed to create webhook: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to create webhook")
		}
		return
	}

	h.respondSuccess(w, webhook)
}

// HandleGetWebhook retrieves a specific webhook action by ID
func (h *Handler) HandleGetWebhook(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()
	webhookID := chi.URLParam(r, "id")

	if webhookID == "" {
		h.respondError(w, http.StatusBadRequest, "Webhook ID is required")
		return
	}

	webhook, err := h.manager.GetWebhookAction(ctx, webhookID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			h.respondError(w, http.StatusNotFound, "Webhook not found")
		} else {
			log.Printf("[Alerts] Failed to get webhook: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to load webhook")
		}
		return
	}

	if !h.requireActionInScope(w, r, webhook.FractalID, webhook.PrismID, "Webhook") {
		return
	}

	h.respondSuccess(w, webhook)
}

// HandleUpdateWebhook updates an existing webhook action
func (h *Handler) HandleUpdateWebhook(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()
	webhookID := chi.URLParam(r, "id")

	if webhookID == "" {
		h.respondError(w, http.StatusBadRequest, "Webhook ID is required")
		return
	}

	existing, err := h.manager.GetWebhookAction(ctx, webhookID)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Webhook not found")
		return
	}
	if !h.requireActionInScope(w, r, existing.FractalID, existing.PrismID, "Webhook") {
		return
	}

	var req WebhookUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	webhook, err := h.manager.UpdateWebhookAction(ctx, webhookID, req)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			h.respondError(w, http.StatusNotFound, "Webhook not found")
		} else {
			log.Printf("[Alerts] Failed to update webhook: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to update webhook")
		}
		return
	}

	h.respondSuccess(w, webhook)
}

// HandleDeleteWebhook removes a webhook action
func (h *Handler) HandleDeleteWebhook(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()
	webhookID := chi.URLParam(r, "id")

	if webhookID == "" {
		h.respondError(w, http.StatusBadRequest, "Webhook ID is required")
		return
	}

	existing, err := h.manager.GetWebhookAction(ctx, webhookID)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Webhook not found")
		return
	}
	if !h.requireActionInScope(w, r, existing.FractalID, existing.PrismID, "Webhook") {
		return
	}

	if err := h.manager.DeleteWebhookAction(ctx, webhookID); err != nil {
		if strings.Contains(err.Error(), "not found") {
			h.respondError(w, http.StatusNotFound, "Webhook not found")
		} else if strings.Contains(err.Error(), "associated with") {
			h.respondError(w, http.StatusConflict, err.Error())
		} else {
			log.Printf("[Alerts] Failed to delete webhook: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to delete webhook")
		}
		return
	}

	api.WriteOK(w, "Webhook deleted successfully")
}

// HandleTestWebhook sends a test payload to a webhook
func (h *Handler) HandleTestWebhook(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()
	webhookID := chi.URLParam(r, "id")

	if webhookID == "" {
		h.respondError(w, http.StatusBadRequest, "Webhook ID is required")
		return
	}

	existing, err := h.manager.GetWebhookAction(ctx, webhookID)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Webhook not found")
		return
	}
	if !h.requireActionInScope(w, r, existing.FractalID, existing.PrismID, "Webhook") {
		return
	}

	result, err := h.manager.TestWebhookAction(ctx, webhookID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			h.respondError(w, http.StatusNotFound, "Webhook not found")
		} else {
			log.Printf("[Alerts] Failed to test webhook: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to test webhook")
		}
		return
	}

	api.WriteMessage(w, "Webhook test completed", result)
}

// WebhookTestRequest tests a webhook configuration that may not be saved yet.
// Send false renders the body only, which is how a template is checked without
// reaching the destination.
type WebhookTestRequest struct {
	WebhookCreateRequest
	Send bool `json:"send"`
}

// HandleTestWebhookConfig renders, and optionally delivers, a test payload for a
// posted webhook configuration. It is what makes the editor's Test button usable
// on unsaved edits: without it a template can only be checked by saving first.
func (h *Handler) HandleTestWebhookConfig(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req WebhookTestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Send && strings.TrimSpace(req.URL) == "" {
		h.respondError(w, http.StatusBadRequest, "Webhook URL is required to send a test")
		return
	}

	fractalID, prismID, ok := h.requireScope(w, r, "webhook test")
	if !ok {
		return
	}

	includeAlertLink := true
	if req.IncludeAlertLink != nil {
		includeAlertLink = *req.IncludeAlertLink
	}

	webhook := WebhookAction{
		Name:             req.Name,
		URL:              req.URL,
		Method:           req.Method,
		Headers:          req.Headers,
		AuthType:         req.AuthType,
		AuthConfig:       req.AuthConfig,
		TimeoutSecs:      req.TimeoutSeconds,
		RetryCount:       req.RetryCount,
		IncludeAlertLink: includeAlertLink,
		BodyMode:         NormalizeBodyMode(req.BodyMode),
		BodyTemplate:     req.BodyTemplate,
		ContentType:      req.ContentType,
		FractalID:        fractalID,
		PrismID:          prismID,
	}

	api.WriteMessage(w, "Webhook test completed", h.manager.TestWebhookConfig(ctx, webhook, req.Send))
}

// ============================
// Fractal Action Management Endpoints
// ============================

// HandleListFractalActions retrieves fractal actions in the current fractal/prism scope
func (h *Handler) HandleListFractalActions(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()

	fractalID, prismID, ok := h.requireScope(w, r, "fractal actions")
	if !ok {
		return
	}

	enabledOnly := r.URL.Query().Get("enabled") == "true"

	fractalActions, err := h.manager.ListFractalActions(ctx, enabledOnly, fractalID, prismID)
	if err != nil {
		log.Printf("[Alerts] Failed to list fractal actions: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load fractal actions")
		return
	}

	api.WriteList(w, fractalActions)
}

// HandleCreateFractalAction creates a new fractal action
func (h *Handler) HandleCreateFractalAction(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()

	var req FractalActionCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Validate required fields
	if strings.TrimSpace(req.Name) == "" {
		h.respondError(w, http.StatusBadRequest, "Fractal action name is required")
		return
	}
	if strings.TrimSpace(req.TargetFractalID) == "" {
		h.respondError(w, http.StatusBadRequest, "Target fractal ID is required")
		return
	}

	fractalID, prismID, ok := h.requireScope(w, r, "fractal action create")
	if !ok {
		return
	}

	fractalAction, err := h.manager.CreateFractalAction(ctx, req, h.attributionUser(r), fractalID, prismID)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key value") || strings.Contains(err.Error(), "already exists") {
			h.respondError(w, http.StatusConflict, "Fractal action name already exists in this scope")
		} else {
			log.Printf("[Alerts] Failed to create fractal action: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to create fractal action")
		}
		return
	}

	h.respondSuccess(w, fractalAction)
}

// HandleGetFractalAction retrieves a specific fractal action by ID
func (h *Handler) HandleGetFractalAction(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()
	fractalActionID := chi.URLParam(r, "id")

	if fractalActionID == "" {
		h.respondError(w, http.StatusBadRequest, "Fractal action ID is required")
		return
	}

	fractalAction, err := h.manager.GetFractalAction(ctx, fractalActionID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			h.respondError(w, http.StatusNotFound, "Fractal action not found")
		} else {
			log.Printf("[Alerts] Failed to get fractal action: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to load fractal action")
		}
		return
	}

	if !h.requireActionInScope(w, r, fractalAction.FractalID, fractalAction.PrismID, "Fractal action") {
		return
	}

	h.respondSuccess(w, fractalAction)
}

// HandleUpdateFractalAction updates an existing fractal action
func (h *Handler) HandleUpdateFractalAction(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()
	fractalActionID := chi.URLParam(r, "id")

	if fractalActionID == "" {
		h.respondError(w, http.StatusBadRequest, "Fractal action ID is required")
		return
	}

	existing, err := h.manager.GetFractalAction(ctx, fractalActionID)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Fractal action not found")
		return
	}
	if !h.requireActionInScope(w, r, existing.FractalID, existing.PrismID, "Fractal action") {
		return
	}

	var req FractalActionUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	fractalAction, err := h.manager.UpdateFractalAction(ctx, fractalActionID, req)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			h.respondError(w, http.StatusNotFound, "Fractal action not found")
		} else {
			log.Printf("[Alerts] Failed to update fractal action: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to update fractal action")
		}
		return
	}

	h.respondSuccess(w, fractalAction)
}

// HandleDeleteFractalAction removes a fractal action
func (h *Handler) HandleDeleteFractalAction(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()
	fractalActionID := chi.URLParam(r, "id")

	if fractalActionID == "" {
		h.respondError(w, http.StatusBadRequest, "Fractal action ID is required")
		return
	}

	existing, err := h.manager.GetFractalAction(ctx, fractalActionID)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Fractal action not found")
		return
	}
	if !h.requireActionInScope(w, r, existing.FractalID, existing.PrismID, "Fractal action") {
		return
	}

	if err := h.manager.DeleteFractalAction(ctx, fractalActionID); err != nil {
		if strings.Contains(err.Error(), "not found") {
			h.respondError(w, http.StatusNotFound, "Fractal action not found")
		} else if strings.Contains(err.Error(), "associated with") {
			h.respondError(w, http.StatusConflict, err.Error())
		} else {
			log.Printf("[Alerts] Failed to delete fractal action: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to delete fractal action")
		}
		return
	}

	api.WriteOK(w, "Fractal action deleted successfully")
}

// ============================
// Helper Methods
// ============================

// respondSuccess sends a successful JSON response
func (h *Handler) respondSuccess(w http.ResponseWriter, data interface{}) {
	api.WriteSuccess(w, data)
}

// respondError sends an error JSON response
func (h *Handler) respondError(w http.ResponseWriter, statusCode int, message string) {
	api.WriteError(w, statusCode, message)
}

// getCurrentUser extracts the current username from the request context
// The auth middleware sets a *User object in the context
func (h *Handler) getCurrentUser(r *http.Request) string {
	if user := r.Context().Value("user"); user != nil {
		if userObj, ok := user.(*storage.User); ok {
			return userObj.Username
		}
	}
	return ""
}

// attributionUser returns the username to persist in created_by/updated_by.
// It differs from getCurrentUser: API keys authenticate as a synthetic
// principal with no users row, and those columns are foreign keys.
func (h *Handler) attributionUser(r *http.Request) string {
	return auth.AttributionUsername(r.Context())
}

// getUserObj extracts the full user object from the request context.
func (h *Handler) getUserObj(r *http.Request) *storage.User {
	if user, ok := r.Context().Value("user").(*storage.User); ok {
		return user
	}
	return nil
}

// requireRoleOnFractal checks the user has the required role on a specific fractal (used for resource-by-ID operations).
func (h *Handler) requireRoleOnFractal(w http.ResponseWriter, r *http.Request, fractalID string, required rbac.Role) bool {
	user := h.getUserObj(r)
	if user == nil {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return false
	}
	if user.IsAdmin {
		return true
	}
	// API key users have their role pre-resolved by the auth middleware, because the
	// synthetic "apikey_<id>" username has no fractal_permissions rows to query.
	//
	// That pre-resolved role is a role on the key's *own* scope, so it may only be
	// applied to a resource in that scope. Handlers here pass the fractal of the
	// resource being touched, and without this check a key scoped to one fractal would
	// satisfy the role check for an alert in every other one.
	if authType, _ := r.Context().Value("auth_type").(string); authType == "api_key" {
		scoped, _ := r.Context().Value("selected_fractal").(string)
		if fractalID != "" && scoped != fractalID {
			h.respondError(w, http.StatusForbidden, "Insufficient permissions")
			return false
		}
		fractalRole := rbac.RoleFromContext(r.Context())
		if !rbac.HasAccess(user, fractalRole, required) {
			h.respondError(w, http.StatusForbidden, "Insufficient permissions")
			return false
		}
		return true
	}
	if h.rbacResolver == nil {
		return h.requireRole(w, r, required)
	}
	role := h.rbacResolver.ResolveRole(r.Context(), user, fractalID)
	if !rbac.HasAccess(user, role, required) {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return false
	}
	return true
}

// requireRoleOnPrism checks the user has the required role on a specific prism.
func (h *Handler) requireRoleOnPrism(w http.ResponseWriter, r *http.Request, prismID string, required rbac.Role) bool {
	user := h.getUserObj(r)
	if user == nil {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return false
	}
	if user.IsAdmin {
		return true
	}
	if h.rbacResolver == nil {
		prismRole := rbac.PrismRoleFromContext(r.Context())
		if !rbac.HasAccess(user, prismRole, required) {
			h.respondError(w, http.StatusForbidden, "Insufficient permissions")
			return false
		}
		return true
	}
	role := h.rbacResolver.ResolvePrismRoleWithAdmin(r.Context(), user, prismID)
	if !rbac.HasAccess(user, role, required) {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return false
	}
	return true
}

// requireRole checks that the current user has at least the given role on the session fractal or prism.
func (h *Handler) requireRole(w http.ResponseWriter, r *http.Request, required rbac.Role) bool {
	user := h.getUserObj(r)
	fractalRole := rbac.RoleFromContext(r.Context())
	prismRole := rbac.PrismRoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, required) && !rbac.HasAccess(user, prismRole, required) {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return false
	}
	return true
}

// parseIntParam safely parses an integer parameter
func parseIntParam(param string) (int, error) {
	var result int
	if _, err := fmt.Sscanf(param, "%d", &result); err != nil {
		return 0, err
	}
	return result, nil
}

// requireActionInScope verifies that an action's (fractal_id, prism_id) matches the
// caller's current session scope. It writes a 404 and returns false on mismatch.
// resourceName is used for the error message (e.g. "Webhook", "Fractal action").
func (h *Handler) requireActionInScope(w http.ResponseWriter, r *http.Request, actionFractalID, actionPrismID, resourceName string) bool {
	fractalID, prismID, ok := h.requireScope(w, r, resourceName)
	if !ok {
		return false
	}
	inScope := (prismID != "" && actionPrismID == prismID) ||
		(fractalID != "" && actionFractalID == fractalID)
	if !inScope {
		h.respondError(w, http.StatusNotFound, resourceName+" not found")
		return false
	}
	return true
}

// requireScope resolves the request scope and answers the request itself when
// there is none. Every scoped handler goes through it: an empty scope reaching
// the manager builds a query with no scope predicate, which lists every alert in
// every fractal and prism.
func (h *Handler) requireScope(w http.ResponseWriter, r *http.Request, what string) (fractalID, prismID string, ok bool) {
	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[Alerts] Failed to get scope for %s: %v", what, err)
		h.respondError(w, http.StatusInternalServerError, "Failed to determine scope")
		return "", "", false
	}
	if fractalID == "" && prismID == "" {
		h.respondError(w, http.StatusBadRequest, "No fractal or prism selected")
		return "", "", false
	}
	return fractalID, prismID, true
}

// getScope returns the current fractal or prism scope from the request context.
// At most one is non-empty; both are empty when the request declared no scope.
// Handlers go through requireScope rather than calling this directly.
func (h *Handler) getScope(r *http.Request) (string, string, error) {
	if prismID, _ := r.Context().Value("selected_prism").(string); prismID != "" {
		return "", prismID, nil
	}
	if fractalID, _ := r.Context().Value("selected_fractal").(string); fractalID != "" {
		return fractalID, "", nil
	}
	// A caller that declared no scope has none; callers reject that below.
	if h.fractalManager == nil || fractals.NoScopeDeclared(r.Context()) {
		return "", "", nil
	}
	defaultFractal, err := h.fractalManager.GetDefaultFractal(r.Context())
	if err != nil {
		return "", "", fmt.Errorf("failed to get default fractal: %w", err)
	}
	return defaultFractal.ID, "", nil
}

// HandleDuplicateAlert copies a feed alert as a standalone editable alert.
func (h *Handler) HandleDuplicateAlert(w http.ResponseWriter, r *http.Request) {
	user := h.getCurrentUser(r)
	if user == "" {
		h.respondError(w, http.StatusUnauthorized, "authentication required")
		return
	}

	alertID := chi.URLParam(r, "id")

	// Verify access to the alert's fractal
	existing, err := h.manager.GetAlert(r.Context(), alertID)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Alert not found")
		return
	}
	if existing.FractalID != "" {
		if !h.requireRoleOnFractal(w, r, existing.FractalID, rbac.RoleAnalyst) {
			return
		}
	} else if existing.PrismID != "" {
		if !h.requireRoleOnPrism(w, r, existing.PrismID, rbac.RoleAnalyst) {
			return
		}
	}

	alert, err := h.manager.DuplicateAlert(r.Context(), alertID, h.attributionUser(r))
	if err != nil {
		log.Printf("[Alerts] Failed to duplicate alert: %v", err)
		h.respondError(w, http.StatusBadRequest, "Failed to duplicate alert")
		return
	}

	h.respondSuccess(w, alert)
}

// BatchToggleFeedAlertsRequest addresses feed alerts by id or by filter.
type BatchToggleFeedAlertsRequest struct {
	AlertIDs []string         `json:"alert_ids"`
	Enabled  bool             `json:"enabled"`
	Filter   *FeedAlertFilter `json:"filter"`
}

// FeedAlertFilter selects feed alerts by the same criteria the list endpoint
// accepts, so a toggle can address a whole filtered set without naming ids.
type FeedAlertFilter struct {
	Search   string `json:"search"`
	Status   string `json:"status"`
	FeedID   string `json:"feed_id"`
	Severity string `json:"severity"`
	Label    string `json:"label"`
}

// HandleBatchToggleFeedAlerts enables or disables feed alerts, addressed either
// by explicit ID or by the same filter the table is showing. The client only
// holds one page of IDs, so "Enable Filtered" sends the filter and lets Postgres
// resolve the set.
func (h *Handler) HandleBatchToggleFeedAlerts(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleAnalyst) {
		return
	}

	var req BatchToggleFeedAlertsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	var count int
	var err error
	switch {
	case req.Filter != nil:
		fractalID, prismID, ok := h.requireScope(w, r, "feed alert batch toggle")
		if !ok {
			return
		}
		unset := func(v string) string {
			if v == "all" {
				return ""
			}
			return v
		}
		count, err = h.manager.BatchToggleFeedAlertsFiltered(r.Context(), FeedAlertQuery{
			FractalID: fractalID,
			PrismID:   prismID,
			Search:    req.Filter.Search,
			Status:    unset(req.Filter.Status),
			FeedID:    unset(req.Filter.FeedID),
			Severity:  unset(req.Filter.Severity),
			Label:     unset(req.Filter.Label),
		}, req.Enabled, h.attributionUser(r))
	case len(req.AlertIDs) == 0:
		h.respondError(w, http.StatusBadRequest, "alert_ids or filter required")
		return
	case len(req.AlertIDs) > 5000:
		h.respondError(w, http.StatusBadRequest, "too many alert IDs (max 5000)")
		return
	default:
		count, err = h.manager.BatchToggleFeedAlerts(r.Context(), req.AlertIDs, req.Enabled, h.attributionUser(r))
	}
	if err != nil {
		log.Printf("[Alerts] Failed to batch toggle feed alerts: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to update feed alerts")
		return
	}

	h.respondSuccess(w, map[string]int{"toggled": count})
}

// BatchToggleAlertsRequest enables or disables a set of alerts.
type BatchToggleAlertsRequest struct {
	AlertIDs []string `json:"alert_ids"`
	Enabled  bool     `json:"enabled"`
}

// HandleBatchToggleAlerts enables or disables a set of non-feed alerts by ID.
func (h *Handler) HandleBatchToggleAlerts(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleAnalyst) {
		return
	}

	var req BatchToggleAlertsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if len(req.AlertIDs) == 0 {
		h.respondError(w, http.StatusBadRequest, "alert_ids required")
		return
	}
	if len(req.AlertIDs) > 5000 {
		h.respondError(w, http.StatusBadRequest, "too many alert IDs (max 5000)")
		return
	}

	count, err := h.manager.BatchToggleAlerts(r.Context(), req.AlertIDs, req.Enabled, h.attributionUser(r))
	if err != nil {
		log.Printf("[Alerts] Failed to batch toggle alerts: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to update alerts")
		return
	}

	h.respondSuccess(w, map[string]int{"toggled": count})
}

// ToggleFeedAlertRequest enables or disables one feed alert.
type ToggleFeedAlertRequest struct {
	Enabled bool `json:"enabled"`
}

// HandleToggleFeedAlert enables or disables a single feed alert.
func (h *Handler) HandleToggleFeedAlert(w http.ResponseWriter, r *http.Request) {
	userObj := h.getUserObj(r)
	if userObj == nil {
		h.respondError(w, http.StatusUnauthorized, "authentication required")
		return
	}

	alertID := chi.URLParam(r, "id")

	// Fetch alert to verify fractal access
	alert, err := h.manager.GetAlert(r.Context(), alertID)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Alert not found")
		return
	}
	if alert.FractalID != "" {
		if !h.requireRoleOnFractal(w, r, alert.FractalID, rbac.RoleAnalyst) {
			return
		}
	} else if alert.PrismID != "" {
		if !h.requireRoleOnPrism(w, r, alert.PrismID, rbac.RoleAnalyst) {
			return
		}
	}

	var req ToggleFeedAlertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if err := h.manager.ToggleFeedAlert(r.Context(), alertID, req.Enabled, h.attributionUser(r)); err != nil {
		log.Printf("[Alerts] Failed to toggle feed alert: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to update feed alerts")
		return
	}

	h.respondSuccess(w, nil)
}

// ============================
// Email Action Handlers
// ============================

func (h *Handler) HandleListEmailActions(w http.ResponseWriter, r *http.Request) {

	fractalID, prismID, ok := h.requireScope(w, r, "email actions")
	if !ok {
		return
	}

	actions, err := h.manager.ListEmailActions(r.Context(), fractalID, prismID)
	if err != nil {
		log.Printf("[Alerts] Failed to list email actions: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load email actions")
		return
	}

	api.WriteList(w, actions)
}

func (h *Handler) HandleCreateEmailAction(w http.ResponseWriter, r *http.Request) {

	var req EmailActionCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	fractalID, prismID, ok := h.requireScope(w, r, "email action create")
	if !ok {
		return
	}

	action, err := h.manager.CreateEmailAction(r.Context(), req, h.attributionUser(r), fractalID, prismID)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "already exists") {
			h.respondError(w, http.StatusConflict, "Email action name already exists in this scope")
		} else {
			log.Printf("[Alerts] Failed to create email action: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to create email action")
		}
		return
	}

	h.respondSuccess(w, action)
}

func (h *Handler) HandleGetEmailAction(w http.ResponseWriter, r *http.Request) {

	id := chi.URLParam(r, "id")
	action, err := h.manager.GetEmailAction(r.Context(), id)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			h.respondError(w, http.StatusNotFound, "Email action not found")
		} else {
			h.respondError(w, http.StatusInternalServerError, "Failed to get email action")
		}
		return
	}

	if !h.requireActionInScope(w, r, action.FractalID, action.PrismID, "Email action") {
		return
	}

	h.respondSuccess(w, action)
}

func (h *Handler) HandleUpdateEmailAction(w http.ResponseWriter, r *http.Request) {

	id := chi.URLParam(r, "id")
	existing, err := h.manager.GetEmailAction(r.Context(), id)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Email action not found")
		return
	}
	if !h.requireActionInScope(w, r, existing.FractalID, existing.PrismID, "Email action") {
		return
	}

	var req EmailActionUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	action, err := h.manager.UpdateEmailAction(r.Context(), id, req)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			h.respondError(w, http.StatusNotFound, "Email action not found")
		} else if strings.Contains(err.Error(), "duplicate key") {
			h.respondError(w, http.StatusConflict, "Email action name already exists")
		} else {
			log.Printf("[Alerts] Failed to update email action: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to update email action")
		}
		return
	}

	h.respondSuccess(w, action)
}

func (h *Handler) HandleDeleteEmailAction(w http.ResponseWriter, r *http.Request) {

	id := chi.URLParam(r, "id")
	existing, err := h.manager.GetEmailAction(r.Context(), id)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Email action not found")
		return
	}
	if !h.requireActionInScope(w, r, existing.FractalID, existing.PrismID, "Email action") {
		return
	}

	if err := h.manager.DeleteEmailAction(r.Context(), id); err != nil {
		if strings.Contains(err.Error(), "not found") {
			h.respondError(w, http.StatusNotFound, "Email action not found")
		} else {
			log.Printf("[Alerts] Failed to delete email action: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to delete email action")
		}
		return
	}

	h.respondSuccess(w, nil)
}

// ActionTestResult reports the outcome of exercising an alert action.
type ActionTestResult struct {
	Success  bool   `json:"success"`
	Error    string `json:"error"`
	Duration string `json:"duration"`
}

func (h *Handler) HandleTestEmailAction(w http.ResponseWriter, r *http.Request) {

	id := chi.URLParam(r, "id")
	ctx := r.Context()

	action, err := h.manager.GetEmailAction(ctx, id)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "Email action not found")
		return
	}
	if !h.requireActionInScope(w, r, action.FractalID, action.PrismID, "Email action") {
		return
	}

	testAlert := &Alert{
		ID:          "test-alert-id",
		Name:        "Test Alert",
		Description: "This is a test email from Bifract to verify your email action configuration.",
		Severity:    "medium",
		Labels:      []string{"test"},
		QueryString: "level=error",
		FractalID:   "test-fractal",
	}

	result := h.manager.engine.emailClient.Send(ctx, *action, testAlert, "Test Alert", []map[string]interface{}{
		{"timestamp": time.Now().Format(time.RFC3339), "message": "Test log entry", "level": "error"},
	})

	h.respondSuccess(w, ActionTestResult{Success: result.Success, Error: result.Error, Duration: result.Duration.String()})
}

// ============================
// SMTP Settings Handlers
// ============================

func (h *Handler) HandleGetSMTPSettings(w http.ResponseWriter, r *http.Request) {

	raw, err := h.manager.pg.GetSetting(r.Context(), "smtp_config")
	if err != nil {
		// Not configured yet
		h.respondSuccess(w, nil)
		return
	}

	var config SMTPConfig
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		h.respondError(w, http.StatusInternalServerError, "Failed to parse SMTP config")
		return
	}

	// Mask password for display
	if config.Password != "" {
		config.Password = "********"
	}

	h.respondSuccess(w, config)
}

func (h *Handler) HandleUpdateSMTPSettings(w http.ResponseWriter, r *http.Request) {

	var config SMTPConfig
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if strings.TrimSpace(config.Host) == "" {
		h.respondError(w, http.StatusBadRequest, "SMTP host is required")
		return
	}
	if strings.TrimSpace(config.FromAddress) == "" {
		h.respondError(w, http.StatusBadRequest, "From address is required")
		return
	}

	// If password is masked, preserve existing
	if config.Password == "********" {
		raw, err := h.manager.pg.GetSetting(r.Context(), "smtp_config")
		if err == nil {
			var existing SMTPConfig
			if json.Unmarshal([]byte(raw), &existing) == nil {
				config.Password = existing.Password
			}
		}
	}

	configJSON, err := json.Marshal(config)
	if err != nil {
		h.respondError(w, http.StatusInternalServerError, "Failed to serialize config")
		return
	}

	if err := h.manager.pg.SetSetting(r.Context(), "smtp_config", string(configJSON)); err != nil {
		log.Printf("[Alerts] Failed to save SMTP settings: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to save SMTP settings")
		return
	}

	h.respondSuccess(w, nil)
}
