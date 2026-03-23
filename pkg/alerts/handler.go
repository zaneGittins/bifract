package alerts

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"bifract/pkg/fractals"
	"bifract/pkg/rbac"
	"bifract/pkg/storage"
)

// Handler provides HTTP endpoints for alert and webhook management
type Handler struct {
	manager        *Manager
	fractalManager *fractals.Manager
	rbacResolver   *rbac.Resolver
}

// APIResponse represents a standard API response
type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// NewHandler creates a new alert API handler
func NewHandler(manager *Manager) *Handler {
	return &Handler{
		manager: manager,
	}
}

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
func (h *Handler) HandleListAlerts(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleViewer) {
		return
	}

	ctx := r.Context()

	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[Alerts] Failed to get selected index: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to determine fractal context")
		return
	}

	enabledOnly := r.URL.Query().Get("enabled") == "true"

	alerts, err := h.manager.ListAlerts(ctx, enabledOnly, fractalID, prismID)
	if err != nil {
		log.Printf("[Alerts] Failed to list alerts: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load alerts")
		return
	}

	h.respondSuccess(w, map[string]interface{}{
		"alerts": alerts,
		"count":  len(alerts),
	})
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

	// Enforce API key permissions
	if authType, _ := ctx.Value("auth_type").(string); authType == "api_key" {
		perms, _ := ctx.Value("api_key_permissions").(map[string]interface{})
		if canManage, ok := perms["alert_manage"].(bool); !ok || !canManage {
			h.respondError(w, http.StatusForbidden, "API key does not have alert management permission")
			return
		}
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

	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[Alerts] Failed to get selected index: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to determine fractal context")
		return
	}

	alert, err := h.manager.CreateAlert(ctx, req, username, fractalID, prismID)
	if err != nil {
		if strings.Contains(err.Error(), "invalid query syntax") || strings.Contains(err.Error(), "cannot use aggregate") {
			h.respondError(w, http.StatusBadRequest, err.Error())
		} else if strings.Contains(err.Error(), "duplicate key value") || strings.Contains(err.Error(), "already exists") {
			h.respondError(w, http.StatusConflict, "Alert name already exists")
		} else {
			log.Printf("[Alerts] Failed to create alert: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to create alert")
		}
		return
	}

	h.respondSuccess(w, map[string]interface{}{
		"alert": alert,
	})
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
	}

	h.respondSuccess(w, map[string]interface{}{
		"alert": alert,
	})
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
	} else if !h.requireRole(w, r, rbac.RoleAnalyst) {
		return
	}

	// Enforce API key permissions
	if authType, _ := ctx.Value("auth_type").(string); authType == "api_key" {
		perms, _ := ctx.Value("api_key_permissions").(map[string]interface{})
		if canManage, ok := perms["alert_manage"].(bool); !ok || !canManage {
			h.respondError(w, http.StatusForbidden, "API key does not have alert management permission")
			return
		}
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

	// Get current user from context
	username := h.getCurrentUser(r)

	// Update alert
	alert, err := h.manager.UpdateAlert(ctx, alertID, req, username)
	if err != nil {
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

	h.respondSuccess(w, map[string]interface{}{
		"alert": alert,
	})
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
	} else if !h.requireRole(w, r, rbac.RoleAnalyst) {
		return
	}

	// Enforce API key permissions
	if authType, _ := ctx.Value("auth_type").(string); authType == "api_key" {
		perms, _ := ctx.Value("api_key_permissions").(map[string]interface{})
		if canManage, ok := perms["alert_manage"].(bool); !ok || !canManage {
			h.respondError(w, http.StatusForbidden, "API key does not have alert management permission")
			return
		}
	}

	if err := h.manager.DeleteAlert(ctx, alertID); err != nil {
		if strings.Contains(err.Error(), "not found") {
			h.respondError(w, http.StatusNotFound, "Alert not found")
		} else {
			log.Printf("[Alerts] Failed to delete alert: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to delete alert")
		}
		return
	}

	h.respondSuccess(w, map[string]interface{}{
		"message": "Alert deleted successfully",
	})
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

	if !h.requireRole(w, r, rbac.RoleAnalyst) {
		return
	}

	var yamlContent string
	var normalizerID string

	contentType := r.Header.Get("Content-Type")
	if strings.HasPrefix(contentType, "application/json") {
		var req struct {
			YAMLContent  string `json:"yaml_content"`
			NormalizerID string `json:"normalizer_id"`
		}
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

	fractalID, _, err := h.getScope(r)
	if err != nil {
		log.Printf("[Alerts] Failed to get selected index: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to determine fractal context")
		return
	}

	alert, err := h.manager.ImportFromYAML(ctx, yamlContent, username, fractalID, normalizerID)
	if err != nil {
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

	h.respondSuccess(w, map[string]interface{}{
		"alert":   alert,
		"message": "Alert imported successfully",
	})
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

	h.respondSuccess(w, map[string]interface{}{
		"executions": executions,
		"count":      len(executions),
		"limit":      limit,
		"offset":     offset,
	})
}

// ============================
// Webhook Management Endpoints
// ============================

// HandleListWebhooks retrieves all webhook actions
func (h *Handler) HandleListWebhooks(w http.ResponseWriter, r *http.Request) {
	user := h.getUserObj(r)
	if user == nil || !user.IsAdmin {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	ctx := r.Context()

	enabledOnly := r.URL.Query().Get("enabled") == "true"

	webhooks, err := h.manager.ListWebhookActions(ctx, enabledOnly)
	if err != nil {
		log.Printf("[Alerts] Failed to list webhooks: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load webhooks")
		return
	}

	h.respondSuccess(w, map[string]interface{}{
		"webhooks": webhooks,
		"count":    len(webhooks),
	})
}

// HandleCreateWebhook creates a new webhook action
func (h *Handler) HandleCreateWebhook(w http.ResponseWriter, r *http.Request) {
	user := h.getUserObj(r)
	if user == nil || !user.IsAdmin {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	ctx := r.Context()
	username := user.Username

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

	webhook, err := h.manager.CreateWebhookAction(ctx, req, username)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key value") || strings.Contains(err.Error(), "already exists") {
			h.respondError(w, http.StatusConflict, "Webhook name already exists")
		} else {
			log.Printf("[Alerts] Failed to create webhook: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to create webhook")
		}
		return
	}

	h.respondSuccess(w, map[string]interface{}{
		"webhook": webhook,
	})
}

// HandleGetWebhook retrieves a specific webhook action by ID
func (h *Handler) HandleGetWebhook(w http.ResponseWriter, r *http.Request) {
	user := h.getUserObj(r)
	if user == nil || !user.IsAdmin {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

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

	h.respondSuccess(w, map[string]interface{}{
		"webhook": webhook,
	})
}

// HandleUpdateWebhook updates an existing webhook action
func (h *Handler) HandleUpdateWebhook(w http.ResponseWriter, r *http.Request) {
	user := h.getUserObj(r)
	if user == nil || !user.IsAdmin {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	ctx := r.Context()
	webhookID := chi.URLParam(r, "id")

	if webhookID == "" {
		h.respondError(w, http.StatusBadRequest, "Webhook ID is required")
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

	h.respondSuccess(w, map[string]interface{}{
		"webhook": webhook,
	})
}

// HandleDeleteWebhook removes a webhook action
func (h *Handler) HandleDeleteWebhook(w http.ResponseWriter, r *http.Request) {
	user := h.getUserObj(r)
	if user == nil || !user.IsAdmin {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	ctx := r.Context()
	webhookID := chi.URLParam(r, "id")

	if webhookID == "" {
		h.respondError(w, http.StatusBadRequest, "Webhook ID is required")
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

	h.respondSuccess(w, map[string]interface{}{
		"message": "Webhook deleted successfully",
	})
}

// HandleTestWebhook sends a test payload to a webhook
func (h *Handler) HandleTestWebhook(w http.ResponseWriter, r *http.Request) {
	user := h.getUserObj(r)
	if user == nil || !user.IsAdmin {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	ctx := r.Context()
	webhookID := chi.URLParam(r, "id")

	if webhookID == "" {
		h.respondError(w, http.StatusBadRequest, "Webhook ID is required")
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

	h.respondSuccess(w, map[string]interface{}{
		"test_result": result,
		"message":     "Webhook test completed",
	})
}

// ============================
// Fractal Action Management Endpoints
// ============================

// HandleListFractalActions retrieves all fractal actions
func (h *Handler) HandleListFractalActions(w http.ResponseWriter, r *http.Request) {
	user := h.getUserObj(r)
	if user == nil || !user.IsAdmin {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	ctx := r.Context()

	enabledOnly := r.URL.Query().Get("enabled") == "true"

	fractalActions, err := h.manager.ListFractalActions(ctx, enabledOnly)
	if err != nil {
		log.Printf("[Alerts] Failed to list fractal actions: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load fractal actions")
		return
	}

	h.respondSuccess(w, map[string]interface{}{
		"fractal_actions": fractalActions,
		"count":           len(fractalActions),
	})
}

// HandleCreateFractalAction creates a new fractal action
func (h *Handler) HandleCreateFractalAction(w http.ResponseWriter, r *http.Request) {
	user := h.getUserObj(r)
	if user == nil || !user.IsAdmin {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	ctx := r.Context()
	username := user.Username

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

	fractalAction, err := h.manager.CreateFractalAction(ctx, req, username)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key value") || strings.Contains(err.Error(), "already exists") {
			h.respondError(w, http.StatusConflict, "Fractal action name already exists")
		} else {
			log.Printf("[Alerts] Failed to create fractal action: %v", err)
			h.respondError(w, http.StatusInternalServerError, "Failed to create fractal action")
		}
		return
	}

	h.respondSuccess(w, map[string]interface{}{
		"fractal_action": fractalAction,
	})
}

// HandleGetFractalAction retrieves a specific fractal action by ID
func (h *Handler) HandleGetFractalAction(w http.ResponseWriter, r *http.Request) {
	user := h.getUserObj(r)
	if user == nil || !user.IsAdmin {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

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

	h.respondSuccess(w, map[string]interface{}{
		"fractal_action": fractalAction,
	})
}

// HandleUpdateFractalAction updates an existing fractal action
func (h *Handler) HandleUpdateFractalAction(w http.ResponseWriter, r *http.Request) {
	user := h.getUserObj(r)
	if user == nil || !user.IsAdmin {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	ctx := r.Context()
	fractalActionID := chi.URLParam(r, "id")

	if fractalActionID == "" {
		h.respondError(w, http.StatusBadRequest, "Fractal action ID is required")
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

	h.respondSuccess(w, map[string]interface{}{
		"fractal_action": fractalAction,
	})
}

// HandleDeleteFractalAction removes a fractal action
func (h *Handler) HandleDeleteFractalAction(w http.ResponseWriter, r *http.Request) {
	user := h.getUserObj(r)
	if user == nil || !user.IsAdmin {
		h.respondError(w, http.StatusForbidden, "Insufficient permissions")
		return
	}

	ctx := r.Context()
	fractalActionID := chi.URLParam(r, "id")

	if fractalActionID == "" {
		h.respondError(w, http.StatusBadRequest, "Fractal action ID is required")
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

	h.respondSuccess(w, map[string]interface{}{
		"message": "Fractal action deleted successfully",
	})
}

// ============================
// Helper Methods
// ============================

// respondSuccess sends a successful JSON response
func (h *Handler) respondSuccess(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(APIResponse{
		Success: true,
		Data:    data,
	})
}

// respondError sends an error JSON response
func (h *Handler) respondError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(APIResponse{
		Success: false,
		Error:   message,
	})
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
	// API key users have their role pre-resolved by the auth middleware;
	// querying fractal_permissions would fail because the synthetic
	// "apikey_<id>" username has no DB entries.
	if authType, _ := r.Context().Value("auth_type").(string); authType == "api_key" {
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

// requireRole checks that the current user has at least the given role on the session fractal.
func (h *Handler) requireRole(w http.ResponseWriter, r *http.Request, required rbac.Role) bool {
	user := h.getUserObj(r)
	fractalRole := rbac.RoleFromContext(r.Context())
	if !rbac.HasAccess(user, fractalRole, required) {
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

// getScope returns the current fractal or prism scope from the request context.
// Returns (fractalID, prismID, error) — exactly one will be non-empty when err == nil.
func (h *Handler) getScope(r *http.Request) (string, string, error) {
	if prismID, _ := r.Context().Value("selected_prism").(string); prismID != "" {
		return "", prismID, nil
	}
	if fractalID, _ := r.Context().Value("selected_fractal").(string); fractalID != "" {
		return fractalID, "", nil
	}
	if h.fractalManager == nil {
		return "", "", nil
	}
	defaultFractal, err := h.fractalManager.GetDefaultFractal(r.Context())
	if err != nil {
		return "", "", fmt.Errorf("failed to get default fractal: %w", err)
	}
	return defaultFractal.ID, "", nil
}

// getSelectedFractal retrieves the selected fractal for the current user session (fractal-only, for backwards compat).
func (h *Handler) getSelectedFractal(r *http.Request) (string, error) {
	fractalID, _, err := h.getScope(r)
	return fractalID, err
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
	}

	alert, err := h.manager.DuplicateAlert(r.Context(), alertID, user)
	if err != nil {
		log.Printf("[Alerts] Failed to duplicate alert: %v", err)
		h.respondError(w, http.StatusBadRequest, "Failed to duplicate alert")
		return
	}

	h.respondSuccess(w, alert)
}

// HandleListFeedAlerts returns all feed alerts for the current fractal (viewer+).
func (h *Handler) HandleListFeedAlerts(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleViewer) {
		return
	}

	fractalID, prismID, err := h.getScope(r)
	if err != nil {
		log.Printf("[Alerts] Failed to get scope for feed alerts: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load feed alerts")
		return
	}

	alerts, err := h.manager.ListAllFeedAlerts(r.Context(), fractalID, prismID)
	if err != nil {
		log.Printf("[Alerts] Failed to list feed alerts: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to load feed alerts")
		return
	}

	h.respondSuccess(w, alerts)
}

// HandleBatchToggleFeedAlerts enables or disables a set of feed alerts by ID.
func (h *Handler) HandleBatchToggleFeedAlerts(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleAnalyst) {
		return
	}
	user := h.getCurrentUser(r)

	var req struct {
		AlertIDs []string `json:"alert_ids"`
		Enabled  bool     `json:"enabled"`
	}
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

	count, err := h.manager.BatchToggleFeedAlerts(r.Context(), req.AlertIDs, req.Enabled, user)
	if err != nil {
		log.Printf("[Alerts] Failed to batch toggle feed alerts: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to update feed alerts")
		return
	}

	h.respondSuccess(w, map[string]int{"toggled": count})
}

// HandleBatchToggleAlerts enables or disables a set of non-feed alerts by ID.
func (h *Handler) HandleBatchToggleAlerts(w http.ResponseWriter, r *http.Request) {
	if !h.requireRole(w, r, rbac.RoleAnalyst) {
		return
	}
	user := h.getCurrentUser(r)

	var req struct {
		AlertIDs []string `json:"alert_ids"`
		Enabled  bool     `json:"enabled"`
	}
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

	count, err := h.manager.BatchToggleAlerts(r.Context(), req.AlertIDs, req.Enabled, user)
	if err != nil {
		log.Printf("[Alerts] Failed to batch toggle alerts: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to update alerts")
		return
	}

	h.respondSuccess(w, map[string]int{"toggled": count})
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
	if !h.requireRoleOnFractal(w, r, alert.FractalID, rbac.RoleAnalyst) {
		return
	}

	var req struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if err := h.manager.ToggleFeedAlert(r.Context(), alertID, req.Enabled, userObj.Username); err != nil {
		log.Printf("[Alerts] Failed to toggle feed alert: %v", err)
		h.respondError(w, http.StatusInternalServerError, "Failed to update feed alerts")
		return
	}

	h.respondSuccess(w, nil)
}