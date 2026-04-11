package alerts

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v3"
	"bifract/pkg/normalizers"
	"bifract/pkg/parser"
	"bifract/pkg/sigma"
	"bifract/pkg/storage"
)

// Manager handles CRUD operations and YAML import/export for alerts and webhooks
type Manager struct {
	pg                *storage.PostgresClient
	engine            *Engine
	normalizerManager *normalizers.Manager
}

// nullableID returns nil for empty strings so the DB stores NULL, and the
// raw string otherwise. Used for scope columns (fractal_id / prism_id)
// where exactly one side is populated.
func nullableID(id string) interface{} {
	if id == "" {
		return nil
	}
	return id
}

// scopedCountQuery builds a SELECT COUNT(*) ... WHERE id = ANY($1) AND <scope>
// query for an action table, enforcing that rows belong to the given scope.
// Callers must provide exactly one of fractalID or prismID.
func scopedCountQuery(table string, ids []string, fractalID, prismID string) (string, []interface{}) {
	args := []interface{}{pq.Array(ids)}
	scope := "FALSE"
	if prismID != "" {
		args = append(args, prismID)
		scope = "prism_id = $2"
	} else if fractalID != "" {
		args = append(args, fractalID)
		scope = "fractal_id = $2"
	}
	return fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id = ANY($1) AND %s", table, scope), args
}

// YAMLAlert represents the YAML format for alert definitions
type YAMLAlert struct {
	Name                string   `yaml:"name"`
	Description         string   `yaml:"description"`
	QueryString         string   `yaml:"queryString"`
	AlertType           string   `yaml:"alertType"`
	Severity            string   `yaml:"severity,omitempty"`
	ActionNames         []string `yaml:"actionNames"`
	Labels              []string `yaml:"labels"`
	References          []string `yaml:"references,omitempty"`
	Enabled             bool     `yaml:"enabled"`
	ThrottleTimeSeconds int      `yaml:"throttleTimeSeconds"`
	ThrottleField       string   `yaml:"throttleField"`
	WindowDuration      *int     `yaml:"windowDuration,omitempty"`
	ScheduleCron        *string  `yaml:"scheduleCron,omitempty"`
	QueryWindowSeconds  *int     `yaml:"queryWindowSeconds,omitempty"`
}

// AlertCreateRequest represents a request to create a new alert
type AlertCreateRequest struct {
	Name                  string   `json:"name"`
	Description           string   `json:"description"`
	QueryString           string   `json:"query_string"`
	AlertType             string   `json:"alert_type"`
	WebhookActionIDs      []string `json:"webhook_action_ids"`
	FractalActionIDs      []string `json:"fractal_action_ids"`
	DictionaryActionIDs   []string `json:"dictionary_action_ids"`
	EmailActionIDs        []string `json:"email_action_ids"`
	Labels                []string `json:"labels"`
	References            []string `json:"references"`
	Severity              string   `json:"severity"`
	Enabled               bool     `json:"enabled"`
	ThrottleTimeSeconds   int      `json:"throttle_time_seconds"`
	ThrottleField         string   `json:"throttle_field"`
	WindowDuration        *int     `json:"window_duration,omitempty"`
	ScheduleCron          *string  `json:"schedule_cron,omitempty"`
	QueryWindowSeconds    *int     `json:"query_window_seconds,omitempty"`
}

// AlertUpdateRequest represents a request to update an existing alert
type AlertUpdateRequest struct {
	Name                  string   `json:"name"`
	Description           string   `json:"description"`
	QueryString           string   `json:"query_string"`
	AlertType             string   `json:"alert_type"`
	WebhookActionIDs      []string `json:"webhook_action_ids"`
	FractalActionIDs      []string `json:"fractal_action_ids"`
	DictionaryActionIDs   []string `json:"dictionary_action_ids"`
	EmailActionIDs        []string `json:"email_action_ids"`
	Labels                []string `json:"labels"`
	References            []string `json:"references"`
	Severity              string   `json:"severity"`
	Enabled               bool     `json:"enabled"`
	ThrottleTimeSeconds   int      `json:"throttle_time_seconds"`
	ThrottleField         string   `json:"throttle_field"`
	WindowDuration        *int     `json:"window_duration,omitempty"`
	ScheduleCron          *string  `json:"schedule_cron,omitempty"`
	QueryWindowSeconds    *int     `json:"query_window_seconds,omitempty"`
}

// EmailActionCreateRequest represents a request to create a new email action
type EmailActionCreateRequest struct {
	Name            string   `json:"name"`
	Recipients      []string `json:"recipients"`
	SubjectTemplate string   `json:"subject_template"`
	BodyTemplate    string   `json:"body_template"`
	Enabled         bool     `json:"enabled"`
}

// EmailActionUpdateRequest represents a request to update an existing email action
type EmailActionUpdateRequest struct {
	Name            string   `json:"name"`
	Recipients      []string `json:"recipients"`
	SubjectTemplate string   `json:"subject_template"`
	BodyTemplate    string   `json:"body_template"`
	Enabled         bool     `json:"enabled"`
}

// WebhookCreateRequest represents a request to create a new webhook action
type WebhookCreateRequest struct {
	Name             string            `json:"name"`
	URL              string            `json:"url"`
	Method           string            `json:"method"`
	Headers          map[string]string `json:"headers"`
	AuthType         string            `json:"auth_type"`
	AuthConfig       map[string]string `json:"auth_config"`
	TimeoutSeconds   int               `json:"timeout_seconds"`
	RetryCount       int               `json:"retry_count"`
	IncludeAlertLink *bool             `json:"include_alert_link"`
	Enabled          bool              `json:"enabled"`
}

// WebhookUpdateRequest represents a request to update an existing webhook action
type WebhookUpdateRequest struct {
	Name             string            `json:"name"`
	URL              string            `json:"url"`
	Method           string            `json:"method"`
	Headers          map[string]string `json:"headers"`
	AuthType         string            `json:"auth_type"`
	AuthConfig       map[string]string `json:"auth_config"`
	TimeoutSeconds   int               `json:"timeout_seconds"`
	RetryCount       int               `json:"retry_count"`
	IncludeAlertLink *bool             `json:"include_alert_link"`
	Enabled          bool              `json:"enabled"`
}

// FractalActionCreateRequest represents a request to create a new fractal action
type FractalActionCreateRequest struct {
	Name               string            `json:"name"`
	Description        string            `json:"description"`
	TargetFractalID    string            `json:"target_fractal_id"`
	PreserveTimestamp  bool              `json:"preserve_timestamp"`
	AddAlertContext    bool              `json:"add_alert_context"`
	FieldMappings      map[string]string `json:"field_mappings"`
	MaxLogsPerTrigger  int               `json:"max_logs_per_trigger"`
	Enabled            bool              `json:"enabled"`
}

// FractalActionUpdateRequest represents a request to update an existing fractal action
type FractalActionUpdateRequest struct {
	Name               string            `json:"name"`
	Description        string            `json:"description"`
	TargetFractalID    string            `json:"target_fractal_id"`
	PreserveTimestamp  bool              `json:"preserve_timestamp"`
	AddAlertContext    bool              `json:"add_alert_context"`
	FieldMappings      map[string]string `json:"field_mappings"`
	MaxLogsPerTrigger  int               `json:"max_logs_per_trigger"`
	Enabled            bool              `json:"enabled"`
}

// NewManager creates a new alert manager
func NewManager(pg *storage.PostgresClient, engine *Engine, normalizerMgr *normalizers.Manager) *Manager {
	return &Manager{
		pg:                pg,
		engine:            engine,
		normalizerManager: normalizerMgr,
	}
}

// ImportFromYAML parses YAML content and creates/updates an alert.
// If the YAML is a Sigma rule, it is automatically translated to BQL.
// normalizerID optionally specifies a normalizer to map Sigma field names.
func (m *Manager) ImportFromYAML(ctx context.Context, yamlContent string, createdBy string, fractalID, prismID string, normalizerID string) (*Alert, error) {
	// Auto-detect Sigma rules
	if sigma.IsSigmaRule(yamlContent) {
		return m.importSigmaRule(ctx, yamlContent, createdBy, fractalID, prismID, normalizerID)
	}

	var yamlAlert YAMLAlert
	if err := yaml.Unmarshal([]byte(yamlContent), &yamlAlert); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	// Validate required fields
	if strings.TrimSpace(yamlAlert.Name) == "" {
		return nil, fmt.Errorf("alert name is required")
	}
	if strings.TrimSpace(yamlAlert.QueryString) == "" {
		return nil, fmt.Errorf("query string is required")
	}

	// Validate query syntax using existing parser
	_, err := parser.ParseQuery(yamlAlert.QueryString)
	if err != nil {
		return nil, fmt.Errorf("invalid query syntax: %w", err)
	}

	// Resolve webhook actions by name within the import's scope
	webhookIDs, err := m.resolveWebhookActionsByName(ctx, yamlAlert.ActionNames, fractalID, prismID)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve webhook actions: %w", err)
	}

	// Check if alert already exists (update vs create)
	existingAlert, err := m.GetAlertByName(ctx, yamlAlert.Name)
	if err == nil {
		// Alert exists - update it
		updateReq := AlertUpdateRequest{
			Name:                yamlAlert.Name,
			Description:         yamlAlert.Description,
			QueryString:         yamlAlert.QueryString,
			AlertType:           yamlAlert.AlertType,
			Severity:            yamlAlert.Severity,
			WebhookActionIDs:    webhookIDs,
			Labels:              yamlAlert.Labels,
			References:          yamlAlert.References,
			Enabled:             yamlAlert.Enabled,
			ThrottleTimeSeconds: yamlAlert.ThrottleTimeSeconds,
			ThrottleField:       yamlAlert.ThrottleField,
			WindowDuration:      yamlAlert.WindowDuration,
			ScheduleCron:        yamlAlert.ScheduleCron,
			QueryWindowSeconds:  yamlAlert.QueryWindowSeconds,
		}
		return m.UpdateAlert(ctx, existingAlert.ID, updateReq, createdBy)
	}

	// Alert doesn't exist - create new one
	createReq := AlertCreateRequest{
		Name:                yamlAlert.Name,
		Description:         yamlAlert.Description,
		QueryString:         yamlAlert.QueryString,
		AlertType:           yamlAlert.AlertType,
		Severity:            yamlAlert.Severity,
		WebhookActionIDs:    webhookIDs,
		Labels:              yamlAlert.Labels,
		References:          yamlAlert.References,
		Enabled:             yamlAlert.Enabled,
		ThrottleTimeSeconds: yamlAlert.ThrottleTimeSeconds,
		ThrottleField:       yamlAlert.ThrottleField,
		WindowDuration:      yamlAlert.WindowDuration,
		ScheduleCron:        yamlAlert.ScheduleCron,
		QueryWindowSeconds:  yamlAlert.QueryWindowSeconds,
	}

	return m.CreateAlert(ctx, createReq, createdBy, fractalID, prismID)
}

// importSigmaRule translates a Sigma YAML rule into a BQL-based alert.
func (m *Manager) importSigmaRule(ctx context.Context, yamlContent string, createdBy string, fractalID, prismID string, normalizerID string) (*Alert, error) {
	rule, err := sigma.ParseSigmaRule(yamlContent)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Sigma rule: %w", err)
	}

	// Build field mapper from normalizer if specified
	var fieldMapper func(string) string
	if normalizerID != "" && m.normalizerManager != nil {
		compiled := m.normalizerManager.CompileByID(ctx, normalizerID)
		fieldMapper = sigma.BuildFieldMapper(compiled)
	}

	// Translate detection to BQL
	queryString, err := sigma.Translate(rule, fieldMapper)
	if err != nil {
		return nil, fmt.Errorf("failed to translate Sigma rule: %w", err)
	}

	// Validate the generated query parses correctly
	_, err = parser.ParseQuery(queryString)
	if err != nil {
		return nil, fmt.Errorf("generated BQL query is invalid: %w (query: %s)", err, queryString)
	}

	// Map Sigma metadata to alert fields
	labels := sigmaLabels(rule)
	description := sigmaDescription(rule)

	// Check if alert with same title already exists
	existingAlert, err := m.GetAlertByName(ctx, rule.Title)
	if err == nil {
		updateReq := AlertUpdateRequest{
			Name:        rule.Title,
			Description: description,
			QueryString: queryString,
			AlertType:   "event",
			Severity:    sigmaSeverity(rule.Level),
			Labels:      labels,
			References:  rule.References,
			Enabled:     false,
		}
		return m.UpdateAlert(ctx, existingAlert.ID, updateReq, createdBy)
	}

	createReq := AlertCreateRequest{
		Name:        rule.Title,
		Description: description,
		QueryString: queryString,
		AlertType:   "event",
		Severity:    sigmaSeverity(rule.Level),
		Labels:      labels,
		References:  rule.References,
		Enabled:     false, // Disabled by default for review
	}

	return m.CreateAlert(ctx, createReq, createdBy, fractalID, prismID)
}

func sigmaSeverity(level string) string {
	switch strings.ToLower(level) {
	case "critical":
		return "critical"
	case "high":
		return "high"
	case "medium":
		return "medium"
	case "low":
		return "low"
	case "informational":
		return "info"
	default:
		return "medium"
	}
}

func sigmaLabels(rule *sigma.SigmaRule) []string {
	var labels []string
	if rule.Level != "" {
		labels = append(labels, "sigma:"+rule.Level)
	}
	for _, tag := range rule.Tags {
		labels = append(labels, tag)
	}
	if rule.LogSource.Product != "" {
		labels = append(labels, "product:"+rule.LogSource.Product)
	}
	if rule.LogSource.Category != "" {
		labels = append(labels, "category:"+rule.LogSource.Category)
	}
	return labels
}

func sigmaDescription(rule *sigma.SigmaRule) string {
	var parts []string
	if rule.Description != "" {
		parts = append(parts, rule.Description)
	}
	if rule.ID != "" {
		parts = append(parts, "Sigma ID: "+rule.ID)
	}
	if rule.Author != "" {
		parts = append(parts, "Author: "+rule.Author)
	}
	if len(rule.FalsePositives) > 0 {
		parts = append(parts, "False positives: "+strings.Join(rule.FalsePositives, ", "))
	}
	return strings.Join(parts, "\n")
}

// CreateAlert creates a new alert scoped to either a fractal or a prism (pass one, leave other empty).
func (m *Manager) CreateAlert(ctx context.Context, req AlertCreateRequest, createdBy string, fractalID, prismID string) (*Alert, error) {
	// Validate query syntax
	parsedQuery, err := parser.ParseQuery(req.QueryString)
	if err != nil {
		return nil, fmt.Errorf("invalid query syntax: %w", err)
	}

	// Validate webhook action IDs exist in the current scope
	if err := m.validateWebhookActionIDs(ctx, req.WebhookActionIDs, fractalID, prismID); err != nil {
		return nil, fmt.Errorf("invalid webhook actions: %w", err)
	}

	// Validate fractal action IDs exist in the current scope
	if err := m.validateFractalActionIDs(ctx, req.FractalActionIDs, fractalID, prismID); err != nil {
		return nil, fmt.Errorf("invalid fractal actions: %w", err)
	}

	// Validate dictionary action IDs exist in the current scope
	if err := m.validateDictionaryActionIDs(ctx, req.DictionaryActionIDs, fractalID, prismID); err != nil {
		return nil, fmt.Errorf("invalid dictionary actions: %w", err)
	}

	// Validate email action IDs exist in the current scope
	if err := m.validateEmailActionIDs(ctx, req.EmailActionIDs, fractalID, prismID); err != nil {
		return nil, fmt.Errorf("invalid email actions: %w", err)
	}

	// Start transaction
	tx, err := m.pg.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Insert alert — set either fractal_id or prism_id based on scope
	alertID := ""
	var fractalIDPtr, prismIDPtr interface{}
	if prismID != "" {
		prismIDPtr = prismID
	} else {
		fractalIDPtr = fractalID
	}
	alertType := req.AlertType
	if alertType == "" {
		alertType = "event"
	}

	// Validate compound alert requirements
	if alertType == "compound" {
		if req.WindowDuration == nil || *req.WindowDuration <= 0 {
			return nil, fmt.Errorf("compound alerts require a positive window_duration")
		}
	}

	// Event alerts must not use aggregate functions
	if alertType == "event" && queryHasAggregation(parsedQuery) {
		return nil, fmt.Errorf("event alerts cannot use aggregate functions (groupby, count, sum, etc.); use a compound alert instead")
	}

	// Validate scheduled alert requirements
	if alertType == "scheduled" {
		if req.ScheduleCron == nil || strings.TrimSpace(*req.ScheduleCron) == "" {
			return nil, fmt.Errorf("scheduled alerts require a cron expression (schedule_cron)")
		}
		if req.QueryWindowSeconds == nil || *req.QueryWindowSeconds <= 0 {
			return nil, fmt.Errorf("scheduled alerts require a positive query_window_seconds")
		}
		cronParser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		if _, err := cronParser.Parse(*req.ScheduleCron); err != nil {
			return nil, fmt.Errorf("invalid cron expression: %w", err)
		}
	}

	severity := req.Severity
	if severity == "" {
		severity = "medium"
	}

	query := `
		INSERT INTO alerts (name, description, query_string, alert_type, enabled, throttle_time_seconds, throttle_field, labels, "references", severity, created_by, fractal_id, prism_id, window_duration, schedule_cron, query_window_seconds)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		RETURNING id
	`
	err = tx.QueryRow(ctx, query,
		req.Name, req.Description, req.QueryString, alertType, req.Enabled,
		req.ThrottleTimeSeconds, req.ThrottleField, pq.Array(req.Labels), pq.Array(req.References), severity, createdBy, fractalIDPtr, prismIDPtr, req.WindowDuration,
		req.ScheduleCron, req.QueryWindowSeconds,
	).Scan(&alertID)
	if err != nil {
		return nil, fmt.Errorf("failed to insert alert: %w", err)
	}

	// Insert webhook action associations
	if len(req.WebhookActionIDs) > 0 {
		if err := m.associateWebhookActions(ctx, tx, alertID, req.WebhookActionIDs); err != nil {
			return nil, err
		}
	}

	// Insert fractal action associations
	if len(req.FractalActionIDs) > 0 {
		if err := m.associateFractalActions(ctx, tx, alertID, req.FractalActionIDs); err != nil {
			return nil, err
		}
	}

	// Insert dictionary action associations
	if len(req.DictionaryActionIDs) > 0 {
		if err := m.associateDictionaryActions(ctx, tx, alertID, req.DictionaryActionIDs); err != nil {
			return nil, err
		}
	}

	// Insert email action associations
	if len(req.EmailActionIDs) > 0 {
		if err := m.associateEmailActions(ctx, tx, alertID, req.EmailActionIDs); err != nil {
			return nil, err
		}
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Refresh engine cache
	if err := m.engine.RefreshAlerts(ctx); err != nil {
		fmt.Printf("Warning: failed to refresh alert cache: %v\n", err)
	}

	// Return the created alert
	alert, err := m.GetAlert(ctx, alertID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve created alert: %w", err)
	}

	alert.ParsedQuery = parsedQuery
	return alert, nil
}

// UpdateAlert updates an existing alert
func (m *Manager) UpdateAlert(ctx context.Context, alertID string, req AlertUpdateRequest, username string) (*Alert, error) {
	// Validate query syntax
	parsedQuery, err := parser.ParseQuery(req.QueryString)
	if err != nil {
		return nil, fmt.Errorf("invalid query syntax: %w", err)
	}

	// Look up the alert's existing scope so action validation stays scoped.
	var existingFractalID, existingPrismID string
	if err := m.pg.QueryRow(ctx,
		`SELECT COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, '') FROM alerts WHERE id = $1`,
		alertID,
	).Scan(&existingFractalID, &existingPrismID); err != nil {
		return nil, fmt.Errorf("failed to load alert scope: %w", err)
	}

	// Validate webhook action IDs exist in the alert's scope
	if err := m.validateWebhookActionIDs(ctx, req.WebhookActionIDs, existingFractalID, existingPrismID); err != nil {
		return nil, fmt.Errorf("invalid webhook actions: %w", err)
	}

	// Validate fractal action IDs exist in the alert's scope
	if err := m.validateFractalActionIDs(ctx, req.FractalActionIDs, existingFractalID, existingPrismID); err != nil {
		return nil, fmt.Errorf("invalid fractal actions: %w", err)
	}

	// Validate dictionary action IDs exist in the alert's scope
	if err := m.validateDictionaryActionIDs(ctx, req.DictionaryActionIDs, existingFractalID, existingPrismID); err != nil {
		return nil, fmt.Errorf("invalid dictionary actions: %w", err)
	}

	// Validate email action IDs exist in the alert's scope
	if err := m.validateEmailActionIDs(ctx, req.EmailActionIDs, existingFractalID, existingPrismID); err != nil {
		return nil, fmt.Errorf("invalid email actions: %w", err)
	}

	// Start transaction
	tx, err := m.pg.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Determine alert type (preserve existing if not specified)
	alertType := req.AlertType
	if alertType == "" {
		alertType = "event"
	}

	// Validate compound alert requirements
	if alertType == "compound" {
		if req.WindowDuration == nil || *req.WindowDuration <= 0 {
			return nil, fmt.Errorf("compound alerts require a positive window_duration")
		}
	}

	// Event alerts must not use aggregate functions
	if alertType == "event" && queryHasAggregation(parsedQuery) {
		return nil, fmt.Errorf("event alerts cannot use aggregate functions (groupby, count, sum, etc.); use a compound alert instead")
	}

	// Validate scheduled alert requirements
	if alertType == "scheduled" {
		if req.ScheduleCron == nil || strings.TrimSpace(*req.ScheduleCron) == "" {
			return nil, fmt.Errorf("scheduled alerts require a cron expression (schedule_cron)")
		}
		if req.QueryWindowSeconds == nil || *req.QueryWindowSeconds <= 0 {
			return nil, fmt.Errorf("scheduled alerts require a positive query_window_seconds")
		}
		cronParser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		if _, err := cronParser.Parse(*req.ScheduleCron); err != nil {
			return nil, fmt.Errorf("invalid cron expression: %w", err)
		}
	}

	severity := req.Severity
	if severity == "" {
		severity = "medium"
	}

	// Update alert (clear disabled_reason when re-enabling)
	query := `
		UPDATE alerts
		SET name = $2, description = $3, query_string = $4, enabled = $5,
		    throttle_time_seconds = $6, throttle_field = $7, labels = $8,
		    "references" = $9, severity = $10, updated_by = $11,
		    alert_type = $12, window_duration = $13,
		    schedule_cron = $14, query_window_seconds = $15,
		    disabled_reason = CASE WHEN $5 = true THEN NULL ELSE disabled_reason END,
		    updated_at = NOW()
		WHERE id = $1
	`
	result, err := tx.Exec(ctx, query,
		alertID, req.Name, req.Description, req.QueryString, req.Enabled,
		req.ThrottleTimeSeconds, req.ThrottleField, pq.Array(req.Labels),
		pq.Array(req.References), severity, username, alertType, req.WindowDuration,
		req.ScheduleCron, req.QueryWindowSeconds,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to update alert: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return nil, fmt.Errorf("alert not found")
	}

	// Remove existing webhook associations
	_, err = tx.Exec(ctx, "DELETE FROM alert_webhook_actions WHERE alert_id = $1", alertID)
	if err != nil {
		return nil, fmt.Errorf("failed to remove existing webhook associations: %w", err)
	}

	// Insert new webhook action associations
	if len(req.WebhookActionIDs) > 0 {
		if err := m.associateWebhookActions(ctx, tx, alertID, req.WebhookActionIDs); err != nil {
			return nil, err
		}
	}

	// Remove existing fractal action associations
	_, err = tx.Exec(ctx, "DELETE FROM alert_fractal_actions WHERE alert_id = $1", alertID)
	if err != nil {
		return nil, fmt.Errorf("failed to remove existing fractal action associations: %w", err)
	}

	// Insert new fractal action associations
	if len(req.FractalActionIDs) > 0 {
		if err := m.associateFractalActions(ctx, tx, alertID, req.FractalActionIDs); err != nil {
			return nil, err
		}
	}

	// Remove existing dictionary action associations
	_, err = tx.Exec(ctx, "DELETE FROM alert_dictionary_actions WHERE alert_id = $1", alertID)
	if err != nil {
		return nil, fmt.Errorf("failed to remove existing dictionary action associations: %w", err)
	}

	// Insert new dictionary action associations
	if len(req.DictionaryActionIDs) > 0 {
		if err := m.associateDictionaryActions(ctx, tx, alertID, req.DictionaryActionIDs); err != nil {
			return nil, err
		}
	}

	// Remove existing email action associations
	_, err = tx.Exec(ctx, "DELETE FROM alert_email_actions WHERE alert_id = $1", alertID)
	if err != nil {
		return nil, fmt.Errorf("failed to remove existing email action associations: %w", err)
	}

	// Insert new email action associations
	if len(req.EmailActionIDs) > 0 {
		if err := m.associateEmailActions(ctx, tx, alertID, req.EmailActionIDs); err != nil {
			return nil, err
		}
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Refresh engine cache
	if err := m.engine.RefreshAlerts(ctx); err != nil {
		fmt.Printf("Warning: failed to refresh alert cache: %v\n", err)
	}

	// Return the updated alert
	alert, err := m.GetAlert(ctx, alertID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve updated alert: %w", err)
	}

	alert.ParsedQuery = parsedQuery
	return alert, nil
}

// GetAlert retrieves an alert by ID
func (m *Manager) GetAlert(ctx context.Context, alertID string) (*Alert, error) {
	query := `
		SELECT a.id, a.name, COALESCE(a.description, ''), a.query_string, COALESCE(a.alert_type, 'event'), a.enabled,
		       COALESCE(a.throttle_time_seconds, 0), COALESCE(a.throttle_field, ''), a.labels, a."references",
		       COALESCE(a.severity, 'medium'), COALESCE(a.fractal_id::text, ''),
		       COALESCE(a.created_by, ''), COALESCE(a.updated_by, ''), a.created_at, a.updated_at, a.last_triggered,
		       COALESCE(a.disabled_reason, ''), COALESCE(a.window_duration, 0),
		       COALESCE(a.schedule_cron, ''), COALESCE(a.query_window_seconds, 0),
		       COALESCE(json_agg(
		           json_build_object(
		               'id', wa.id,
		               'name', wa.name,
		               'url', wa.url,
		               'method', wa.method,
		               'headers', wa.headers,
		               'auth_type', wa.auth_type,
		               'auth_config', wa.auth_config,
		               'timeout_seconds', wa.timeout_seconds,
		               'retry_count', wa.retry_count,
		               'enabled', wa.enabled
		           ) ORDER BY wa.name
		       ) FILTER (WHERE wa.id IS NOT NULL), '[]'::json) as webhook_actions,
		       COALESCE(json_agg(
		           json_build_object(
		               'id', fa.id,
		               'name', fa.name,
		               'description', fa.description,
		               'target_fractal_id', fa.target_fractal_id,
		               'preserve_timestamp', fa.preserve_timestamp,
		               'add_alert_context', fa.add_alert_context,
		               'field_mappings', fa.field_mappings,
		               'max_logs_per_trigger', fa.max_logs_per_trigger,
		               'enabled', fa.enabled
		           ) ORDER BY fa.name
		       ) FILTER (WHERE fa.id IS NOT NULL), '[]'::json) as fractal_actions,
		       COALESCE(json_agg(
		           json_build_object(
		               'id', ea.id,
		               'name', ea.name,
		               'recipients', ea.recipients,
		               'subject_template', ea.subject_template,
		               'body_template', ea.body_template,
		               'enabled', ea.enabled
		           ) ORDER BY ea.name
		       ) FILTER (WHERE ea.id IS NOT NULL), '[]'::json) as email_actions
		FROM alerts a
		LEFT JOIN alert_webhook_actions awa ON a.id = awa.alert_id
		LEFT JOIN webhook_actions wa ON awa.webhook_id = wa.id AND wa.enabled = true
		LEFT JOIN alert_fractal_actions afa ON a.id = afa.alert_id
		LEFT JOIN fractal_actions fa ON afa.fractal_action_id = fa.id AND fa.enabled = true
		LEFT JOIN alert_email_actions aea ON a.id = aea.alert_id
		LEFT JOIN email_actions ea ON aea.email_action_id = ea.id AND ea.enabled = true
		WHERE a.id = $1
		GROUP BY a.id, a.name, a.description, a.query_string, a.alert_type, a.enabled,
		         a.throttle_time_seconds, a.throttle_field, a.labels, a."references", a.severity, a.fractal_id,
		         a.created_by, a.updated_by, a.created_at, a.updated_at, a.last_triggered,
		         a.disabled_reason, a.window_duration, a.schedule_cron, a.query_window_seconds
	`

	var alert Alert
	var webhookActionsJSON []byte
	var fractalActionsJSON []byte
	var emailActionsJSON []byte

	err := m.pg.QueryRow(ctx, query, alertID).Scan(
		&alert.ID, &alert.Name, &alert.Description, &alert.QueryString, &alert.AlertType,
		&alert.Enabled, &alert.ThrottleTimeSeconds, &alert.ThrottleField,
		pq.Array(&alert.Labels), pq.Array(&alert.References), &alert.Severity, &alert.FractalID, &alert.CreatedBy, &alert.UpdatedBy,
		&alert.CreatedAt, &alert.UpdatedAt,
		&alert.LastTriggered, &alert.DisabledReason, &alert.WindowDuration,
		&alert.ScheduleCron, &alert.QueryWindowSeconds,
		&webhookActionsJSON, &fractalActionsJSON, &emailActionsJSON,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get alert: %w", err)
	}

	// Parse webhook actions from JSON
	if err := json.Unmarshal(webhookActionsJSON, &alert.WebhookActions); err != nil {
		return nil, fmt.Errorf("failed to parse webhook actions: %w", err)
	}

	// Parse fractal actions from JSON
	if err := json.Unmarshal(fractalActionsJSON, &alert.FractalActions); err != nil {
		return nil, fmt.Errorf("failed to parse fractal actions: %w", err)
	}

	// Parse email actions from JSON
	if err := json.Unmarshal(emailActionsJSON, &alert.EmailActions); err != nil {
		return nil, fmt.Errorf("failed to parse email actions: %w", err)
	}

	// Load dictionary action IDs
	alert.DictionaryActionIDs = m.loadDictionaryActionIDs(ctx, alert.ID)

	// Load email action IDs
	alert.EmailActionIDs = m.loadEmailActionIDs(ctx, alert.ID)

	return &alert, nil
}

// GetAlertByName retrieves an alert by name
func (m *Manager) GetAlertByName(ctx context.Context, name string) (*Alert, error) {
	query := `SELECT id FROM alerts WHERE name = $1`
	var alertID string
	err := m.pg.QueryRow(ctx, query, name).Scan(&alertID)
	if err != nil {
		return nil, fmt.Errorf("alert not found: %w", err)
	}
	return m.GetAlert(ctx, alertID)
}

// ListAlerts retrieves all alerts with optional filtering.
// Pass either fractalID or prismID (not both); the other should be empty.
func (m *Manager) ListAlerts(ctx context.Context, enabledOnly bool, fractalID, prismID string) ([]*Alert, error) {
	baseQuery := `
		SELECT a.id, a.name, a.description, a.query_string, COALESCE(a.alert_type, 'event'), a.enabled,
		       a.throttle_time_seconds, a.throttle_field, a.labels, a."references",
		       COALESCE(a.severity, 'medium'), COALESCE(a.fractal_id::text, ''), COALESCE(a.prism_id::text, ''),
		       COALESCE(a.created_by, ''), COALESCE(a.updated_by, ''), a.created_at, a.updated_at, a.last_triggered,
		       COALESCE(a.disabled_reason, ''),
		       (SELECT ae.execution_time_ms FROM alert_executions ae WHERE ae.alert_id = a.id ORDER BY ae.triggered_at DESC LIMIT 1),
		       a.window_duration,
		       COALESCE(a.schedule_cron, ''), COALESCE(a.query_window_seconds, 0),
		       COALESCE(json_agg(
		           json_build_object(
		               'id', wa.id,
		               'name', wa.name,
		               'url', wa.url,
		               'method', wa.method,
		               'headers', wa.headers,
		               'auth_type', wa.auth_type,
		               'auth_config', wa.auth_config,
		               'timeout_seconds', wa.timeout_seconds,
		               'retry_count', wa.retry_count,
		               'enabled', wa.enabled
		           ) ORDER BY wa.name
		       ) FILTER (WHERE wa.id IS NOT NULL), '[]'::json) as webhook_actions,
		       COALESCE(json_agg(
		           json_build_object(
		               'id', fa.id,
		               'name', fa.name,
		               'description', fa.description,
		               'target_fractal_id', fa.target_fractal_id,
		               'preserve_timestamp', fa.preserve_timestamp,
		               'add_alert_context', fa.add_alert_context,
		               'field_mappings', fa.field_mappings,
		               'max_logs_per_trigger', fa.max_logs_per_trigger,
		               'enabled', fa.enabled
		           ) ORDER BY fa.name
		       ) FILTER (WHERE fa.id IS NOT NULL), '[]'::json) as fractal_actions,
		       COALESCE(json_agg(
		           json_build_object(
		               'id', da.id,
		               'name', da.name
		           ) ORDER BY da.name
		       ) FILTER (WHERE da.id IS NOT NULL), '[]'::json) as dictionary_actions,
		       COALESCE(json_agg(
		           json_build_object(
		               'id', ea.id,
		               'name', ea.name
		           ) ORDER BY ea.name
		       ) FILTER (WHERE ea.id IS NOT NULL), '[]'::json) as email_actions
		FROM alerts a
		LEFT JOIN alert_webhook_actions awa ON a.id = awa.alert_id
		LEFT JOIN webhook_actions wa ON awa.webhook_id = wa.id AND wa.enabled = true
		LEFT JOIN alert_fractal_actions afa ON a.id = afa.alert_id
		LEFT JOIN fractal_actions fa ON afa.fractal_action_id = fa.id AND fa.enabled = true
		LEFT JOIN alert_dictionary_actions ada ON a.id = ada.alert_id
		LEFT JOIN dictionary_actions da ON ada.dictionary_action_id = da.id AND da.enabled = true
		LEFT JOIN alert_email_actions aea ON a.id = aea.alert_id
		LEFT JOIN email_actions ea ON aea.email_action_id = ea.id AND ea.enabled = true
	`

	var whereConditions []string
	var args []interface{}

	// Filter by scope (prism or fractal)
	if prismID != "" {
		whereConditions = append(whereConditions, "a.prism_id = $1")
		args = append(args, prismID)
	} else if fractalID != "" {
		whereConditions = append(whereConditions, "a.fractal_id = $1")
		args = append(args, fractalID)
	}

	// Exclude feed alerts from the main listing (they have their own tab)
	whereConditions = append(whereConditions, "a.feed_id IS NULL")

	// Add enabled filter if requested
	if enabledOnly {
		idx := len(args) + 1
		whereConditions = append(whereConditions, fmt.Sprintf("a.enabled = $%d", idx))
		args = append(args, true)
	}

	var whereClause string
	if len(whereConditions) > 0 {
		whereClause = " WHERE " + strings.Join(whereConditions, " AND ")
	}

	query := baseQuery + whereClause + `
		GROUP BY a.id, a.name, a.description, a.query_string, a.alert_type, a.enabled,
		         a.throttle_time_seconds, a.throttle_field, a.labels, a."references", a.severity, a.fractal_id, a.prism_id,
		         a.created_by, a.updated_by, a.created_at, a.updated_at, a.last_triggered,
		         a.disabled_reason, a.window_duration, a.schedule_cron, a.query_window_seconds
		ORDER BY a.name
	`

	rows, err := m.pg.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list alerts: %w", err)
	}
	defer rows.Close()

	var alerts []*Alert
	for rows.Next() {
		var alert Alert
		var webhookActionsJSON []byte
		var fractalActionsJSON []byte
		var dictionaryActionsJSON []byte
		var emailActionsJSON []byte

		err := rows.Scan(
			&alert.ID, &alert.Name, &alert.Description, &alert.QueryString, &alert.AlertType,
			&alert.Enabled, &alert.ThrottleTimeSeconds, &alert.ThrottleField,
			pq.Array(&alert.Labels), pq.Array(&alert.References), &alert.Severity, &alert.FractalID, &alert.PrismID,
			&alert.CreatedBy, &alert.UpdatedBy, &alert.CreatedAt, &alert.UpdatedAt,
			&alert.LastTriggered, &alert.DisabledReason, &alert.LastExecutionTimeMs,
			&alert.WindowDuration,
			&alert.ScheduleCron, &alert.QueryWindowSeconds,
			&webhookActionsJSON, &fractalActionsJSON, &dictionaryActionsJSON, &emailActionsJSON,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan alert: %w", err)
		}

		// Parse webhook actions from JSON
		if err := json.Unmarshal(webhookActionsJSON, &alert.WebhookActions); err != nil {
			return nil, fmt.Errorf("failed to parse webhook actions: %w", err)
		}

		// Parse fractal actions from JSON
		if err := json.Unmarshal(fractalActionsJSON, &alert.FractalActions); err != nil {
			return nil, fmt.Errorf("failed to parse fractal actions: %w", err)
		}

		// Parse dictionary action refs from JSON
		if err := json.Unmarshal(dictionaryActionsJSON, &alert.DictionaryActionRefs); err != nil {
			return nil, fmt.Errorf("failed to parse dictionary actions: %w", err)
		}

		// Parse email action refs from JSON
		var emailRefs []DictionaryActionRef
		if err := json.Unmarshal(emailActionsJSON, &emailRefs); err != nil {
			return nil, fmt.Errorf("failed to parse email actions: %w", err)
		}
		for _, ref := range emailRefs {
			alert.EmailActionIDs = append(alert.EmailActionIDs, ref.ID)
		}

		alerts = append(alerts, &alert)
	}

	return alerts, nil
}

// DeleteAlert removes an alert and its associations
func (m *Manager) DeleteAlert(ctx context.Context, alertID string) error {
	result, err := m.pg.Exec(ctx, "DELETE FROM alerts WHERE id = $1", alertID)
	if err != nil {
		return fmt.Errorf("failed to delete alert: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("alert not found")
	}

	// Refresh engine cache
	if err := m.engine.RefreshAlerts(ctx); err != nil {
		fmt.Printf("Warning: failed to refresh alert cache: %v\n", err)
	}

	return nil
}

// resolveWebhookActionsByName converts webhook action names to IDs within a scope.
func (m *Manager) resolveWebhookActionsByName(ctx context.Context, actionNames []string, fractalID, prismID string) ([]string, error) {
	if len(actionNames) == 0 {
		return []string{}, nil
	}

	args := []interface{}{pq.Array(actionNames)}
	scope := "FALSE"
	if prismID != "" {
		args = append(args, prismID)
		scope = "prism_id = $2"
	} else if fractalID != "" {
		args = append(args, fractalID)
		scope = "fractal_id = $2"
	}
	query := fmt.Sprintf(`SELECT id, name FROM webhook_actions WHERE name = ANY($1) AND enabled = true AND %s`, scope)
	rows, err := m.pg.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query webhook actions: %w", err)
	}
	defer rows.Close()

	nameToID := make(map[string]string)
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, fmt.Errorf("failed to scan webhook action: %w", err)
		}
		nameToID[name] = id
	}

	var webhookIDs []string
	var missingNames []string

	for _, name := range actionNames {
		if id, exists := nameToID[name]; exists {
			webhookIDs = append(webhookIDs, id)
		} else {
			missingNames = append(missingNames, name)
		}
	}

	if len(missingNames) > 0 {
		return nil, fmt.Errorf("webhook actions not found: %s", strings.Join(missingNames, ", "))
	}

	return webhookIDs, nil
}

// loadDictionaryActionIDs returns the dictionary action IDs linked to an alert.
func (m *Manager) loadDictionaryActionIDs(ctx context.Context, alertID string) []string {
	rows, err := m.pg.Query(ctx,
		"SELECT dictionary_action_id FROM alert_dictionary_actions WHERE alert_id = $1", alertID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			ids = append(ids, id)
		}
	}
	return ids
}

// loadEmailActionIDs returns the email action IDs linked to an alert.
func (m *Manager) loadEmailActionIDs(ctx context.Context, alertID string) []string {
	rows, err := m.pg.Query(ctx,
		"SELECT email_action_id FROM alert_email_actions WHERE alert_id = $1", alertID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			ids = append(ids, id)
		}
	}
	return ids
}

// validateDictionaryActionIDs validates that all dictionary action IDs exist
// and belong to the given fractal/prism scope.
func (m *Manager) validateDictionaryActionIDs(ctx context.Context, ids []string, fractalID, prismID string) error {
	if len(ids) == 0 {
		return nil
	}
	query, args := scopedCountQuery("dictionary_actions", ids, fractalID, prismID)
	var count int
	if err := m.pg.QueryRow(ctx, query, args...).Scan(&count); err != nil {
		return fmt.Errorf("failed to validate dictionary actions: %w", err)
	}
	if count != len(ids) {
		return fmt.Errorf("one or more dictionary actions not found in current scope")
	}
	return nil
}

// associateDictionaryActions creates associations between an alert and dictionary actions.
func (m *Manager) associateDictionaryActions(ctx context.Context, tx storage.Tx, alertID string, ids []string) error {
	for _, id := range ids {
		_, err := tx.Exec(ctx,
			"INSERT INTO alert_dictionary_actions (alert_id, dictionary_action_id) VALUES ($1, $2)",
			alertID, id,
		)
		if err != nil {
			return fmt.Errorf("failed to associate dictionary action: %w", err)
		}
	}
	return nil
}

func (m *Manager) validateEmailActionIDs(ctx context.Context, ids []string, fractalID, prismID string) error {
	if len(ids) == 0 {
		return nil
	}
	query, args := scopedCountQuery("email_actions", ids, fractalID, prismID)
	var count int
	if err := m.pg.QueryRow(ctx, query, args...).Scan(&count); err != nil {
		return fmt.Errorf("failed to validate email actions: %w", err)
	}
	if count != len(ids) {
		return fmt.Errorf("one or more email actions not found in current scope")
	}
	return nil
}

func (m *Manager) associateEmailActions(ctx context.Context, tx storage.Tx, alertID string, ids []string) error {
	for _, id := range ids {
		_, err := tx.Exec(ctx,
			"INSERT INTO alert_email_actions (alert_id, email_action_id) VALUES ($1, $2)",
			alertID, id,
		)
		if err != nil {
			return fmt.Errorf("failed to associate email action: %w", err)
		}
	}
	return nil
}

// aggregateCommands is the set of BQL pipeline commands that perform aggregation.
var aggregateCommands = map[string]bool{
	"groupby":    true,
	"count":      true,
	"sum":        true,
	"avg":        true,
	"min":        true,
	"max":        true,
	"multi":      true,
	"percentile": true,
	"stddev":     true,
	"median":     true,
	"mad":        true,
	"top":        true,
}

// queryHasAggregation returns true if the parsed query contains any aggregate
// commands (groupby, count, sum, etc.).
func queryHasAggregation(pipeline *parser.PipelineNode) bool {
	if pipeline == nil {
		return false
	}
	for _, cmd := range pipeline.Commands {
		if aggregateCommands[cmd.Name] {
			return true
		}
		// table() with aggregate arguments (count, sum, avg, etc.)
		if cmd.Name == "table" {
			for _, arg := range cmd.Arguments {
				if aggregateCommands[arg] {
					return true
				}
			}
		}
	}
	return false
}

// validateWebhookActionIDs validates that all webhook action IDs exist, are enabled,
// and belong to the given fractal/prism scope.
func (m *Manager) validateWebhookActionIDs(ctx context.Context, webhookIDs []string, fractalID, prismID string) error {
	if len(webhookIDs) == 0 {
		return nil
	}

	query, args := scopedCountQuery("webhook_actions", webhookIDs, fractalID, prismID)
	query += " AND enabled = true"
	var count int
	if err := m.pg.QueryRow(ctx, query, args...).Scan(&count); err != nil {
		return fmt.Errorf("failed to validate webhook actions: %w", err)
	}

	if count != len(webhookIDs) {
		return fmt.Errorf("one or more webhook actions not found, disabled, or not in current scope")
	}

	return nil
}

// associateWebhookActions creates associations between an alert and webhook actions
func (m *Manager) associateWebhookActions(ctx context.Context, tx storage.Tx, alertID string, webhookIDs []string) error {
	for _, webhookID := range webhookIDs {
		_, err := tx.Exec(ctx,
			"INSERT INTO alert_webhook_actions (alert_id, webhook_id) VALUES ($1, $2)",
			alertID, webhookID,
		)
		if err != nil {
			return fmt.Errorf("failed to associate webhook action: %w", err)
		}
	}
	return nil
}

// validateFractalActionIDs validates that all fractal action IDs exist, are enabled,
// and belong to the given fractal/prism scope.
func (m *Manager) validateFractalActionIDs(ctx context.Context, fractalActionIDs []string, fractalID, prismID string) error {
	if len(fractalActionIDs) == 0 {
		return nil
	}

	query, args := scopedCountQuery("fractal_actions", fractalActionIDs, fractalID, prismID)
	query += " AND enabled = true"
	var count int
	if err := m.pg.QueryRow(ctx, query, args...).Scan(&count); err != nil {
		return fmt.Errorf("failed to validate fractal actions: %w", err)
	}

	if count != len(fractalActionIDs) {
		return fmt.Errorf("one or more fractal actions not found, disabled, or not in current scope")
	}

	return nil
}

// associateFractalActions creates associations between an alert and fractal actions
func (m *Manager) associateFractalActions(ctx context.Context, tx storage.Tx, alertID string, fractalActionIDs []string) error {
	for _, fractalActionID := range fractalActionIDs {
		_, err := tx.Exec(ctx,
			"INSERT INTO alert_fractal_actions (alert_id, fractal_action_id) VALUES ($1, $2)",
			alertID, fractalActionID,
		)
		if err != nil {
			return fmt.Errorf("failed to associate fractal action: %w", err)
		}
	}
	return nil
}

// ============================
// Webhook Action Management
// ============================

// CreateWebhookAction creates a new webhook action scoped to the given fractal or prism.
// Pass exactly one of fractalID or prismID.
func (m *Manager) CreateWebhookAction(ctx context.Context, req WebhookCreateRequest, createdBy, fractalID, prismID string) (*WebhookAction, error) {
	if (fractalID == "") == (prismID == "") {
		return nil, fmt.Errorf("exactly one of fractal_id or prism_id must be set")
	}

	// Set defaults
	if req.Method == "" {
		req.Method = "POST"
	}
	if req.TimeoutSeconds <= 0 {
		req.TimeoutSeconds = 30
	}
	if req.RetryCount < 0 {
		req.RetryCount = 3
	}
	if req.Headers == nil {
		req.Headers = make(map[string]string)
	}
	if req.AuthConfig == nil {
		req.AuthConfig = make(map[string]string)
	}

	// Validate URL format (basic validation)
	if strings.TrimSpace(req.URL) == "" {
		return nil, fmt.Errorf("webhook URL is required")
	}

	includeAlertLink := true
	if req.IncludeAlertLink != nil {
		includeAlertLink = *req.IncludeAlertLink
	}

	var webhookID string
	query := `
		INSERT INTO webhook_actions (name, url, method, headers, auth_type, auth_config, timeout_seconds, retry_count, include_alert_link, enabled, created_by, fractal_id, prism_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		RETURNING id
	`

	headersJSON, _ := json.Marshal(req.Headers)
	authConfigJSON, _ := json.Marshal(req.AuthConfig)

	err := m.pg.QueryRow(ctx, query,
		req.Name, req.URL, req.Method, string(headersJSON), req.AuthType,
		string(authConfigJSON), req.TimeoutSeconds, req.RetryCount, includeAlertLink, req.Enabled, createdBy,
		nullableID(fractalID), nullableID(prismID),
	).Scan(&webhookID)
	if err != nil {
		return nil, fmt.Errorf("failed to create webhook action: %w", err)
	}

	return m.GetWebhookAction(ctx, webhookID)
}

// UpdateWebhookAction updates an existing webhook action
func (m *Manager) UpdateWebhookAction(ctx context.Context, webhookID string, req WebhookUpdateRequest) (*WebhookAction, error) {
	// Set defaults
	if req.Method == "" {
		req.Method = "POST"
	}
	if req.TimeoutSeconds <= 0 {
		req.TimeoutSeconds = 30
	}
	if req.RetryCount < 0 {
		req.RetryCount = 3
	}
	if req.Headers == nil {
		req.Headers = make(map[string]string)
	}
	if req.AuthConfig == nil {
		req.AuthConfig = make(map[string]string)
	}

	includeAlertLink := true
	if req.IncludeAlertLink != nil {
		includeAlertLink = *req.IncludeAlertLink
	}

	query := `
		UPDATE webhook_actions
		SET name = $2, url = $3, method = $4, headers = $5, auth_type = $6,
		    auth_config = $7, timeout_seconds = $8, retry_count = $9, include_alert_link = $10, enabled = $11, updated_at = NOW()
		WHERE id = $1
	`

	headersJSON, _ := json.Marshal(req.Headers)
	authConfigJSON, _ := json.Marshal(req.AuthConfig)

	result, err := m.pg.Exec(ctx, query,
		webhookID, req.Name, req.URL, req.Method, string(headersJSON), req.AuthType,
		string(authConfigJSON), req.TimeoutSeconds, req.RetryCount, includeAlertLink, req.Enabled,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to update webhook action: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return nil, fmt.Errorf("webhook action not found")
	}

	return m.GetWebhookAction(ctx, webhookID)
}

// GetWebhookAction retrieves a webhook action by ID
func (m *Manager) GetWebhookAction(ctx context.Context, webhookID string) (*WebhookAction, error) {
	query := `
		SELECT id, name, url, method, headers, auth_type, auth_config, timeout_seconds, retry_count, include_alert_link, enabled,
		       COALESCE(created_by, ''), created_at, updated_at,
		       COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, '')
		FROM webhook_actions
		WHERE id = $1
	`

	var webhook WebhookAction
	var headersJSON, authConfigJSON string
	var createdBy string
	var createdAt, updatedAt time.Time

	err := m.pg.QueryRow(ctx, query, webhookID).Scan(
		&webhook.ID, &webhook.Name, &webhook.URL, &webhook.Method,
		&headersJSON, &webhook.AuthType, &authConfigJSON,
		&webhook.TimeoutSecs, &webhook.RetryCount, &webhook.IncludeAlertLink, &webhook.Enabled,
		&createdBy, &createdAt, &updatedAt,
		&webhook.FractalID, &webhook.PrismID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get webhook action: %w", err)
	}

	// Parse JSON fields
	if err := json.Unmarshal([]byte(headersJSON), &webhook.Headers); err != nil {
		webhook.Headers = make(map[string]string)
	}
	if err := json.Unmarshal([]byte(authConfigJSON), &webhook.AuthConfig); err != nil {
		webhook.AuthConfig = make(map[string]string)
	}

	return &webhook, nil
}

// ListWebhookActions retrieves all webhook actions scoped to the given fractal or prism.
// Pass exactly one of fractalID or prismID.
func (m *Manager) ListWebhookActions(ctx context.Context, enabledOnly bool, fractalID, prismID string) ([]*WebhookAction, error) {
	baseQuery := `
		SELECT id, name, url, method, headers, auth_type, auth_config, timeout_seconds, retry_count, include_alert_link, enabled,
		       COALESCE(created_by, ''), created_at, updated_at,
		       COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, '')
		FROM webhook_actions
	`

	var conditions []string
	var args []interface{}
	if prismID != "" {
		conditions = append(conditions, fmt.Sprintf("prism_id = $%d", len(args)+1))
		args = append(args, prismID)
	} else if fractalID != "" {
		conditions = append(conditions, fmt.Sprintf("fractal_id = $%d", len(args)+1))
		args = append(args, fractalID)
	}
	if enabledOnly {
		conditions = append(conditions, "enabled = true")
	}

	var whereClause string
	if len(conditions) > 0 {
		whereClause = " WHERE " + strings.Join(conditions, " AND ")
	}

	query := baseQuery + whereClause + " ORDER BY name"

	rows, err := m.pg.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list webhook actions: %w", err)
	}
	defer rows.Close()

	var webhooks []*WebhookAction
	for rows.Next() {
		var webhook WebhookAction
		var headersJSON, authConfigJSON string
		var createdBy string
		var createdAt, updatedAt time.Time

		err := rows.Scan(
			&webhook.ID, &webhook.Name, &webhook.URL, &webhook.Method,
			&headersJSON, &webhook.AuthType, &authConfigJSON,
			&webhook.TimeoutSecs, &webhook.RetryCount, &webhook.IncludeAlertLink, &webhook.Enabled,
			&createdBy, &createdAt, &updatedAt,
			&webhook.FractalID, &webhook.PrismID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan webhook action: %w", err)
		}

		// Parse JSON fields
		if err := json.Unmarshal([]byte(headersJSON), &webhook.Headers); err != nil {
			webhook.Headers = make(map[string]string)
		}
		if err := json.Unmarshal([]byte(authConfigJSON), &webhook.AuthConfig); err != nil {
			webhook.AuthConfig = make(map[string]string)
		}

		webhooks = append(webhooks, &webhook)
	}

	return webhooks, nil
}

// DeleteWebhookAction removes a webhook action
func (m *Manager) DeleteWebhookAction(ctx context.Context, webhookID string) error {
	// Check if webhook is associated with any alerts
	var alertCount int
	err := m.pg.QueryRow(ctx, "SELECT COUNT(*) FROM alert_webhook_actions WHERE webhook_id = $1", webhookID).Scan(&alertCount)
	if err != nil {
		return fmt.Errorf("failed to check webhook associations: %w", err)
	}

	if alertCount > 0 {
		return fmt.Errorf("cannot delete webhook action: it is associated with %d alert(s)", alertCount)
	}

	result, err := m.pg.Exec(ctx, "DELETE FROM webhook_actions WHERE id = $1", webhookID)
	if err != nil {
		return fmt.Errorf("failed to delete webhook action: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("webhook action not found")
	}

	return nil
}

// TestWebhookAction sends a test payload to a webhook action
func (m *Manager) TestWebhookAction(ctx context.Context, webhookID string) (*WebhookResult, error) {
	webhook, err := m.GetWebhookAction(ctx, webhookID)
	if err != nil {
		return nil, fmt.Errorf("failed to get webhook action: %w", err)
	}

	// Create a test alert and results
	testAlert := &Alert{
		ID:          "test-alert",
		Name:        "Test Alert",
		Description: "This is a test alert to verify webhook configuration",
		QueryString: "test=true",
		Labels:      []string{"test"},
	}

	testResults := []map[string]interface{}{
		{
			"timestamp": time.Now().Format("2006-01-02 15:04:05"),
			"message":   "Test log message for webhook verification",
			"level":     "info",
		},
	}

	// Use the webhook client to send test payload
	webhookClient := NewWebhookClient("")
	result := webhookClient.Send(ctx, *webhook, testAlert, testAlert.Name, testResults)

	return &result, nil
}

// ============================
// Fractal Action Management
// ============================

// CreateFractalAction creates a new fractal action scoped to the given fractal or prism.
// Pass exactly one of fractalID or prismID.
func (m *Manager) CreateFractalAction(ctx context.Context, req FractalActionCreateRequest, createdBy, fractalID, prismID string) (*FractalAction, error) {
	if (fractalID == "") == (prismID == "") {
		return nil, fmt.Errorf("exactly one of fractal_id or prism_id must be set")
	}

	// Set defaults
	if req.MaxLogsPerTrigger <= 0 {
		req.MaxLogsPerTrigger = 1000
	}
	if req.FieldMappings == nil {
		req.FieldMappings = make(map[string]string)
	}

	// Validate required fields
	if strings.TrimSpace(req.Name) == "" {
		return nil, fmt.Errorf("fractal action name is required")
	}
	if strings.TrimSpace(req.TargetFractalID) == "" {
		return nil, fmt.Errorf("target fractal ID is required")
	}

	// Verify target fractal exists
	var fractalExists bool
	err := m.pg.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM fractals WHERE id = $1)", req.TargetFractalID).Scan(&fractalExists)
	if err != nil {
		return nil, fmt.Errorf("failed to verify target fractal: %w", err)
	}
	if !fractalExists {
		return nil, fmt.Errorf("target fractal not found")
	}

	fieldMappingsJSON, err := json.Marshal(req.FieldMappings)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal field mappings: %w", err)
	}

	query := `
		INSERT INTO fractal_actions (name, description, target_fractal_id, preserve_timestamp,
		                           add_alert_context, field_mappings, max_logs_per_trigger, enabled, created_by,
		                           fractal_id, prism_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING id, created_at, updated_at
	`

	var fractalActionID string
	var createdAt, updatedAt time.Time
	err = m.pg.QueryRow(ctx, query,
		req.Name, req.Description, req.TargetFractalID, req.PreserveTimestamp,
		req.AddAlertContext, string(fieldMappingsJSON), req.MaxLogsPerTrigger,
		req.Enabled, createdBy,
		nullableID(fractalID), nullableID(prismID),
	).Scan(&fractalActionID, &createdAt, &updatedAt)

	if err != nil {
		return nil, fmt.Errorf("failed to create fractal action: %w", err)
	}

	return m.GetFractalAction(ctx, fractalActionID)
}

// UpdateFractalAction updates an existing fractal action
func (m *Manager) UpdateFractalAction(ctx context.Context, fractalActionID string, req FractalActionUpdateRequest) (*FractalAction, error) {
	// Set defaults
	if req.MaxLogsPerTrigger <= 0 {
		req.MaxLogsPerTrigger = 1000
	}
	if req.FieldMappings == nil {
		req.FieldMappings = make(map[string]string)
	}

	// Validate required fields
	if strings.TrimSpace(req.Name) == "" {
		return nil, fmt.Errorf("fractal action name is required")
	}
	if strings.TrimSpace(req.TargetFractalID) == "" {
		return nil, fmt.Errorf("target fractal ID is required")
	}

	// Verify target fractal exists
	var fractalExists bool
	err := m.pg.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM fractals WHERE id = $1)", req.TargetFractalID).Scan(&fractalExists)
	if err != nil {
		return nil, fmt.Errorf("failed to verify target fractal: %w", err)
	}
	if !fractalExists {
		return nil, fmt.Errorf("target fractal not found")
	}

	fieldMappingsJSON, err := json.Marshal(req.FieldMappings)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal field mappings: %w", err)
	}

	query := `
		UPDATE fractal_actions
		SET name = $2, description = $3, target_fractal_id = $4, preserve_timestamp = $5,
		    add_alert_context = $6, field_mappings = $7, max_logs_per_trigger = $8, enabled = $9,
		    updated_at = NOW()
		WHERE id = $1
	`

	result, err := m.pg.Exec(ctx, query,
		fractalActionID, req.Name, req.Description, req.TargetFractalID,
		req.PreserveTimestamp, req.AddAlertContext, string(fieldMappingsJSON),
		req.MaxLogsPerTrigger, req.Enabled,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to update fractal action: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return nil, fmt.Errorf("fractal action not found")
	}

	return m.GetFractalAction(ctx, fractalActionID)
}

// GetFractalAction retrieves a fractal action by ID
func (m *Manager) GetFractalAction(ctx context.Context, fractalActionID string) (*FractalAction, error) {
	query := `
		SELECT fa.id, fa.name, fa.description, fa.target_fractal_id, fa.preserve_timestamp,
		       fa.add_alert_context, fa.field_mappings, fa.max_logs_per_trigger, fa.enabled,
		       COALESCE(fa.fractal_id::text, ''), COALESCE(fa.prism_id::text, '')
		FROM fractal_actions fa
		WHERE fa.id = $1
	`

	var action FractalAction
	var fieldMappingsJSON []byte

	err := m.pg.QueryRow(ctx, query, fractalActionID).Scan(
		&action.ID, &action.Name, &action.Description, &action.TargetFractalID,
		&action.PreserveTimestamp, &action.AddAlertContext, &fieldMappingsJSON,
		&action.MaxLogsPerTrigger, &action.Enabled,
		&action.FractalID, &action.PrismID,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get fractal action: %w", err)
	}

	// Parse field mappings
	if err := json.Unmarshal(fieldMappingsJSON, &action.FieldMappings); err != nil {
		return nil, fmt.Errorf("failed to parse field mappings: %w", err)
	}

	return &action, nil
}

// ListFractalActions retrieves fractal actions scoped to the given fractal or prism.
// Pass exactly one of fractalID or prismID.
func (m *Manager) ListFractalActions(ctx context.Context, enabledOnly bool, fractalID, prismID string) ([]FractalAction, error) {
	query := `
		SELECT fa.id, fa.name, fa.description, fa.target_fractal_id, fa.preserve_timestamp,
		       fa.add_alert_context, fa.field_mappings, fa.max_logs_per_trigger, fa.enabled,
		       COALESCE(fa.fractal_id::text, ''), COALESCE(fa.prism_id::text, ''),
		       f.name as target_fractal_name
		FROM fractal_actions fa
		JOIN fractals f ON fa.target_fractal_id = f.id
	`

	var conditions []string
	var args []interface{}
	if prismID != "" {
		conditions = append(conditions, fmt.Sprintf("fa.prism_id = $%d", len(args)+1))
		args = append(args, prismID)
	} else if fractalID != "" {
		conditions = append(conditions, fmt.Sprintf("fa.fractal_id = $%d", len(args)+1))
		args = append(args, fractalID)
	}
	if enabledOnly {
		conditions = append(conditions, "fa.enabled = true")
	}
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += " ORDER BY fa.name"

	rows, err := m.pg.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query fractal actions: %w", err)
	}
	defer rows.Close()

	var actions []FractalAction
	for rows.Next() {
		var action FractalAction
		var fieldMappingsJSON []byte
		var targetFractalName string

		err := rows.Scan(
			&action.ID, &action.Name, &action.Description, &action.TargetFractalID,
			&action.PreserveTimestamp, &action.AddAlertContext, &fieldMappingsJSON,
			&action.MaxLogsPerTrigger, &action.Enabled,
			&action.FractalID, &action.PrismID,
			&targetFractalName,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan fractal action: %w", err)
		}

		// Parse field mappings
		if err := json.Unmarshal(fieldMappingsJSON, &action.FieldMappings); err != nil {
			return nil, fmt.Errorf("failed to parse field mappings: %w", err)
		}

		// Add target fractal name for UI display
		action.Description = fmt.Sprintf("%s (→ %s)", action.Description, targetFractalName)

		actions = append(actions, action)
	}

	return actions, nil
}

// DeleteFractalAction removes a fractal action
func (m *Manager) DeleteFractalAction(ctx context.Context, fractalActionID string) error {
	// Check if fractal action is associated with any alerts
	var alertCount int
	err := m.pg.QueryRow(ctx, "SELECT COUNT(*) FROM alert_fractal_actions WHERE fractal_action_id = $1", fractalActionID).Scan(&alertCount)
	if err != nil {
		return fmt.Errorf("failed to check fractal action associations: %w", err)
	}

	if alertCount > 0 {
		return fmt.Errorf("cannot delete fractal action: it is associated with %d alert(s)", alertCount)
	}

	// Delete the fractal action
	result, err := m.pg.Exec(ctx, "DELETE FROM fractal_actions WHERE id = $1", fractalActionID)
	if err != nil {
		return fmt.Errorf("failed to delete fractal action: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("fractal action not found")
	}

	return nil
}

// ============================
// Feed Alert Management
// ============================

// CreateFeedAlert creates an alert that belongs to a feed.
func (m *Manager) CreateFeedAlert(ctx context.Context, name, description, queryString, alertType, severity string,
	labels, references []string, feedID, rulePath, ruleHash, fractalID, prismID, createdBy string) (*Alert, error) {

	parsedQuery, err := parser.ParseQuery(queryString)
	if err != nil {
		return nil, fmt.Errorf("invalid query syntax: %w", err)
	}

	if alertType == "" {
		alertType = "event"
	}

	if alertType == "event" && queryHasAggregation(parsedQuery) {
		return nil, fmt.Errorf("event alerts cannot use aggregate functions")
	}

	var fractalIDPtr, prismIDPtr interface{}
	if fractalID != "" {
		fractalIDPtr = fractalID
	}
	if prismID != "" {
		prismIDPtr = prismID
	}

	if severity == "" {
		severity = "medium"
	}

	query := `
		INSERT INTO alerts (name, description, query_string, alert_type, severity, enabled, labels, "references",
		                    created_by, fractal_id, prism_id, feed_id, feed_rule_path, feed_rule_hash)
		VALUES ($1, $2, $3, $4, $5, false, $6, $7, $8, $9, $10, $11, $12, $13)
		RETURNING id
	`
	var alertID string
	err = m.pg.QueryRow(ctx, query,
		name, description, queryString, alertType, severity, pq.Array(labels), pq.Array(references),
		createdBy, fractalIDPtr, prismIDPtr, feedID, rulePath, ruleHash,
	).Scan(&alertID)
	if err != nil {
		return nil, fmt.Errorf("create feed alert: %w", err)
	}

	// Skip per-alert cache refresh and GetAlert for feed alerts (caller refreshes once at end)
	return &Alert{ID: alertID, Name: name}, nil
}

// UpdateFeedAlert updates an alert that belongs to a feed. In addition to the
// rule content, it also re-asserts the alert's scope from the owning feed's
// current scope - so if an alert's scope somehow drifted from its feed, the
// next sync puts it back where it belongs. fractalID and prismID must come
// from the parent feed and exactly one must be set.
func (m *Manager) UpdateFeedAlert(ctx context.Context, alertID, name, description, queryString, alertType, severity string,
	labels, references []string, ruleHash, updatedBy, fractalID, prismID string) error {

	if (fractalID == "") == (prismID == "") {
		return fmt.Errorf("exactly one of fractal_id or prism_id must be set")
	}

	_, err := parser.ParseQuery(queryString)
	if err != nil {
		return fmt.Errorf("invalid query syntax: %w", err)
	}

	if alertType == "" {
		alertType = "event"
	}
	if severity == "" {
		severity = "medium"
	}

	var fractalIDPtr, prismIDPtr interface{}
	if fractalID != "" {
		fractalIDPtr = fractalID
	}
	if prismID != "" {
		prismIDPtr = prismID
	}

	_, err = m.pg.Exec(ctx, `
		UPDATE alerts
		SET name = $2, description = $3, query_string = $4, alert_type = $5,
		    severity = $6, labels = $7, "references" = $8, feed_rule_hash = $9, updated_by = $10,
		    fractal_id = $11, prism_id = $12
		WHERE id = $1 AND feed_id IS NOT NULL
	`, alertID, name, description, queryString, alertType, severity,
		pq.Array(labels), pq.Array(references), ruleHash, updatedBy,
		fractalIDPtr, prismIDPtr)
	if err != nil {
		return fmt.Errorf("update feed alert: %w", err)
	}

	if err := m.engine.RefreshAlerts(ctx); err != nil {
		fmt.Printf("Warning: failed to refresh alert cache: %v\n", err)
	}

	return nil
}

// DeleteFeedAlertsNotIn deletes all feed alerts for a given feed that are NOT in the provided path set.
func (m *Manager) DeleteFeedAlertsNotIn(ctx context.Context, feedID string, keepPaths []string) (int, error) {
	if len(keepPaths) == 0 {
		// Delete all alerts for this feed
		result, err := m.pg.Exec(ctx, "DELETE FROM alerts WHERE feed_id = $1", feedID)
		if err != nil {
			return 0, fmt.Errorf("delete feed alerts: %w", err)
		}
		rows, _ := result.RowsAffected()
		if rows > 0 {
			m.engine.RefreshAlerts(ctx)
		}
		return int(rows), nil
	}

	result, err := m.pg.Exec(ctx,
		"DELETE FROM alerts WHERE feed_id = $1 AND feed_rule_path != ALL($2)",
		feedID, pq.Array(keepPaths))
	if err != nil {
		return 0, fmt.Errorf("delete feed alerts: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows > 0 {
		m.engine.RefreshAlerts(ctx)
	}
	return int(rows), nil
}

// GetFeedAlertByPath retrieves a feed alert's key fields by feed ID and rule path.
// Returns a lightweight Alert with only ID, FeedRuleHash, and FeedRulePath populated.
func (m *Manager) GetFeedAlertByPath(ctx context.Context, feedID, rulePath string) (*Alert, error) {
	var a Alert
	err := m.pg.QueryRow(ctx,
		"SELECT id, COALESCE(feed_rule_hash, '') FROM alerts WHERE feed_id = $1 AND feed_rule_path = $2",
		feedID, rulePath).Scan(&a.ID, &a.FeedRuleHash)
	if err != nil {
		return nil, err
	}
	a.FeedRulePath = rulePath
	return &a, nil
}

// ListFeedAlerts returns all alerts belonging to a specific feed.
func (m *Manager) ListFeedAlerts(ctx context.Context, feedID string) ([]*Alert, error) {
	query := `
		SELECT id, name, description, query_string, COALESCE(alert_type, 'event'), enabled,
		       labels, "references", COALESCE(severity, 'medium'), COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, ''),
		       COALESCE(feed_id::text, ''), COALESCE(feed_rule_path, ''), COALESCE(feed_rule_hash, ''),
		       COALESCE(created_by, ''), created_at, updated_at, COALESCE(disabled_reason, '')
		FROM alerts
		WHERE feed_id = $1
		ORDER BY name
	`
	rows, err := m.pg.Query(ctx, query, feedID)
	if err != nil {
		return nil, fmt.Errorf("list feed alerts: %w", err)
	}
	defer rows.Close()

	var alerts []*Alert
	for rows.Next() {
		var a Alert
		err := rows.Scan(
			&a.ID, &a.Name, &a.Description, &a.QueryString, &a.AlertType, &a.Enabled,
			pq.Array(&a.Labels), pq.Array(&a.References), &a.Severity, &a.FractalID, &a.PrismID,
			&a.FeedID, &a.FeedRulePath, &a.FeedRuleHash,
			&a.CreatedBy, &a.CreatedAt, &a.UpdatedAt, &a.DisabledReason,
		)
		if err != nil {
			return nil, fmt.Errorf("scan feed alert: %w", err)
		}
		alerts = append(alerts, &a)
	}
	return alerts, nil
}

// ListAllFeedAlerts returns all feed alerts for a fractal or prism (across all feeds).
func (m *Manager) ListAllFeedAlerts(ctx context.Context, fractalID, prismID string) ([]*Alert, error) {
	var whereClause string
	var arg interface{}
	if prismID != "" {
		whereClause = "a.prism_id = $1"
		arg = prismID
	} else {
		whereClause = "a.fractal_id = $1"
		arg = fractalID
	}

	query := fmt.Sprintf(`
		SELECT a.id, a.name, a.description, a.query_string, COALESCE(a.alert_type, 'event'), a.enabled,
		       a.labels, a."references", COALESCE(a.severity, 'medium'), COALESCE(a.fractal_id::text, ''), COALESCE(a.prism_id::text, ''),
		       COALESCE(a.feed_id::text, ''), COALESCE(a.feed_rule_path, ''), COALESCE(a.feed_rule_hash, ''),
		       COALESCE(a.created_by, ''), a.created_at, a.updated_at, COALESCE(a.disabled_reason, ''),
		       a.last_triggered,
		       (SELECT ae.execution_time_ms FROM alert_executions ae WHERE ae.alert_id = a.id ORDER BY ae.triggered_at DESC LIMIT 1),
		       COALESCE(f.name, '') as feed_name
		FROM alerts a
		LEFT JOIN alert_feeds f ON a.feed_id = f.id
		WHERE %s AND a.feed_id IS NOT NULL
		ORDER BY f.name, a.name
	`, whereClause)
	rows, err := m.pg.Query(ctx, query, arg)
	if err != nil {
		return nil, fmt.Errorf("list all feed alerts: %w", err)
	}
	defer rows.Close()

	var alerts []*Alert
	for rows.Next() {
		var a Alert
		var feedName string
		err := rows.Scan(
			&a.ID, &a.Name, &a.Description, &a.QueryString, &a.AlertType, &a.Enabled,
			pq.Array(&a.Labels), pq.Array(&a.References), &a.Severity, &a.FractalID, &a.PrismID,
			&a.FeedID, &a.FeedRulePath, &a.FeedRuleHash,
			&a.CreatedBy, &a.CreatedAt, &a.UpdatedAt, &a.DisabledReason,
			&a.LastTriggered, &a.LastExecutionTimeMs,
			&feedName,
		)
		if err != nil {
			return nil, fmt.Errorf("scan feed alert: %w", err)
		}
		// Store feed name in labels for UI display
		if feedName != "" {
			a.Labels = append([]string{"feed:" + feedName}, a.Labels...)
		}
		alerts = append(alerts, &a)
	}
	return alerts, nil
}

// EnableFeedAlerts enables or disables all alerts for a given feed.
func (m *Manager) EnableFeedAlerts(ctx context.Context, feedID string, enabled bool, updatedBy string) error {
	_, err := m.pg.Exec(ctx,
		"UPDATE alerts SET enabled = $1, updated_by = $2, disabled_reason = '' WHERE feed_id = $3",
		enabled, updatedBy, feedID)
	if err != nil {
		return fmt.Errorf("toggle feed alerts: %w", err)
	}
	m.engine.RefreshAlerts(ctx)
	return nil
}

// ToggleFeedAlert enables or disables a single feed alert.
func (m *Manager) ToggleFeedAlert(ctx context.Context, alertID string, enabled bool, updatedBy string) error {
	result, err := m.pg.Exec(ctx,
		"UPDATE alerts SET enabled = $1, updated_by = $2, disabled_reason = '' WHERE id = $3 AND feed_id IS NOT NULL",
		enabled, updatedBy, alertID)
	if err != nil {
		return fmt.Errorf("toggle feed alert: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("feed alert not found")
	}
	m.engine.RefreshAlerts(ctx)
	return nil
}

// BatchToggleAlerts enables or disables a set of non-feed alerts by ID.
func (m *Manager) BatchToggleAlerts(ctx context.Context, alertIDs []string, enabled bool, updatedBy string) (int, error) {
	if len(alertIDs) == 0 {
		return 0, nil
	}
	result, err := m.pg.Exec(ctx,
		"UPDATE alerts SET enabled = $1, updated_by = $2, disabled_reason = '' WHERE id = ANY($3) AND feed_id IS NULL",
		enabled, updatedBy, pq.Array(alertIDs))
	if err != nil {
		return 0, fmt.Errorf("batch toggle alerts: %w", err)
	}
	rows, _ := result.RowsAffected()
	m.engine.RefreshAlerts(ctx)
	return int(rows), nil
}

// BatchToggleFeedAlerts enables or disables a set of feed alerts by ID.
func (m *Manager) BatchToggleFeedAlerts(ctx context.Context, alertIDs []string, enabled bool, updatedBy string) (int, error) {
	if len(alertIDs) == 0 {
		return 0, nil
	}
	result, err := m.pg.Exec(ctx,
		"UPDATE alerts SET enabled = $1, updated_by = $2, disabled_reason = '' WHERE id = ANY($3) AND feed_id IS NOT NULL",
		enabled, updatedBy, pq.Array(alertIDs))
	if err != nil {
		return 0, fmt.Errorf("batch toggle feed alerts: %w", err)
	}
	rows, _ := result.RowsAffected()
	m.engine.RefreshAlerts(ctx)
	return int(rows), nil
}

// RefreshCache triggers a full refresh of the alert engine cache.
func (m *Manager) RefreshCache(ctx context.Context) {
	m.engine.RefreshAlerts(ctx)
}

// DuplicateAlert copies a feed alert as a standalone (manual) alert.
func (m *Manager) DuplicateAlert(ctx context.Context, alertID, createdBy string) (*Alert, error) {
	source, err := m.GetAlert(ctx, alertID)
	if err != nil {
		return nil, fmt.Errorf("source alert not found: %w", err)
	}

	// Generate a unique name
	newName := source.Name + " (copy)"

	req := AlertCreateRequest{
		Name:                newName,
		Description:         source.Description,
		QueryString:         source.QueryString,
		AlertType:           source.AlertType,
		Labels:              source.Labels,
		References:          source.References,
		Enabled:             false,
		ThrottleTimeSeconds: source.ThrottleTimeSeconds,
		ThrottleField:       source.ThrottleField,
		WindowDuration:      source.WindowDuration,
		ScheduleCron:        source.ScheduleCron,
		QueryWindowSeconds:  source.QueryWindowSeconds,
	}

	fractalID := source.FractalID
	prismID := source.PrismID

	return m.CreateAlert(ctx, req, createdBy, fractalID, prismID)
}

// ============================
// Email Action CRUD
// ============================

// CreateEmailAction creates a new email action scoped to the given fractal or prism.
// Pass exactly one of fractalID or prismID.
func (m *Manager) CreateEmailAction(ctx context.Context, req EmailActionCreateRequest, createdBy, fractalID, prismID string) (*EmailAction, error) {
	if (fractalID == "") == (prismID == "") {
		return nil, fmt.Errorf("exactly one of fractal_id or prism_id must be set")
	}
	if strings.TrimSpace(req.Name) == "" {
		return nil, fmt.Errorf("email action name is required")
	}
	if len(req.Recipients) == 0 {
		return nil, fmt.Errorf("at least one recipient is required")
	}

	var id string
	err := m.pg.QueryRow(ctx,
		`INSERT INTO email_actions (name, recipients, subject_template, body_template, enabled, created_by, fractal_id, prism_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
		req.Name, pq.Array(req.Recipients), req.SubjectTemplate, req.BodyTemplate, req.Enabled, createdBy,
		nullableID(fractalID), nullableID(prismID),
	).Scan(&id)
	if err != nil {
		return nil, fmt.Errorf("failed to create email action: %w", err)
	}

	return m.GetEmailAction(ctx, id)
}

func (m *Manager) GetEmailAction(ctx context.Context, id string) (*EmailAction, error) {
	var action EmailAction
	err := m.pg.QueryRow(ctx,
		`SELECT id, name, recipients, COALESCE(subject_template, ''), COALESCE(body_template, ''), enabled,
		        COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, '')
		 FROM email_actions WHERE id = $1`, id,
	).Scan(&action.ID, &action.Name, pq.Array(&action.Recipients), &action.SubjectTemplate, &action.BodyTemplate, &action.Enabled,
		&action.FractalID, &action.PrismID)
	if err != nil {
		return nil, fmt.Errorf("failed to get email action: %w", err)
	}
	return &action, nil
}

// ListEmailActions retrieves email actions scoped to the given fractal or prism.
// Pass exactly one of fractalID or prismID.
func (m *Manager) ListEmailActions(ctx context.Context, fractalID, prismID string) ([]EmailAction, error) {
	baseQuery := `SELECT id, name, recipients, COALESCE(subject_template, ''), COALESCE(body_template, ''), enabled,
	              COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, '')
	              FROM email_actions`

	var args []interface{}
	var where string
	if prismID != "" {
		where = " WHERE prism_id = $1"
		args = append(args, prismID)
	} else if fractalID != "" {
		where = " WHERE fractal_id = $1"
		args = append(args, fractalID)
	}

	rows, err := m.pg.Query(ctx, baseQuery+where+" ORDER BY name", args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list email actions: %w", err)
	}
	defer rows.Close()

	var actions []EmailAction
	for rows.Next() {
		var a EmailAction
		if err := rows.Scan(&a.ID, &a.Name, pq.Array(&a.Recipients), &a.SubjectTemplate, &a.BodyTemplate, &a.Enabled,
			&a.FractalID, &a.PrismID); err != nil {
			return nil, fmt.Errorf("failed to scan email action: %w", err)
		}
		actions = append(actions, a)
	}
	return actions, nil
}

func (m *Manager) UpdateEmailAction(ctx context.Context, id string, req EmailActionUpdateRequest) (*EmailAction, error) {
	if strings.TrimSpace(req.Name) == "" {
		return nil, fmt.Errorf("email action name is required")
	}
	if len(req.Recipients) == 0 {
		return nil, fmt.Errorf("at least one recipient is required")
	}

	result, err := m.pg.Exec(ctx,
		`UPDATE email_actions SET name = $2, recipients = $3, subject_template = $4, body_template = $5, enabled = $6
		 WHERE id = $1`,
		id, req.Name, pq.Array(req.Recipients), req.SubjectTemplate, req.BodyTemplate, req.Enabled,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to update email action: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return nil, fmt.Errorf("email action not found")
	}

	if err := m.engine.RefreshAlerts(ctx); err != nil {
		fmt.Printf("Warning: failed to refresh alert cache: %v\n", err)
	}

	return m.GetEmailAction(ctx, id)
}

func (m *Manager) DeleteEmailAction(ctx context.Context, id string) error {
	result, err := m.pg.Exec(ctx, "DELETE FROM email_actions WHERE id = $1", id)
	if err != nil {
		return fmt.Errorf("failed to delete email action: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("email action not found")
	}

	if err := m.engine.RefreshAlerts(ctx); err != nil {
		fmt.Printf("Warning: failed to refresh alert cache: %v\n", err)
	}

	return nil
}