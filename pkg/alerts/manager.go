package alerts

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"bifract/pkg/attack"
	"bifract/pkg/normalizers"
	"bifract/pkg/parser"
	"bifract/pkg/sigma"
	"bifract/pkg/storage"
	"github.com/lib/pq"
	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v3"
)

// ErrAlertNotFound distinguishes a missing alert from a failed lookup, so
// handlers answer 404 rather than 500 for an id that simply does not exist.
var ErrAlertNotFound = errors.New("alert not found")

// Manager handles CRUD operations and YAML import/export for alerts and webhooks
type Manager struct {
	pg                *storage.PostgresClient
	engine            *Engine
	normalizerManager *normalizers.Manager
	// testRunner evaluates alert tests for policies that read their outcome.
	testRunner *TestRunner
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
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
	QueryString string `yaml:"queryString"`
	AlertType   string `yaml:"alertType"`
	Severity    string `yaml:"severity,omitempty"`
	// Every action this alert runs, of any kind, named the way the editor lists
	// them. Names resolve across all four action tables on import.
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
	Name                string    `json:"name"`
	Description         string    `json:"description"`
	QueryString         string    `json:"query_string"`
	AlertType           AlertType `json:"alert_type"`
	WebhookActionIDs    []string  `json:"webhook_action_ids"`
	FractalActionIDs    []string  `json:"fractal_action_ids"`
	DictionaryActionIDs []string  `json:"dictionary_action_ids"`
	EmailActionIDs      []string  `json:"email_action_ids"`
	Labels              []string  `json:"labels"`
	References          []string  `json:"references"`
	Severity            Severity  `json:"severity"`
	Enabled             bool      `json:"enabled"`
	ThrottleTimeSeconds int       `json:"throttle_time_seconds"`
	ThrottleField       string    `json:"throttle_field"`
	WindowDuration      *int      `json:"window_duration,omitempty"`
	ScheduleCron        *string   `json:"schedule_cron,omitempty"`
	QueryWindowSeconds  *int      `json:"query_window_seconds,omitempty"`

	// Tests replaces the alert's test corpus. Nil leaves it untouched, so a client
	// that knows nothing about tests cannot wipe them with an ordinary update.
	Tests *[]AlertTest `json:"tests,omitempty"`
}

// AlertUpdateRequest represents a request to update an existing alert
type AlertUpdateRequest struct {
	Name                string    `json:"name"`
	Description         string    `json:"description"`
	QueryString         string    `json:"query_string"`
	AlertType           AlertType `json:"alert_type"`
	WebhookActionIDs    []string  `json:"webhook_action_ids"`
	FractalActionIDs    []string  `json:"fractal_action_ids"`
	DictionaryActionIDs []string  `json:"dictionary_action_ids"`
	EmailActionIDs      []string  `json:"email_action_ids"`
	Labels              []string  `json:"labels"`
	References          []string  `json:"references"`
	Severity            Severity  `json:"severity"`
	Enabled             bool      `json:"enabled"`
	ThrottleTimeSeconds int       `json:"throttle_time_seconds"`
	ThrottleField       string    `json:"throttle_field"`
	WindowDuration      *int      `json:"window_duration,omitempty"`
	ScheduleCron        *string   `json:"schedule_cron,omitempty"`
	QueryWindowSeconds  *int      `json:"query_window_seconds,omitempty"`

	// Tests replaces the alert's test corpus. Nil leaves it untouched, so a client
	// that knows nothing about tests cannot wipe them with an ordinary update.
	Tests *[]AlertTest `json:"tests,omitempty"`
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
	Name              string            `json:"name"`
	Description       string            `json:"description"`
	TargetFractalID   string            `json:"target_fractal_id"`
	PreserveTimestamp bool              `json:"preserve_timestamp"`
	AddAlertContext   bool              `json:"add_alert_context"`
	FieldMappings     map[string]string `json:"field_mappings"`
	MaxLogsPerTrigger int               `json:"max_logs_per_trigger"`
	Enabled           bool              `json:"enabled"`
}

// FractalActionUpdateRequest represents a request to update an existing fractal action
type FractalActionUpdateRequest struct {
	Name              string            `json:"name"`
	Description       string            `json:"description"`
	TargetFractalID   string            `json:"target_fractal_id"`
	PreserveTimestamp bool              `json:"preserve_timestamp"`
	AddAlertContext   bool              `json:"add_alert_context"`
	FieldMappings     map[string]string `json:"field_mappings"`
	MaxLogsPerTrigger int               `json:"max_logs_per_trigger"`
	Enabled           bool              `json:"enabled"`
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

	// Resolve every action name before any write, so a bad name fails the import
	// instead of leaving the alert half-wired.
	actions, err := m.resolveActionNames(ctx, yamlAlert.ActionNames, fractalID, prismID)
	if err != nil {
		return nil, err
	}

	// Check if alert already exists (update vs create)
	existingAlert, err := m.GetAlertByName(ctx, yamlAlert.Name)
	if err == nil {
		// Alert exists - update it
		updateReq := AlertUpdateRequest{
			Name:                yamlAlert.Name,
			Description:         yamlAlert.Description,
			QueryString:         yamlAlert.QueryString,
			AlertType:           AlertType(yamlAlert.AlertType),
			Severity:            Severity(yamlAlert.Severity),
			WebhookActionIDs:    actions.Webhook,
			FractalActionIDs:    actions.Fractal,
			DictionaryActionIDs: actions.Dictionary,
			EmailActionIDs:      actions.Email,
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
		AlertType:           AlertType(yamlAlert.AlertType),
		Severity:            Severity(yamlAlert.Severity),
		WebhookActionIDs:    actions.Webhook,
		FractalActionIDs:    actions.Fractal,
		DictionaryActionIDs: actions.Dictionary,
		EmailActionIDs:      actions.Email,
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
	labels := sigma.BuildLabels(rule)
	description := sigmaDescription(rule)

	// Check if alert with same title already exists
	existingAlert, err := m.GetAlertByName(ctx, rule.Title)
	if err == nil {
		updateReq := AlertUpdateRequest{
			Name:        rule.Title,
			Description: description,
			QueryString: queryString,
			AlertType:   "event",
			Severity:    SeverityFromLevel(rule.Level),
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
		Severity:    SeverityFromLevel(rule.Level),
		Labels:      labels,
		References:  rule.References,
		Enabled:     false, // Disabled by default for review
	}

	return m.CreateAlert(ctx, createReq, createdBy, fractalID, prismID)
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
	if err := validateAlertType(string(req.AlertType)); err != nil {
		return nil, err
	}

	parsedQuery, err := parser.ParseQuery(req.QueryString)
	if err != nil {
		return nil, fmt.Errorf("invalid query syntax: %w", err)
	}
	if err := validateWindowContract(parsedQuery); err != nil {
		return nil, err
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

	alertType := req.AlertType
	if alertType == "" {
		alertType = "event"
	}
	severity := req.Severity
	if severity == "" {
		severity = "medium"
	}

	var proposedTests []AlertTest
	if req.Tests != nil {
		proposedTests = *req.Tests
	}
	if err := m.requireGate(ctx, fractalID, prismID, ChangeCreate, false); err != nil {
		return nil, err
	}
	if err := m.enforcePolicies(ctx, fractalID, prismID, NewPolicySubject(
		revisionContentFromRequest(AlertUpdateRequest(req), string(alertType), string(severity)), proposedTests,
	)); err != nil {
		return nil, err
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

	query := `
		INSERT INTO alerts (name, description, query_string, alert_type, enabled, throttle_time_seconds, throttle_field, labels, "references", severity, created_by, fractal_id, prism_id, window_duration, schedule_cron, query_window_seconds)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		RETURNING id
	`
	err = tx.QueryRow(ctx, query,
		req.Name, req.Description, req.QueryString, alertType, req.Enabled,
		req.ThrottleTimeSeconds, req.ThrottleField, pq.Array(req.Labels), pq.Array(req.References), severity, storage.NullableUser(createdBy), fractalIDPtr, prismIDPtr, req.WindowDuration,
		req.ScheduleCron, req.QueryWindowSeconds,
	).Scan(&alertID)
	if err != nil {
		return nil, fmt.Errorf("failed to insert alert: %w", err)
	}

	// Record the created definition as revision 1. The head revision always mirrors
	// the alert's current definition.
	created := revisionContentFromRequest(AlertUpdateRequest(req), string(alertType), string(severity))
	createdHash, err := created.Hash()
	if err != nil {
		return nil, fmt.Errorf("failed to hash alert definition: %w", err)
	}
	if err := insertRevision(ctx, tx, alertID, 1, created, createdHash, "created", createdBy, createdBy, revisionRetention()); err != nil {
		return nil, err
	}

	if req.Tests != nil {
		if err := m.replaceTests(ctx, tx, alertID, *req.Tests, createdBy); err != nil {
			return nil, err
		}
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
	if err := validateWindowContract(parsedQuery); err != nil {
		return nil, err
	}

	// Look up the alert's existing scope so action validation stays scoped.
	var existingFractalID, existingPrismID string
	var isFeedAlert bool
	if err := m.pg.QueryRow(ctx,
		`SELECT COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, ''), feed_id IS NOT NULL FROM alerts WHERE id = $1`,
		alertID,
	).Scan(&existingFractalID, &existingPrismID, &isFeedAlert); err != nil {
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

	updateType := req.AlertType
	if updateType == "" {
		updateType = "event"
	}
	updateSeverity := req.Severity
	if updateSeverity == "" {
		updateSeverity = "medium"
	}

	// Feed-managed alerts are exempt: their definition comes from upstream, so a rule
	// an analyst cannot satisfy would only make the feed unusable.
	if err := m.requireGate(ctx, existingFractalID, existingPrismID, ChangeUpdate, isFeedAlert); err != nil {
		return nil, err
	}

	if !isFeedAlert {
		proposedTests, err := m.testsForPolicy(ctx, alertID, req.Tests)
		if err != nil {
			return nil, err
		}
		if err := m.enforcePolicies(ctx, existingFractalID, existingPrismID, NewPolicySubject(
			revisionContentFromRequest(req, string(updateType), string(updateSeverity)), proposedTests,
		)); err != nil {
			return nil, err
		}
	}

	// Start transaction
	tx, err := m.pg.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Lock the alert for the rest of the transaction. Revision numbering needs a
	// serialization point, and without it two concurrent edits can also lose one.
	var lockedID string
	if err := tx.QueryRow(ctx, `SELECT id::text FROM alerts WHERE id = $1 FOR UPDATE`, alertID).Scan(&lockedID); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrAlertNotFound
		}
		return nil, fmt.Errorf("failed to lock alert: %w", err)
	}

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

	// Record the new definition before anything is written, so the pre-edit state is
	// still readable for the seed revision on alerts that predate history.
	if err := writeRevision(ctx, tx, alertID, revisionContentFromRequest(req, string(alertType), string(severity)), username, username, revisionRetention()); err != nil {
		return nil, err
	}

	if req.Tests != nil {
		if err := m.replaceTests(ctx, tx, alertID, *req.Tests, username); err != nil {
			return nil, err
		}
	}

	// Update alert (clear disabled_reason when re-enabling). Re-enabling also
	// resets last_evaluated_at to near-now, mirroring the zero-cursor fallback
	// in engine.go, so a rule that was disabled doesn't wake up with a stale
	// cursor and force a cold-table catch-up scan across the gap.
	query := `
		UPDATE alerts
		SET name = $2, description = $3, query_string = $4, enabled = $5,
		    throttle_time_seconds = $6, throttle_field = $7, labels = $8,
		    "references" = $9, severity = $10, updated_by = $11,
		    alert_type = $12, window_duration = $13,
		    schedule_cron = $14, query_window_seconds = $15,
		    disabled_reason = CASE WHEN $5 = true THEN NULL ELSE disabled_reason END,
		    last_evaluated_at = CASE WHEN $5 = true AND enabled = false THEN NOW() - INTERVAL '5 minutes' ELSE last_evaluated_at END,
		    updated_at = NOW()
		WHERE id = $1
	`
	result, err := tx.Exec(ctx, query,
		alertID, req.Name, req.Description, req.QueryString, req.Enabled,
		req.ThrottleTimeSeconds, req.ThrottleField, pq.Array(req.Labels),
		pq.Array(req.References), severity, storage.NullableUser(username), alertType, req.WindowDuration,
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

// Per-alert action aggregates. These are correlated subqueries rather than joins
// on purpose: joining all four child tables multiplies the rows, and each json_agg
// then repeats every entry once per row contributed by the other kinds.
const (
	alertWebhookActionsJSON = `(SELECT COALESCE(json_agg(
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
		               'include_alert_link', wa.include_alert_link,
		               'enabled', wa.enabled
		           ) ORDER BY wa.name), '[]'::json)
		   FROM alert_webhook_actions awa
		   JOIN webhook_actions wa ON awa.webhook_id = wa.id AND wa.enabled = true
		  WHERE awa.alert_id = a.id) as webhook_actions`

	alertFractalActionsJSON = `(SELECT COALESCE(json_agg(
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
		           ) ORDER BY fa.name), '[]'::json)
		   FROM alert_fractal_actions afa
		   JOIN fractal_actions fa ON afa.fractal_action_id = fa.id AND fa.enabled = true
		  WHERE afa.alert_id = a.id) as fractal_actions`

	alertDictionaryActionsJSON = `(SELECT COALESCE(json_agg(
		           json_build_object('id', da.id, 'name', da.name) ORDER BY da.name), '[]'::json)
		   FROM alert_dictionary_actions ada
		   JOIN dictionary_actions da ON ada.dictionary_action_id = da.id AND da.enabled = true
		  WHERE ada.alert_id = a.id) as dictionary_actions`

	alertEmailActionsJSON = `(SELECT COALESCE(json_agg(
		           json_build_object(
		               'id', ea.id,
		               'name', ea.name,
		               'recipients', ea.recipients,
		               'subject_template', ea.subject_template,
		               'body_template', ea.body_template,
		               'enabled', ea.enabled
		           ) ORDER BY ea.name), '[]'::json)
		   FROM alert_email_actions aea
		   JOIN email_actions ea ON aea.email_action_id = ea.id AND ea.enabled = true
		  WHERE aea.alert_id = a.id) as email_actions`

	alertEmailActionRefsJSON = `(SELECT COALESCE(json_agg(
		           json_build_object('id', ea.id, 'name', ea.name) ORDER BY ea.name), '[]'::json)
		   FROM alert_email_actions aea
		   JOIN email_actions ea ON aea.email_action_id = ea.id AND ea.enabled = true
		  WHERE aea.alert_id = a.id) as email_actions`
)

// GetAlert retrieves an alert by ID
func (m *Manager) GetAlert(ctx context.Context, alertID string) (*Alert, error) {
	query := `
		SELECT a.id, a.name, COALESCE(a.description, ''), a.query_string, COALESCE(a.alert_type, 'event'), a.enabled,
		       COALESCE(a.throttle_time_seconds, 0), COALESCE(a.throttle_field, ''), a.labels, a."references",
		       COALESCE(a.severity, 'medium'), COALESCE(a.fractal_id::text, ''), COALESCE(a.prism_id::text, ''),
		       COALESCE(a.feed_id::text, ''), COALESCE(a.feed_rule_path, ''),
		       COALESCE(a.created_by, ''), COALESCE(a.updated_by, ''), a.created_at, a.updated_at, a.last_triggered,
		       COALESCE(a.disabled_reason, ''), COALESCE(a.window_duration, 0),
		       COALESCE(a.schedule_cron, ''), COALESCE(a.query_window_seconds, 0),
		       ` + alertWebhookActionsJSON + `,
		       ` + alertFractalActionsJSON + `,
		       ` + alertDictionaryActionsJSON + `,
		       ` + alertEmailActionsJSON + `
		FROM alerts a
		WHERE a.id = $1
	`

	var alert Alert
	var webhookActionsJSON []byte
	var fractalActionsJSON []byte
	var dictionaryActionsJSON []byte
	var emailActionsJSON []byte

	err := m.pg.QueryRow(ctx, query, alertID).Scan(
		&alert.ID, &alert.Name, &alert.Description, &alert.QueryString, &alert.AlertType,
		&alert.Enabled, &alert.ThrottleTimeSeconds, &alert.ThrottleField,
		pq.Array(&alert.Labels), pq.Array(&alert.References), &alert.Severity, &alert.FractalID, &alert.PrismID,
		&alert.FeedID, &alert.FeedRulePath, &alert.CreatedBy, &alert.UpdatedBy,
		&alert.CreatedAt, &alert.UpdatedAt,
		&alert.LastTriggered, &alert.DisabledReason, &alert.WindowDuration,
		&alert.ScheduleCron, &alert.QueryWindowSeconds,
		&webhookActionsJSON, &fractalActionsJSON, &dictionaryActionsJSON, &emailActionsJSON,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrAlertNotFound
		}
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

	// Parse dictionary action refs from JSON
	if err := json.Unmarshal(dictionaryActionsJSON, &alert.DictionaryActionRefs); err != nil {
		return nil, fmt.Errorf("failed to parse dictionary actions: %w", err)
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
		       a.last_execution_time_ms,
		       a.window_duration,
		       COALESCE(a.schedule_cron, ''), COALESCE(a.query_window_seconds, 0),
		       ` + alertWebhookActionsJSON + `,
		       ` + alertFractalActionsJSON + `,
		       ` + alertDictionaryActionsJSON + `,
		       ` + alertEmailActionRefsJSON + `
		FROM alerts a
	`

	// No scope means no alerts, not every alert: without this the query below
	// carries no scope predicate and spans every fractal and prism.
	if prismID == "" && fractalID == "" {
		return nil, fmt.Errorf("no fractal or prism scope")
	}

	var whereConditions []string
	var args []interface{}

	// Filter by scope (prism or fractal)
	if prismID != "" {
		whereConditions = append(whereConditions, "a.prism_id = $1")
		args = append(args, prismID)
	} else {
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

// ListCoverageRows returns the narrow projection of every alert in scope that the
// ATT&CK coverage map needs, feed alerts included.
//
// ListAlerts is deliberately not reused: it json_aggs four action tables per row
// and excludes feed alerts, and feed alerts are the bulk of ATT&CK-tagged rules.
func (m *Manager) ListCoverageRows(ctx context.Context, fractalID, prismID string) ([]attack.RuleRow, error) {
	query := `
		SELECT a.id, a.name, COALESCE(a.severity, 'medium'), a.enabled, a.labels,
		       COALESCE(a.feed_id::text, ''), COALESCE(f.name, '')
		FROM alerts a
		LEFT JOIN alert_feeds f ON f.id = a.feed_id
	`
	var args []interface{}
	if prismID != "" {
		query += " WHERE a.prism_id = $1"
		args = append(args, prismID)
	} else if fractalID != "" {
		query += " WHERE a.fractal_id = $1"
		args = append(args, fractalID)
	}
	query += " ORDER BY a.name"

	rows, err := m.pg.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list coverage rows: %w", err)
	}
	defer rows.Close()

	var out []attack.RuleRow
	for rows.Next() {
		var r attack.RuleRow
		if err := rows.Scan(&r.ID, &r.Name, &r.Severity, &r.Enabled, pq.Array(&r.Labels), &r.FeedID, &r.FeedName); err != nil {
			return nil, fmt.Errorf("failed to scan coverage row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// DeleteAlert removes an alert and its associations
func (m *Manager) DeleteAlert(ctx context.Context, alertID string) error {
	// Deleting a detection is the change nobody reviews and everybody regrets, so it
	// is gated like any other. Feed-managed alerts stay exempt.
	var fractalID, prismID string
	var isFeedAlert bool
	if err := m.pg.QueryRow(ctx,
		`SELECT COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, ''), feed_id IS NOT NULL FROM alerts WHERE id = $1`,
		alertID,
	).Scan(&fractalID, &prismID, &isFeedAlert); err != nil {
		if err == sql.ErrNoRows {
			return ErrAlertNotFound
		}
		return fmt.Errorf("failed to load alert scope: %w", err)
	}
	if err := m.requireGate(ctx, fractalID, prismID, ChangeDelete, isFeedAlert); err != nil {
		return err
	}

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

// actionKind names one alert action type and its scoped lookup table. All four
// share the id/name/enabled/fractal_id/prism_id shape, so one lookup covers them.
type actionKind struct {
	label string
	table string
}

var alertActionKinds = []actionKind{
	{"webhook", "webhook_actions"},
	{"fractal", "fractal_actions"},
	{"dictionary", "dictionary_actions"},
	{"email", "email_actions"},
}

// resolvedActions holds action IDs grouped by kind, ready for a create or update request.
type resolvedActions struct {
	Webhook    []string
	Fractal    []string
	Dictionary []string
	Email      []string
}

func (r *resolvedActions) add(kind string, id string) {
	switch kind {
	case "webhook":
		r.Webhook = append(r.Webhook, id)
	case "fractal":
		r.Fractal = append(r.Fractal, id)
	case "dictionary":
		r.Dictionary = append(r.Dictionary, id)
	case "email":
		r.Email = append(r.Email, id)
	}
}

// resolveActionNames maps action names to IDs across every action kind, matching
// how the editor presents them as one list. A name that matches nothing, or that
// matches more than one kind, is an error: silently guessing would wire an alert
// to an action the file did not ask for.
func (m *Manager) resolveActionNames(ctx context.Context, actionNames []string, fractalID, prismID string) (resolvedActions, error) {
	resolved := resolvedActions{Webhook: []string{}, Fractal: []string{}, Dictionary: []string{}, Email: []string{}}
	if len(actionNames) == 0 {
		return resolved, nil
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
	// Table and label are package constants, never caller input.
	selects := make([]string, 0, len(alertActionKinds))
	for _, kind := range alertActionKinds {
		selects = append(selects, fmt.Sprintf(
			`SELECT id::text, name, '%s' AS kind FROM %s WHERE name = ANY($1) AND enabled = true AND %s`,
			kind.label, kind.table, scope))
	}

	rows, err := m.pg.Query(ctx, strings.Join(selects, " UNION ALL "), args...)
	if err != nil {
		return resolved, fmt.Errorf("failed to query actions: %w", err)
	}
	defer rows.Close()

	type match struct{ id, kind string }
	matches := make(map[string][]match, len(actionNames))
	for rows.Next() {
		var id, name, kind string
		if err := rows.Scan(&id, &name, &kind); err != nil {
			return resolved, fmt.Errorf("failed to scan action: %w", err)
		}
		matches[name] = append(matches[name], match{id, kind})
	}
	if err := rows.Err(); err != nil {
		return resolved, fmt.Errorf("failed to read actions: %w", err)
	}

	var missing []string
	for _, name := range actionNames {
		found := matches[name]
		switch {
		case len(found) == 0:
			missing = append(missing, name)
		case len(found) > 1:
			kinds := make([]string, 0, len(found))
			for _, f := range found {
				kinds = append(kinds, f.kind)
			}
			sort.Strings(kinds)
			return resolved, fmt.Errorf("action name %q is ambiguous: it matches %s actions, rename one of them", name, strings.Join(kinds, " and "))
		default:
			resolved.add(found[0].kind, found[0].id)
		}
	}

	if len(missing) > 0 {
		return resolved, fmt.Errorf("actions not found: %s", strings.Join(missing, ", "))
	}

	return resolved, nil
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
		string(authConfigJSON), req.TimeoutSeconds, req.RetryCount, includeAlertLink, req.Enabled, storage.NullableUser(createdBy),
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
		req.Enabled, storage.NullableUser(createdBy),
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
func (m *Manager) CreateFeedAlert(ctx context.Context, name, description, queryString, alertType string, severity Severity,
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
	tx, err := m.pg.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var alertID string
	err = tx.QueryRow(ctx, query,
		name, description, queryString, alertType, severity, pq.Array(labels), pq.Array(references),
		storage.NullableUser(createdBy), fractalIDPtr, prismIDPtr, feedID, rulePath, ruleHash,
	).Scan(&alertID)
	if err != nil {
		return nil, fmt.Errorf("create feed alert: %w", err)
	}

	content := feedRevisionFields{
		name: name, description: description, queryString: queryString,
		alertType: alertType, severity: string(severity), labels: labels, references: references,
	}.applyTo(RevisionContent{})
	hash, err := content.Hash()
	if err != nil {
		return nil, fmt.Errorf("failed to hash alert definition: %w", err)
	}
	if err := insertRevision(ctx, tx, alertID, 1, content, hash, "created", createdBy, feedAuthorLabel, revisionRetention()); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Skip per-alert cache refresh and GetAlert for feed alerts (caller refreshes once at end)
	return &Alert{ID: alertID, Name: name}, nil
}

// UpdateFeedAlert updates an alert that belongs to a feed. In addition to the
// rule content, it also re-asserts the alert's scope from the owning feed's
// current scope - so if an alert's scope somehow drifted from its feed, the
// next sync puts it back where it belongs. fractalID and prismID must come
// from the parent feed and exactly one must be set.
func (m *Manager) UpdateFeedAlert(ctx context.Context, alertID, name, description, queryString, alertType string, severity Severity,
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

	tx, err := m.pg.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Feed sync calls this for every rule on every cycle, so the revision is only
	// recorded when the definition actually changed. Fields the feed does not own
	// (throttle, window, action wiring) carry over from the live alert.
	current, err := loadRevisionContentTx(ctx, tx, alertID)
	if err != nil {
		return err
	}
	content := feedRevisionFields{
		name: name, description: description, queryString: queryString,
		alertType: alertType, severity: string(severity), labels: labels, references: references,
	}.applyTo(current)
	if err := writeRevision(ctx, tx, alertID, content, updatedBy, feedAuthorLabel, revisionRetention()); err != nil {
		return err
	}

	result, err := tx.Exec(ctx, `
		UPDATE alerts
		SET name = $2, description = $3, query_string = $4, alert_type = $5,
		    severity = $6, labels = $7, "references" = $8, feed_rule_hash = $9, updated_by = $10,
		    fractal_id = $11, prism_id = $12
		WHERE id = $1 AND feed_id IS NOT NULL
	`, alertID, name, description, queryString, alertType, severity,
		pq.Array(labels), pq.Array(references), ruleHash, storage.NullableUser(updatedBy),
		fractalIDPtr, prismIDPtr)
	if err != nil {
		return fmt.Errorf("update feed alert: %w", err)
	}
	// A non-feed alert matches nothing here; roll back rather than leave a revision
	// describing an update that did not happen.
	if n, err := result.RowsAffected(); err == nil && n == 0 {
		return fmt.Errorf("update feed alert: alert not found or not feed-managed")
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
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

// EnableFeedAlerts enables or disables all alerts for a given feed.
// Re-enabling resets last_evaluated_at to near-now (mirroring the zero-cursor
// fallback in engine.go) so a rule that was disabled doesn't wake up with a
// stale cursor and force a cold-table catch-up scan across the gap.
func (m *Manager) EnableFeedAlerts(ctx context.Context, feedID string, enabled bool, updatedBy string) error {
	_, err := m.pg.Exec(ctx,
		`UPDATE alerts SET enabled = $1, updated_by = $2, disabled_reason = '',
		    last_evaluated_at = CASE WHEN $1 = true AND enabled = false THEN NOW() - INTERVAL '5 minutes' ELSE last_evaluated_at END
		 WHERE feed_id = $3`,
		enabled, storage.NullableUser(updatedBy), feedID)
	if err != nil {
		return fmt.Errorf("toggle feed alerts: %w", err)
	}
	m.engine.RefreshAlerts(ctx)
	return nil
}

// ToggleFeedAlert enables or disables a single feed alert.
// Re-enabling resets last_evaluated_at to near-now; see EnableFeedAlerts.
func (m *Manager) ToggleFeedAlert(ctx context.Context, alertID string, enabled bool, updatedBy string) error {
	result, err := m.pg.Exec(ctx,
		`UPDATE alerts SET enabled = $1, updated_by = $2, disabled_reason = '',
		    last_evaluated_at = CASE WHEN $1 = true AND enabled = false THEN NOW() - INTERVAL '5 minutes' ELSE last_evaluated_at END
		 WHERE id = $3 AND feed_id IS NOT NULL`,
		enabled, storage.NullableUser(updatedBy), alertID)
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
// Re-enabling resets last_evaluated_at to near-now; see EnableFeedAlerts.
func (m *Manager) BatchToggleAlerts(ctx context.Context, alertIDs []string, enabled bool, updatedBy string) (int, error) {
	if len(alertIDs) == 0 {
		return 0, nil
	}
	result, err := m.pg.Exec(ctx,
		`UPDATE alerts SET enabled = $1, updated_by = $2, disabled_reason = '',
		    last_evaluated_at = CASE WHEN $1 = true AND enabled = false THEN NOW() - INTERVAL '5 minutes' ELSE last_evaluated_at END
		 WHERE id = ANY($3) AND feed_id IS NULL`,
		enabled, storage.NullableUser(updatedBy), pq.Array(alertIDs))
	if err != nil {
		return 0, fmt.Errorf("batch toggle alerts: %w", err)
	}
	rows, _ := result.RowsAffected()
	m.engine.RefreshAlerts(ctx)
	return int(rows), nil
}

// BatchToggleFeedAlerts enables or disables a set of feed alerts by ID.
// Re-enabling resets last_evaluated_at to near-now; see EnableFeedAlerts.
func (m *Manager) BatchToggleFeedAlerts(ctx context.Context, alertIDs []string, enabled bool, updatedBy string) (int, error) {
	if len(alertIDs) == 0 {
		return 0, nil
	}
	result, err := m.pg.Exec(ctx,
		`UPDATE alerts SET enabled = $1, updated_by = $2, disabled_reason = '',
		    last_evaluated_at = CASE WHEN $1 = true AND enabled = false THEN NOW() - INTERVAL '5 minutes' ELSE last_evaluated_at END
		 WHERE id = ANY($3) AND feed_id IS NOT NULL`,
		enabled, storage.NullableUser(updatedBy), pq.Array(alertIDs))
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
		AlertType:           AlertType(source.AlertType),
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
		req.Name, pq.Array(req.Recipients), req.SubjectTemplate, req.BodyTemplate, req.Enabled, storage.NullableUser(createdBy),
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

// validateWindowContract rejects a query that correlates across events without
// bounding how far back that correlation reaches. Alerts evaluate over a moving
// window, so an unbounded correlation can never be evaluated correctly: no finite
// lookback guarantees the evidence is whole.
func validateWindowContract(pipeline *parser.PipelineNode) error {
	lookback, completion := parser.QueryWindowContract(pipeline)
	if completion != "" && lookback <= 0 {
		return fmt.Errorf("this query correlates events across time and needs a bound to run as an alert; add within= (e.g. within=5m)")
	}
	return nil
}
