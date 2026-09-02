package settings

import (
	"bifract/pkg/api"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"bifract/pkg/mfa"
	"bifract/pkg/storage"
)

// TimestampField represents a timestamp field configuration
type TimestampField struct {
	Field  string `json:"field"`  // Field name to extract (e.g., "system_time")
	Format string `json:"format"` // Time format (e.g., "2006-01-02T15:04:05.0000000Z07:00")
}

// minAlertEvalIntervalSeconds floors the admin-configurable alert evaluation
// interval so a mistyped setting can't put the engine into a tight loop that
// hammers ClickHouse with every enabled alert's query every few seconds.
const minAlertEvalIntervalSeconds = 60

// Alert definition history. The cap is per alert and covers the current definition
// plus its predecessors, so 10 means the head plus nine earlier versions.
const (
	defaultAlertRevisionRetention = 10
	minAlertRevisionRetention     = 2
	maxAlertRevisionRetention     = 100
)

// Recall (archive search) knobs. Defaults live here and in pkg/archive
// (settings.go), which reads the same keys directly from the settings table so
// the workers stay decoupled from this package; keep the two in sync.
const (
	defaultRecallTimeoutSeconds = 900 // 15m -- archive scans over object storage are legitimately slow
	minRecallTimeoutSeconds     = 30  // floor so a mistype cannot make every recall fail instantly
	defaultRecallConcurrency    = 5
	maxRecallConcurrency        = 16 // ceiling: the search worker pool is sized to this
)

// Schema sweep cadence. This is the knob that trades ClickHouse load against how
// quickly a newly ingested field appears on the schema tab: the sweep is the only
// thing that measures field distribution, storage, and column capacity.
const (
	defaultAPIKeyRateLimit            = 100 // requests per second per key; 0 = uncapped
	defaultSchemaSweepIntervalMinutes = 15
	minSchemaSweepIntervalMinutes     = 5 // floor: each pass samples every fractal
)

// pgr() severity calibration. The single knob is a share of activity, not a score cutoff: an
// admin can answer "how much can my team triage" but not "what should lambda be", and a raw
// cutoff silently changes meaning every time the scoring model changes. The 0-1 cutoffs are
// derived from the deployment's own observed score distribution (see pkg/pgrcal), so the flagged
// share stays where the admin put it across model changes, fleet growth, and baseline maturity.
const (
	defaultPgrSensitivityPercent = 2.0  // flag the top 2% of edges as high
	minPgrSensitivityPercent     = 0.1  // floor: below this a graph flags essentially nothing
	maxPgrSensitivityPercent     = 50.0 // ceiling: past this "high" stops meaning anything
)

// Settings holds all Bifract configuration
type Settings struct {
	TimestampFields          []TimestampField `json:"timestamp_fields"`
	AlertTimeoutSeconds      int              `json:"alert_timeout_seconds"`
	QueryTimeoutSeconds      int              `json:"query_timeout_seconds"`
	AlertEvalIntervalSeconds int              `json:"alert_eval_interval_seconds"`
	// AlertRevisionRetention is how many definition revisions each alert keeps.
	AlertRevisionRetention int `json:"alert_revision_retention"`
	// Recall (archive search) knobs, live-editable from the admin settings page.
	RecallTimeoutSeconds int   `json:"recall_timeout_seconds"` // per-search wall clock; >= minRecallTimeoutSeconds
	RecallMaxBytesRead   int64 `json:"recall_max_bytes_read"`  // ClickHouse max_bytes_to_read guard; 0 = unlimited
	RecallConcurrency    int   `json:"recall_concurrency"`     // max concurrent recall searches; 1..maxRecallConcurrency
	// QueryCPUPercent is the share of each ClickHouse node's cores interactive user
	// searches may use, so one expensive search cannot starve ingestion. 0 = uncapped.
	QueryCPUPercent int `json:"query_cpu_percent"`
	// QueryMemoryPercent is the share of each ClickHouse node's memory budget any one
	// interactive search may use before it is failed. 0 = uncapped.
	QueryMemoryPercent int `json:"query_memory_percent"`
	// Recall (archive scan) shares, scheduled behind interactive search.
	RecallCPUPercent    int `json:"recall_cpu_percent"`
	RecallMemoryPercent int `json:"recall_memory_percent"`
	// SchemaSweepIntervalMinutes is how often the background schema measurement
	// runs. Nothing reads it on the request path; the schema tab renders whatever
	// the last sweep wrote.
	SchemaSweepIntervalMinutes int `json:"schema_sweep_interval_minutes"`
	// APIKeyRateLimit caps how fast one API key may call the API, so a runaway
	// integration cannot crowd out everyone else. Per key, not per IP. 0 = uncapped.
	APIKeyRateLimit int `json:"api_key_rate_limit"`
	// PgrSensitivityPercent is the share of pgr() edges that should render as high severity.
	// Medium derives from it; the 0-1 cutoffs are derived from observed scores, not set here.
	PgrSensitivityPercent float64 `json:"pgr_sensitivity_percent"`
	// RequireMFA makes every local account enroll a TOTP authenticator before it
	// can use the app. SSO accounts are exempt because the identity provider owns
	// their second factor, and API keys are unaffected: they are not interactive
	// logins and carry their own scoping and revocation.
	RequireMFA bool `json:"require_mfa"`
	mu         sync.RWMutex
	pg         *storage.PostgresClient
}

// queryLimitsApplier applies changed search resource shares to ClickHouse (CREATE OR
// REPLACE WORKLOAD). Registered by the server at startup; nil in tools that only read
// settings, where the DDL has no meaning.
var queryLimitsApplier func(limits storage.WorkloadLimits) error

// RegisterQueryLimitsApplier wires the ClickHouse-side reconcile invoked when an admin
// changes a query resource share, so it takes effect without a restart.
func RegisterQueryLimitsApplier(fn func(limits storage.WorkloadLimits) error) {
	queryLimitsApplier = fn
}

// capabilityReporter reports what the ClickHouse server actually permits.
// Registered by the server at startup; nil in tools that only read settings.
var capabilityReporter func() storage.Capabilities

// RegisterCapabilityReporter wires the live capability set into the settings
// response, so the UI can say a control is unavailable and why.
//
// Capabilities are deliberately NOT a field on Settings: Settings is persisted to
// Postgres and capabilities are runtime state that must never be written back.
func RegisterCapabilityReporter(fn func() storage.Capabilities) {
	capabilityReporter = fn
}

// currentCapabilities returns the live capability set, or nil when unreported.
func currentCapabilities() storage.Capabilities {
	if capabilityReporter == nil {
		return nil
	}
	return capabilityReporter()
}

// ValidationError is a settings value an admin can correct, whose message is safe
// and useful to show them. Anything else from Update is an internal failure and
// stays generic.
type ValidationError struct{ msg string }

func (e ValidationError) Error() string { return e.msg }

func validationErrorf(format string, args ...interface{}) error {
	return ValidationError{msg: fmt.Sprintf(format, args...)}
}

// Global settings instance
var globalSettings *Settings

// Initialize settings from database
func Init(pg *storage.PostgresClient) error {
	// Default timestamp fields - system_time is checked first
	defaultTimestampFields := []TimestampField{
		{Field: "system_time", Format: "2006-01-02T15:04:05.999999999Z07:00"},
		{Field: "timestamp", Format: time.RFC3339Nano},
		{Field: "@timestamp", Format: time.RFC3339Nano},
		{Field: "time", Format: time.RFC3339Nano},
	}

	globalSettings = &Settings{
		TimestampFields:            defaultTimestampFields,
		AlertTimeoutSeconds:        5,  // 5s default for alert queries
		QueryTimeoutSeconds:        60, // 60s default for search queries
		AlertEvalIntervalSeconds:   60, // 60s default between alert engine ticks
		AlertRevisionRetention:     defaultAlertRevisionRetention,
		RecallTimeoutSeconds:       defaultRecallTimeoutSeconds,
		RecallMaxBytesRead:         0, // unlimited by default (admin opts into a cap)
		RecallConcurrency:          defaultRecallConcurrency,
		QueryCPUPercent:            storage.DefaultQueryCPUPercent,
		QueryMemoryPercent:         storage.DefaultQueryMemoryPercent,
		RecallCPUPercent:           storage.DefaultRecallCPUPercent,
		RecallMemoryPercent:        storage.DefaultRecallMemoryPercent,
		SchemaSweepIntervalMinutes: defaultSchemaSweepIntervalMinutes,
		APIKeyRateLimit:            defaultAPIKeyRateLimit,
		PgrSensitivityPercent:      defaultPgrSensitivityPercent,
		pg:                         pg,
	}

	// Load from database
	ctx := context.Background()

	// Load timestamp_fields setting
	timestampFieldsJSON, err := pg.GetSetting(ctx, "timestamp_fields")
	if err == nil && timestampFieldsJSON != "" {
		var fields []TimestampField
		if err := json.Unmarshal([]byte(timestampFieldsJSON), &fields); err == nil {
			globalSettings.TimestampFields = fields
		}
	}

	// Load alert_timeout_seconds
	alertTimeout, err := pg.GetSetting(ctx, "alert_timeout_seconds")
	if err == nil && alertTimeout != "" {
		if v, err := strconv.Atoi(alertTimeout); err == nil && v > 0 {
			globalSettings.AlertTimeoutSeconds = v
		}
	}

	// Load query_timeout_seconds (0 = unlimited)
	queryTimeout, err := pg.GetSetting(ctx, "query_timeout_seconds")
	if err == nil && queryTimeout != "" {
		if v, err := strconv.Atoi(queryTimeout); err == nil && v >= 0 {
			globalSettings.QueryTimeoutSeconds = v
		}
	}

	// Load alert_eval_interval_seconds
	alertEvalInterval, err := pg.GetSetting(ctx, "alert_eval_interval_seconds")
	if err == nil && alertEvalInterval != "" {
		if v, err := strconv.Atoi(alertEvalInterval); err == nil && v >= minAlertEvalIntervalSeconds {
			globalSettings.AlertEvalIntervalSeconds = v
		}
	}

	// Load recall_timeout_seconds
	if v, err := pg.GetSetting(ctx, "recall_timeout_seconds"); err == nil && v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= minRecallTimeoutSeconds {
			globalSettings.RecallTimeoutSeconds = n
		}
	}

	// Load recall_max_bytes_read (0 = unlimited)
	if v, err := pg.GetSetting(ctx, "recall_max_bytes_read"); err == nil && v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			globalSettings.RecallMaxBytesRead = n
		}
	}

	// Load recall_concurrency
	if v, err := pg.GetSetting(ctx, "recall_concurrency"); err == nil && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			globalSettings.RecallConcurrency = clampRecallConcurrency(n)
		}
	}

	// Load alert_revision_retention
	if v, err := pg.GetSetting(ctx, "alert_revision_retention"); err == nil && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			globalSettings.AlertRevisionRetention = clampAlertRevisionRetention(n)
		}
	}

	// Load schema_sweep_interval_minutes
	if v, err := pg.GetSetting(ctx, "schema_sweep_interval_minutes"); err == nil && v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= minSchemaSweepIntervalMinutes {
			globalSettings.SchemaSweepIntervalMinutes = n
		}
	}

	// Load require_mfa
	if v, err := pg.GetSetting(ctx, "require_mfa"); err == nil && v != "" {
		globalSettings.RequireMFA = v == "true"
	}

	// Load api_key_rate_limit (0 = uncapped)
	if v, err := pg.GetSetting(ctx, "api_key_rate_limit"); err == nil && v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			globalSettings.APIKeyRateLimit = n
		}
	}

	// Load pgr_sensitivity_percent
	if v, err := pg.GetSetting(ctx, "pgr_sensitivity_percent"); err == nil && v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			globalSettings.PgrSensitivityPercent = ClampPgrSensitivity(f)
		}
	}

	// Load query_cpu_percent (0 = uncapped)
	if v, err := pg.GetSetting(ctx, "query_cpu_percent"); err == nil && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			globalSettings.QueryCPUPercent = storage.ClampQueryLimitPercent(n)
		}
	}

	// Load query_memory_percent (0 = uncapped)
	if v, err := pg.GetSetting(ctx, "query_memory_percent"); err == nil && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			globalSettings.QueryMemoryPercent = storage.ClampQueryLimitPercent(n)
		}
	}

	// Load recall workload shares (0 = uncapped)
	if v, err := pg.GetSetting(ctx, "recall_cpu_percent"); err == nil && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			globalSettings.RecallCPUPercent = storage.ClampQueryLimitPercent(n)
		}
	}
	if v, err := pg.GetSetting(ctx, "recall_memory_percent"); err == nil && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			globalSettings.RecallMemoryPercent = storage.ClampQueryLimitPercent(n)
		}
	}

	return nil
}

// ClampPgrSensitivity bounds the pgr severity share. A zero value means "unset" (an older client
// omitting the field), which maps to the default rather than to "flag nothing".
func ClampPgrSensitivity(f float64) float64 {
	if f <= 0 {
		return defaultPgrSensitivityPercent
	}
	if f < minPgrSensitivityPercent {
		return minPgrSensitivityPercent
	}
	if f > maxPgrSensitivityPercent {
		return maxPgrSensitivityPercent
	}
	return f
}

// clampAlertRevisionRetention bounds how much alert history is kept. The floor keeps
// a rollback target available; the ceiling bounds unbounded growth across every alert.
func clampAlertRevisionRetention(n int) int {
	if n < minAlertRevisionRetention {
		return minAlertRevisionRetention
	}
	if n > maxAlertRevisionRetention {
		return maxAlertRevisionRetention
	}
	return n
}

// clampRecallConcurrency bounds the recall concurrency setting to [1, ceiling].
func clampRecallConcurrency(n int) int {
	if n < 1 {
		return 1
	}
	if n > maxRecallConcurrency {
		return maxRecallConcurrency
	}
	return n
}

// Get returns a copy of the current settings
func Get() Settings {
	if globalSettings == nil {
		return Settings{
			TimestampFields: []TimestampField{
				{Field: "system_time", Format: "2006-01-02T15:04:05.999999999Z07:00"},
				{Field: "timestamp", Format: time.RFC3339Nano},
				{Field: "@timestamp", Format: time.RFC3339Nano},
			},
			AlertTimeoutSeconds:        5,
			QueryTimeoutSeconds:        60,
			AlertEvalIntervalSeconds:   60,
			AlertRevisionRetention:     defaultAlertRevisionRetention,
			RecallTimeoutSeconds:       defaultRecallTimeoutSeconds,
			RecallMaxBytesRead:         0,
			APIKeyRateLimit:            defaultAPIKeyRateLimit,
			PgrSensitivityPercent:      defaultPgrSensitivityPercent,
			RecallConcurrency:          defaultRecallConcurrency,
			QueryCPUPercent:            storage.DefaultQueryCPUPercent,
			QueryMemoryPercent:         storage.DefaultQueryMemoryPercent,
			RecallCPUPercent:           storage.DefaultRecallCPUPercent,
			RecallMemoryPercent:        storage.DefaultRecallMemoryPercent,
			SchemaSweepIntervalMinutes: defaultSchemaSweepIntervalMinutes,
		}
	}
	globalSettings.mu.RLock()
	defer globalSettings.mu.RUnlock()
	return Settings{
		APIKeyRateLimit:            globalSettings.APIKeyRateLimit,
		PgrSensitivityPercent:      globalSettings.PgrSensitivityPercent,
		RequireMFA:                 globalSettings.RequireMFA,
		TimestampFields:            globalSettings.TimestampFields,
		AlertTimeoutSeconds:        globalSettings.AlertTimeoutSeconds,
		QueryTimeoutSeconds:        globalSettings.QueryTimeoutSeconds,
		AlertEvalIntervalSeconds:   globalSettings.AlertEvalIntervalSeconds,
		AlertRevisionRetention:     globalSettings.AlertRevisionRetention,
		RecallTimeoutSeconds:       globalSettings.RecallTimeoutSeconds,
		RecallMaxBytesRead:         globalSettings.RecallMaxBytesRead,
		RecallConcurrency:          globalSettings.RecallConcurrency,
		QueryCPUPercent:            globalSettings.QueryCPUPercent,
		QueryMemoryPercent:         globalSettings.QueryMemoryPercent,
		RecallCPUPercent:           globalSettings.RecallCPUPercent,
		RecallMemoryPercent:        globalSettings.RecallMemoryPercent,
		SchemaSweepIntervalMinutes: globalSettings.SchemaSweepIntervalMinutes,
	}
}

// Update updates the global settings and persists to database
func Update(s *Settings) error {
	if globalSettings == nil {
		return nil
	}

	if s.AlertEvalIntervalSeconds < minAlertEvalIntervalSeconds {
		return validationErrorf("alert evaluation interval must be at least %d seconds", minAlertEvalIntervalSeconds)
	}
	if s.RecallTimeoutSeconds < minRecallTimeoutSeconds {
		return validationErrorf("Recall timeout must be at least %d seconds", minRecallTimeoutSeconds)
	}
	if s.RecallMaxBytesRead < 0 {
		return validationErrorf("Recall max bytes read cannot be negative (0 = unlimited)")
	}
	if s.APIKeyRateLimit < 0 {
		return validationErrorf("API key rate limit cannot be negative (0 means uncapped)")
	}
	// Without a pepper there is no key to encrypt TOTP secrets with, and storing
	// them in the clear would make the second factor worthless against anyone who
	// can read the database.
	if s.RequireMFA && !mfa.KeyAvailable() {
		return validationErrorf("Two-factor authentication needs BIFRACT_PASSWORD_PEPPER set so enrollment secrets can be encrypted at rest")
	}
	if s.SchemaSweepIntervalMinutes < minSchemaSweepIntervalMinutes {
		return validationErrorf("Schema measurement interval must be at least %d minutes", minSchemaSweepIntervalMinutes)
	}
	s.RecallConcurrency = clampRecallConcurrency(s.RecallConcurrency)
	s.AlertRevisionRetention = clampAlertRevisionRetention(s.AlertRevisionRetention)
	s.PgrSensitivityPercent = ClampPgrSensitivity(s.PgrSensitivityPercent)
	s.QueryCPUPercent = storage.ClampQueryLimitPercent(s.QueryCPUPercent)
	s.QueryMemoryPercent = storage.ClampQueryLimitPercent(s.QueryMemoryPercent)
	s.RecallCPUPercent = storage.ClampQueryLimitPercent(s.RecallCPUPercent)
	s.RecallMemoryPercent = storage.ClampQueryLimitPercent(s.RecallMemoryPercent)
	// Rejected rather than silently scaled: an admin who asks for more memory than
	// the node has should be told, not shown a number that is not what runs. The
	// headroom left over is what inserts, their MV cascade, and merges rely on.
	if total := s.QueryMemoryPercent + s.RecallMemoryPercent; total > storage.MaxCombinedQueryMemoryPercent {
		return validationErrorf("search and Recall memory shares total %d%%; they must total at most %d%% so merges and ingestion keep headroom",
			total, storage.MaxCombinedQueryMemoryPercent)
	}

	globalSettings.mu.Lock()
	globalSettings.QueryCPUPercent = s.QueryCPUPercent
	globalSettings.QueryMemoryPercent = s.QueryMemoryPercent
	globalSettings.RecallCPUPercent = s.RecallCPUPercent
	globalSettings.RecallMemoryPercent = s.RecallMemoryPercent
	globalSettings.TimestampFields = s.TimestampFields
	globalSettings.AlertTimeoutSeconds = s.AlertTimeoutSeconds
	globalSettings.QueryTimeoutSeconds = s.QueryTimeoutSeconds
	globalSettings.AlertEvalIntervalSeconds = s.AlertEvalIntervalSeconds
	globalSettings.AlertRevisionRetention = s.AlertRevisionRetention
	globalSettings.APIKeyRateLimit = s.APIKeyRateLimit
	globalSettings.PgrSensitivityPercent = s.PgrSensitivityPercent
	globalSettings.RequireMFA = s.RequireMFA
	globalSettings.RecallTimeoutSeconds = s.RecallTimeoutSeconds
	globalSettings.RecallMaxBytesRead = s.RecallMaxBytesRead
	globalSettings.RecallConcurrency = s.RecallConcurrency
	globalSettings.SchemaSweepIntervalMinutes = s.SchemaSweepIntervalMinutes
	// Unlocked before persisting: the ClickHouse workload DDL below can take seconds
	// on a cluster, and every search reads Get() under this lock.
	globalSettings.mu.Unlock()

	// Persist to database
	ctx := context.Background()

	// Save timestamp_fields as JSON
	timestampFieldsJSON, err := json.Marshal(s.TimestampFields)
	if err != nil {
		return err
	}
	if err := globalSettings.pg.SetSetting(ctx, "timestamp_fields", string(timestampFieldsJSON)); err != nil {
		return err
	}

	// Save alert_timeout_seconds
	if err := globalSettings.pg.SetSetting(ctx, "alert_timeout_seconds", fmt.Sprintf("%d", s.AlertTimeoutSeconds)); err != nil {
		return err
	}

	// Save query_timeout_seconds
	if err := globalSettings.pg.SetSetting(ctx, "query_timeout_seconds", fmt.Sprintf("%d", s.QueryTimeoutSeconds)); err != nil {
		return err
	}

	// Save alert_eval_interval_seconds
	if err := globalSettings.pg.SetSetting(ctx, "alert_eval_interval_seconds", fmt.Sprintf("%d", s.AlertEvalIntervalSeconds)); err != nil {
		return err
	}

	// Save recall knobs
	if err := globalSettings.pg.SetSetting(ctx, "recall_timeout_seconds", fmt.Sprintf("%d", s.RecallTimeoutSeconds)); err != nil {
		return err
	}
	if err := globalSettings.pg.SetSetting(ctx, "recall_max_bytes_read", fmt.Sprintf("%d", s.RecallMaxBytesRead)); err != nil {
		return err
	}
	if err := globalSettings.pg.SetSetting(ctx, "recall_concurrency", fmt.Sprintf("%d", s.RecallConcurrency)); err != nil {
		return err
	}
	if err := globalSettings.pg.SetSetting(ctx, "alert_revision_retention", fmt.Sprintf("%d", s.AlertRevisionRetention)); err != nil {
		return err
	}
	if err := globalSettings.pg.SetSetting(ctx, "pgr_sensitivity_percent", strconv.FormatFloat(s.PgrSensitivityPercent, 'f', -1, 64)); err != nil {
		return err
	}

	if err := globalSettings.pg.SetSetting(ctx, "api_key_rate_limit", fmt.Sprintf("%d", s.APIKeyRateLimit)); err != nil {
		return err
	}
	if err := globalSettings.pg.SetSetting(ctx, "require_mfa", fmt.Sprintf("%t", s.RequireMFA)); err != nil {
		return err
	}
	if err := globalSettings.pg.SetSetting(ctx, "schema_sweep_interval_minutes", fmt.Sprintf("%d", s.SchemaSweepIntervalMinutes)); err != nil {
		return err
	}
	if err := globalSettings.pg.SetSetting(ctx, "query_cpu_percent", fmt.Sprintf("%d", s.QueryCPUPercent)); err != nil {
		return err
	}
	if err := globalSettings.pg.SetSetting(ctx, "query_memory_percent", fmt.Sprintf("%d", s.QueryMemoryPercent)); err != nil {
		return err
	}
	if err := globalSettings.pg.SetSetting(ctx, "recall_cpu_percent", fmt.Sprintf("%d", s.RecallCPUPercent)); err != nil {
		return err
	}
	if err := globalSettings.pg.SetSetting(ctx, "recall_memory_percent", fmt.Sprintf("%d", s.RecallMemoryPercent)); err != nil {
		return err
	}

	// Apply the shares to ClickHouse. Unconditional rather than only on change, so
	// that re-saving repairs a startup reconcile that failed (ClickHouse briefly
	// unavailable), which an admin otherwise could not do from the UI at all. The
	// values are already persisted, so a failure here is reported and self-heals on
	// the next restart.
	if queryLimitsApplier != nil {
		if err := queryLimitsApplier(storage.WorkloadLimits{
			SearchCPUPercent:    s.QueryCPUPercent,
			SearchMemoryPercent: s.QueryMemoryPercent,
			RecallCPUPercent:    s.RecallCPUPercent,
			RecallMemoryPercent: s.RecallMemoryPercent,
		}); err != nil {
			return fmt.Errorf("search limits saved but not applied: %w", err)
		}
	}
	return nil
}

// Handler handles settings API requests
type Handler struct {
	pg *storage.PostgresClient
}

func NewHandler(pg *storage.PostgresClient) *Handler {
	return &Handler{pg: pg}
}

type SettingsResponse struct {
	Success  bool     `json:"success"`
	Settings Settings `json:"settings"`
	// Capabilities is a sibling of Settings, not part of it: a stored value stays
	// stored even when the server currently refuses to apply it, and the UI reads
	// this to say so rather than showing a share that is not in force.
	Capabilities storage.Capabilities `json:"capabilities,omitempty"`
	Error        string               `json:"error,omitempty"`
}

func (h *Handler) HandleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Admin only - settings may contain sensitive configuration

	respondJSON(w, http.StatusOK, SettingsResponse{
		Success:      true,
		Settings:     Get(),
		Capabilities: currentCapabilities(),
	})
}

func (h *Handler) HandleUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Only admins can update settings

	// Decode onto the current settings, not a zero value, so a partial POST (the
	// UI autosaves one control at a time) only changes the keys it sends and
	// leaves every other setting -- timestamp fields, the other recall knobs --
	// at its current value instead of resetting it.
	settings := Get()
	// Get() shares TimestampFields' backing array with the live global; a body
	// that includes timestamp_fields would make json reset+append into that shared
	// array in place, racing the ingest hot path that iterates Get().TimestampFields.
	// Copy it so the decode can only touch this goroutine's slice.
	settings.TimestampFields = append([]TimestampField(nil), settings.TimestampFields...)
	if err := json.NewDecoder(r.Body).Decode(&settings); err != nil {
		respondJSON(w, http.StatusBadRequest, SettingsResponse{
			Success: false,
			Error:   "Invalid JSON",
		})
		return
	}

	if err := Update(&settings); err != nil {
		var ve ValidationError
		if errors.As(err, &ve) {
			respondJSON(w, http.StatusBadRequest, SettingsResponse{Success: false, Error: ve.Error()})
			return
		}
		log.Printf("[Settings] save failed: %v", err)
		respondJSON(w, http.StatusInternalServerError, SettingsResponse{
			Success: false,
			Error:   "Failed to save settings",
		})
		return
	}

	respondJSON(w, http.StatusOK, SettingsResponse{
		Success:  true,
		Settings: Get(),
	})
}

func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	api.WriteJSON(w, status, data)
}
