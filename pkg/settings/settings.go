package settings

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

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

// Recall (archive search) knobs. Defaults live here and in pkg/archive
// (settings.go), which reads the same keys directly from the settings table so
// the workers stay decoupled from this package; keep the two in sync.
const (
	defaultRecallTimeoutSeconds = 900 // 15m -- archive scans over object storage are legitimately slow
	minRecallTimeoutSeconds     = 30  // floor so a mistype cannot make every recall fail instantly
	defaultRecallConcurrency    = 5
	maxRecallConcurrency        = 16 // ceiling: the search worker pool is sized to this
)

// Settings holds all Bifract configuration
type Settings struct {
	TimestampFields          []TimestampField `json:"timestamp_fields"`
	AlertTimeoutSeconds      int              `json:"alert_timeout_seconds"`
	QueryTimeoutSeconds      int              `json:"query_timeout_seconds"`
	AlertEvalIntervalSeconds int              `json:"alert_eval_interval_seconds"`
	// Recall (archive search) knobs, live-editable from the admin settings page.
	RecallTimeoutSeconds int   `json:"recall_timeout_seconds"` // per-search wall clock; >= minRecallTimeoutSeconds
	RecallMaxBytesRead   int64 `json:"recall_max_bytes_read"`  // ClickHouse max_bytes_to_read guard; 0 = unlimited
	RecallConcurrency    int   `json:"recall_concurrency"`     // max concurrent recall searches; 1..maxRecallConcurrency
	mu                   sync.RWMutex
	pg                   *storage.PostgresClient
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
		TimestampFields:          defaultTimestampFields,
		AlertTimeoutSeconds:      5,  // 5s default for alert queries
		QueryTimeoutSeconds:      60, // 60s default for search queries
		AlertEvalIntervalSeconds: 60, // 60s default between alert engine ticks
		RecallTimeoutSeconds:     defaultRecallTimeoutSeconds,
		RecallMaxBytesRead:       0, // unlimited by default (admin opts into a cap)
		RecallConcurrency:        defaultRecallConcurrency,
		pg:                       pg,
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

	return nil
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
			AlertTimeoutSeconds:      5,
			QueryTimeoutSeconds:      60,
			AlertEvalIntervalSeconds: 60,
			RecallTimeoutSeconds:     defaultRecallTimeoutSeconds,
			RecallMaxBytesRead:       0,
			RecallConcurrency:        defaultRecallConcurrency,
		}
	}
	globalSettings.mu.RLock()
	defer globalSettings.mu.RUnlock()
	return Settings{
		TimestampFields:          globalSettings.TimestampFields,
		AlertTimeoutSeconds:      globalSettings.AlertTimeoutSeconds,
		QueryTimeoutSeconds:      globalSettings.QueryTimeoutSeconds,
		AlertEvalIntervalSeconds: globalSettings.AlertEvalIntervalSeconds,
		RecallTimeoutSeconds:     globalSettings.RecallTimeoutSeconds,
		RecallMaxBytesRead:       globalSettings.RecallMaxBytesRead,
		RecallConcurrency:        globalSettings.RecallConcurrency,
	}
}

// Update updates the global settings and persists to database
func Update(s *Settings) error {
	if globalSettings == nil {
		return nil
	}

	if s.AlertEvalIntervalSeconds < minAlertEvalIntervalSeconds {
		return fmt.Errorf("alert_eval_interval_seconds must be at least %d", minAlertEvalIntervalSeconds)
	}
	if s.RecallTimeoutSeconds < minRecallTimeoutSeconds {
		return fmt.Errorf("recall_timeout_seconds must be at least %d", minRecallTimeoutSeconds)
	}
	if s.RecallMaxBytesRead < 0 {
		return fmt.Errorf("recall_max_bytes_read cannot be negative (0 = unlimited)")
	}
	s.RecallConcurrency = clampRecallConcurrency(s.RecallConcurrency)

	globalSettings.mu.Lock()
	defer globalSettings.mu.Unlock()
	globalSettings.TimestampFields = s.TimestampFields
	globalSettings.AlertTimeoutSeconds = s.AlertTimeoutSeconds
	globalSettings.QueryTimeoutSeconds = s.QueryTimeoutSeconds
	globalSettings.AlertEvalIntervalSeconds = s.AlertEvalIntervalSeconds
	globalSettings.RecallTimeoutSeconds = s.RecallTimeoutSeconds
	globalSettings.RecallMaxBytesRead = s.RecallMaxBytesRead
	globalSettings.RecallConcurrency = s.RecallConcurrency

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
	return globalSettings.pg.SetSetting(ctx, "recall_concurrency", fmt.Sprintf("%d", s.RecallConcurrency))
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
	Error    string   `json:"error,omitempty"`
}

func (h *Handler) HandleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Admin only - settings may contain sensitive configuration
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil || !user.IsAdmin {
		respondJSON(w, http.StatusForbidden, SettingsResponse{
			Success: false,
			Error:   "Admin access required",
		})
		return
	}

	respondJSON(w, http.StatusOK, SettingsResponse{
		Success:  true,
		Settings: Get(),
	})
}

func (h *Handler) HandleUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Only admins can update settings
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil || !user.IsAdmin {
		respondJSON(w, http.StatusForbidden, SettingsResponse{
			Success: false,
			Error:   "Only administrators can update settings",
		})
		return
	}

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
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
