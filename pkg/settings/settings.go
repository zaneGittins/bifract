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

// Settings holds all Bifract configuration
type Settings struct {
	TimestampFields     []TimestampField `json:"timestamp_fields"`
	AlertTimeoutSeconds int              `json:"alert_timeout_seconds"`
	QueryTimeoutSeconds int              `json:"query_timeout_seconds"`
	mu                  sync.RWMutex
	pg                  *storage.PostgresClient
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
		TimestampFields:     defaultTimestampFields,
		AlertTimeoutSeconds: 5,  // 5s default for alert queries
		QueryTimeoutSeconds: 60, // 60s default for search queries
		pg:                  pg,
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

	return nil
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
			AlertTimeoutSeconds: 5,
			QueryTimeoutSeconds: 60,
		}
	}
	globalSettings.mu.RLock()
	defer globalSettings.mu.RUnlock()
	return Settings{
		TimestampFields:     globalSettings.TimestampFields,
		AlertTimeoutSeconds: globalSettings.AlertTimeoutSeconds,
		QueryTimeoutSeconds: globalSettings.QueryTimeoutSeconds,
	}
}

// Update updates the global settings and persists to database
func Update(s *Settings) error {
	if globalSettings == nil {
		return nil
	}

	globalSettings.mu.Lock()
	defer globalSettings.mu.Unlock()
	globalSettings.TimestampFields = s.TimestampFields
	globalSettings.AlertTimeoutSeconds = s.AlertTimeoutSeconds
	globalSettings.QueryTimeoutSeconds = s.QueryTimeoutSeconds

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
	return globalSettings.pg.SetSetting(ctx, "query_timeout_seconds", fmt.Sprintf("%d", s.QueryTimeoutSeconds))
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

	var settings Settings
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
