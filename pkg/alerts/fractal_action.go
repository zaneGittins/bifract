package alerts

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"bifract/pkg/storage"
)

// FractalActionClient handles sending logs to other fractals
type FractalActionClient struct {
	ch *storage.ClickHouseClient
	pg *storage.PostgresClient
}

// FractalAction represents a "send to fractal" action configuration
type FractalAction struct {
	ID                 string            `json:"id"`
	Name               string            `json:"name"`
	Description        string            `json:"description"`
	TargetFractalID    string            `json:"target_fractal_id"`
	PreserveTimestamp  bool              `json:"preserve_timestamp"`
	AddAlertContext    bool              `json:"add_alert_context"`
	FieldMappings      map[string]string `json:"field_mappings"`
	MaxLogsPerTrigger  int               `json:"max_logs_per_trigger"`
	Enabled            bool              `json:"enabled"`
}

// FractalResult represents the outcome of sending logs to a fractal
type FractalResult struct {
	FractalActionID    string        `json:"fractal_action_id"`
	FractalActionName  string        `json:"fractal_action_name"`
	TargetFractalID    string        `json:"target_fractal_id"`
	Success            bool          `json:"success"`
	LogsSent           int           `json:"logs_sent"`
	ResponseTime       time.Duration `json:"response_time"`
	Error              string        `json:"error,omitempty"`
	ExecutedAt         time.Time     `json:"executed_at"`
}

// NewFractalActionClient creates a new fractal action client
func NewFractalActionClient(ch *storage.ClickHouseClient, pg *storage.PostgresClient) *FractalActionClient {
	return &FractalActionClient{
		ch: ch,
		pg: pg,
	}
}

// Send processes alert results and sends matching logs to the target fractal.
// resolvedName is the alert name with any {{field}} templates replaced.
func (f *FractalActionClient) Send(ctx context.Context, action FractalAction, alert *Alert, resolvedName string, matchingLogs []map[string]interface{}) FractalResult {
	start := time.Now()

	result := FractalResult{
		FractalActionID:   action.ID,
		FractalActionName: action.Name,
		TargetFractalID:   action.TargetFractalID,
		ExecutedAt:        start,
	}

	// Limit logs if configured
	logsToSend := matchingLogs
	if action.MaxLogsPerTrigger > 0 && len(matchingLogs) > action.MaxLogsPerTrigger {
		logsToSend = matchingLogs[:action.MaxLogsPerTrigger]
	}

	// Transform logs for ingestion into target fractal
	logEntries, err := f.transformLogsForFractal(action, alert, resolvedName, logsToSend)
	if err != nil {
		result.Error = fmt.Sprintf("Failed to transform logs: %v", err)
		result.ResponseTime = time.Since(start)
		return result
	}

	// Ingest logs into target fractal
	if len(logEntries) > 0 {
		err = f.ch.InsertLogs(ctx, logEntries)
		if err != nil {
			result.Error = fmt.Sprintf("Failed to ingest logs to fractal %s: %v", action.TargetFractalID, err)
		} else {
			result.Success = true
			result.LogsSent = len(logEntries)
		}
	}

	result.ResponseTime = time.Since(start)
	return result
}

// transformLogsForFractal converts query results into log entries for the target fractal
func (f *FractalActionClient) transformLogsForFractal(action FractalAction, alert *Alert, resolvedName string, results []map[string]interface{}) ([]storage.LogEntry, error) {
	var logEntries []storage.LogEntry

	// Get source fractal name
	sourceFractalName := f.getFractalName(alert.FractalID)

	for _, result := range results {
		// Create a copy of the original log data to preserve all fields
		logData := make(map[string]interface{})

		// Copy all original fields, extracting nested fields from 'fields' map
		for key, value := range result {
			if key == "fields" {
				// Extract nested fields and promote them to top level
				// Try both map[string]interface{} and map[string]string
				if fieldsMap, ok := value.(map[string]interface{}); ok {
					for fieldKey, fieldValue := range fieldsMap {
						logData[fieldKey] = fieldValue
					}
				} else if fieldsMap, ok := value.(map[string]string); ok {
					for fieldKey, fieldValue := range fieldsMap {
						logData[fieldKey] = fieldValue
					}
				} else {
					// If fields isn't a map, keep it as-is
					logData[key] = value
				}
			} else {
				// Copy other top-level fields as-is
				logData[key] = value
			}
		}

		// Preserve the original log_id before it gets replaced by the
		// new entry's log_id so consumers can trace back to the source.
		if origID, ok := logData["log_id"]; ok {
			logData["source_log_id"] = origID
		}

		// Add alert context fields directly to the log data if configured
		if action.AddAlertContext {
			logData["alert_name"] = resolvedName
			if resolvedName != alert.Name {
				logData["alert_template_name"] = alert.Name
			}
			logData["alert_id"] = alert.ID
			logData["source_fractal"] = sourceFractalName
			logData["forwarded_at"] = time.Now().Format(time.RFC3339)
			logData["fractal_action_id"] = action.ID
			logData["alert_forwarded"] = true
		}

		// Convert the complete log data to JSON (preserving all fields + context)
		logJSON, err := json.Marshal(logData)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal log data: %w", err)
		}

		// Create base log entry with the complete JSON
		logEntry := storage.LogEntry{
			RawLog:    string(logJSON),
			Timestamp: time.Now(),     // Default to current time
			FractalID: action.TargetFractalID,
			Fields:    make(map[string]string),
		}

		// Handle timestamp preservation
		if action.PreserveTimestamp {
			if ts, exists := result["timestamp"]; exists {
				if timestamp, ok := ts.(time.Time); ok {
					logEntry.Timestamp = timestamp
				} else if timestampStr, ok := ts.(string); ok {
					if parsedTime, err := time.Parse(time.RFC3339, timestampStr); err == nil {
						logEntry.Timestamp = parsedTime
					}
				}
			}
		}

		// Generate log ID based on timestamp and raw log content
		logEntry.LogID = storage.GenerateLogID(logEntry.Timestamp, logEntry.RawLog)

		// Add the generated log_id to the log data for indexing
		logData["log_id"] = logEntry.LogID

		// Populate Fields map with all log data for indexing/searching
		for key, value := range logData {
			// Preserve the original format for better searching
			switch v := value.(type) {
			case string:
				logEntry.Fields[key] = v
			case int, int8, int16, int32, int64:
				logEntry.Fields[key] = fmt.Sprintf("%d", v)
			case uint, uint8, uint16, uint32, uint64:
				logEntry.Fields[key] = fmt.Sprintf("%d", v)
			case float32, float64:
				logEntry.Fields[key] = fmt.Sprintf("%g", v)
			case bool:
				if v {
					logEntry.Fields[key] = "true"
				} else {
					logEntry.Fields[key] = "false"
				}
			default:
				logEntry.Fields[key] = fmt.Sprintf("%v", v)
			}
		}

		// Apply field mappings if configured (can override existing fields)
		if len(action.FieldMappings) > 0 {
			for sourceField, targetField := range action.FieldMappings {
				if value, exists := result[sourceField]; exists {
					logEntry.Fields[targetField] = fmt.Sprintf("%v", value)
				}
			}
		}

		logEntries = append(logEntries, logEntry)
	}

	return logEntries, nil
}

// getFractalName looks up the fractal name by ID
func (f *FractalActionClient) getFractalName(fractalID string) string {
	var fractalName string
	query := "SELECT name FROM fractals WHERE id = $1"

	err := f.pg.QueryRow(context.Background(), query, fractalID).Scan(&fractalName)
	if err != nil {
		// Return the ID as fallback if lookup fails
		return fractalID
	}

	return fractalName
}