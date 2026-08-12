package alerts

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
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
	ID                string            `json:"id"`
	Name              string            `json:"name"`
	Description       string            `json:"description"`
	TargetFractalID   string            `json:"target_fractal_id"`
	PreserveTimestamp bool              `json:"preserve_timestamp"`
	AddAlertContext   bool              `json:"add_alert_context"`
	FieldMappings     map[string]string `json:"field_mappings"`
	MaxLogsPerTrigger int               `json:"max_logs_per_trigger"`
	Enabled           bool              `json:"enabled"`
	FractalID         string            `json:"fractal_id,omitempty"`
	PrismID           string            `json:"prism_id,omitempty"`
}

// FractalResult represents the outcome of sending logs to a fractal
type FractalResult struct {
	FractalActionID   string        `json:"fractal_action_id"`
	FractalActionName string        `json:"fractal_action_name"`
	TargetFractalID   string        `json:"target_fractal_id"`
	Success           bool          `json:"success"`
	LogsSent          int           `json:"logs_sent"`
	ResponseTime      time.Duration `json:"response_time"`
	Error             string        `json:"error,omitempty"`
	ExecutedAt        time.Time     `json:"executed_at"`
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
	logEntries, err := f.transformLogsForFractal(ctx, action, alert, resolvedName, logsToSend)
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
func (f *FractalActionClient) transformLogsForFractal(ctx context.Context, action FractalAction, alert *Alert, resolvedName string, results []map[string]interface{}) ([]storage.LogEntry, error) {
	var logEntries []storage.LogEntry

	// Get source fractal name
	sourceFractalName := f.getFractalName(alert.FractalID)

	for _, result := range results {
		logData := f.mergeRowFields(ctx, result)

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
			logData["alert_severity"] = alert.Severity
			// The rule's labels carry its attack.* tags (Sigma imports keep them
			// verbatim). Without them a forwarded detection is invisible to
			// mitre(), which is the difference between "what we can detect" and
			// "what fired" being comparable or not. Encoded as a JSON array string,
			// the same shape every other detection source ships tags in.
			if len(alert.Labels) > 0 {
				if labels, err := json.Marshal(alert.Labels); err == nil {
					logData["alert_labels"] = string(labels)
				}
			}
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
			Timestamp: time.Now(), // Default to current time
			FractalID: action.TargetFractalID,
			Fields:    make(map[string]string),
		}

		// Handle timestamp preservation. Query rows carry timestamps as formatted
		// strings, not time.Time, so this must go through RowTime; asserting
		// time.Time here silently left every forwarded log stamped time.Now().
		if action.PreserveTimestamp {
			if ts, ok := storage.RowTime(result, "timestamp"); ok {
				logEntry.Timestamp = ts
			}
		}

		// Generate log ID based on timestamp and raw log content
		logEntry.LogID = storage.GenerateLogID(logEntry.Timestamp, logEntry.RawLog)

		// Add the generated log_id to the log data for indexing
		logData["log_id"] = logEntry.LogID

		// Populate Fields map with all log data for indexing/searching
		for key, value := range logData {
			logEntry.Fields[key] = fieldString(value)
		}

		// Apply field mappings if configured (can override existing fields).
		// Sourced from logData, not the raw row, so a mapping can name any
		// hydrated field and not just the ones the query projected.
		if len(action.FieldMappings) > 0 {
			for sourceField, targetField := range action.FieldMappings {
				if value, exists := logData[sourceField]; exists {
					logEntry.Fields[targetField] = fieldString(value)
				}
			}
		}

		logEntries = append(logEntries, logEntry)
	}

	return logEntries, nil
}

// mergeRowFields flattens a query result row into forwardable log data.
//
// The nested field map is overlaid first so top-level columns always win, which
// matches the precedence in ResolveTemplateName and dictionaries.getLogField. It
// also matters for correctness: a field the alert filtered on is projected
// top-level as a String cast, while parseLogFields drops declared type hints the
// log did not carry, so top-level-wins keeps those values exactly as they were
// before hydration existed.
//
// A row carrying norm_log as a raw string instead of a parsed map comes from an
// unpruned projection (a pipeline command). Parsing it here costs no query and
// makes forwarded logs the same shape either way.
func (f *FractalActionClient) mergeRowFields(ctx context.Context, row map[string]interface{}) map[string]interface{} {
	logData := make(map[string]interface{}, len(row))

	switch nested := row["fields"].(type) {
	case map[string]interface{}:
		for k, v := range nested {
			logData[k] = v
		}
	case map[string]string:
		for k, v := range nested {
			logData[k] = v
		}
	}
	if normLog, ok := row["norm_log"].(string); ok && normLog != "" && f.ch != nil {
		for k, v := range f.ch.ParseLogFields(ctx, normLog) {
			logData[k] = v
		}
	}

	for key, value := range row {
		if key == "fields" || key == "norm_log" {
			continue
		}
		logData[key] = value
	}
	return logData
}

// fieldString renders a log value for the indexed Fields map. Hydrated rows carry
// native JSON types, so this has to cover more than the String casts the pruned
// projection produced: %g would turn a nanosecond epoch into "1.7e+18", and %v
// would render a nested object as "map[a:1]".
func fieldString(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case bool:
		if v {
			return "true"
		}
		return "false"
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case map[string]interface{}, []interface{}:
		if b, err := json.Marshal(v); err == nil {
			return string(b)
		}
		return fmt.Sprintf("%v", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// getFractalName looks up the fractal name by ID
func (f *FractalActionClient) getFractalName(fractalID string) string {
	if f.pg == nil {
		return fractalID
	}
	var fractalName string
	query := "SELECT name FROM fractals WHERE id = $1"

	err := f.pg.QueryRow(context.Background(), query, fractalID).Scan(&fractalName)
	if err != nil {
		// Return the ID as fallback if lookup fails
		return fractalID
	}

	return fractalName
}
