package ingest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"bifract/pkg/ingesttokens"
	"bifract/pkg/normalizers"
	"bifract/pkg/settings"
	"bifract/pkg/storage"
)

// IngestHandler handles log ingestion via HTTP.
// Logs are parsed and pushed onto a buffered IngestQueue, decoupling
// reception from ClickHouse insertion.
type IngestHandler struct {
	queue        *IngestQueue
	maxBodySize  int64 // max request body in bytes (0 = unlimited)
	tokenCache   *ingesttokens.TokenCache
	tokenStorage *ingesttokens.Storage
	quotaManager *QuotaManager
}

type IngestResponse struct {
	Success bool   `json:"success"`
	Count   int    `json:"count"`
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
}

// NewIngestHandler creates an ingest handler backed by a queue.
func NewIngestHandler(queue *IngestQueue, maxBodySize int64,
	tokenCache *ingesttokens.TokenCache, tokenStorage *ingesttokens.Storage) *IngestHandler {
	return &IngestHandler{
		queue:        queue,
		maxBodySize:  maxBodySize,
		tokenCache:   tokenCache,
		tokenStorage: tokenStorage,
	}
}

// SetQuotaManager attaches a QuotaManager for per-fractal disk quota checks.
func (h *IngestHandler) SetQuotaManager(qm *QuotaManager) {
	h.quotaManager = qm
}

// HandleIngest processes incoming logs and pushes them onto the ingestion queue.
func (h *IngestHandler) HandleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Validate ingest token (always required)
	tokenData, err := h.validateIngestToken(r)
	if err != nil {
		respondJSON(w, http.StatusUnauthorized, IngestResponse{
			Success: false,
			Error:   "Invalid or missing ingest token",
		})
		return
	}

	// Early rejection: if the system is under pressure, respond 429
	// before reading/parsing the body to save CPU.
	if !h.queue.Healthy() {
		w.Header().Set("Retry-After", "2")
		respondJSON(w, http.StatusTooManyRequests, IngestResponse{
			Success: false,
			Error:   "Ingestion backend under pressure. Retry after backoff.",
		})
		return
	}

	// Enforce body size limit
	bodyReader := r.Body
	if h.maxBodySize > 0 {
		bodyReader = http.MaxBytesReader(w, r.Body, h.maxBodySize)
	}

	body, err := io.ReadAll(bodyReader)
	if err != nil {
		if err.Error() == "http: request body too large" {
			respondJSON(w, http.StatusRequestEntityTooLarge, IngestResponse{
				Success: false,
				Error:   fmt.Sprintf("Request body exceeds %d byte limit", h.maxBodySize),
			})
			return
		}
		respondJSON(w, http.StatusBadRequest, IngestResponse{
			Success: false,
			Error:   "Failed to read request body",
		})
		return
	}
	defer r.Body.Close()

	logs, err := h.parseLogsWithToken(body, tokenData)
	if err != nil {
		respondJSON(w, http.StatusBadRequest, IngestResponse{
			Success: false,
			Error:   "Failed to parse logs. Supported formats: JSON array, single JSON object, or NDJSON.",
		})
		return
	}

	if len(logs) == 0 {
		respondJSON(w, http.StatusBadRequest, IngestResponse{
			Success: false,
			Error:   "No valid logs found in request",
		})
		return
	}

	// Assign fractal from token to all log entries
	for i := range logs {
		logs[i].FractalID = tokenData.FractalID
	}

	// Per-fractal disk quota check (reject action only; rollover is handled post-insert).
	if h.quotaManager != nil {
		var batchBytes int64
		for i := range logs {
			batchBytes += int64(len(logs[i].RawLog))
		}
		if !h.quotaManager.CheckQuota(tokenData.FractalID, batchBytes) {
			w.Header().Set("Retry-After", "30")
			respondJSON(w, http.StatusTooManyRequests, IngestResponse{
				Success: false,
				Error:   "Fractal disk quota exceeded. Clear old logs or increase quota.",
			})
			return
		}
	}

	// Push onto the ingestion queue (non-blocking).
	// Enqueue returns false if the queue is full, workers are unhealthy,
	// or ClickHouse CPU backpressure is active.
	if !h.queue.Enqueue(logs) {
		w.Header().Set("Retry-After", "2")
		respondJSON(w, http.StatusTooManyRequests, IngestResponse{
			Success: false,
			Error:   "Ingestion queue full. Retry after backoff.",
		})
		return
	}

	// Update token usage stats asynchronously.
	// Use a detached context since r.Context() is cancelled after the handler returns.
	go func(tokenID string, count int) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.tokenStorage.UpdateUsageStats(ctx, tokenID, count); err != nil {
			log.Printf("[Ingest] failed to update token usage: %v", err)
		}
	}(tokenData.TokenID, len(logs))

	respondJSON(w, http.StatusOK, IngestResponse{
		Success: true,
		Count:   len(logs),
		Message: fmt.Sprintf("Accepted %d log(s) for ingestion", len(logs)),
	})
}

// validateIngestToken extracts and validates a bearer token from the request.
// Accepts "Bearer <token>" and "APIKey <token>" prefixes for compatibility
// with Elasticsearch clients (e.g. Velociraptor's go-elasticsearch).
func (h *IngestHandler) validateIngestToken(r *http.Request) (*ingesttokens.ValidatedToken, error) {
	authHeader := r.Header.Get("Authorization")
	var rawToken string
	upperAuth := strings.ToUpper(authHeader)
	switch {
	case strings.HasPrefix(upperAuth, "BEARER "):
		rawToken = authHeader[len("Bearer "):]
	case strings.HasPrefix(upperAuth, "APIKEY "):
		rawToken = authHeader[len("APIKey "):]
	}

	if rawToken == "" {
		return nil, fmt.Errorf("ingest token required: set Authorization: Bearer <token> or ApiKey <token>")
	}

	tokenHash := ingesttokens.HashToken(rawToken)

	// Check cache first
	if h.tokenCache != nil {
		if cached, ok := h.tokenCache.Get(tokenHash); ok {
			return cached, nil
		}
	}

	// Cache miss: DB lookup
	validated, err := h.tokenStorage.ValidateToken(r.Context(), rawToken)
	if err != nil {
		return nil, err
	}

	// Populate cache
	if h.tokenCache != nil {
		h.tokenCache.Set(tokenHash, validated)
	}

	return validated, nil
}

// parseLogsWithToken parses logs using per-token config (parser type, normalization, timestamp fields).
func (h *IngestHandler) parseLogsWithToken(data []byte, token *ingesttokens.ValidatedToken) ([]storage.LogEntry, error) {
	switch token.ParserType {
	case "kv":
		return h.parseKVLogs(data, token)
	case "syslog":
		return h.parseSyslogLogs(data, token)
	default:
		return h.parseJSONLogsWithConfig(data, token.Normalizer, token.TimestampFields)
	}
}

func (h *IngestHandler) parseJSONLogsWithConfig(data []byte, norm *normalizers.CompiledNormalizer, tsFields []ingesttokens.TsField) ([]storage.LogEntry, error) {
	var logs []storage.LogEntry

	// Try to parse as JSON array first
	var jsonArray []map[string]interface{}
	if err := json.Unmarshal(data, &jsonArray); err == nil {
		for _, obj := range jsonArray {
			log, err := h.parseLogObjectWithConfig(obj, norm, tsFields)
			if err != nil {
				continue
			}
			logs = append(logs, log)
		}
		return logs, nil
	}

	// Try to parse as single JSON object
	var jsonObj map[string]interface{}
	if err := json.Unmarshal(data, &jsonObj); err == nil {
		log, err := h.parseLogObjectWithConfig(jsonObj, norm, tsFields)
		if err != nil {
			return nil, err
		}
		logs = append(logs, log)
		return logs, nil
	}

	// Try to parse as newline-delimited JSON (NDJSON)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, bufio.MaxScanTokenSize), 10*1024*1024) // 10MB max line
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			continue
		}

		log, err := h.parseLogObjectWithConfig(obj, norm, tsFields)
		if err != nil {
			continue
		}
		logs = append(logs, log)
	}

	if len(logs) > 0 {
		return logs, nil
	}

	return nil, fmt.Errorf("unable to parse logs: not valid JSON array, object, or NDJSON")
}

func (h *IngestHandler) parseLogObjectWithConfig(obj map[string]interface{}, norm *normalizers.CompiledNormalizer, tsFields []ingesttokens.TsField) (storage.LogEntry, error) {
	entry := storage.LogEntry{
		Fields: make(map[string]string),
	}

	rawBytes, err := json.Marshal(obj)
	if err != nil {
		return entry, fmt.Errorf("failed to marshal raw log: %w", err)
	}
	entry.RawLog = string(rawBytes)

	h.flattenJSON(obj, "", entry.Fields, norm)

	ingestTime := time.Now()
	entry.Timestamp = h.extractTimestamp(entry.Fields, tsFields, norm)

	if entry.Timestamp.IsZero() {
		entry.Timestamp = ingestTime
	}

	entry.IngestTimestamp = ingestTime
	entry.Fields["ingesttimestamp"] = ingestTime.Format(time.RFC3339Nano)
	entry.LogID = storage.GenerateLogID(entry.Timestamp, entry.RawLog)

	return entry, nil
}

// parseKVLogs parses key=value formatted logs.
func (h *IngestHandler) parseKVLogs(data []byte, token *ingesttokens.ValidatedToken) ([]storage.LogEntry, error) {
	var logs []storage.LogEntry

	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, bufio.MaxScanTokenSize), 10*1024*1024) // 10MB max line
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}

		entry := storage.LogEntry{
			RawLog: line,
			Fields: make(map[string]string),
		}

		h.parseKVLine(line, entry.Fields, token.Normalizer)

		ingestTime := time.Now()
		entry.Timestamp = h.extractTimestamp(entry.Fields, token.TimestampFields, token.Normalizer)
		if entry.Timestamp.IsZero() {
			entry.Timestamp = ingestTime
		}

		entry.IngestTimestamp = ingestTime
		entry.Fields["ingesttimestamp"] = ingestTime.Format(time.RFC3339Nano)
		entry.LogID = storage.GenerateLogID(entry.Timestamp, entry.RawLog)

		logs = append(logs, entry)
	}

	if len(logs) == 0 {
		return nil, fmt.Errorf("no valid KV log lines found")
	}
	return logs, nil
}

// parseKVLine parses a single key=value line into the fields map.
// Supports: key=value, key="quoted value", key='quoted value'
func (h *IngestHandler) parseKVLine(line string, fields map[string]string, norm *normalizers.CompiledNormalizer) {
	i := 0
	for i < len(line) {
		// Skip whitespace
		for i < len(line) && line[i] == ' ' {
			i++
		}
		if i >= len(line) {
			break
		}

		// Read key
		keyStart := i
		for i < len(line) && line[i] != '=' && line[i] != ' ' {
			i++
		}
		if i >= len(line) || line[i] != '=' {
			// No = found, skip this token
			for i < len(line) && line[i] != ' ' {
				i++
			}
			continue
		}
		key := line[keyStart:i]
		i++ // skip '='

		if i >= len(line) {
			fields[normalizeField(key, norm)] = ""
			break
		}

		// Read value
		var value string
		if line[i] == '"' || line[i] == '\'' {
			quote := line[i]
			i++ // skip opening quote
			valStart := i
			for i < len(line) && line[i] != quote {
				i++
			}
			value = line[valStart:i]
			if i < len(line) {
				i++ // skip closing quote
			}
		} else {
			valStart := i
			for i < len(line) && line[i] != ' ' {
				i++
			}
			value = line[valStart:i]
		}

		fields[normalizeField(key, norm)] = value
	}
}

// normalizeField applies the compiled normalizer to a field name.
// If norm is nil, the field name is returned unchanged.
func normalizeField(field string, norm *normalizers.CompiledNormalizer) string {
	if norm == nil {
		return field
	}
	return norm.ApplyFieldName(field)
}

const maxFlattenDepth = 64
const maxFlattenFields = 1000

// flattenJSON recursively flattens nested JSON objects.
// If the normalizer has "flatten_leaf" enabled, only the last dot-segment is used as field name.
// Otherwise, dot notation is used (e.g. "data.field.name").
func (h *IngestHandler) flattenJSON(obj map[string]interface{}, prefix string, fields map[string]string, norm *normalizers.CompiledNormalizer) {
	h.flattenJSONDepth(obj, prefix, fields, norm, 0)
}

func (h *IngestHandler) flattenJSONDepth(obj map[string]interface{}, prefix string, fields map[string]string, norm *normalizers.CompiledNormalizer, depth int) {
	if depth >= maxFlattenDepth {
		if _, set := fields["_bifract_truncated"]; !set {
			fields["_bifract_truncated"] = "true"
			fields["_bifract_truncation_reason"] = "max_depth"
		}
		return
	}
	hasFlatten := norm != nil && norm.HasFlatten

	for key, value := range obj {
		if len(fields) >= maxFlattenFields {
			fields["_bifract_truncated"] = "true"
			fields["_bifract_truncation_reason"] = "max_fields"
			return
		}
		fieldName := key
		if prefix != "" {
			fieldName = prefix + "." + key
		}

		switch v := value.(type) {
		case map[string]interface{}:
			h.flattenJSONDepth(v, fieldName, fields, norm, depth+1)
		default:
			outKey := fieldName
			if hasFlatten {
				outKey = lastSegment(fieldName)
			}
			normalized := normalizeField(outKey, norm)
			// On collision, fall back to the full dot-notation path
			if _, exists := fields[normalized]; exists && hasFlatten {
				normalized = normalizeField(fieldName, norm)
			}
			fields[normalized] = stringifyValue(v)
		}
	}
}

// stringifyValue converts an arbitrary JSON value to its string representation.
func stringifyValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case float64:
		return fmt.Sprintf("%v", val)
	case bool:
		return fmt.Sprintf("%v", val)
	case nil:
		return ""
	default:
		b, _ := json.Marshal(val)
		return string(b)
	}
}

// lastSegment returns the part after the last dot, or the whole string if no dot.
func lastSegment(s string) string {
	if idx := strings.LastIndex(s, "."); idx >= 0 {
		return s[idx+1:]
	}
	return s
}

// extractTimestamp tries per-token fields, then normalizer fields, then global settings, then common field names.
func (h *IngestHandler) extractTimestamp(fields map[string]string, tsFields []ingesttokens.TsField, norm *normalizers.CompiledNormalizer) time.Time {
	// Try per-token configured timestamp fields first
	for _, tsField := range tsFields {
		if val, ok := fields[tsField.Field]; ok && val != "" {
			if ts := h.parseTimestampWithFormat(val, tsField.Format); !ts.IsZero() {
				return ts
			}
		}
	}

	// Try normalizer's timestamp fields
	if len(tsFields) == 0 && norm != nil && len(norm.TimestampFields) > 0 {
		for _, tsField := range norm.TimestampFields {
			if val, ok := fields[tsField.Field]; ok && val != "" {
				if ts := h.parseTimestampWithFormat(val, tsField.Format); !ts.IsZero() {
					return ts
				}
			}
		}
	}

	// Fall back to global settings if neither token nor normalizer had fields
	if len(tsFields) == 0 && (norm == nil || len(norm.TimestampFields) == 0) {
		globalTsFields := settings.Get().TimestampFields
		for _, tsField := range globalTsFields {
			if val, ok := fields[tsField.Field]; ok && val != "" {
				if ts := h.parseTimestampWithFormat(val, tsField.Format); !ts.IsZero() {
					return ts
				}
			}
		}
	}

	// Last resort: try common field names with auto-detection
	fallbackFields := []string{"timestamp", "@timestamp", "time", "ts", "_time"}
	for _, field := range fallbackFields {
		if val, ok := fields[field]; ok && val != "" {
			if ts := h.parseTimestamp(val); !ts.IsZero() {
				return ts
			}
		}
	}

	return time.Time{}
}

func (h *IngestHandler) parseTimestampWithFormat(val interface{}, format string) time.Time {
	switch v := val.(type) {
	case string:
		switch format {
		case "unix":
			var seconds int64
			if _, err := fmt.Sscanf(v, "%d", &seconds); err == nil {
				return time.Unix(seconds, 0)
			}
		case "unixmilli", "unixmillis", "unixms":
			var millis int64
			if _, err := fmt.Sscanf(v, "%d", &millis); err == nil {
				return time.Unix(0, millis*int64(time.Millisecond))
			}
		case "unixmicro", "unixmicros", "unixμs":
			var micros int64
			if _, err := fmt.Sscanf(v, "%d", &micros); err == nil {
				return time.Unix(0, micros*int64(time.Microsecond))
			}
		case "unixnano", "unixnanos", "unixns":
			var nanos int64
			if _, err := fmt.Sscanf(v, "%d", &nanos); err == nil {
				return time.Unix(0, nanos)
			}
		default:
			if t, err := time.Parse(format, v); err == nil {
				return t
			}
		}

	case float64:
		switch format {
		case "unix":
			return time.Unix(int64(v), 0)
		case "unixmilli", "unixmillis", "unixms":
			return time.Unix(0, int64(v)*int64(time.Millisecond))
		case "unixmicro", "unixmicros", "unixμs":
			return time.Unix(0, int64(v)*int64(time.Microsecond))
		case "unixnano", "unixnanos", "unixns":
			return time.Unix(0, int64(v))
		default:
			if v > 1e12 {
				return time.Unix(0, int64(v)*int64(time.Millisecond))
			}
			return time.Unix(int64(v), 0)
		}

	case int64:
		switch format {
		case "unix":
			return time.Unix(v, 0)
		case "unixmilli", "unixmillis", "unixms":
			return time.Unix(0, v*int64(time.Millisecond))
		case "unixmicro", "unixmicros", "unixμs":
			return time.Unix(0, v*int64(time.Microsecond))
		case "unixnano", "unixnanos", "unixns":
			return time.Unix(0, v)
		default:
			if v > 1e12 {
				return time.Unix(0, v*int64(time.Millisecond))
			}
			return time.Unix(v, 0)
		}
	}

	return time.Time{}
}

func (h *IngestHandler) parseTimestamp(val interface{}) time.Time {
	switch v := val.(type) {
	case string:
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02T15:04:05.999999999Z07:00",
			"2006-01-02T15:04:05.000Z07:00",
			"2006-01-02T15:04:05.000Z",
			"2006-01-02T15:04:05Z",
			"2006-01-02 15:04:05",
			"2006-01-02 15:04:05.000",
		}

		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t
			}
		}

	case float64:
		if v > 1e15 {
			return time.Unix(0, int64(v)*int64(time.Microsecond))
		} else if v > 1e12 {
			return time.Unix(0, int64(v)*int64(time.Millisecond))
		}
		return time.Unix(int64(v), 0)

	case int64:
		if v > 1e15 {
			return time.Unix(0, v*int64(time.Microsecond))
		} else if v > 1e12 {
			return time.Unix(0, v*int64(time.Millisecond))
		}
		return time.Unix(v, 0)
	}

	return time.Time{}
}

func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
