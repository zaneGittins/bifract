package ingest

import (
	"bifract/pkg/api"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"bifract/pkg/ingesttokens"
	"bifract/pkg/normalizers"
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

	defer r.Body.Close()

	body, err := readRequestBody(w, r, h.maxBodySize)
	if err != nil {
		if errors.Is(err, errBodyTooLarge) {
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
			log, err := BuildLogEntry(obj, norm, tsFields)
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
		log, err := BuildLogEntry(jsonObj, norm, tsFields)
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

		log, err := BuildLogEntry(obj, norm, tsFields)
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

		h.parseKVLine(line, entry.Fields)

		// Apply normalizer transforms
		if token.Normalizer != nil {
			entry.Fields = token.Normalizer.ApplyTransforms(entry.Fields)
		}
		entry.Normalizer = token.Normalizer.Stamp()

		ingestTime := time.Now()
		entry.Timestamp = ExtractTimestamp(entry.Fields, token.TimestampFields, token.Normalizer)
		if entry.Timestamp.IsZero() {
			entry.Timestamp = ingestTime
		}

		entry.IngestTimestamp = ingestTime
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
// Field names are stored as-is; normalization is applied separately.
func (h *IngestHandler) parseKVLine(line string, fields map[string]string) {
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
			fields[key] = ""
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

		fields[key] = value
	}
}

func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	api.WriteJSON(w, status, data)
}
