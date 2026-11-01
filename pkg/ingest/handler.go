package ingest

import (
	"bifract/pkg/api"
	"context"
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
		status, msg := tokenErrorStatus(w, err)
		respondJSON(w, status, IngestResponse{Success: false, Error: msg})
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
	if errors.Is(err, errLineTooLong) {
		respondJSON(w, http.StatusRequestEntityTooLarge, IngestResponse{
			Success: false,
			Error:   "Request rejected: " + err.Error(),
		})
		return
	}
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

	switch err := h.submit(w, tokenData, logs); {
	case errors.Is(err, errQuotaExceeded):
		respondJSON(w, http.StatusTooManyRequests, IngestResponse{
			Success: false,
			Error:   "Fractal disk quota exceeded. Clear old logs or increase quota.",
		})
		return
	case err != nil:
		respondJSON(w, http.StatusTooManyRequests, IngestResponse{
			Success: false,
			Error:   "Ingestion queue full. Retry after backoff.",
		})
		return
	}

	respondJSON(w, http.StatusOK, IngestResponse{
		Success: true,
		Count:   len(logs),
		Message: fmt.Sprintf("Accepted %d log(s) for ingestion", len(logs)),
	})
}

var (
	errQuotaExceeded = errors.New("fractal disk quota exceeded")
	errQueueFull     = errors.New("ingestion queue full")
)

// submit routes logs to the token's fractal, checks the fractal's disk quota and
// queues them, then records token usage. A rejection is errQuotaExceeded or
// errQueueFull, both retryable 429s, with Retry-After already set on w.
func (h *IngestHandler) submit(w http.ResponseWriter, token *ingesttokens.ValidatedToken, logs []storage.LogEntry) error {
	for i := range logs {
		logs[i].FractalID = token.FractalID
	}

	// Reject action only; rollover is handled post-insert.
	if h.quotaManager != nil {
		var batchBytes int64
		for i := range logs {
			batchBytes += int64(len(logs[i].RawLog))
		}
		if !h.quotaManager.CheckQuota(token.FractalID, batchBytes) {
			w.Header().Set("Retry-After", "30")
			return errQuotaExceeded
		}
	}

	// Enqueue fails when the queue is full, workers are unhealthy, or ClickHouse
	// CPU backpressure is active.
	if !h.queue.Enqueue(logs) {
		w.Header().Set("Retry-After", "2")
		return errQueueFull
	}

	// Detached context: r.Context() is cancelled once the handler returns.
	go func(tokenID string, count int) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.tokenStorage.UpdateUsageStats(ctx, tokenID, count); err != nil {
			log.Printf("[Ingest] failed to update token usage: %v", err)
		}
	}(token.TokenID, len(logs))
	return nil
}

// tokenErrorStatus maps a validateIngestToken error to a response. A token that
// could not be checked is a 503, never a 401: shippers drop a batch on 401 but retry a 503.
func tokenErrorStatus(w http.ResponseWriter, err error) (int, string) {
	if errors.Is(err, ingesttokens.ErrTokenRejected) {
		return http.StatusUnauthorized, "Invalid or missing ingest token"
	}
	log.Printf("Warning: ingest token check failed: %v", err)
	w.Header().Set("Retry-After", "2")
	return http.StatusServiceUnavailable, "Ingest token could not be checked"
}

// validateIngestToken extracts and validates a bearer token from the request.
// Accepts "Bearer <token>", plus "APIKey <token>" for Elasticsearch clients (e.g.
// Velociraptor's go-elasticsearch) and "Splunk <token>" for HEC clients.
func (h *IngestHandler) validateIngestToken(r *http.Request) (*ingesttokens.ValidatedToken, error) {
	return h.lookupIngestToken(r.Context(), ingestTokenFromHeader(r))
}

// ingestTokenFromHeader returns the token of a recognised Authorization scheme, or "".
func ingestTokenFromHeader(r *http.Request) string {
	authHeader := r.Header.Get("Authorization")
	upperAuth := strings.ToUpper(authHeader)
	for _, scheme := range []string{"BEARER ", "APIKEY ", "SPLUNK "} {
		if strings.HasPrefix(upperAuth, scheme) {
			return authHeader[len(scheme):]
		}
	}
	return ""
}

// lookupIngestToken validates a raw token, through the cache when there is one.
func (h *IngestHandler) lookupIngestToken(ctx context.Context, rawToken string) (*ingesttokens.ValidatedToken, error) {
	if rawToken == "" {
		return nil, fmt.Errorf("%w: set Authorization: Bearer <token>, ApiKey <token> or Splunk <token>", ingesttokens.ErrTokenRejected)
	}

	tokenHash := ingesttokens.HashToken(rawToken)
	var gen uint64
	if h.tokenCache != nil {
		if cached, ok := h.tokenCache.Get(tokenHash); ok {
			return cached, nil
		}
		gen = h.tokenCache.Generation()
	}

	validated, err := h.tokenStorage.ValidateToken(ctx, rawToken)
	if err != nil {
		return nil, err
	}
	if h.tokenCache != nil {
		h.tokenCache.Set(tokenHash, validated, gen)
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

// parseJSONLogsWithConfig decodes numbers as json.Number (normalizers.DecodeJSON),
// so 64-bit IDs keep their exact digits in the fields and the raw log.
func (h *IngestHandler) parseJSONLogsWithConfig(data []byte, norm *normalizers.CompiledNormalizer, tsFields []ingesttokens.TsField) ([]storage.LogEntry, error) {
	var logs []storage.LogEntry

	// Try to parse as JSON array first
	var jsonArray []map[string]interface{}
	if err := normalizers.DecodeJSON(data, &jsonArray); err == nil {
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
	if err := normalizers.DecodeJSON(data, &jsonObj); err == nil {
		log, err := BuildLogEntry(jsonObj, norm, tsFields)
		if err != nil {
			return nil, err
		}
		logs = append(logs, log)
		return logs, nil
	}

	// Try to parse as newline-delimited JSON (NDJSON)
	scanner := newLineScanner(data)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var obj map[string]interface{}
		if err := normalizers.DecodeJSON([]byte(line), &obj); err != nil {
			continue
		}

		log, err := BuildLogEntry(obj, norm, tsFields)
		if err != nil {
			continue
		}
		logs = append(logs, log)
	}
	if err := lineScanErr(scanner); err != nil {
		return nil, err
	}

	if len(logs) > 0 {
		return logs, nil
	}

	return nil, fmt.Errorf("unable to parse logs: not valid JSON array, object, or NDJSON")
}

// parseKVLogs parses key=value formatted logs.
func (h *IngestHandler) parseKVLogs(data []byte, token *ingesttokens.ValidatedToken) ([]storage.LogEntry, error) {
	var logs []storage.LogEntry

	scanner := newLineScanner(data)
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
	if err := lineScanErr(scanner); err != nil {
		return nil, err
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
