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
	"time"

	"bifract/pkg/ingesttokens"
	"bifract/pkg/normalizers"
	"bifract/pkg/storage"
)

// ElasticBulkHandler handles Elasticsearch bulk API format.
// Used by Velociraptor's elastic_upload plugin.
type ElasticBulkHandler struct {
	handler *IngestHandler
}

type ElasticBulkResponse struct {
	Took   int               `json:"took"`
	Errors bool              `json:"errors"`
	Items  []ElasticBulkItem `json:"items"`
}

type ElasticBulkItem struct {
	Index  ElasticIndexResult `json:"index,omitempty"`
	Create ElasticIndexResult `json:"create,omitempty"`
}

type ElasticIndexResult struct {
	Index  string `json:"_index"`
	ID     string `json:"_id,omitempty"`
	Status int    `json:"status"`
	Result string `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

// NewElasticBulkHandler creates a bulk handler that shares the same
// IngestHandler (and therefore the same queue) as the standard endpoint.
func NewElasticBulkHandler(handler *IngestHandler) *ElasticBulkHandler {
	return &ElasticBulkHandler{handler: handler}
}

func (h *ElasticBulkHandler) HandleBulk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	startTime := time.Now()

	// Validate ingest token (always required)
	tokenData, err := h.handler.validateIngestToken(r)
	if err != nil {
		respondElasticError(w, http.StatusUnauthorized, err.Error())
		return
	}

	// Early rejection: if the system is under pressure, respond 429
	// before reading/parsing the body to save CPU.
	if !h.handler.queue.Healthy() {
		w.Header().Set("Retry-After", "2")
		respondElasticError(w, http.StatusTooManyRequests, "Ingestion backend under pressure")
		return
	}

	// Enforce body size limit
	bodyReader := io.Reader(r.Body)
	if h.handler.maxBodySize > 0 {
		bodyReader = http.MaxBytesReader(w, r.Body, h.handler.maxBodySize)
	}

	body, err := io.ReadAll(bodyReader)
	if err != nil {
		if err.Error() == "http: request body too large" {
			respondElasticError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("Request body exceeds %d byte limit", h.handler.maxBodySize))
			return
		}
		respondElasticError(w, http.StatusBadRequest, "Failed to read request body")
		return
	}
	defer r.Body.Close()

	logs, items, err := h.parseBulkRequest(body, tokenData.Normalizer, tokenData.TimestampFields)
	if err != nil {
		respondElasticError(w, http.StatusBadRequest, "Failed to parse bulk request")
		return
	}

	if len(logs) == 0 {
		respondJSON(w, http.StatusOK, ElasticBulkResponse{
			Took:   int(time.Since(startTime).Milliseconds()),
			Errors: false,
			Items:  items,
		})
		return
	}

	// Assign fractal from token to all logs
	for i := range logs {
		logs[i].FractalID = tokenData.FractalID
	}

	// Push onto the shared ingestion queue
	if !h.handler.queue.Enqueue(logs) {
		for i := range items {
			if items[i].Index.Status == 201 {
				items[i].Index.Status = 429
				items[i].Index.Result = "too_many_requests"
				items[i].Index.Error = "Ingestion queue full"
			}
			if items[i].Create.Status == 201 {
				items[i].Create.Status = 429
				items[i].Create.Result = "too_many_requests"
				items[i].Create.Error = "Ingestion queue full"
			}
		}
		respondJSON(w, http.StatusTooManyRequests, ElasticBulkResponse{
			Took:   int(time.Since(startTime).Milliseconds()),
			Errors: true,
			Items:  items,
		})
		return
	}

	// Update token usage stats asynchronously (detached context so it survives handler return)
	go func(tokenID string, count int) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.handler.tokenStorage.UpdateUsageStats(ctx, tokenID, count); err != nil {
			log.Printf("[Ingest] failed to update token usage: %v", err)
		}
	}(tokenData.TokenID, len(logs))

	respondJSON(w, http.StatusOK, ElasticBulkResponse{
		Took:   int(time.Since(startTime).Milliseconds()),
		Errors: false,
		Items:  items,
	})
}

func (h *ElasticBulkHandler) parseBulkRequest(data []byte, norm *normalizers.CompiledNormalizer, tsFields []ingesttokens.TsField) ([]storage.LogEntry, []ElasticBulkItem, error) {
	var logs []storage.LogEntry
	var items []ElasticBulkItem

	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, bufio.MaxScanTokenSize), 10*1024*1024) // 10MB max line
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		actionLine := scanner.Text()
		if actionLine == "" {
			continue
		}

		var action map[string]interface{}
		if err := json.Unmarshal([]byte(actionLine), &action); err != nil {
			continue
		}

		actionType := ""
		var actionMeta map[string]interface{}
		if meta, ok := action["index"].(map[string]interface{}); ok {
			actionType = "index"
			actionMeta = meta
		} else if meta, ok := action["create"].(map[string]interface{}); ok {
			actionType = "create"
			actionMeta = meta
		} else if _, ok := action["delete"]; ok {
			continue
		} else if _, ok := action["update"]; ok {
			continue
		} else {
			continue
		}

		if !scanner.Scan() {
			break
		}
		lineNum++
		docLine := scanner.Text()
		if docLine == "" {
			continue
		}

		var doc map[string]interface{}
		if err := json.Unmarshal([]byte(docLine), &doc); err != nil {
			item := ElasticBulkItem{}
			if actionType == "index" {
				item.Index = ElasticIndexResult{
					Index:  getStringOrDefault(actionMeta, "_index", "bifract"),
					Status: 400,
					Result: "error",
					Error:  "Failed to parse document",
				}
			} else {
				item.Create = ElasticIndexResult{
					Index:  getStringOrDefault(actionMeta, "_index", "bifract"),
					Status: 400,
					Result: "error",
					Error:  "Failed to parse document",
				}
			}
			items = append(items, item)
			continue
		}

		logEntry, err := h.handler.parseLogObjectWithConfig(doc, norm, tsFields)
		if err != nil {
			item := ElasticBulkItem{}
			if actionType == "index" {
				item.Index = ElasticIndexResult{
					Index:  getStringOrDefault(actionMeta, "_index", "bifract"),
					Status: 400,
					Result: "error",
					Error:  "Failed to parse log entry",
				}
			} else {
				item.Create = ElasticIndexResult{
					Index:  getStringOrDefault(actionMeta, "_index", "bifract"),
					Status: 400,
					Result: "error",
					Error:  "Failed to parse log entry",
				}
			}
			items = append(items, item)
			continue
		}

		logs = append(logs, logEntry)

		item := ElasticBulkItem{}
		if actionType == "index" {
			item.Index = ElasticIndexResult{
				Index:  getStringOrDefault(actionMeta, "_index", "bifract"),
				ID:     getStringOrDefault(actionMeta, "_id", ""),
				Status: 201,
				Result: "created",
			}
		} else {
			item.Create = ElasticIndexResult{
				Index:  getStringOrDefault(actionMeta, "_index", "bifract"),
				ID:     getStringOrDefault(actionMeta, "_id", ""),
				Status: 201,
				Result: "created",
			}
		}
		items = append(items, item)
	}

	return logs, items, nil
}

func getStringOrDefault(m map[string]interface{}, key string, defaultVal string) string {
	if val, ok := m[key].(string); ok {
		return val
	}
	return defaultVal
}

func respondElasticError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]interface{}{
			"type":   "parse_exception",
			"reason": message,
		},
		"status": status,
	})
}
