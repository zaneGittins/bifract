package ingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"bifract/pkg/ingesttokens"
	"bifract/pkg/normalizers"
	"bifract/pkg/storage"
	"bifract/pkg/watermark"
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

// ElasticBulkItem is one entry of a bulk response. Exactly one action key is set,
// the action of the line it answers: clients pair items with actions by position
// and read the result under whichever key is present.
type ElasticBulkItem struct {
	Index  ElasticIndexResult `json:"index,omitzero"`
	Create ElasticIndexResult `json:"create,omitzero"`
	Delete ElasticIndexResult `json:"delete,omitzero"`
	Update ElasticIndexResult `json:"update,omitzero"`
}

type ElasticIndexResult struct {
	Index  string            `json:"_index"`
	ID     string            `json:"_id,omitempty"`
	Status int               `json:"status"`
	Result string            `json:"result,omitempty"`
	Error  *ElasticItemError `json:"error,omitempty"`
}

// ElasticItemError is the per-item error object Elasticsearch clients decode.
type ElasticItemError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

// newBulkItem returns an item holding res under action's key.
func newBulkItem(action string, res ElasticIndexResult) ElasticBulkItem {
	var item ElasticBulkItem
	*item.result(action) = res
	return item
}

func (it *ElasticBulkItem) result(action string) *ElasticIndexResult {
	switch action {
	case "create":
		return &it.Create
	case "delete":
		return &it.Delete
	case "update":
		return &it.Update
	}
	return &it.Index
}

// results returns every action slot of the item; only one is ever set.
func (it *ElasticBulkItem) results() []*ElasticIndexResult {
	return []*ElasticIndexResult{&it.Index, &it.Create, &it.Delete, &it.Update}
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
		status, msg := tokenErrorStatus(w, err)
		if status == http.StatusUnauthorized {
			msg = err.Error()
		}
		respondElasticError(w, status, msg)
		return
	}

	// Early rejection: if the system is under pressure, respond 429
	// before reading/parsing the body to save CPU.
	if !h.handler.queue.Healthy() {
		w.Header().Set("Retry-After", "2")
		respondElasticError(w, http.StatusTooManyRequests, "Ingestion backend under pressure")
		return
	}

	defer r.Body.Close()

	body, err := readRequestBody(w, r, h.handler.maxBodySize)
	if err != nil {
		if errors.Is(err, errBodyTooLarge) {
			respondElasticError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("Request body exceeds %d byte limit", h.handler.maxBodySize))
			return
		}
		respondElasticError(w, http.StatusBadRequest, "Failed to read request body")
		return
	}

	// Rows are stamped from here on, so hold the watermark until they are queued.
	stampHold := watermark.Local.Hold()
	defer watermark.Local.Done(stampHold)

	logs, items, err := h.parseBulkRequest(body, tokenData.Normalizer, tokenData.TimestampFields)
	if errors.Is(err, errLineTooLong) {
		respondElasticError(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	}
	if err != nil {
		respondElasticError(w, http.StatusBadRequest, "Failed to parse bulk request")
		return
	}

	if len(logs) == 0 {
		// Every document may have failed to parse: errors must say so, since bulk
		// clients only inspect items when it is set.
		respondJSON(w, http.StatusOK, ElasticBulkResponse{
			Took:   int(time.Since(startTime).Milliseconds()),
			Errors: bulkItemsHaveErrors(items),
			Items:  items,
		})
		return
	}

	switch err := h.handler.submit(w, tokenData, logs); {
	case errors.Is(err, errQuotaExceeded):
		respondBulkRejected(w, startTime, items, "Fractal disk quota exceeded")
		return
	case err != nil:
		respondBulkRejected(w, startTime, items, "Ingestion queue full")
		return
	}

	respondJSON(w, http.StatusOK, ElasticBulkResponse{
		Took:   int(time.Since(startTime).Milliseconds()),
		Errors: bulkItemsHaveErrors(items),
		Items:  items,
	})
}

func bulkItemsHaveErrors(items []ElasticBulkItem) bool {
	for i := range items {
		for _, res := range items[i].results() {
			if res.Status >= 400 {
				return true
			}
		}
	}
	return false
}

// respondBulkRejected turns every accepted item into a retryable 429, so a bulk
// client retries exactly the documents that were not ingested.
func respondBulkRejected(w http.ResponseWriter, startTime time.Time, items []ElasticBulkItem, reason string) {
	for i := range items {
		for _, res := range items[i].results() {
			if res.Status == 201 {
				res.Status = 429
				res.Result = ""
				res.Error = &ElasticItemError{Type: "es_rejected_execution_exception", Reason: reason}
			}
		}
	}
	respondJSON(w, http.StatusTooManyRequests, ElasticBulkResponse{
		Took:   int(time.Since(startTime).Milliseconds()),
		Errors: true,
		Items:  items,
	})
}

// parseBulkRequest reads action/document line pairs. Every recognized action line
// (index, create, delete, update) yields exactly one item, in order, because bulk
// clients match items to actions by position: a missing item shifts every later
// result onto the wrong document. Lines that are not an action at all are skipped.
func (h *ElasticBulkHandler) parseBulkRequest(data []byte, norm *normalizers.CompiledNormalizer, tsFields []ingesttokens.TsField) ([]storage.LogEntry, []ElasticBulkItem, error) {
	var logs []storage.LogEntry
	var items []ElasticBulkItem

	scanner := newLineScanner(data)

	for scanner.Scan() {
		actionLine := scanner.Text()
		if strings.TrimSpace(actionLine) == "" {
			continue
		}

		actionType, actionMeta, ok := parseBulkAction(actionLine)
		if !ok {
			continue
		}
		base := ElasticIndexResult{
			Index: getStringOrDefault(actionMeta, "_index", "bifract"),
			ID:    getStringOrDefault(actionMeta, "_id", ""),
		}
		fail := func(errType, reason string) {
			res := base
			res.Status = http.StatusBadRequest
			res.Error = &ElasticItemError{Type: errType, Reason: reason}
			items = append(items, newBulkItem(actionType, res))
		}

		switch actionType {
		case "delete", "update":
			// update carries a body line (partial doc or script); delete does not.
			if actionType == "update" {
				scanner.Scan()
			}
			fail("illegal_argument_exception", "bulk action ["+actionType+"] is not supported: ingest is append-only")
			continue
		}

		if !scanner.Scan() || strings.TrimSpace(scanner.Text()) == "" {
			fail("action_request_validation_exception", "Validation Failed: 1: source is missing;")
			continue
		}
		docLine := scanner.Text()

		var doc map[string]interface{}
		if err := normalizers.DecodeJSON([]byte(docLine), &doc); err != nil {
			fail("mapper_parsing_exception", "Failed to parse document")
			continue
		}

		logEntry, err := BuildLogEntry(doc, norm, tsFields)
		if err != nil {
			fail("mapper_parsing_exception", "Failed to parse log entry")
			continue
		}

		logs = append(logs, logEntry)

		res := base
		res.Status = http.StatusCreated
		res.Result = "created"
		items = append(items, newBulkItem(actionType, res))
	}
	if err := lineScanErr(scanner); err != nil {
		return nil, nil, err
	}

	return logs, items, nil
}

// parseBulkAction reads an action line: an object keyed by index, create, delete
// or update whose value is the metadata object. Anything else is not an action.
func parseBulkAction(line string) (string, map[string]interface{}, bool) {
	var action map[string]interface{}
	if err := json.Unmarshal([]byte(line), &action); err != nil {
		return "", nil, false
	}
	for _, t := range []string{"index", "create", "delete", "update"} {
		if v, present := action[t]; present {
			meta, isObj := v.(map[string]interface{})
			return t, meta, isObj
		}
	}
	return "", nil, false
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
