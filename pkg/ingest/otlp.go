package ingest

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"bifract/pkg/ingesttokens"
	"bifract/pkg/normalizers"
	"bifract/pkg/storage"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// OTLPHandler handles OpenTelemetry Protocol (OTLP) log ingestion over HTTP.
// Supports both protobuf and JSON content types per the OTLP/HTTP specification.
type OTLPHandler struct {
	handler *IngestHandler
}

// NewOTLPHandler creates an OTLP handler that shares the same IngestHandler
// (and therefore the same queue) as the standard and Elasticsearch endpoints.
func NewOTLPHandler(handler *IngestHandler) *OTLPHandler {
	return &OTLPHandler{handler: handler}
}

// HandleLogs processes OTLP ExportLogsServiceRequest payloads.
// Accepts application/x-protobuf, application/protobuf, and application/json.
func (h *OTLPHandler) HandleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Validate ingest token (always required)
	tokenData, err := h.handler.validateIngestToken(r)
	if err != nil {
		h.respondError(w, r, http.StatusUnauthorized, "Invalid or missing ingest token")
		return
	}

	// Early rejection under backpressure
	if !h.handler.queue.Healthy() {
		w.Header().Set("Retry-After", "2")
		h.respondError(w, r, http.StatusTooManyRequests, "Ingestion backend under pressure")
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
			h.respondError(w, r, http.StatusRequestEntityTooLarge,
				fmt.Sprintf("Request body exceeds %d byte limit", h.handler.maxBodySize))
			return
		}
		h.respondError(w, r, http.StatusBadRequest, "Failed to read request body")
		return
	}
	defer r.Body.Close()

	// Parse the OTLP request based on content type
	req := &collectorlogs.ExportLogsServiceRequest{}
	contentType := r.Header.Get("Content-Type")

	switch {
	case strings.Contains(contentType, "application/json"):
		if err := protojson.Unmarshal(body, req); err != nil {
			h.respondError(w, r, http.StatusBadRequest, "Failed to parse OTLP JSON request")
			return
		}
	case strings.Contains(contentType, "application/x-protobuf"),
		strings.Contains(contentType, "application/protobuf"):
		if err := proto.Unmarshal(body, req); err != nil {
			h.respondError(w, r, http.StatusBadRequest, "Failed to parse OTLP protobuf request")
			return
		}
	default:
		// Try protobuf first, fall back to JSON
		if err := proto.Unmarshal(body, req); err != nil {
			if err2 := protojson.Unmarshal(body, req); err2 != nil {
				h.respondError(w, r, http.StatusBadRequest, "Failed to parse OTLP request: set Content-Type to application/x-protobuf or application/json")
				return
			}
		}
	}

	// Convert OTLP log records to LogEntry
	logs := h.parseOTLPRequest(req, tokenData.Normalizer, tokenData.TimestampFields)

	if len(logs) == 0 {
		h.respondSuccess(w, r, 0)
		return
	}

	// Assign fractal from token
	for i := range logs {
		logs[i].FractalID = tokenData.FractalID
	}

	// Per-fractal disk quota check
	if h.handler.quotaManager != nil {
		var batchBytes int64
		for i := range logs {
			batchBytes += int64(len(logs[i].RawLog))
		}
		if !h.handler.quotaManager.CheckQuota(tokenData.FractalID, batchBytes) {
			w.Header().Set("Retry-After", "30")
			h.respondError(w, r, http.StatusTooManyRequests, "Fractal disk quota exceeded")
			return
		}
	}

	// Push onto the shared ingestion queue
	if !h.handler.queue.Enqueue(logs) {
		w.Header().Set("Retry-After", "2")
		h.respondError(w, r, http.StatusTooManyRequests, "Ingestion queue full")
		return
	}

	// Update token usage stats asynchronously
	go func(tokenID string, count int) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.handler.tokenStorage.UpdateUsageStats(ctx, tokenID, count); err != nil {
			log.Printf("[Ingest] failed to update token usage: %v", err)
		}
	}(tokenData.TokenID, len(logs))

	h.respondSuccess(w, r, len(logs))
}

// parseOTLPRequest converts an OTLP ExportLogsServiceRequest into LogEntry slices.
func (h *OTLPHandler) parseOTLPRequest(req *collectorlogs.ExportLogsServiceRequest, norm *normalizers.CompiledNormalizer, tsFields []ingesttokens.TsField) []storage.LogEntry {
	var logs []storage.LogEntry

	for _, rl := range req.GetResourceLogs() {
		resourceAttrs := kvListToMapRaw(rl.GetResource().GetAttributes(), "resource.")

		for _, sl := range rl.GetScopeLogs() {
			scopeAttrs := buildScopeAttrsRaw(sl.GetScope())

			for _, lr := range sl.GetLogRecords() {
				entry := h.convertLogRecord(lr, rl.GetResource(), sl.GetScope(), resourceAttrs, scopeAttrs, norm, tsFields)
				logs = append(logs, entry)
			}
		}
	}

	return logs
}

// convertLogRecord maps a single OTLP LogRecord to a storage.LogEntry.
func (h *OTLPHandler) convertLogRecord(
	lr *logspb.LogRecord,
	resource *resourcepb.Resource,
	scope *commonpb.InstrumentationScope,
	resourceAttrs map[string]string,
	scopeAttrs map[string]string,
	norm *normalizers.CompiledNormalizer,
	tsFields []ingesttokens.TsField,
) storage.LogEntry {
	entry := storage.LogEntry{
		Fields: make(map[string]string),
	}

	ingestTime := time.Now()

	// Timestamp: prefer TimeUnixNano, then ObservedTimeUnixNano, then ingest time
	if lr.GetTimeUnixNano() > 0 {
		entry.Timestamp = time.Unix(0, int64(lr.GetTimeUnixNano()))
	} else if lr.GetObservedTimeUnixNano() > 0 {
		entry.Timestamp = time.Unix(0, int64(lr.GetObservedTimeUnixNano()))
	} else {
		entry.Timestamp = ingestTime
	}

	// Populate fields from log record attributes (raw keys, no normalization)
	logAttrs := kvListToMapRaw(lr.GetAttributes(), "")
	for k, v := range logAttrs {
		entry.Fields[k] = v
	}

	// Merge resource attributes (prefixed with "resource.")
	for k, v := range resourceAttrs {
		entry.Fields[k] = v
	}

	// Merge scope attributes (prefixed with "scope.")
	for k, v := range scopeAttrs {
		entry.Fields[k] = v
	}

	// Body -> "message" field
	if lr.GetBody() != nil {
		entry.Fields["message"] = anyValueToString(lr.GetBody())
	}

	// Severity
	if lr.GetSeverityText() != "" {
		entry.Fields["severity_text"] = lr.GetSeverityText()
	}
	if lr.GetSeverityNumber() != logspb.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED {
		entry.Fields["severity_number"] = fmt.Sprintf("%d", int32(lr.GetSeverityNumber()))
	}

	// Trace context
	if len(lr.GetTraceId()) > 0 {
		entry.Fields["trace_id"] = hex.EncodeToString(lr.GetTraceId())
	}
	if len(lr.GetSpanId()) > 0 {
		entry.Fields["span_id"] = hex.EncodeToString(lr.GetSpanId())
	}
	if lr.GetFlags() != 0 {
		entry.Fields["flags"] = fmt.Sprintf("%d", lr.GetFlags())
	}

	// Observed timestamp as a field
	if lr.GetObservedTimeUnixNano() > 0 {
		observed := time.Unix(0, int64(lr.GetObservedTimeUnixNano()))
		entry.Fields["observed_timestamp"] = observed.Format(time.RFC3339Nano)
	}

	// Build RawLog: serialize the full OTLP context as JSON for the raw log viewer
	entry.RawLog = h.buildRawLog(lr, resource, scope)

	// Apply normalizer transforms (flatten, snake_case, lowercase, etc.)
	if norm != nil {
		entry.Fields = norm.ApplyTransforms(entry.Fields)
	}

	// If timestamp was not set from OTLP native fields, try the extraction pipeline
	if lr.GetTimeUnixNano() == 0 && lr.GetObservedTimeUnixNano() == 0 {
		extracted := h.handler.extractTimestamp(entry.Fields, tsFields, norm)
		if !extracted.IsZero() {
			entry.Timestamp = extracted
		}
	}

	entry.IngestTimestamp = ingestTime
	entry.Fields["ingesttimestamp"] = ingestTime.Format(time.RFC3339Nano)
	entry.LogID = storage.GenerateLogID(entry.Timestamp, entry.RawLog)

	return entry
}

// buildRawLog serializes the full OTLP log record with resource and scope context as JSON.
func (h *OTLPHandler) buildRawLog(lr *logspb.LogRecord, resource *resourcepb.Resource, scope *commonpb.InstrumentationScope) string {
	raw := make(map[string]interface{})

	// Body
	if lr.GetBody() != nil {
		raw["body"] = anyValueToInterface(lr.GetBody())
	}

	// Severity
	if lr.GetSeverityText() != "" {
		raw["severity_text"] = lr.GetSeverityText()
	}
	if lr.GetSeverityNumber() != logspb.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED {
		raw["severity_number"] = int32(lr.GetSeverityNumber())
	}

	// Timestamp
	if lr.GetTimeUnixNano() > 0 {
		raw["timestamp"] = time.Unix(0, int64(lr.GetTimeUnixNano())).Format(time.RFC3339Nano)
	}
	if lr.GetObservedTimeUnixNano() > 0 {
		raw["observed_timestamp"] = time.Unix(0, int64(lr.GetObservedTimeUnixNano())).Format(time.RFC3339Nano)
	}

	// Log attributes
	if attrs := lr.GetAttributes(); len(attrs) > 0 {
		raw["attributes"] = kvListToInterface(attrs)
	}

	// Trace context
	if len(lr.GetTraceId()) > 0 {
		raw["trace_id"] = hex.EncodeToString(lr.GetTraceId())
	}
	if len(lr.GetSpanId()) > 0 {
		raw["span_id"] = hex.EncodeToString(lr.GetSpanId())
	}

	// Resource
	if resource != nil && len(resource.GetAttributes()) > 0 {
		raw["resource"] = kvListToInterface(resource.GetAttributes())
	}

	// Scope
	if scope != nil {
		scopeMap := make(map[string]interface{})
		if scope.GetName() != "" {
			scopeMap["name"] = scope.GetName()
		}
		if scope.GetVersion() != "" {
			scopeMap["version"] = scope.GetVersion()
		}
		if len(scope.GetAttributes()) > 0 {
			scopeMap["attributes"] = kvListToInterface(scope.GetAttributes())
		}
		if len(scopeMap) > 0 {
			raw["scope"] = scopeMap
		}
	}

	b, _ := json.Marshal(raw)
	return string(b)
}

// buildScopeAttrsRaw extracts scope name, version, and attributes as prefixed fields
// without applying normalization.
func buildScopeAttrsRaw(scope *commonpb.InstrumentationScope) map[string]string {
	attrs := make(map[string]string)
	if scope == nil {
		return attrs
	}
	if scope.GetName() != "" {
		attrs["scope.name"] = scope.GetName()
	}
	if scope.GetVersion() != "" {
		attrs["scope.version"] = scope.GetVersion()
	}
	for k, v := range kvListToMapRaw(scope.GetAttributes(), "scope.") {
		attrs[k] = v
	}
	return attrs
}

// kvListToMapRaw converts OTLP KeyValue pairs to a flat string map with an optional prefix.
// No normalization is applied; that is handled by the normalizer pipeline.
func kvListToMapRaw(kvs []*commonpb.KeyValue, prefix string) map[string]string {
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[prefix+kv.GetKey()] = anyValueToString(kv.GetValue())
	}
	return m
}

// kvListToInterface converts OTLP KeyValue pairs to a map for JSON serialization.
func kvListToInterface(kvs []*commonpb.KeyValue) map[string]interface{} {
	m := make(map[string]interface{}, len(kvs))
	for _, kv := range kvs {
		m[kv.GetKey()] = anyValueToInterface(kv.GetValue())
	}
	return m
}

// anyValueToString converts an OTLP AnyValue to a string representation.
func anyValueToString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch val := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return val.StringValue
	case *commonpb.AnyValue_BoolValue:
		return fmt.Sprintf("%v", val.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", val.IntValue)
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", val.DoubleValue)
	case *commonpb.AnyValue_BytesValue:
		return hex.EncodeToString(val.BytesValue)
	case *commonpb.AnyValue_ArrayValue:
		b, _ := json.Marshal(anyValueArrayToInterface(val.ArrayValue))
		return string(b)
	case *commonpb.AnyValue_KvlistValue:
		b, _ := json.Marshal(kvListValToInterface(val.KvlistValue))
		return string(b)
	default:
		return ""
	}
}

// anyValueToInterface converts an OTLP AnyValue to a native Go interface{} for JSON marshaling.
func anyValueToInterface(v *commonpb.AnyValue) interface{} {
	if v == nil {
		return nil
	}
	switch val := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return val.StringValue
	case *commonpb.AnyValue_BoolValue:
		return val.BoolValue
	case *commonpb.AnyValue_IntValue:
		return val.IntValue
	case *commonpb.AnyValue_DoubleValue:
		return val.DoubleValue
	case *commonpb.AnyValue_BytesValue:
		return hex.EncodeToString(val.BytesValue)
	case *commonpb.AnyValue_ArrayValue:
		return anyValueArrayToInterface(val.ArrayValue)
	case *commonpb.AnyValue_KvlistValue:
		return kvListValToInterface(val.KvlistValue)
	default:
		return nil
	}
}

func anyValueArrayToInterface(arr *commonpb.ArrayValue) []interface{} {
	if arr == nil {
		return nil
	}
	result := make([]interface{}, len(arr.GetValues()))
	for i, v := range arr.GetValues() {
		result[i] = anyValueToInterface(v)
	}
	return result
}

func kvListValToInterface(kvl *commonpb.KeyValueList) map[string]interface{} {
	if kvl == nil {
		return nil
	}
	m := make(map[string]interface{}, len(kvl.GetValues()))
	for _, kv := range kvl.GetValues() {
		m[kv.GetKey()] = anyValueToInterface(kv.GetValue())
	}
	return m
}

// respondSuccess sends an OTLP-compatible success response.
func (h *OTLPHandler) respondSuccess(w http.ResponseWriter, r *http.Request, count int) {
	resp := &collectorlogs.ExportLogsServiceResponse{}

	if isJSONRequest(r) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		b, _ := protojson.Marshal(resp)
		w.Write(b)
	} else {
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		b, _ := proto.Marshal(resp)
		w.Write(b)
	}
}

// respondError sends an error response. Uses JSON for JSON requests, plain text otherwise.
func (h *OTLPHandler) respondError(w http.ResponseWriter, r *http.Request, status int, message string) {
	// OTLP spec: errors are HTTP status codes with a body describing the error.
	// Use JSON for consistency since most OTLP clients handle it well.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error":  message,
		"status": status,
	})
}

// isJSONRequest returns true if the request used JSON content type.
func isJSONRequest(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Content-Type"), "application/json")
}
