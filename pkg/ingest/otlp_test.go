package ingest

import (
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"bifract/pkg/normalizers"
)

func strVal(s string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: s}}
}
func intVal(i int64) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: i}}
}
func dblVal(f float64) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: f}}
}
func kv(k string, v *commonpb.AnyValue) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: v}
}

func TestAnyValueToString(t *testing.T) {
	tests := []struct {
		name string
		v    *commonpb.AnyValue
		want string
	}{
		{"nil", nil, ""},
		{"unset", &commonpb.AnyValue{}, ""},
		{"string", strVal("héllo"), "héllo"},
		{"bool", &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}, "true"},
		{"int", intVal(-9007199254740993), "-9007199254740993"},
		{"double", dblVal(0.25), "0.25"},
		// Same rendering as JSON ingest, so value="1000000" matches either path.
		{"whole double has no exponent", dblVal(1e6), "1000000"},
		{"bytes are hex", &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{0xde, 0xad}}}, "dead"},
		{"array", &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{
			Values: []*commonpb.AnyValue{strVal("a"), intVal(2), nil}}}}, `["a",2,null]`},
		{"kvlist", &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{
			Values: []*commonpb.KeyValue{kv("b", intVal(1)), kv("a", &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{
				KvlistValue: &commonpb.KeyValueList{Values: []*commonpb.KeyValue{kv("deep", strVal("x"))}}}})}}}}, `{"a":{"deep":"x"},"b":1}`},
		{"empty array", &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{}}, "null"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := anyValueToString(tc.v); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
	// Non-finite doubles still produce a value rather than an empty field.
	if got := anyValueToString(dblVal(math.Inf(1))); got != "+Inf" {
		t.Errorf("+Inf rendered %q", got)
	}
}

func fullRecord() (*logspb.LogRecord, *resourcepb.Resource, *commonpb.InstrumentationScope) {
	lr := &logspb.LogRecord{
		TimeUnixNano:         uint64(time.Date(2026, 7, 1, 12, 0, 0, 123456789, time.UTC).UnixNano()),
		ObservedTimeUnixNano: uint64(time.Date(2026, 7, 1, 12, 0, 1, 0, time.UTC).UnixNano()),
		SeverityNumber:       logspb.SeverityNumber_SEVERITY_NUMBER_ERROR,
		SeverityText:         "ERROR",
		Body:                 strVal("disk full"),
		Attributes:           []*commonpb.KeyValue{kv("user", strVal("alice")), kv("message", strVal("shadowed by body")), kv("retries", intVal(3))},
		TraceId:              []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
		SpanId:               []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11},
		Flags:                1,
	}
	res := &resourcepb.Resource{Attributes: []*commonpb.KeyValue{kv("service.name", strVal("api")), kv("host.name", strVal("node-1"))}}
	scope := &commonpb.InstrumentationScope{Name: "otel-go", Version: "1.2.3", Attributes: []*commonpb.KeyValue{kv("lib", strVal("zap"))}}
	return lr, res, scope
}

func TestConvertLogRecord(t *testing.T) {
	h := &OTLPHandler{handler: &IngestHandler{}}
	lr, res, scope := fullRecord()
	e := h.convertLogRecord(lr, res, scope, kvListToMapRaw(res.GetAttributes(), "resource."), buildScopeAttrsRaw(scope), nil, nil)

	want := map[string]string{
		"user":                  "alice",
		"retries":               "3",
		"message":               "disk full", // the body wins over an attribute of the same name
		"severity_text":         "ERROR",
		"severity_number":       "17",
		"trace_id":              "0102030405060708090a0b0c0d0e0f10",
		"span_id":               "aabbccddeeff0011",
		"flags":                 "1",
		"resource.service.name": "api",
		"resource.host.name":    "node-1",
		"scope.name":            "otel-go",
		"scope.version":         "1.2.3",
		"scope.lib":             "zap",
		"observed_timestamp":    time.Date(2026, 7, 1, 12, 0, 1, 0, time.UTC).Local().Format(time.RFC3339Nano),
	}
	for k, v := range want {
		if e.Fields[k] != v {
			t.Errorf("Fields[%q] = %q, want %q", k, e.Fields[k], v)
		}
	}
	if len(e.Fields) != len(want) {
		t.Errorf("unexpected extra fields: %v", e.Fields)
	}
	if !e.Timestamp.Equal(time.Date(2026, 7, 1, 12, 0, 0, 123456789, time.UTC)) {
		t.Errorf("Timestamp = %v, want TimeUnixNano", e.Timestamp)
	}
	if e.IngestTimestamp.IsZero() || e.LogID == "" || e.Normalizer != "" {
		t.Errorf("entry not ready to insert: %+v", e)
	}

	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(e.RawLog), &raw); err != nil {
		t.Fatalf("RawLog is not JSON: %v", err)
	}
	if raw["body"] != "disk full" || raw["severity_number"] != float64(17) || raw["trace_id"] != want["trace_id"] {
		t.Errorf("RawLog = %s", e.RawLog)
	}
	if attrs, _ := raw["attributes"].(map[string]interface{}); attrs["retries"] != float64(3) {
		t.Errorf("RawLog attributes keep native types: %v", raw["attributes"])
	}
	if r, _ := raw["resource"].(map[string]interface{}); r["service.name"] != "api" {
		t.Errorf("RawLog resource = %v", raw["resource"])
	}
	if s, _ := raw["scope"].(map[string]interface{}); s["name"] != "otel-go" || s["version"] != "1.2.3" {
		t.Errorf("RawLog scope = %v", raw["scope"])
	}
}

// TestConvertLogRecordTimestampFallbacks: event time comes from TimeUnixNano,
// then ObservedTimeUnixNano, then the normal extraction pipeline over the
// attributes, and only then the ingest time.
func TestConvertLogRecordTimestampFallbacks(t *testing.T) {
	h := &OTLPHandler{handler: &IngestHandler{}}
	observed := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	e := h.convertLogRecord(&logspb.LogRecord{ObservedTimeUnixNano: uint64(observed.UnixNano())}, nil, nil, nil, nil, nil, nil)
	if !e.Timestamp.Equal(observed) {
		t.Errorf("observed fallback: %v", e.Timestamp)
	}

	e = h.convertLogRecord(&logspb.LogRecord{Attributes: []*commonpb.KeyValue{kv("timestamp", strVal("2026-03-03T03:03:03Z"))}}, nil, nil, nil, nil, nil, nil)
	if !e.Timestamp.Equal(utc("2026-03-03T03:03:03Z")) {
		t.Errorf("attribute fallback: %v", e.Timestamp)
	}

	e = h.convertLogRecord(&logspb.LogRecord{Body: strVal("no time")}, nil, nil, nil, nil, nil, nil)
	if !e.Timestamp.Equal(e.IngestTimestamp) {
		t.Errorf("ingest-time fallback: ts %v ingest %v", e.Timestamp, e.IngestTimestamp)
	}
	if _, ok := e.Fields["observed_timestamp"]; ok {
		t.Error("observed_timestamp set without an observed time")
	}
	if e.RawLog != `{"body":"no time"}` {
		t.Errorf("minimal RawLog = %s", e.RawLog)
	}
}

func TestConvertLogRecordAppliesNormalizer(t *testing.T) {
	h := &OTLPHandler{handler: &IngestHandler{}}
	norm := (&normalizers.Normalizer{
		Name:          "otel",
		Version:       2,
		FieldMappings: []normalizers.FieldMapping{{Sources: []string{"resource.host.name"}, Target: "computer_name"}},
	}).Compile()
	lr, res, scope := fullRecord()
	e := h.convertLogRecord(lr, res, scope, kvListToMapRaw(res.GetAttributes(), "resource."), buildScopeAttrsRaw(scope), norm, nil)
	if e.Fields["computer_name"] != "node-1" || e.Normalizer != "otel@2" {
		t.Fatalf("normalizer not applied: %v %q", e.Fields, e.Normalizer)
	}
}

func TestParseOTLPRequestFlattensResourceAndScope(t *testing.T) {
	h := &OTLPHandler{handler: &IngestHandler{}}
	req := &collectorlogs.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{
		{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{kv("service.name", strVal("a"))}},
			ScopeLogs: []*logspb.ScopeLogs{
				{Scope: &commonpb.InstrumentationScope{Name: "s1"}, LogRecords: []*logspb.LogRecord{{Body: strVal("1")}, {Body: strVal("2")}}},
				{LogRecords: []*logspb.LogRecord{{Body: strVal("3")}}},
			},
		},
		{ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{Body: strVal("4")}}}}},
	}}
	logs := h.parseOTLPRequest(req, nil, nil)
	if len(logs) != 4 {
		t.Fatalf("got %d logs, want 4", len(logs))
	}
	if logs[0].Fields["resource.service.name"] != "a" || logs[0].Fields["scope.name"] != "s1" {
		t.Errorf("log 1 fields = %v", logs[0].Fields)
	}
	if _, ok := logs[2].Fields["scope.name"]; ok || logs[2].Fields["resource.service.name"] != "a" {
		t.Errorf("log 3 (no scope) fields = %v", logs[2].Fields)
	}
	// Resource attributes must not leak into the next resource's logs.
	if _, ok := logs[3].Fields["resource.service.name"]; ok {
		t.Errorf("log 4 inherited another resource's attributes: %v", logs[3].Fields)
	}
}

func otlpRequest(bodies ...string) *collectorlogs.ExportLogsServiceRequest {
	var recs []*logspb.LogRecord
	for _, b := range bodies {
		recs = append(recs, &logspb.LogRecord{Body: strVal(b)})
	}
	return &collectorlogs.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{ScopeLogs: []*logspb.ScopeLogs{{LogRecords: recs}}}}}
}

func TestHandleLogsEncodings(t *testing.T) {
	req := otlpRequest("first", "second")
	pb, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	js, err := protojson.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		body   []byte
		opts   []reqOpt
		wantCT string
	}{
		{"protobuf", pb, []reqOpt{withHeader("Content-Type", "application/x-protobuf")}, "application/x-protobuf"},
		{"protobuf alt type", pb, []reqOpt{withHeader("Content-Type", "application/protobuf")}, "application/x-protobuf"},
		{"json", js, []reqOpt{withHeader("Content-Type", "application/json; charset=utf-8")}, "application/json"},
		{"unlabelled protobuf", pb, nil, "application/x-protobuf"},
		{"unlabelled json", js, nil, "application/x-protobuf"},
		// Collectors gzip by default; the OTLP/HTTP spec requires servers to accept it.
		{"gzip protobuf", gzipBytes(t, pb), []reqOpt{withHeader("Content-Type", "application/x-protobuf"), withHeader("Content-Encoding", "gzip")}, "application/x-protobuf"},
		{"gzip json", gzipBytes(t, js), []reqOpt{withHeader("Content-Type", "application/json"), withHeader("Content-Encoding", "gzip")}, "application/json"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, tokenRow{fractalID: "frac-otel"})
			w, _ := doBytes(NewOTLPHandler(f.h).HandleLogs, http.MethodPost, tc.body, tc.opts...)
			if w.Code != http.StatusOK {
				t.Fatalf("status %d: %s", w.Code, w.Body)
			}
			if ct := w.Header().Get("Content-Type"); ct != tc.wantCT {
				t.Errorf("Content-Type = %q, want %q", ct, tc.wantCT)
			}
			reply := &collectorlogs.ExportLogsServiceResponse{}
			var err error
			if tc.wantCT == "application/json" {
				err = protojson.Unmarshal(w.Body.Bytes(), reply)
			} else {
				err = proto.Unmarshal(w.Body.Bytes(), reply)
			}
			if err != nil {
				t.Errorf("reply does not decode as ExportLogsServiceResponse: %v", err)
			}
			logs := f.q.queued()
			if len(logs) != 2 || logs[0].Fields["message"] != "first" || logs[1].FractalID != "frac-otel" {
				t.Fatalf("queued %+v", logs)
			}
			if args := waitForUsage(t, f.pg); args[1] != int64(2) {
				t.Errorf("usage count = %v", args[1])
			}
		})
	}
}

func TestHandleLogsRejections(t *testing.T) {
	pb, _ := proto.Marshal(otlpRequest("x"))
	tests := []struct {
		name       string
		body       []byte
		opts       []reqOpt
		setup      func(*fixture)
		wantStatus int
		retryAfter string
		bodyRead   bool
		wantErr    string
	}{
		{name: "no token", body: pb, opts: []reqOpt{noAuth()}, wantStatus: 401, wantErr: "Invalid or missing"},
		{name: "token store down", body: pb, opts: []reqOpt{withAuth("Bearer " + brokenToken)}, wantStatus: 503, retryAfter: "2"},
		{name: "pressure", body: pb, setup: func(f *fixture) { f.q.memState.active.Store(1) }, wantStatus: 429, retryAfter: "2"},
		{name: "bad protobuf", body: []byte("\xff\xff\xff"), opts: []reqOpt{withHeader("Content-Type", "application/x-protobuf")}, wantStatus: 400, bodyRead: true, wantErr: "protobuf"},
		{name: "bad json", body: []byte("{"), opts: []reqOpt{withHeader("Content-Type", "application/json")}, wantStatus: 400, bodyRead: true, wantErr: "JSON"},
		{name: "unlabelled garbage", body: []byte("\xff\xff\xff"), wantStatus: 400, bodyRead: true, wantErr: "Content-Type"},
		{name: "too large", body: pb, setup: func(f *fixture) { f.h.maxBodySize = 4 }, wantStatus: 413, bodyRead: true, wantErr: "byte limit"},
		{name: "queue full", body: pb, setup: func(f *fixture) { f.q.accumBytes.Store(f.q.bufferBytes + 1) }, wantStatus: 429, retryAfter: "2", bodyRead: true, wantErr: "queue full"},
		{name: "quota", body: pb, setup: func(f *fixture) { f.h.SetQuotaManager(quotaFor("frac-1", 1, 1, "reject")) }, wantStatus: 429, retryAfter: "30", bodyRead: true, wantErr: "quota"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, tokenRow{})
			if tc.setup != nil {
				tc.setup(f)
			}
			w, read := doBytes(NewOTLPHandler(f.h).HandleLogs, http.MethodPost, tc.body, tc.opts...)
			if w.Code != tc.wantStatus {
				t.Fatalf("status %d, want %d: %s", w.Code, tc.wantStatus, w.Body)
			}
			if got := w.Header().Get("Retry-After"); got != tc.retryAfter {
				t.Errorf("Retry-After = %q, want %q", got, tc.retryAfter)
			}
			if read != tc.bodyRead {
				t.Errorf("body read = %v, want %v", read, tc.bodyRead)
			}
			var env struct {
				Error  string `json:"error"`
				Status int    `json:"status"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil || env.Status != tc.wantStatus || !strings.Contains(env.Error, tc.wantErr) {
				t.Errorf("error body = %s (%v), want status %d and %q", w.Body, err, tc.wantStatus, tc.wantErr)
			}
			if n := len(f.q.queued()); n != 0 {
				t.Errorf("%d logs queued", n)
			}
		})
	}

	f := newFixture(t, tokenRow{})
	if w, _ := do(NewOTLPHandler(f.h).HandleLogs, http.MethodGet, ""); w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status %d, want 405", w.Code)
	}
	// An export with no records is a success with nothing queued.
	empty, _ := proto.Marshal(&collectorlogs.ExportLogsServiceRequest{})
	w, _ := doBytes(NewOTLPHandler(f.h).HandleLogs, http.MethodPost, empty, withHeader("Content-Type", "application/x-protobuf"))
	if w.Code != http.StatusOK || len(f.q.queued()) != 0 {
		t.Fatalf("empty export: %d", w.Code)
	}
	noUsage(t, f.pg)
}

// TestBuildRawLogKeepsNativeTypes: the raw log viewer shows attribute values with
// their OTLP types (numbers stay numbers, nested lists stay nested), unlike the
// flattened string fields.
func TestBuildRawLogKeepsNativeTypes(t *testing.T) {
	h := &OTLPHandler{handler: &IngestHandler{}}
	lr := &logspb.LogRecord{Attributes: []*commonpb.KeyValue{
		kv("b", &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}),
		kv("d", dblVal(1.5)),
		kv("i", intVal(7)),
		kv("x", &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{1, 2}}}),
		kv("l", &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: []*commonpb.AnyValue{intVal(1), strVal("two")}}}}),
		kv("m", &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{Values: []*commonpb.KeyValue{kv("k", dblVal(2))}}}}),
		kv("nilkv", &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{}}),
		kv("unset", &commonpb.AnyValue{}),
		kv("absent", nil),
	}}
	raw := h.buildRawLog(lr, &resourcepb.Resource{}, &commonpb.InstrumentationScope{})
	want := `{"attributes":{"absent":null,"b":true,"d":1.5,"i":7,"l":[1,"two"],"m":{"k":2},"nilkv":null,"unset":null,"x":"0102"}}`
	if raw != want {
		t.Fatalf("raw log =\n%s\nwant\n%s", raw, want)
	}
}
