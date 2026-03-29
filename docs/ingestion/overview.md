# Sending Logs

## Overview

Bifract accepts logs via HTTP and inserts them into ClickHouse through a buffered worker pool. All ingestion endpoints require token-based authentication and are rate-limited.

## Authentication

Ingest tokens are scoped per-fractal and carry per-token configuration (parser type, normalization, timestamp fields). Each fractal gets a default token automatically on creation.

### Getting a Token

1. Navigate to the **Ingest** tab within a fractal
2. Copy the token (format: `bifract_ingest_{32_hex_chars}`)
3. Include it in the `Authorization` header:

```bash
curl -X POST http://localhost:8443/api/v1/ingest \
  -H "Authorization: Bearer bifract_ingest_abc123..." \
  -H "Content-Type: application/json" \
  -d '[{"event":"login","user":"admin"}]'
```

Requests without a valid token receive `401 Unauthorized`.

### Token Features

Each token has its own configuration:
- **Parser type**: `json` (default), `kv` (key-value), or `syslog`
- **Normalization**: Automatically normalize field names
- **Timestamp fields**: Define which fields hold timestamps

## Supported Formats

All formats are sent to `POST /api/v1/ingest`.

**JSON array** (multiple logs in one request):
```bash
curl -X POST http://localhost:8443/api/v1/ingest \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '[{"event":"login","user":"admin"},{"event":"logout","user":"admin"}]'
```

**Single object:**
```bash
curl -X POST http://localhost:8443/api/v1/ingest \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"message":"user login","source_ip":"10.0.0.1"}'
```

**NDJSON** (newline-delimited JSON):
```bash
curl -X POST http://localhost:8443/api/v1/ingest \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data-binary @logs.ndjson
```

### Elasticsearch Bulk API

Bifract also accepts `POST /_bulk` and `PUT /_bulk` for compatibility with Elasticsearch-style clients. These endpoints also require the `Authorization: Bearer` header.

### OpenTelemetry (OTLP)

Bifract accepts OTLP/HTTP log exports at `POST /v1/logs`, the standard OTLP endpoint path. Both protobuf (`application/x-protobuf`) and JSON (`application/json`) content types are supported.

```bash
curl -X POST http://localhost:8443/v1/logs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "resourceLogs": [{
      "resource": {
        "attributes": [
          {"key": "service.name", "value": {"stringValue": "my-service"}}
        ]
      },
      "scopeLogs": [{
        "logRecords": [{
          "timeUnixNano": "1711063200000000000",
          "severityText": "INFO",
          "body": {"stringValue": "Request completed successfully"},
          "attributes": [
            {"key": "http.method", "value": {"stringValue": "GET"}}
          ]
        }]
      }]
    }]
  }'
```

**Field mapping:**

| OTLP field | Bifract field |
|---|---|
| `Body` | `message` |
| `SeverityText` | `severity_text` |
| `SeverityNumber` | `severity_number` |
| `TraceId` | `trace_id` (hex) |
| `SpanId` | `span_id` (hex) |
| `Resource.Attributes` | Prefixed with `resource.` (e.g. `resource.service.name`) |
| `Scope.Name/Version` | `scope.name`, `scope.version` |
| `Scope.Attributes` | Prefixed with `scope.` |
| `Attributes` | Stored directly (e.g. `http.method`) |
| `TimeUnixNano` | `timestamp` |

**OpenTelemetry Collector configuration:**

```yaml
exporters:
  otlphttp:
    endpoint: https://bifract.example.com
    headers:
      Authorization: "Bearer bifract_ingest_..."
    # Only send logs - Bifract does not accept traces or metrics
    signal:
      traces:
        enabled: false
      metrics:
        enabled: false

service:
  pipelines:
    logs:
      exporters: [otlphttp]
```

Normalizers configured on the ingest token apply to OTLP field names the same way they do for other formats.

## Fractal Routing

Each ingest token is scoped to a single fractal. Logs are routed to the fractal associated with the token.

## Timestamps

Bifract extracts timestamps automatically:

1. Token-configured timestamp fields (set per token in the Ingest tab)
2. Configured timestamp fields (set in Settings)
3. Common fields: `timestamp`, `@timestamp`, `time`, `ts`, `_time`
4. Falls back to ingestion time if none found

Supported formats: RFC3339, unix seconds/millis/micros/nanos, and common ISO variants.

## Parsing Philosophy

Bifract is intentionally minimal when it comes to parsing. It accepts well-structured log formats (JSON, key-value, syslog) and focuses on what happens *after* logs arrive: storage, querying, alerting, and collaboration at scale.

Complex log parsing and transformation (extracting fields from unstructured text, grok patterns, multi-line assembly, etc.) is deliberately out of scope. Mature, battle-tested tools already exist for this:

- **Logstash** - Broad plugin ecosystem for parsing and routing
- **Cribl** - Stream processing and log transformation
- **Fluentd / Fluent Bit** - Lightweight log collection and parsing
- **Vector** - High-performance log pipeline

Use these tools upstream of Bifract to parse raw logs into structured formats before ingestion.

### What Bifract Does Handle

**Normalization.** Bifract normalizes field names across log sources to ensure consistency. This means alert rules, saved queries, and dashboards work reliably regardless of which source produced the log. Normalization can be configured per ingest token.
