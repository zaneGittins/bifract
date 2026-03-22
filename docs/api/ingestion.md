# Log Ingestion

Ingestion endpoints require token authentication and are rate-limited. Logs are accepted into a buffered queue and inserted into ClickHouse asynchronously by a worker pool.

## Bifract native format

```
POST /api/v1/ingest
Content-Type: application/json
```

Accepts three formats:

**JSON array:**
```json
[
  {"event_id": 1, "image": "C:\\Windows\\System32\\cmd.exe"},
  {"event_id": 4624, "user": "SYSTEM"}
]
```

**Single object:**
```json
{"message": "user login", "user": "admin", "source_ip": "10.0.0.1"}
```

**NDJSON (newline-delimited):**
```
{"event_id": 1, "image": "powershell.exe"}
{"event_id": 4624, "user": "admin"}
```

## Fractal routing

Each ingest token is scoped to a single fractal. Logs are routed to the fractal associated with the token.

## Elasticsearch bulk format (Velociraptor compatible)

```
POST /_bulk
PUT /_bulk
```

Accepts standard Elasticsearch NDJSON bulk format:

```
{"index": {"_index": "logs"}}
{"event_id": 1, "image": "powershell.exe"}
{"create": {}}
{"event_id": 4624, "user": "admin"}
```

Response follows the Elasticsearch bulk response schema.

## OpenTelemetry (OTLP)

```
POST /v1/logs
Content-Type: application/x-protobuf | application/json
```

Accepts OTLP/HTTP `ExportLogsServiceRequest` in either protobuf or JSON encoding. This is the standard OTLP endpoint path, so most collectors and agents work without custom path configuration.

Resource attributes are stored with a `resource.` prefix, scope attributes with a `scope.` prefix, and log record attributes are stored directly. The log body is mapped to the `message` field. Trace and span IDs are stored as hex strings.

Returns an `ExportLogsServiceResponse` (empty on full success) using the same content type as the request.

## Response codes

| Status | Meaning |
|--------|---------|
| `200` | Logs accepted into ingestion queue |
| `400` | Invalid payload or no valid logs found |
| `401` | Missing or invalid ingest token |
| `413` | Request body exceeds size limit (default 200MB) |
| `429` | Rate limit exceeded or ingestion queue full. Retry with backoff |

## Tuning

| Environment variable | Default | Description |
|---------------------|---------|-------------|
| `BIFRACT_INGEST_WORKERS` | 4 | Worker goroutines inserting into ClickHouse |
| `BIFRACT_INGEST_QUEUE_SIZE` | 500 | Pending batch slots (each slot = one request's logs) |
| `BIFRACT_MAX_BODY_SIZE` | 209715200 | Max request body in bytes (200MB) |
| `BIFRACT_INGEST_RATE_LIMIT` | 10000 | Sustained requests/second |
| `BIFRACT_INGEST_RATE_BURST` | 20000 | Burst capacity |
