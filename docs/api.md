# API Reference

All API endpoints are under `/api/v1`. Responses are JSON.

## Authentication

Most endpoints require authentication via session cookie or API key.

### Session (browser)

```
POST /api/v1/auth/login
Content-Type: application/json

{"username": "admin", "password": "changeme"}
```

Sets an `HttpOnly` session cookie valid for 24 hours.

```
POST /api/v1/auth/logout
```

### API Key

Include the key in one of two ways:

```
Authorization: Bearer bifract_<key>
X-API-Key: bifract_<key>
```

API keys are scoped to a specific fractal. See [API Keys](administration/ingest-tokens.md) for key management.

---

## Log Ingestion

Ingestion endpoints require token authentication and are rate-limited. Logs are accepted into a buffered queue and inserted into ClickHouse asynchronously by a worker pool.

### Bifract native format

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

### Fractal routing

Each ingest token is scoped to a single fractal. Logs are routed to the fractal associated with the token.

### Elasticsearch bulk format (Velociraptor compatible)

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

### Response codes

| Status | Meaning |
|--------|---------|
| `200` | Logs accepted into ingestion queue |
| `400` | Invalid JSON or no valid logs found |
| `413` | Request body exceeds size limit (default 200MB) |
| `429` | Rate limit exceeded or ingestion queue full. Retry with backoff |

### Tuning

| Environment variable | Default | Description |
|---------------------|---------|-------------|
| `BIFRACT_INGEST_WORKERS` | 4 | Worker goroutines inserting into ClickHouse |
| `BIFRACT_INGEST_QUEUE_SIZE` | 500 | Pending batch slots (each slot = one request's logs) |
| `BIFRACT_MAX_BODY_SIZE` | 209715200 | Max request body in bytes (200MB) |
| `BIFRACT_INGEST_RATE_LIMIT` | 10000 | Sustained requests/second |
| `BIFRACT_INGEST_RATE_BURST` | 20000 | Burst capacity |

---

## Querying

```
POST /api/v1/query
Content-Type: application/json
Authorization: Bearer bifract_<key>
```

**Request body:**

```json
{
  "query": "event_id=1 | groupBy(image) | count()",
  "fractal_id": "uuid-of-fractal",
  "start": "2026-01-01T00:00:00Z",
  "end": "2026-01-02T00:00:00Z"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `query` | string | Quandrix query (required) |
| `fractal_id` | string | Target fractal UUID (uses session default if omitted) |
| `start` | string | RFC3339 start time (defaults to 24 hours ago) |
| `end` | string | RFC3339 end time (defaults to now) |

**Response:**

```json
{
  "success": true,
  "results": [{"image": "powershell.exe", "count": 42}],
  "count": 1,
  "query": "event_id=1 | groupBy(image) | count()",
  "sql": "SELECT ...",
  "execution_ms": 12,
  "field_order": ["image", "count"],
  "is_aggregated": true,
  "chart_type": "",
  "limit_hit": ""
}
```

| Field | Description |
|-------|-------------|
| `results` | Array of result rows |
| `count` | Number of rows returned |
| `sql` | Generated SQL (Quandrix queries only) |
| `field_order` | Column display order |
| `is_aggregated` | True if query used aggregation |
| `chart_type` | `"piechart"`, `"barchart"`, `"graph"`, or empty |
| `limit_hit` | `"search"`, `"bloom"`, `"truncated"`, or empty |

Queries time out after 60 seconds by default (configurable in Settings). Responses exceeding ~50MB are truncated to 1000 rows.

---

## Recent Logs

```
GET /api/v1/logs/recent
```

Returns up to 50 recent logs for the selected fractal. Useful for exploring available fields.

---

## Comments

```
POST   /api/v1/comments
GET    /api/v1/comments/{id}
PUT    /api/v1/comments/{id}
DELETE /api/v1/comments/{id}

GET    /api/v1/logs/{log_id}/comments
DELETE /api/v1/logs/{log_id}/comments

GET    /api/v1/logs/commented
```

**Create comment:**

```json
{
  "log_id": "uuid",
  "content": "Confirmed malicious - escalating to IR team",
  "fractal_id": "uuid"
}
```

---

## Fractals

```
GET    /api/v1/fractals
POST   /api/v1/fractals           (admin)
GET    /api/v1/fractals/{id}
PUT    /api/v1/fractals/{id}      (admin)
DELETE /api/v1/fractals/{id}      (admin)
POST   /api/v1/fractals/{id}/select
GET    /api/v1/fractals/{id}/stats
POST   /api/v1/fractals/stats/refresh  (admin)
```

---

## Alerts

```
GET    /api/v1/alerts
POST   /api/v1/alerts
GET    /api/v1/alerts/{id}
PUT    /api/v1/alerts/{id}
DELETE /api/v1/alerts/{id}
POST   /api/v1/alerts/import
GET    /api/v1/alerts/{id}/executions
```

---

## Webhooks

```
GET    /api/v1/webhooks            (admin)
POST   /api/v1/webhooks            (admin)
GET    /api/v1/webhooks/{id}       (admin)
PUT    /api/v1/webhooks/{id}       (admin)
DELETE /api/v1/webhooks/{id}       (admin)
POST   /api/v1/webhooks/{id}/test  (admin)
```

### Webhook configuration

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Unique webhook name |
| `url` | string | Destination URL |
| `method` | string | HTTP method (default: `POST`) |
| `headers` | object | Custom HTTP headers |
| `auth_type` | string | `none`, `bearer`, or `basic` |
| `auth_config` | object | Auth details (`token` for bearer; `username`/`password` for basic) |
| `timeout_seconds` | int | Request timeout (default: 30) |
| `retry_count` | int | Retry attempts with exponential backoff (default: 3) |
| `include_alert_link` | bool | Include a UI link to the alert results (default: `true`) |

### Alert webhook payload

When an alert fires, each configured webhook receives:

```json
{
  "alert_name": "Security Alert for 10.0.0.5",
  "original_name": "Security Alert for {{src_ip}}",
  "alert_id": "uuid",
  "description": "Detects suspicious login patterns",
  "labels": ["sigma:high", "product:windows"],
  "triggered_at": "2026-03-01T12:34:56Z",
  "query_string": "event_id=4625 | count() > 10",
  "match_count": 15,
  "alert_link": "https://bifract.example.com/?q=...",
  "results": [
    {"src_ip": "10.0.0.5", "user": "admin", "event_id": "4625"}
  ]
}
```

| Field | Description |
|-------|-------------|
| `alert_name` | Resolved name (field templates like `{{src_ip}}` are replaced with values from the first result) |
| `original_name` | Only present if the name contained templates |
| `results` | All matching log records from the evaluation window |
| `match_count` | Number of results |
| `alert_link` | Shareable UI link (only if `include_alert_link` is enabled and `BIFRACT_BASE_URL` is set) |

Webhooks block requests to loopback and private network addresses.

---

## Notebooks

```
GET    /api/v1/notebooks
POST   /api/v1/notebooks
GET    /api/v1/notebooks/{id}
PUT    /api/v1/notebooks/{id}
DELETE /api/v1/notebooks/{id}

POST   /api/v1/notebooks/{id}/sections
PUT    /api/v1/notebooks/{id}/sections/{section_id}
DELETE /api/v1/notebooks/{id}/sections/{section_id}
POST   /api/v1/notebooks/{id}/sections/{section_id}/execute
POST   /api/v1/notebooks/{id}/sections/{section_id}/summarize
POST   /api/v1/notebooks/{id}/sections/reorder
GET    /api/v1/notebooks/ai-status
POST   /api/v1/notebooks/generate-from-comments
POST   /api/v1/notebooks/import
GET    /api/v1/notebooks/{id}/export
```

`POST /api/v1/notebooks/generate-from-comments` accepts `{"tag": "...", "attack_chain": false}` and creates a notebook from all comments with that tag. Set `attack_chain` to `true` to generate a MITRE ATT&CK tactic breakdown instead of a plain AI summary. If a notebook named "Notebook: {tag}" already exists in the fractal, it is replaced.

---

## Dashboards

```
GET    /api/v1/dashboards
POST   /api/v1/dashboards
GET    /api/v1/dashboards/{id}
PUT    /api/v1/dashboards/{id}
DELETE /api/v1/dashboards/{id}

POST   /api/v1/dashboards/{id}/widgets
PUT    /api/v1/dashboards/{id}/widgets/{widget_id}
DELETE /api/v1/dashboards/{id}/widgets/{widget_id}
```

---

## Health

```
GET /api/v1/health
```

Returns `{"status": "healthy"}`. No authentication required.

---

## Status

```
GET /api/v1/status
```

Returns ClickHouse and PostgreSQL status.
