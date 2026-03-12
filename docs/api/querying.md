# Querying

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
| `query` | string | BQL query (required) |
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
| `sql` | Generated SQL (BQL queries only) |
| `field_order` | Column display order |
| `is_aggregated` | True if query used aggregation |
| `chart_type` | `"piechart"`, `"barchart"`, `"graph"`, or empty |
| `limit_hit` | `"search"`, `"bloom"`, `"truncated"`, or empty |

Queries time out after 60 seconds by default (configurable in Settings). Responses exceeding ~50MB are truncated to 1000 rows.

## Recent Logs

```
GET /api/v1/logs/recent
```

Returns up to 50 recent logs for the selected fractal. Useful for exploring available fields.
