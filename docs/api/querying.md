# Querying

```
POST /api/v1/query
Content-Type: application/json
Authorization: Bearer bifract_<key>
```

**Request body:**

```json
{
  "query": "event_id=1 | groupBy(image, function=count())",
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
  "query": "event_id=1 | groupBy(image, function=count())",
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
| `chart_type` | The visualization the query asked for: `"piechart"`, `"barchart"`, `"timechart"`, `"singleval"`, `"histogram"`, `"heatmap"`, `"graph"`, `"mesh"`, `"pgraph"`, `"worldmap"`, or empty for a table |
| `limit_hit` | `"search"`, `"bloom"`, `"truncated"`, or empty |

Queries time out after 60 seconds by default (configurable in **Admin > Settings**). Responses exceeding ~50MB are truncated to 1000 rows.

## Related endpoints

```
POST /api/v1/query/stream       Server-sent-events variant; rows stream as they arrive
POST /api/v1/query/validate     Parse-check a query without running it
POST /api/v1/query/fieldstats   Value distribution for a field across the result set
GET  /api/v1/query/reference    Full BQL command reference (JSON)
GET  /api/v1/query/fields       Schema field catalog
```

`POST /api/v1/query/validate` returns an `error_pos` rune span for the offending text, which is what the in-app editor uses to underline syntax errors.

## Recent Logs

```
GET /api/v1/logs/recent
```

Returns up to 50 recent logs from the last 24 hours for the selected fractal. Useful for exploring available fields.

```
GET  /api/v1/logs/histogram        Per-minute event counts for the selected range
GET  /api/v1/logs/fields           Field names present in the fractal
POST /api/v1/logs/by-timestamp     Fetch a single log's full detail
```
