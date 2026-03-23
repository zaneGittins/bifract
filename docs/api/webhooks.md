# Webhooks

```
GET    /api/v1/webhooks            (admin)
POST   /api/v1/webhooks            (admin)
GET    /api/v1/webhooks/{id}       (admin)
PUT    /api/v1/webhooks/{id}       (admin)
DELETE /api/v1/webhooks/{id}       (admin)
POST   /api/v1/webhooks/{id}/test  (admin)
```

## Configuration

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

## Alert webhook payload

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
