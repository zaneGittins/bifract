# Alert Webhooks

A webhook forwards a firing alert to a system outside Bifract: a chat channel, a ticket
queue, a SOAR playbook. You configure one under **Alerts > Actions**, or through the API;
the operations are in [All Operations](reference.md).

This page covers the two things that description cannot tell you: the fields a webhook
accepts, and the payload Bifract sends you when an alert fires.

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
| `body_mode` | string | `envelope` for the payload below, or `template` to render `body_template` (default: `envelope`) |
| `body_template` | string | Body template, required when `body_mode` is `template` |
| `content_type` | string | `Content-Type` header (default: `application/json`) |
| `enabled` | bool | Whether the webhook fires (default: `true`) |

## Alert webhook payload

When an alert fires, each configured webhook receives:

```json
{
  "alert_name": "Security Alert for 10.0.0.5",
  "original_name": "Security Alert for {{src_ip}}",
  "alert_id": "uuid",
  "description": "Detects suspicious login patterns",
  "severity": "high",
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
| `severity` | The alert's configured severity |
| `results` | All matching log records from the evaluation window |
| `match_count` | Number of results |
| `alert_link` | Shareable UI link (only if `include_alert_link` is enabled and `BIFRACT_BASE_URL` is set) |

## Custom payloads

Set `body_mode` to `template` when the destination expects its own wire format rather
than the envelope above. The template is [Go text/template](https://pkg.go.dev/text/template)
and renders once per delivery.

Available data:

| Name | Description |
|------|-------------|
| `.AlertName`, `.OriginalName`, `.AlertID`, `.Description`, `.Severity` | Alert identity, matching the envelope fields |
| `.Labels`, `.QueryString`, `.MatchCount`, `.AlertLink` | Alert context |
| `.TriggeredAt` | Trigger time, as a Go `time.Time` |
| `.Results` | The matching records, each a map of field name to value |

Available functions:

| Function | Description |
|----------|-------------|
| `toJSON` | Marshal any value to JSON, quoting and escaping strings |
| `field` | Read one key from a result, for names containing dots |
| `unixSeconds`, `unixMillis`, `rfc3339` | Format a time |
| `join`, `lower`, `upper`, `default` | String helpers, taking their subject last so they pipe: `{{.Labels \| join ", "}}` |

Ranging over `.Results` emits one record per match, which is what log-shaped
destinations expect. This sends [Splunk HEC](https://docs.splunk.com/Documentation/Splunk/latest/Data/HECExamples)
events, one per matching log:

```
{{- range .Results}}
{"time":{{unixSeconds $.TriggeredAt}},"sourcetype":"bifract:alert","event":{{toJSON .}}}
{{- end}}
```

Pair it with a `Authorization: Splunk <token>` custom header pointed at
`/services/collector/event`.

Build every value with `toJSON` rather than quoting it in the template. A field
containing a quote or newline produces a malformed body otherwise:

```
{"text":{{toJSON .AlertName}},"severity":{{toJSON .Severity}}}
```

Reach dotted field names with `field`, since `.host.name` is read as nested lookups:

```
{{range .Results}}{{field . "host.name"}}
{{end}}
```

A template that fails to compile is rejected when the webhook is saved, and a
template that fails to render is reported without retrying. Rendered bodies are
capped at 8MB.

### Testing

**Preview payload** renders the current form against sample data without sending.
**Send test** delivers it and shows the status, the response body, and what was sent.
Both work on unsaved edits.

## Network access

Webhook URLs must use `http` or `https`. Bifract does not otherwise restrict the
destination, so a webhook can reach any address the server can reach, including
loopback and private ranges. Creating and editing webhooks is a tenant admin
operation; restrict that role accordingly, and apply egress controls at the network
layer if the server sits where internal services are reachable.
