# Log Ingestion

The request and response contract for the ingestion endpoints. For getting a token and shipper setup, see [Sending Logs](../ingestion/overview.md); the endpoints also appear in [All Operations](reference.md).

All authenticate with an ingest token (`Authorization: Bearer bifract_ingest_...`, or `ApiKey` for Elasticsearch clients and `Splunk` for HEC clients), are rate-limited per client IP, and accept `Content-Encoding: gzip`. Logs land in the fractal the token belongs to. Accepted logs are queued and written to ClickHouse asynchronously.

## Native: `POST /api/v1/ingest`

Send `Content-Type: application/json` with any of:

| Format | Example |
|---|---|
| JSON array | `[{"event_id": 4624, "user": "SYSTEM"}, {"event_id": 1}]` |
| Single object | `{"message": "user login", "source_ip": "10.0.0.1"}` |
| NDJSON | One JSON object per line |

The token's **Parser** decides how the body is read: JSON (above), key=value, or syslog lines.

Response:

```json
{"success": true, "count": 2, "message": "Accepted 2 log(s) for ingestion"}
```

## Elasticsearch bulk: `POST` or `PUT /_bulk`

Standard bulk NDJSON, compatible with Velociraptor and other Elasticsearch clients:

```
{"index": {"_index": "logs"}}
{"event_id": 1, "image": "powershell.exe"}
{"create": {}}
{"event_id": 4624, "user": "admin"}
```

`index` and `create` are accepted. `update` and `delete` fail per item, since ingest is append-only. The response follows the Elasticsearch bulk schema (`took`, `errors`, `items`).

## OpenTelemetry: `POST /v1/logs`

An OTLP/HTTP `ExportLogsServiceRequest`, as `application/x-protobuf` or `application/json`. The reply is an `ExportLogsServiceResponse` in the request's content type, empty on full success. Field mapping is in [Sending Logs](../ingestion/overview.md#opentelemetry-collector).

## Splunk HEC: `POST /services/collector/event` and `/raw`

Splunk HTTP Event Collector envelopes, or raw lines. Responses are HEC shaped (`{"text":"Success","code":0}`). Statuses follow Splunk rather than the table below: an unknown token is `403`, and backpressure is `503`. Mapping and HEC codes are in [Splunk HEC](../ingestion/splunk-hec.md).

## Status codes

| Status | Meaning | Sender should |
|--------|---------|---------------|
| `200` | Accepted into the queue | Continue |
| `400` | Unparseable body, or no valid logs | Fix the payload |
| `401` | Missing or invalid ingest token | Fix the token; do not retry |
| `413` | Body over `BIFRACT_MAX_BODY_SIZE` (default 200 MB) | Send smaller batches |
| `429` | Rate limit, backpressure, full queue, or fractal quota | Retry after `Retry-After` |
| `503` | Token could not be checked (database unreachable) | Retry after `Retry-After` |

What triggers a `429` and how to tune it is in [Ingestion Tuning](../ingestion/tuning.md).
