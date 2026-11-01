# Splunk HEC

Bifract accepts the Splunk HTTP Event Collector (HEC) protocol, so HEC senders such as the OpenTelemetry `splunk_hec` exporter, Vector, Fluent Bit and Cribl can send to Bifract with only a URL and token change. Splunk Universal Forwarders are not HEC senders: their `httpout` speaks the S2S protocol, which Bifract does not accept.

Use an ingest token as the HEC token. Logs land in that token's fractal, and its parser and normalizer apply as on the other endpoints. See [Sending Logs](overview.md#create-a-token-for-a-new-source).

## Endpoints

| Endpoint | Body |
|---|---|
| `POST /services/collector/event` | JSON event envelopes, concatenated or newline-separated |
| `POST /services/collector/raw` | Plain text, one event per line |
| `GET /services/collector/health` | None. Unauthenticated |

`/services/collector` and the `/1.0` variants are aliases. The `POST` endpoints take `Content-Encoding: gzip` and the same per-IP rate limit as other ingest paths. Send the token as `Authorization: Splunk <token>`, `Bearer <token>`, or as the Basic auth password with any user name (`curl -u x:$TOKEN`).

```bash
curl https://bifract.example.com:8443/services/collector/event \
  -H "Authorization: Splunk $TOKEN" \
  -d '{"time": 1791581172.5, "host": "web-1", "sourcetype": "nginx", "event": {"status": 502, "path": "/api"}}
      {"event": "plain text line", "fields": {"region": "eu"}}'
```

## Field mapping

| Envelope key | Bifract |
|---|---|
| `event` (object) | Its keys become fields; the raw log is the event JSON |
| `event` (string) | Read by the token's parser (JSON, key=value or syslog); a line the JSON parser cannot read becomes `message`. The raw log is the string |
| `time` | Event timestamp (epoch seconds, number or string, fractions kept). Without it, the [usual timestamp rules](overview.md#timestamps) apply |
| `host`, `source`, `sourcetype`, `index` | Fields of the same name. The default normalizer maps `host` to `computer_name` |
| `fields` | Each key becomes a field |

On a name clash, `host`/`source`/`sourcetype`/`index` win over `fields`, which win over event keys. The event is unchanged in the raw log, and the metadata is part of the log id, so the same line from two hosts stays two logs. `index` is stored as a field only: the token decides the fractal.

`host`, `source`, `sourcetype` and `index` can also be set as query parameters (`/services/collector/raw?sourcetype=fw&host=fw-1`). They are defaults: they fill only fields the envelope, `fields` and event leave unset.

## Responses

Responses are HEC shaped, for example `{"text":"Success","code":0}`. The one exception is the per-IP rate limit, whose `429` carries the native JSON body; senders retry on the status either way.

| Status | Code | Meaning |
|---|---|---|
| `200` | `0` | Accepted |
| `400` | `5` | Empty body |
| `400` | `6`, `12`, `13` | Invalid envelope, missing `event`, blank `event`. `invalid-event-number` gives its 0-based position; events before it were accepted |
| `400` | `15` | `fields` is not an object |
| `401` | `2`, `3` | Missing token, or an `Authorization` header without one |
| `403` | `4` | Unknown or disabled token |
| `413` | `6` | Body or line too large |
| `429` | | Per-IP rate limit. Retry after `Retry-After` |
| `503` | `9` | Backpressure, full queue, fractal quota, or the token could not be checked. Retry after `Retry-After` |

The health endpoint answers `200` with code `17`, or `503` with code `18` while ingestion is under backpressure.

Indexer acknowledgement (`/services/collector/ack`) is not supported. Responses carry no `ackId`, which Vector treats as delivered; the OTel exporter does not use acknowledgements. Turn `useACK` off in senders that require it.

## Sender examples

OpenTelemetry Collector:

```yaml
exporters:
  splunk_hec:
    endpoint: https://bifract.example.com:8443/services/collector
    token: bifract_ingest_...
    sourcetype: otel
```

Vector:

```yaml
sinks:
  bifract:
    type: splunk_hec_logs
    inputs: [app]
    endpoint: https://bifract.example.com:8443
    default_token: bifract_ingest_...
    encoding:
      codec: json
```

Keep Vector's default `endpoint_target: event`. In `raw` mode Vector joins a batch with no separator, so it would arrive as one line.

Fluent Bit:

```ini
[OUTPUT]
    Name         splunk
    Match        *
    Host         bifract.example.com
    Port         8443
    TLS          On
    Splunk_Token bifract_ingest_...
```
