# Health & Status

## Health

```
GET /api/v1/health
```

Returns `{"status": "healthy"}`. No authentication required. This is the endpoint the container `HEALTHCHECK` uses, on both the app and ingest tiers.

## Status

```
GET /api/v1/status
```

Returns ClickHouse and PostgreSQL status.

```
GET /api/v1/health/clickhouse    ClickHouse connectivity check
GET /api/v1/system/pressure      Current ingest backpressure state
GET /api/v1/version              Running Bifract version
```

`GET /api/v1/system/pressure` reports which of the [backpressure layers](../ingestion/tuning.md) are currently active, which is the quickest way to explain a burst of `429`s.

## Prometheus metrics

When `BIFRACT_METRICS_ENABLED=true`, Prometheus metrics are served on a **separate** listener (`BIFRACT_METRICS_ADDR`, default `:9090`) so it can be firewalled independently of the main application port. It is disabled by default.
