# Ingestion Tuning

## Architecture

```
HTTP Request --> Token Auth --> Healthy? --> Parse --> Enqueue --> [Worker Pool] --> ClickHouse
                    |              |                      |
               Invalid? 401   Pressure? 429      Backpressure? 429
```

Incoming logs are queued in memory and written to ClickHouse in batches by a pool of workers. Each queue slot holds at most 5,000 logs (larger batches are automatically split). Bifract applies layered backpressure to protect the cluster:

1. **Early rejection** - Before reading or parsing the request body, the handler checks whether the system is under pressure. This avoids wasting CPU on JSON parsing during overload.
2. **Consecutive failures** - If 3+ sequential ClickHouse inserts fail, new batches are rejected immediately. Auto-recovers after 30 seconds.
3. **CPU pressure** - A background monitor polls ClickHouse OS CPU metrics every 5 seconds. When CPU exceeds 80%, new batches are rejected. Releases at 60% (hysteresis prevents oscillation). This directly measures ClickHouse health and self-calibrates to any hardware.
4. **Queue depth** - Rejects when the queue exceeds 50% capacity.

All rejections return `429 Too Many Requests` with a `Retry-After` header. With default settings (100 slots, 5,000 logs per slot), the queue can buffer up to 500,000 logs (~1GB).

## Configuration

| Variable | Default | When to Tune |
|----------|---------|--------------|
| `BIFRACT_INGEST_WORKERS` | `4` | ClickHouse CPU is underutilized during bulk imports |
| `BIFRACT_INGEST_QUEUE_SIZE` | `100` | Frequent `429` responses during ingestion spikes (increase for larger deployments) |
| `BIFRACT_INGEST_RATE_LIMIT` | `10000` | Legitimate sources are being rate-limited |
| `BIFRACT_INGEST_RATE_BURST` | `20000` | Bursty sources need higher peak throughput |
| `BIFRACT_MAX_BODY_SIZE` | `209715200` | Need to accept requests larger than 200MB |
