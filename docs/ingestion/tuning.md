# Ingestion Tuning

## Architecture

```
HTTP Request --> Token Auth --> Healthy? --> Parse --> Enqueue --> [Worker Pool] --> ClickHouse
                    |              |                      |
               Invalid? 401   Pressure? 429      Backpressure? 429
```

Incoming logs are queued in memory and written to ClickHouse in batches by a pool of workers. Each queue slot holds at most 5,000 logs (larger batches are automatically split). Bifract applies layered backpressure to protect the cluster:

1. **Early rejection** - Before reading or parsing the request body, the handler checks whether the system is under pressure. This avoids wasting CPU on JSON parsing during overload.
2. **Consecutive failures** - If 3+ sequential ClickHouse inserts fail, new batches are rejected immediately. Auto-recovers 30 seconds after the last failure.
3. **CPU pressure** - A background monitor polls ClickHouse OS CPU metrics every 5 seconds. Activates after 6 consecutive polls above 80% (30 seconds sustained), so a single heavy user query does not interrupt ingest. Releases at 60% (hysteresis prevents oscillation). This directly measures ClickHouse health and self-calibrates to any hardware.
4. **Memory pressure** - Same monitor, activating after 3 consecutive polls above 90% and releasing at 80%. The shorter debounce reflects that sustained high memory is close to an OOM kill, where inserts and merges die first. On containerized and Kubernetes deployments this reads the pod's own cgroup limit, not the node's.
5. **Disk pressure** - Rejects above 90% disk usage, releasing at 80%. ClickHouse degrades near 90% and risks corruption past 95%.
6. **Buffer bytes** - Rejects once the in-process accumulator exceeds its byte cap, which stops Bifract accepting faster than ClickHouse can drain regardless of row counts.
7. **Queue depth** - Rejects when accepting the batch would push the queue past 50% capacity.
8. **Archive spool** - When the [Iceberg archive](../administration/backup-restore.md#iceberg-archive) is enabled, rejects at 90% of the spool's capacity, releasing at 70%. The spool is fail-closed: a batch that cannot be durably spooled is rejected rather than acknowledged.

All rejections return `429 Too Many Requests` with a `Retry-After` header. With default settings (100 slots, 5,000 logs per slot), the queue can buffer up to 500,000 logs (~1GB).

## Configuration

| Variable | Default | When to Tune |
|----------|---------|--------------|
| `BIFRACT_INGEST_WORKERS` | `4` | ClickHouse CPU is underutilized during bulk imports |
| `BIFRACT_INGEST_QUEUE_SIZE` | `100` | Frequent `429` responses during ingestion spikes (increase for larger deployments) |
| `BIFRACT_INGEST_RATE_LIMIT` | `10000` | Legitimate sources are being rate-limited |
| `BIFRACT_INGEST_RATE_BURST` | `20000` | Bursty sources need higher peak throughput |
| `BIFRACT_MAX_BODY_SIZE` | `209715200` | Need to accept requests larger than 200MB |

On Kubernetes, `BIFRACT_INGEST_WORKERS` and `BIFRACT_INGEST_QUEUE_SIZE` are set per resource profile by `--install-k8s` rather than left at these defaults. See the [Sizing Guide](../getting-started/sizing.md#ingest-concurrency).
