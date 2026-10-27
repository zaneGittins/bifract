# Ingestion Tuning

How Bifract protects ClickHouse during ingest, and which settings to change when senders see `429` responses or ClickHouse sits idle.

## How backpressure works

```
Request --> Token check --> Healthy? --> Parse --> Enqueue --> Workers --> ClickHouse
               |               |                      |
             401             429                    429
```

Accepted logs are queued in memory and written to ClickHouse in batches by a worker pool. Every rejection is a `429` with a `Retry-After` header, so a well-behaved sender just retries. Bifract rejects when any of these hold:

| Check | Trips at | Clears at |
|---|---|---|
| Failed inserts | 3 consecutive ClickHouse insert failures | 30 s after the last failure |
| ClickHouse CPU | Above 80% for 6 polls (30 s) | Below 60% |
| ClickHouse memory | Above 90% for 3 polls (15 s); reads the pod's cgroup limit on Kubernetes | Below 80% |
| ClickHouse disk | Above 90% used | Below 80% |
| Buffered bytes | In-process buffer above `BIFRACT_INGEST_BUFFER_BYTES` | Buffer drains |
| Queue depth | Batch would take the queue past half its slots | Queue drains |
| Archive spool | 90% of spool capacity, when the [Iceberg archive](../administration/backup-restore.md#iceberg-archive) is on | Below 70% |
| Fractal disk quota | Fractal is over its quota and set to reject | Space freed or quota raised |
| Rate limit | More than `BIFRACT_INGEST_RATE_LIMIT` requests/second from one client IP | Next second |

The health checks (failures, CPU, memory, disk, spool) run before the body is read, so an overloaded server does not spend CPU parsing. Each queue slot holds at most 5,000 logs; larger requests are split across slots. The archive spool is fail-closed: a batch that cannot be spooled is rejected, never acknowledged.

## Settings

| Variable | Default | Change when |
|----------|---------|-------------|
| `BIFRACT_INGEST_WORKERS` | `4` | ClickHouse CPU is idle during bulk imports |
| `BIFRACT_INGEST_QUEUE_SIZE` | `100` | Frequent `429`s during short spikes |
| `BIFRACT_INGEST_RATE_LIMIT` | `10000` | Legitimate senders hit the per-IP request limit |
| `BIFRACT_INGEST_RATE_BURST` | `20000` | Bursty senders need a higher peak |
| `BIFRACT_MAX_BODY_SIZE` | `209715200` (200 MB) | Requests larger than 200 MB; Caddy also caps the ingest port at 200 MiB |
| `BIFRACT_INGEST_BUFFER_BYTES` | `268435456` (256 MB) | Buffered-bytes rejections while memory is free |

On Kubernetes, `--install-k8s` sets `BIFRACT_INGEST_WORKERS` and `BIFRACT_INGEST_QUEUE_SIZE` per resource profile. See the [Sizing Guide](../getting-started/sizing.md#ingest-concurrency).
