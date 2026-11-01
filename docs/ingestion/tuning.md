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
| Failed inserts | 3 consecutive failed ClickHouse insert attempts | 30 s after the last failure |
| ClickHouse CPU | Above 80% for 6 polls (30 s) | Below 60% |
| ClickHouse memory | Above 90% for 3 polls (15 s); reads the pod's cgroup limit on Kubernetes | Below 80% |
| ClickHouse disk | Above 90% used | Below 80% |
| Buffered bytes | In-process buffer above `BIFRACT_INGEST_BUFFER_BYTES` | Buffer drains |
| Queue depth | Batch would take the queue past half its slots | Queue drains |
| Archive spool | 90% of spool capacity, when the [Iceberg archive](../administration/backup-restore.md#iceberg-archive) is on | Below 70% |
| Fractal disk quota | Fractal is over its quota and set to reject | Space freed or quota raised |
| Rate limit | More than `BIFRACT_INGEST_RATE_LIMIT` requests/second from one client IP | Next second |

The health checks (failures, CPU, memory, disk, spool) run before the body is read, so an overloaded server does not spend CPU parsing. Each queue slot holds at most 5,000 logs; larger requests are split across slots. The archive spool is fail-closed: a batch that cannot be spooled is rejected, never acknowledged.

## What a 200 means

A `200` means the logs are in the ingest buffer (and in the archive spool, when it is on). They are written to ClickHouse within the flush interval (30 seconds by default, `BIFRACT_INGEST_FLUSH_SECONDS`). A failed insert is retried with backoff for about 2 minutes, which rides out a ClickHouse restart, and new requests get `429` meanwhile. Except on sharded clusters, every attempt carries the same deduplication token, so a retry after an insert that committed but reported an error writes no duplicate rows. An error a retry cannot fix, such as a type mismatch, gets 3 attempts. On shutdown, buffered logs are flushed; if ClickHouse is failing, the flush gives up after about 15 seconds. Logs are dropped only when retries run out or a batch cannot be encoded. Each drop is logged, counted in the insert error metrics and raises the Ingest Insert Errors notification. A crash or forced kill loses logs still in the buffer; with the archive on they remain in the spool.

Alerts and model state never read past logs that are still buffered or retrying. Each ingest and app process reports, every 5 seconds, the oldest receive time it has not yet written, and readers stop there. Normally that trails real time by at most the flush interval. While inserts are retrying it holds back, and alerts fire late rather than miss the logs. A process that stops reporting holds readers for up to 5 minutes, then is dropped. **System > Alert engine** shows how far it trails and which process is holding it.

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
