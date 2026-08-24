# Backup, Restore & Archive

Bifract separates two concerns:

- **PostgreSQL backup/restore**: full backup of configuration, users, alerts, saved searches, notebooks, and metadata via the `bifract` CLI. Encrypted with AES-256-GCM.
- **Iceberg archive**: a full, engine-independent copy of **all log data** written to object storage as [Apache Iceberg](https://iceberg.apache.org/) (Parquet + metadata), independent of ClickHouse. This is the durable system of record for disaster recovery and long-term retention.

!!! info "ClickHouse is the hot store; Iceberg is the record of truth"
    With archiving enabled, ClickHouse holds a **bounded hot window** (governed by each fractal's retention) and the Iceberg archive holds the **full history**. Because logs are captured at ingest into a durable spool *before* they reach ClickHouse, the archive is the most complete copy — a ClickHouse loss is recoverable by restoring from Iceberg.

## Encryption Key (Postgres backups)

The encryption key is generated automatically by `bifract` during installation and stored in your `.env` file as `BIFRACT_BACKUP_ENCRYPTION_KEY`.

**Do not lose this key.** Without it, Postgres backups cannot be decrypted. If you migrate or rebuild your Bifract instance, copy the key from your `.env` file.

## Postgres Backup/Restore

Configuration backups are managed through the `bifract` CLI. These back up the entire PostgreSQL database including users, fractal configurations, alerts, saved searches, notebooks, and all metadata. Postgres backups **do not** contain log data.

!!! warning "Postgres backup is required even with archiving"
    The Iceberg **catalog** (the map from each fractal to its data files) lives in Postgres. Losing Postgres without a backup makes the archived Parquet hard to reassemble. Archiving makes Postgres backup *more* important, not less.

Backups are stored to disk by default (`{install_dir}/backups/`). S3-compatible storage (AWS S3, MinIO, DigitalOcean Spaces) is also supported via these `.env` variables:

| Variable | Description |
|----------|-------------|
| `BIFRACT_S3_ENDPOINT` | S3 endpoint URL (e.g., `https://s3.amazonaws.com`) |
| `BIFRACT_S3_BUCKET` | Bucket name |
| `BIFRACT_S3_ACCESS_KEY` | Access key ID |
| `BIFRACT_S3_SECRET_KEY` | Secret access key |
| `BIFRACT_S3_REGION` | Region (e.g., `us-east-1`) |

### Creating a Backup

```bash
bifract --backup --dir /opt/bifract
```

This creates an encrypted backup file in `{dir}/backups/` with the naming pattern `bifract-backup-{timestamp}.bifract-backup`.

### Restoring a Backup

```bash
bifract --restore --dir /opt/bifract --restore-file /opt/bifract/backups/bifract-backup-20250101-120000.bifract-backup
```

**Note:** The Bifract containers must be running for backup and restore operations.

### Cron Example

```cron
0 2 * * * /usr/local/bin/bifract --backup --dir /opt/bifract --non-interactive >> /var/log/bifract-backup.log 2>&1
```

## Iceberg Archive

The archive is **off by default**. When enabled, a `bifract-archiver` sidecar continuously copies every ingested log to object storage as per-fractal Iceberg tables, on any of four backends: **local disk, MinIO, S3, or Azure Blob**.

### How it works

1. **Tee at ingest.** After a log batch is normalized, it is appended to a durable local **spool** on a shared volume *before* the request is acked (fail-closed: if the spool write fails, ingest returns `429` rather than losing data). It then continues to ClickHouse as normal.
2. **Archiver sidecar.** The archiver tails the spool, batches records into Parquet, rolls files on size or time, uploads to object storage, and commits Iceberg metadata to a Postgres-backed catalog. One Iceberg table per fractal, partitioned by ingest date.
3. **Maintenance.** A scheduled job compacts small Parquet files and expires old snapshots to keep metadata bounded.

The archiver ships in the same image as the server (run with a different command). It is **dormant but present**: `bifract --upgrade` / `--upgrade-k8s` provisions the archiver and spool volume, but no archiving happens until you enable it.

### Enabling

The machinery must be provisioned first (present after an install/upgrade on a version that includes it). Then enable it **either way**:

- **Admin UI:** *Admin → Settings → Archive → Iceberg Archive* toggle. (Greyed out until provisioned.)
- **Env / secret:** set `BIFRACT_ARCHIVE_ENABLED=true` (k8s: the `ARCHIVE_ENABLED` secret).

Toggling takes effect within seconds — no redeploy.

### Configuration

Set on the `bifract-archiver` service (compose) or the `bifract-secrets` Secret (k8s):

| Variable | Default | Description |
|----------|---------|-------------|
| `BIFRACT_ARCHIVE_ENABLED` | `false` | Master on/off (also toggleable from the admin UI) |
| `BIFRACT_ARCHIVE_BACKEND` | `disk` | `disk`, `s3`, `minio`, or `azure` |
| `BIFRACT_ARCHIVE_PREFIX` | | Optional path prefix within the bucket/container/dir |
| `BIFRACT_ARCHIVE_SPOOL_MAX_BYTES` | `10 GiB` | Spool capacity; ingest applies backpressure (`429`) near it |
| `BIFRACT_ARCHIVE_ROLL_BYTES` | `128 MiB` | Roll a Parquet file once a fractal has buffered this much log data. Measured on the uncompressed payload, so the file it writes is smaller by whatever compression that data achieves; each commit logs both numbers |
| `BIFRACT_ARCHIVE_ROLL_INTERVAL` | `30m` | ...or after this long, whichever first |

=== "S3 / MinIO"

    | Variable | Description |
    |----------|-------------|
    | `BIFRACT_ARCHIVE_S3_ENDPOINT` | Custom endpoint (required for MinIO; omit for AWS) |
    | `BIFRACT_ARCHIVE_S3_BUCKET` | Bucket name |
    | `BIFRACT_ARCHIVE_S3_REGION` | Region (e.g. `us-east-1`) |
    | `BIFRACT_ARCHIVE_S3_ACCESS_KEY` / `_SECRET_KEY` | Credentials |

=== "Azure Blob"

    | Variable | Description |
    |----------|-------------|
    | `BIFRACT_ARCHIVE_AZURE_ACCOUNT` | Storage account name |
    | `BIFRACT_ARCHIVE_AZURE_KEY` | Account key |
    | `BIFRACT_ARCHIVE_AZURE_CONTAINER` | Container name |
    | `BIFRACT_ARCHIVE_AZURE_ENDPOINT` | Optional custom endpoint (e.g. Azurite) |

=== "Local disk"

    | Variable | Description |
    |----------|-------------|
    | `BIFRACT_ARCHIVE_DISK_PATH` | Directory for the Iceberg warehouse (single-node/testing) |

!!! note "Object storage for multi-node"
    On Kubernetes (or any multi-node deployment) use `s3`, `minio`, or `azure`. The `disk` backend is pod-local and cannot be read back by ClickHouse for restore.

### Status

*System → Archive* shows the live state: enabled/disabled, backend, spool usage + backpressure, archived fractal count, total archived size, and last-commit time (with a liveness indicator for the sidecar).

### Clear catalog

*Admin → Settings → Danger Zone → Clear Iceberg Catalog* resets the archive to zero so you can start fresh. Disable archiving first (the button is guarded otherwise).

This drops the archived tables from the catalog but does **not** delete the data files in object storage. Empty the bucket/container yourself afterwards to reclaim space and to keep leftover files from interfering with new archiving.

### Restore & Reconcile

Two modes replay archived data back into ClickHouse for an **ingest-time** window:

- **Restore** inserts archived rows for the window, skipping any `log_id` already present (idempotent).
- **Reconcile** compares the hot-store and archive counts first and restores only the rows ClickHouse is missing (heals a gap).

!!! note "The window is ingest time, not event time"
    Both the archive and the `logs` table are partitioned by ingest date, so an ingest-time window lets ClickHouse read only the partitions you asked for on either side instead of scanning the whole table. This matches how Recall queries the archive. In practice ingest time and event time differ only by your pipeline's lag.

Each job runs as **one chunk per ingest day**, and records a cursor after every chunk. That means a restore is resumable: if it fails, is cancelled, or its worker restarts, the completed days are not replayed. Use **Resume** on the job to continue from the first unfinished day. It also keeps each insert bounded rather than issuing one unbounded statement across the whole window.

Restored rows land in the normal `logs` table with the same typed JSON fields, so BQL queries and skip indexes work exactly as on freshly-ingested data. Restore requires an object-storage backend so ClickHouse can read the Iceberg tables directly (the `disk` backend is pod-local and unreadable).

#### From the admin UI (recommended)

*System → Archive → Restore from Archive*: pick one or more fractals, a time range, and the mode, then start it. Each fractal becomes an **async job** that a `bifract-archiver` process claims and runs, with a live progress bar and row count. Jobs are claimed by exactly one archiver even when several are running. Pending **and running** jobs can be cancelled: the owning worker interrupts the in-flight insert within a few seconds. Rows already written stay put, so re-running the same window in restore (dedup) mode picks up where it left off. If a worker dies mid-restore, its job is failed automatically once it stops heartbeating rather than sitting at "running" forever.

!!! warning "Restore is heavy"
    Replaying a large window re-inserts potentially billions of rows, which drives ClickHouse CPU up and can trigger ingest backpressure. Throughput is bounded by MergeTree merge cost, so a restore takes roughly as long as ingesting the same data did. Restore the narrowest window you need, and see [Recovery time](#recovery-time) before planning a multi-month replay.

#### From the CLI

Equivalent one-off operations (a container or k8s Job) for scripting or when the archiver is not running:

```bash
# Replay an ingest-time window from Iceberg back into ClickHouse (idempotent; dedups on log_id)
bifract-archiver restore --fractal <fractal-id> --from 2026-01-01 --to 2026-02-01

# Heal a gap: restore only what ClickHouse is missing for the window
bifract-archiver reconcile --fractal <fractal-id> --from 2026-01-01 --to 2026-02-01
```

Both log a resume point as each ingest day completes. If a run is interrupted, re-run it with `--from` set to the last reported resume point to continue instead of starting over.

### Maintenance

Compaction, snapshot expiry, and cleanup run continuously in a dedicated singleton service (`bifract-archiver maintain-loop`), deployed as the `bifract-archiver-maintain` compose service or the `bifract-archive-maintain` Deployment on Kubernetes. It runs scheduled passes on a timer (`BIFRACT_ARCHIVE_MAINTAIN_INTERVAL`, default `1h`) and a Postgres advisory lock guarantees only one pass runs at a time. This is mandatory at high volume to keep the number of Parquet files and Iceberg snapshots bounded.

`bifract-archiver maintain` runs a single pass and exits, for ad-hoc or manual invocation.

A routine pass plans compaction only over partitions ingested within `BIFRACT_ARCHIVE_MAINTAIN_LOOKBACK` (default `72h`), which keeps its planning cost proportional to recent ingest rather than to how much history the archive holds. Because `ingest_date` partitions are sealed the moment the day rolls over and never receive files again, that window covers everything a pass can usefully do. Every `BIFRACT_ARCHIVE_DEEP_COMPACTION_INTERVAL` (default `24h`) one pass plans the whole table instead, picking up anything an earlier pass abandoned at its byte budget or deadline before it aged out of the window. Set the interval to `0` to disable the deep pass and keep every pass strictly bounded.

## Disaster Recovery

Decide your DR posture explicitly. The two systems cover different things:

- **Log data (ClickHouse):** the **Iceberg archive** is your durable, engine-independent copy. A full ClickHouse loss does not lose data, but see [recovery time](#recovery-time) below before assuming the whole hot store can be rebuilt on an incident timeline. To protect the archive against accidental or malicious deletion, enable object-storage **versioning + object lock (WORM)** and, if needed, **cross-region replication** on the archive bucket/container. This is configured on the bucket, not in Bifract.
- **Configuration/metadata (Postgres):** covered by the `bifract` Postgres backup above — and it also holds the Iceberg catalog. Run it on a schedule and store it off-box (S3).

### Recovery time

Durability and recovery speed are different guarantees. Your data is safe in the archive; getting it back into ClickHouse is the slow part, and the limit is not object-storage bandwidth.

Replaying archived rows means re-inserting them into a MergeTree, which pays the same background merge cost the original ingest paid. Restore skips log parsing and normalization, but it re-runs every materialized view and every merge. **Restoring N days costs roughly what ingesting N days cost.** Adding shards raises read and insert throughput but does not remove the merge floor.

Practical consequences at **Large** (500 GB–2 TB/day) and **X-Large**:

- Restoring 90 days is a **days-to-weeks** operation, not an overnight one.
- The hot store may not physically hold it. Shard disks are usually sized for the retention window, not full history.
- Rows older than 7 days arrive without `raw_log`. It lives in a separate `logs_raw` table with a 7-day TTL and is not addressable from BQL anyway, so deep restores return normalized fields only. This does not affect search: `norm_log` and the typed JSON fields are what queries read.

Plan DR around the window you actually need online:

1. **Restore the recent operational window** (days, not months) to get search, alerting, and dashboards working again. This is what restore is designed for and it completes in a practical timeframe.
2. **Query the deep history in place with Recall** (the fractal's **Recall** tab), which reads Iceberg directly and never writes to ClickHouse, so it has no merge cost and no retention ceiling.

Restore the narrowest window that meets the need. If you are reaching for a multi-month restore to answer an investigative question, Recall is almost certainly the better tool.
