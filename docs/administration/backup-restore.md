# Backup, Restore & Archive

Bifract separates two concerns:

- **PostgreSQL backup/restore**: a full backup of configuration, users, alerts, saved searches, notebooks, and metadata via the `bifract` CLI, encrypted with AES-256-GCM. It contains no log data.
- **Iceberg archive**: a full, engine-independent copy of **all log data** in object storage as [Apache Iceberg](https://iceberg.apache.org/) (Parquet + metadata), independent of ClickHouse.

!!! info "ClickHouse is the hot store; Iceberg is the record of truth"
    With archiving enabled, ClickHouse holds a **bounded hot window** (each fractal's retention) and the archive holds the **full history**. Logs are captured at ingest into a durable spool *before* they reach ClickHouse, so the archive is the most complete copy and a ClickHouse loss is recoverable from it.

## Encryption Key (Postgres backups)

`bifract` generates the key during installation and stores it in your `.env` file as `BIFRACT_BACKUP_ENCRYPTION_KEY`.

**Do not lose this key.** Without it, Postgres backups cannot be decrypted. Copy it from `.env` before migrating or rebuilding an instance.

## Postgres Backup/Restore

!!! warning "Postgres backup is required even with archiving"
    The Iceberg **catalog** (the map from each fractal to its data files) lives in Postgres. Losing Postgres without a backup makes the archived Parquet hard to reassemble. Archiving makes Postgres backup *more* important, not less.

Backups go to disk by default (`{install_dir}/backups/`). S3-compatible storage (AWS S3, MinIO, DigitalOcean Spaces) is also supported via these `.env` variables:

| Variable | Description |
|----------|-------------|
| `BIFRACT_S3_ENDPOINT` | S3 endpoint URL (e.g., `https://s3.amazonaws.com`) |
| `BIFRACT_S3_BUCKET` | Bucket name |
| `BIFRACT_S3_ACCESS_KEY` | Access key ID |
| `BIFRACT_S3_SECRET_KEY` | Secret access key |
| `BIFRACT_S3_REGION` | Region (e.g., `us-east-1`) |

The containers must be running for backup and restore.

### Creating a Backup

```bash
bifract --backup --dir /opt/bifract
```

Writes `{dir}/backups/bifract-backup-{timestamp}.bifract-backup`.

### Restoring a Backup

```bash
bifract --restore --dir /opt/bifract --restore-file /opt/bifract/backups/bifract-backup-20250101-120000.bifract-backup
```

### Cron Example

```cron
0 2 * * * /usr/local/bin/bifract --backup --dir /opt/bifract --non-interactive >> /var/log/bifract-backup.log 2>&1
```

## Iceberg Archive

The archive is **off by default**. When enabled, a `bifract-archiver` sidecar continuously copies every ingested log to object storage as per-fractal Iceberg tables, on local disk, MinIO, S3, or Azure Blob.

### How it works

1. **Tee at ingest.** A normalized batch is appended to a durable local **spool** on a shared volume *before* the request is acked, then continues to ClickHouse as normal. A failed spool write returns `429` rather than losing data.
2. **Archiver sidecar.** The archiver tails the spool, batches records into Parquet, rolls files on size or time, uploads them, and commits Iceberg metadata to a Postgres-backed catalog. One table per fractal, partitioned by ingest date.
3. **Maintenance.** A scheduled job compacts small Parquet files and expires old snapshots to keep metadata bounded.

The archiver ships in the server image and runs a different command. `bifract --upgrade` / `--upgrade-k8s` provisions it and the spool volume, but nothing is archived until you enable it.

### Enabling

Once provisioned, enable it either way. Toggling takes effect within seconds, with no redeploy.

- **Admin UI:** *Admin → Settings → Archive → Iceberg Archive*. Greyed out until provisioned.
- **Env / secret:** `BIFRACT_ARCHIVE_ENABLED=true` (k8s: the `ARCHIVE_ENABLED` secret).

### Configuration

Set on the `bifract-archiver` service (compose) or the `bifract-secrets` Secret (k8s):

| Variable | Default | Description |
|----------|---------|-------------|
| `BIFRACT_ARCHIVE_ENABLED` | `false` | Master on/off, also toggleable from the admin UI |
| `BIFRACT_ARCHIVE_BACKEND` | `disk` | `disk`, `s3`, `minio`, or `azure` |
| `BIFRACT_ARCHIVE_PREFIX` | | Optional path prefix within the bucket/container/dir |
| `BIFRACT_ARCHIVE_SPOOL_MAX_BYTES` | `10 GiB` | Spool capacity; ingest applies backpressure (`429`) near it |
| `BIFRACT_ARCHIVE_ROLL_BYTES` | `128 MiB` | Roll a Parquet file once a fractal has buffered this much. Measured uncompressed, so the file written is smaller by whatever compression that data achieves; each commit logs both numbers |
| `BIFRACT_ARCHIVE_ROLL_INTERVAL` | `30m` | ...or after this long, whichever comes first |

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
    On Kubernetes, or any multi-node deployment, use `s3`, `minio`, or `azure`. The `disk` backend is pod-local: ClickHouse cannot read it back, so restore is unavailable on it.

### Status

*System → Archive* shows enabled/disabled, backend, spool usage and backpressure, archived fractal count, total archived size, and last-commit time with a liveness indicator for the sidecar.

### Clear catalog

*Admin → Settings → Danger Zone → Clear Iceberg Catalog* resets the archive to zero. Disable archiving first; the button is guarded otherwise.

This drops the archived tables from the catalog but does **not** delete the data files. Empty the bucket or container yourself afterwards to reclaim space and to keep leftover files from interfering with new archiving.

### Restore & Reconcile

Two modes replay archived data back into ClickHouse for an **ingest-time** window:

- **Restore** inserts archived rows for the window, skipping any `log_id` already present (idempotent).
- **Reconcile** compares hot-store and archive counts first, then restores only the rows ClickHouse is missing.

Restored rows land in the normal `logs` table with the same typed JSON fields, so BQL queries and skip indexes work exactly as on freshly-ingested data.

Each job runs one chunk per ingest day and records a cursor after every chunk, so a failed, cancelled, or restarted job does not replay completed days. Use **Resume** to continue from the first unfinished day. It also keeps each insert bounded instead of issuing one statement across the whole window.

!!! note "The window is ingest time, not event time"
    Both the archive and the `logs` table are partitioned by ingest date, so an ingest-time window reads only the partitions you asked for on either side instead of scanning the whole table. This matches how Recall queries the archive. In practice the two differ only by your pipeline's lag.

#### From the admin UI (recommended)

*System → Archive → Restore from Archive*: pick one or more fractals, a time range, and the mode, then start it. Each fractal becomes an async job with a live progress bar and row count.

- A job is claimed by exactly one archiver, even when several are running.
- Pending **and** running jobs can be cancelled. The owning worker interrupts the in-flight insert within a few seconds, and rows already written stay put, so re-running the window in restore (dedup) mode picks up where it left off.
- A worker that dies mid-restore has its job failed once it stops heartbeating, rather than sitting at "running" forever.

!!! warning "Restore is heavy"
    A large window re-inserts potentially billions of rows, driving ClickHouse CPU up and possibly triggering ingest backpressure. Read [Recovery time](#recovery-time) before planning a multi-month replay.

#### From the CLI

Equivalent one-off operations (a container or k8s Job) for scripting, or when the archiver is not running:

```bash
# Replay an ingest-time window from Iceberg back into ClickHouse (idempotent; dedups on log_id)
bifract-archiver restore --fractal <fractal-id> --from 2026-01-01 --to 2026-02-01

# Heal a gap: restore only what ClickHouse is missing for the window
bifract-archiver reconcile --fractal <fractal-id> --from 2026-01-01 --to 2026-02-01
```

Both log a resume point as each ingest day completes. Re-run an interrupted job with `--from` set to the last reported resume point.

### Maintenance

Compaction, snapshot expiry, and cleanup run in a dedicated singleton service, `bifract-archiver maintain-loop`: the `bifract-archiver-maintain` compose service, or the `bifract-archive-maintain` Deployment on Kubernetes. A Postgres advisory lock guarantees one pass at a time. This is mandatory at high volume to keep the number of Parquet files and Iceberg snapshots bounded.

| Variable | Default | Description |
|----------|---------|-------------|
| `BIFRACT_ARCHIVE_MAINTAIN_INTERVAL` | `1h` | How often a pass runs |
| `BIFRACT_ARCHIVE_MAINTAIN_LOOKBACK` | `72h` | How far back a routine pass plans compaction |
| `BIFRACT_ARCHIVE_DEEP_COMPACTION_INTERVAL` | `24h` | How often one pass plans the whole table instead. `0` disables it |

The lookback keeps planning cost proportional to recent ingest rather than to total history, and covers everything a pass can usefully do: `ingest_date` partitions are sealed the moment the day rolls over and never receive files again. The periodic deep pass picks up whatever an earlier pass abandoned at its byte budget or deadline before it aged out of the window.

`bifract-archiver maintain` runs a single pass and exits, for ad-hoc invocation.

## Disaster Recovery

The two systems cover different things:

- **Log data (ClickHouse):** the **Iceberg archive** is your durable, engine-independent copy. Protect it against accidental or malicious deletion with object-storage **versioning + object lock (WORM)**, and cross-region replication if you need it. That is configured on the bucket, not in Bifract.
- **Configuration/metadata (Postgres):** covered by the Postgres backup above, which also holds the Iceberg catalog. Run it on a schedule and store it off-box.

### Recovery time

Durability and recovery speed are different guarantees. The data is safe in the archive; getting it back into ClickHouse is the slow part, and the limit is not object-storage bandwidth.

Replaying archived rows re-inserts them into a MergeTree, which pays the same background merge cost the original ingest paid. Restore skips parsing and normalization but re-runs every materialized view and every merge. **Restoring N days costs roughly what ingesting N days cost.** Adding shards raises read and insert throughput but does not remove the merge floor.

Practical consequences at **Large** (500 GB to 2 TB/day) and **X-Large**:

- Restoring 90 days is a **days-to-weeks** operation, not an overnight one.
- The hot store may not physically hold it. Shard disks are usually sized for the retention window, not full history.
- Rows older than 7 days arrive without `raw_log`. It lives in a separate `logs_raw` table with a 7-day TTL and is not addressable from BQL anyway, so deep restores return normalized fields only. Search is unaffected: `norm_log` and the typed JSON fields are what queries read.

Plan DR around the window you actually need online:

1. **Restore the recent operational window** (days, not months) to get search, alerting, and dashboards working again.
2. **Query the deep history in place with Recall** (the fractal's **Recall** tab), which reads Iceberg directly and never writes to ClickHouse, so it has no merge cost and no retention ceiling.

Restore the narrowest window that meets the need. If you are reaching for a multi-month restore to answer an investigative question, Recall is the better tool.
