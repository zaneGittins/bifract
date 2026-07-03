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

- **Admin UI:** *Settings → Admin → Limits → Iceberg Archive* toggle. (Greyed out until provisioned.)
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
| `BIFRACT_ARCHIVE_ROLL_BYTES` | `128 MiB` | Roll a Parquet file at this size |
| `BIFRACT_ARCHIVE_ROLL_INTERVAL` | `1h` | ...or after this long, whichever first |

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

### Restore & Reconcile

Restore is a deliberate operation run from the archiver binary (a one-off container or k8s Job):

```bash
# Replay an event-time window from Iceberg back into ClickHouse (idempotent; dedups on log_id)
bifract-archiver restore --fractal <fractal-id> --from 2026-01-01 --to 2026-02-01

# Heal a gap: restore only what ClickHouse is missing for the window
bifract-archiver reconcile --fractal <fractal-id> --from 2026-01-01 --to 2026-02-01
```

Restored rows land in the normal `logs` table with the same typed JSON fields, so BQL queries and skip indexes work exactly as on freshly-ingested data. (Restore requires an object-storage backend so ClickHouse can read the Iceberg tables directly.)

### Maintenance

Compaction, snapshot expiry, and cleanup run as a singleton `maintain` pass (a k8s CronJob, or `bifract-archiver maintain`). It is mandatory at high volume to keep the number of Parquet files and Iceberg snapshots bounded.

## Disaster Recovery

Decide your DR posture explicitly. The two systems cover different things:

- **Log data (ClickHouse):** the **Iceberg archive** is your durable, engine-independent copy — a full ClickHouse loss is recoverable by restoring from Iceberg (`bifract-archiver restore`). For protection against logical deletion or ransomware, enable object-storage **versioning + object lock (WORM)** and, if needed, **cross-region replication** on the archive bucket/container. This is configured on the bucket, not in Bifract.
- **Configuration/metadata (Postgres):** covered by the `bifract` Postgres backup above — and it also holds the Iceberg catalog. Run it on a schedule and store it off-box (S3).
