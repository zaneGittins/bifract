# Backup, Restore & Cold Storage

Bifract separates two concerns:

- **PostgreSQL backup/restore**: full backup of configuration, users, alerts, saved searches, notebooks, and metadata via the `bifract` CLI. Encrypted with AES-256-GCM.
- **Cold storage tiering**: per-fractal tiering of old log data to low-cost object storage (S3 or Azure Blob), where it stays fully searchable in place via BQL.

!!! warning "Cold storage is not a backup"
    Tiering moves log data to cheaper storage but it is still *live* data: a dropped partition, bad migration, or corruption destroys it just the same. Cold storage covers cost and capacity, not disaster recovery. See [Disaster recovery](#disaster-recovery) for where DR responsibility actually sits.

## Encryption Key (Postgres backups)

The encryption key is generated automatically by `bifract` during installation and stored in your `.env` file as `BIFRACT_BACKUP_ENCRYPTION_KEY`.

**Do not lose this key.** Without it, Postgres backups cannot be decrypted. If you migrate or rebuild your Bifract instance, copy the key from your `.env` file.

## Postgres Backup/Restore

Configuration backups are managed through the `bifract` CLI. These back up the entire PostgreSQL database including users, fractal configurations, alerts, saved searches, notebooks, and all metadata. Postgres backups **do not** contain log data.

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

## Cold Storage Tiering

Cold storage moves a fractal's old log partitions from local (hot) disk to object storage. The data stays in the same `logs` table and remains queryable with the same BQL; only the underlying storage changes. Queries that reach into cold data are correct but slower, and the search UI shows a small **cold storage** badge when a time range includes tiered data.

Movement is driven hourly: for each fractal with a cold threshold set, partitions older than that threshold are moved to the cold volume (`ALTER TABLE logs MOVE PARTITION ... TO VOLUME 'cold'`). Because the `logs` table is partitioned by `(fractal_id, day)`, tiering is exact per fractal and per day.

### Enabling Cold Storage

Cold storage is opt-in and disabled by default.

**1. Define the storage policy.** Edit `clickhouse/config.d/storage.xml` (mounted into the ClickHouse container). It is inert by default. Copy the body of either `storage.s3.xml.example` or `storage.azure.xml.example` (in the same directory) into it. The hot volume **must be named `default`** (with the `default` disk): ClickHouse only allows switching a table onto a new policy that still contains the volume from its current policy, and the implicit default policy's volume is named `default`.

**2. Provide credentials** as environment variables on the **clickhouse** service (consumed by `storage.xml` via `from_env`):

=== "S3 / S3-compatible"

    | Variable | Description |
    |----------|-------------|
    | `BIFRACT_COLD_STORAGE_ENDPOINT` | Full object-storage URL incl. bucket and optional prefix, e.g. `https://my-bucket.s3.us-east-1.amazonaws.com/cold/` |
    | `BIFRACT_S3_ACCESS_KEY` | Access key ID |
    | `BIFRACT_S3_SECRET_KEY` | Secret access key |
    | `BIFRACT_S3_REGION` | Region (blank for MinIO) |

=== "Azure Blob"

    | Variable | Description |
    |----------|-------------|
    | `BIFRACT_AZURE_STORAGE_URL` | Blob service URL, e.g. `https://myaccount.blob.core.windows.net/` |
    | `BIFRACT_AZURE_CONTAINER` | Container name |
    | `BIFRACT_AZURE_STORAGE_ACCOUNT` | Account name |
    | `BIFRACT_AZURE_STORAGE_KEY` | Account key |

**3. Tell the app to enable it.** Set `BIFRACT_COLD_STORAGE_BACKEND=s3` (or `azure`) on the bifract app. At startup the app switches the `logs` table onto the `tiered` storage policy.

**4. Set per-fractal thresholds.** In each fractal's **Manage → Lifecycle** tab, set **Move to cold storage after**. It must be less than the deletion (retention) period, or logs would be deleted before they tier.

On Kubernetes, inject the same `storage.xml` and credentials into the ClickHouse pods via your operator's config-file mechanism, and set `COLD_STORAGE_BACKEND` (and the cold credential keys) in the `bifract-secrets` Secret. See `deploy/k8s/clickhouse/clickhouse-installation.yaml`.

### Performance Notes

- The object-storage disk is wrapped in a local read-through **cache** disk (`max_size` in `storage.xml`, default 50 GiB) so cold queries don't fetch from object storage on every granule. Size it to your local disk.
- Skip indexes and the full-text index travel with the data to cold storage, so indexed BQL searches stay accelerated on cold data.
- A `move_factor` safety net auto-moves the oldest parts to cold if hot disk approaches full, independent of per-fractal thresholds.

### Disabling Cold Storage

Once the `logs` table is on the `tiered` policy, ClickHouse pins it there (the policy can't be removed by `ALTER`, and the server won't start if the policy is undefined). So:

- **Pause tiering (recommended):** set `BIFRACT_COLD_STORAGE_BACKEND=none` (k8s: the `COLD_STORAGE_BACKEND` secret) and re-run reconfigure. New data stops tiering; existing cold data stays searchable. **Keep the cold credentials / `storage.xml` in place** so the policy stays defined.
- **Reclaim the bucket:** additionally `ALTER TABLE logs DROP PARTITION ...` the cold partitions. The (now empty) cold disk must remain defined.
- **Remove completely:** rebuild the `logs` table onto the `default` policy (new table, copy hot data, drop, rename). Only then is it safe to clear the cold credentials / delete `storage.xml`.

Do not delete `storage.xml` (or the cold credentials on k8s) while `logs` is still on `tiered` — ClickHouse will fail to start.

### Changing the Cold Target

Treat the cold bucket/account as fixed once data is tiered. Cold parts are bound to the disk's endpoint, so changing the bucket/account/region or switching S3↔Azure makes existing cold data unreadable (the objects are stranded in the old store). The tooling will repoint the disk on reconfigure without warning. To actually move to a new target: `ALTER TABLE logs MOVE PARTITION ... TO VOLUME 'default'` for all cold partitions (needs local disk space), change the credentials, then let it re-tier.

## Disaster Recovery

Decide your DR posture explicitly. Tiering and Postgres backups cover different things:

- **Log data (ClickHouse):** the cold tier gives you object-store **durability** (protection against local disk loss), but not protection against logical deletion, a bad migration, or ransomware. For that, enable object-storage-level **versioning + object lock (WORM)** and, if needed, **cross-region replication** on your cold bucket/container. This is an infrastructure responsibility, configured on the bucket, not in Bifract. Note that a ClickHouse cluster loss is not recoverable from the cold bucket alone, because ClickHouse keeps part metadata on its own nodes; for point-in-time recovery of log data use ClickHouse's native `BACKUP ... TO S3`.
- **Configuration/metadata (Postgres):** covered by the `bifract` Postgres backup above. Run it on a schedule and store it off-box (S3).
