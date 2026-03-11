# Backup, Restore & Archives

Bifract provides two data protection features:

- **PostgreSQL backup/restore**: full system backup of configuration, users, alerts, and metadata via `bifractctl`.
- **ClickHouse archive/restore**: per-fractal log data archives via the web UI.

Both are encrypted with AES-256-GCM using the `BIFRACT_BACKUP_ENCRYPTION_KEY` generated during setup.

## Encryption Key

The encryption key is generated automatically by `bifractctl` during installation and stored in your `.env` file as `BIFRACT_BACKUP_ENCRYPTION_KEY`.

**Do not lose this key.** Without it, backups and archives cannot be decrypted. If you migrate or rebuild your Bifract instance, copy the key from your `.env` file.

## Storage

Backups and archives are stored to disk by default. S3-compatible storage (AWS S3, MinIO, DigitalOcean Spaces) is also supported.

### Disk Storage

- **Backups** (bifractctl): stored in `{install_dir}/backups/`
- **Archives** (web UI): stored in the `bifract-archives` Docker volume mounted at `/archives`

### S3 Storage

Configure these environment variables in your `.env` file to use S3:

| Variable | Description |
|----------|-------------|
| `BIFRACT_S3_ENDPOINT` | S3 endpoint URL (e.g., `https://s3.amazonaws.com` or MinIO URL) |
| `BIFRACT_S3_BUCKET` | Bucket name |
| `BIFRACT_S3_ACCESS_KEY` | Access key ID |
| `BIFRACT_S3_SECRET_KEY` | Secret access key |
| `BIFRACT_S3_REGION` | AWS region (e.g., `us-east-1`) |

When S3 is configured, both backups and archives will use S3 storage. If S3 is not configured, disk storage is used.

## PostgreSQL Backup/Restore

System-level backups are managed through the `bifractctl` CLI. These back up the entire PostgreSQL database including users, fractal configurations, alerts, saved searches, notebooks, and all metadata.

### Creating a Backup

```bash
bifractctl --backup --dir /opt/bifract
```

This creates an encrypted backup file in `{dir}/backups/` with the naming pattern `bifract-backup-{timestamp}.bifract-backup`.

For automated backups (e.g., cron), use non-interactive mode:

```bash
bifractctl --backup --dir /opt/bifract --non-interactive
```

### Listing Backups

```bash
bifractctl --list-backups --dir /opt/bifract
```

### Restoring a Backup

```bash
bifractctl --restore --dir /opt/bifract --restore-file /opt/bifract/backups/bifract-backup-20250101-120000.bifract-backup
```

The restore will: 1) decrypt and decompress the backup, 2) show the backup version and timestamp, 3) prompt for confirmation (unless `--non-interactive`), and 4) replace the current PostgreSQL database with the backup data.

**Note:** The Bifract containers must be running for backup and restore operations.

### Cron Example

```cron
0 2 * * * /usr/local/bin/bifractctl --backup --dir /opt/bifract --non-interactive >> /var/log/bifract-backup.log 2>&1
```

## ClickHouse Archives

Per-fractal log archives are managed through the web UI by fractal administrators.

### Creating an Archive

1. Navigate to your fractal's **Manage** tab
2. Scroll to the **Archives** section
3. Click **Create Archive**

The archive process runs in the background. The status will show as "Archiving" with a spinner while in progress, and "Completed" when done.

Archives include all log data for the fractal. If the fractal has a retention policy configured, only logs within the retention window are archived.

### Automatic Archive Scheduling

Fractals can be configured to create archives automatically on a schedule.

1. Navigate to your fractal's **Manage** tab
2. In the **Archive Schedule** section, select a frequency:
   - **Never** (default): no automatic archives
   - **Daily**: one archive per day
   - **Weekly**: one archive per week
   - **Monthly**: one archive per month
3. Optionally set **Maximum Archives** to limit how many archives are kept. When the limit is exceeded, the oldest archives are automatically deleted.

Scheduled archives use the fractal's retention window (if configured) to scope the archived data. If no retention period is set, the archive includes all logs.

A background scheduler checks every 5 minutes whether any fractals are due for a new archive. Scheduled archives appear in the archive list with a **Scheduled** badge; manual archives show **Manual**. The max archives limit applies to both types.

When both a retention period and archive schedule are configured on the same fractal, Bifract coordinates them automatically: retention enforcement adds a 1-day buffer and skips fractals with an active archive operation, ensuring logs are archived before they are deleted.

### Archive Format

Archives use a versioned NDJSON format, compressed with zstd and encrypted with AES-256-GCM. The first line is a version header (`{"_bifract_archive_version":1}`). Each subsequent line contains: timestamp, raw log, log ID, fractal ID, and ingest timestamp. Parsed fields are **not** stored in the archive; they are re-derived during restore by re-ingesting the raw logs through the normal ingestion pipeline. This dramatically reduces ClickHouse memory usage during archive creation by not reading the `fields` JSON column.

### Tuning

| Variable | Description | Default |
|----------|-------------|---------|
| `BIFRACT_ARCHIVE_MAX_MEMORY` | Per-query ClickHouse memory ceiling (bytes) for archive reads | `2000000000` (2GB) |

### Restoring an Archive

1. Navigate to the **Manage** tab for the fractal that owns the archive
2. Find the archive in the **Archives** section
3. Click **Restore**
4. Provide an **ingest API key** for the target fractal. The token determines which fractal logs are restored into. To restore into a different fractal, use a token from that fractal. The token's parser type (JSON, KV, syslog) must match the archived log format.
5. Optionally uncheck **Clear existing logs** to append instead of replace
6. Confirm the restore operation

Archives can be restored into any fractal you have admin access to by providing an ingest token scoped to that fractal. This is useful for DFIR workflows where you archive production data and later restore it into a dedicated investigation fractal.

The restore runs in the background. Logs are sent to the ingest endpoint in batches, subject to the same rate limiting and backpressure as normal ingestion. Progress can be monitored via the status indicator.

**Note:** Since restore goes through the ingestion pipeline, timestamps are re-extracted from the raw log data using the current token/normalizer configuration. If the configuration changed since the original ingest, timestamps may differ.

### Deleting an Archive

Click **Delete** next to any archive to remove it. This deletes both the database record and the stored archive file. This action cannot be undone.

## Limitations

- Only one archive or restore operation can run per fractal at a time (scheduled archives wait for active operations)
- Archives capture a single fractal's data; there is no multi-fractal archive
- PostgreSQL backups include the full database; selective table restore is not supported