# Backup, Restore & Archives

Bifract provides two data protection features:

- **PostgreSQL backup/restore**: full system backup of configuration, users, alerts, and metadata via `bifract`.
- **ClickHouse archive/restore**: per-fractal log data archives via the web UI.

Both are encrypted with AES-256-GCM using the `BIFRACT_BACKUP_ENCRYPTION_KEY` generated during setup.

## Encryption Key

The encryption key is generated automatically by `bifract` during installation and stored in your `.env` file as `BIFRACT_BACKUP_ENCRYPTION_KEY`.

**Do not lose this key.** Without it, backups and archives cannot be decrypted. If you migrate or rebuild your Bifract instance, copy the key from your `.env` file.

## Storage

Backups and archives are stored to disk by default. S3-compatible storage (AWS S3, MinIO, DigitalOcean Spaces) is also supported.

### Disk Storage

- **Backups** (bifract): stored in `{install_dir}/backups/`
- **Archives** (web UI): stored in the `bifract-archives` Docker volume mounted at `/archives`

### S3 Storage

Configure these environment variables in your `.env` file to use S3:

| Variable | Description |
|----------|-------------|
| `BIFRACT_S3_ENDPOINT` | S3 endpoint URL (e.g., `https://s3.amazonaws.com`) |
| `BIFRACT_S3_BUCKET` | Bucket name |
| `BIFRACT_S3_ACCESS_KEY` | Access key ID |
| `BIFRACT_S3_SECRET_KEY` | Secret access key |
| `BIFRACT_S3_REGION` | Region (e.g., `us-east-1`) |

When S3 is configured, both backups and archives will use S3 storage. If S3 is not configured, disk storage is used.

## Postgres Backup/Restore

Configuration backups are managed through the `bifract` CLI. These back up the entire PostgreSQL database including users, fractal configurations, alerts, saved searches, notebooks, and all metadata. Postgres backups **do not** contain log data.

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

## ClickHouse Archives

Per-fractal log archives are managed through the web UI by fractal administrators.

### Creating an Archive

1. Navigate to your fractal's **Manage** tab
2. Scroll to the **Archives** section
3. Click **Create Archive**

The archive process runs in the background. The status will show as "Archiving" with a spinner while in progress, and "Completed" when done.

### Tuning

| Variable | Description | Default |
|----------|-------------|---------|
| `BIFRACT_ARCHIVE_MAX_MEMORY` | Per-query ClickHouse memory ceiling (bytes) for archive reads | `3000000000` (3GB) |
| `BIFRACT_ARCHIVE_MAX_DURATION` | Maximum wall-clock time an archive is allowed to run (Go duration string) | `24h` |
| `BIFRACT_ARCHIVE_MAX_ERROR_TIME` | Maximum cumulative time spent waiting on retries before the archive fails (Go duration string) | `30m` |

### Deleting an Archive

Click **Delete** next to any archive to remove it. This deletes both the database record and the stored archive file. This action cannot be undone.