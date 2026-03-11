# Disk Quotas

Disk quotas let you cap how much raw log data a fractal can store. When a fractal reaches its quota, Bifract either rejects new logs or automatically deletes the oldest ones to make room.

## Configuration

Quotas are configured per fractal from **Settings > Fractals > [Fractal] > Manage > Disk Quota**.

| Field | Description |
|-------|-------------|
| **Quota (GB)** | Maximum raw log storage for this fractal. Leave blank for unlimited. |
| **Action** | What happens when the quota is reached: `Reject` or `Rollover`. |

The quota applies to the raw log bytes stored in ClickHouse (`sum(length(raw_log))`). Compressed on-disk size will be smaller, but the quota tracks uncompressed bytes for simplicity.

### API

```
PUT /api/v1/fractals/{id}/disk-quota
Content-Type: application/json

{
  "quota_bytes": 10737418240,
  "action": "reject"
}
```

Set `quota_bytes` to `null` to remove the quota. Valid `action` values are `"reject"` and `"rollover"`.

## Enforcement Modes

### Reject

New log batches are refused with `429 Too Many Requests` once the fractal's estimated usage plus the incoming batch would exceed the quota. Existing logs are not modified.

The client should treat 429 responses with a longer backoff than standard queue pressure. The `Retry-After` header is set to `30` seconds.

This mode is the default and is suitable when you want to preserve existing logs and signal upstream shippers to slow down or route elsewhere.

### Rollover

New logs are always accepted. After a successful insert, if estimated usage exceeds the quota, Bifract asynchronously deletes the oldest logs until usage is back at 80% of the quota. This provides headroom before the next deletion is triggered.

Rollover uses the ClickHouse primary key `(fractal_id, timestamp, log_id)` for an index-efficient scan to find the cutoff timestamp, then performs a lightweight delete of all logs at or before that point.

This mode is suitable for long-running streams where recent data is most valuable and you want zero rejection of incoming traffic.

## Accounting

Usage is tracked using a combination of Postgres-persisted statistics and in-memory deltas:

- **Base**: `size_bytes` from the `fractals` table, refreshed every 10 minutes by the background stats job.
- **Delta**: Bytes and log counts accumulated in memory since the last stats refresh. Updated immediately after each successful ClickHouse insert.
- **Estimated usage**: `base + delta` at the time of check or insert.

The quota cache itself is reloaded from Postgres every 5 minutes, so quota changes take effect within 5 minutes without a restart.

### Clearing Logs

When logs are cleared for a fractal (via **Settings > Danger Zone > Clear Logs**), the in-memory delta is reset immediately. This means the quota meter drops to near zero right away rather than waiting for the next 10-minute stats refresh.

## Limitations

- The quota is checked against an estimate, not an exact byte count. The estimate is accurate to within the last insert batch.
- Rollover deletion is asynchronous. There is a short window where usage can exceed the quota between a successful insert and the background deletion completing.
- Quota changes apply to new ingestion only. Existing data above the quota is not deleted when switching to `reject` mode.
