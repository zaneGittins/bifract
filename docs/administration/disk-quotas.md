# Disk Quotas

Disk quotas let you cap how much raw log data a fractal can store. When a fractal reaches its quota, Bifract either rejects new logs or automatically deletes the oldest ones to make room.

## Configuration

Quotas are configured per fractal from **[Fractal] > Manage > Lifecycle**.

| Field | Description |
|-------|-------------|
| **Quota (GB)** | Maximum raw log storage for this fractal. Leave blank for unlimited. |
| **Action** | What happens when the quota is reached: `Reject` or `Rollover`. |

The quota applies to the raw log bytes stored in ClickHouse. Compressed on-disk size will be smaller, but the quota tracks uncompressed bytes for simplicity.


## Enforcement Modes

### Reject

New log batches are refused with `429 Too Many Requests` once the fractal's estimated usage plus the incoming batch would exceed the quota. Existing logs are not modified.

The client should treat 429 responses with a longer backoff than standard queue pressure. The `Retry-After` header is set to `30` seconds.

This mode is the default and is suitable when you want to preserve existing logs and signal upstream shippers to slow down or route elsewhere.

### Rollover

New logs are always accepted. After a successful insert, if estimated usage exceeds the quota, Bifract asynchronously deletes the oldest logs until usage is back at 80% of the quota. This provides headroom before the next deletion is triggered.

Rollover uses the ClickHouse primary key `(fractal_id, timestamp, log_id)` for an index-efficient scan to find the cutoff timestamp, then performs a lightweight delete of all logs at or before that point.

This mode is suitable for long-running streams where recent data is most valuable and you want zero rejection of incoming traffic.