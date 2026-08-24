# Disk Quotas

Disk quotas let you cap how much raw log data a fractal can store. When a fractal reaches its quota, Bifract either rejects new logs or automatically deletes the oldest ones to make room.

## Configuration

Quotas are configured per fractal from **[Fractal] > Manage > Lifecycle**.

| Field | Description |
|-------|-------------|
| **Quota (GB)** | Maximum raw log storage for this fractal. Leave blank for unlimited. |
| **Action** | What happens when the quota is reached: `Reject` or `Rollover`. |

Quota usage is estimated from ClickHouse part metadata, not by scanning logs, so checking it is cheap and always reflects the real table.


## Enforcement Modes

### Reject

New log batches are refused with `429 Too Many Requests` once the fractal's estimated usage plus the incoming batch would exceed the quota. Existing logs are not modified.

The client should treat 429 responses with a longer backoff than standard queue pressure. The `Retry-After` header is set to `30` seconds.

This mode is the default and is suitable when you want to preserve existing logs and signal upstream shippers to slow down or route elsewhere.

### Rollover

New logs are always accepted. A background sweep runs once a minute, and for any rollover fractal over its quota it drops the oldest data until usage is back at 80% of the quota. This provides headroom before the next trim is triggered.

Rollover drops whole ClickHouse **partitions** (one fractal per ingest day each), which is a near-instant metadata operation rather than a row-level mutation. Because it works a day at a time, actual usage after a trim can sit noticeably below the 80% target when a fractal has few, large days. Oldest means oldest *ingested*, so a trim evicts the data you have held longest rather than whichever day carries the oldest event timestamps.

Usage is read fresh from ClickHouse part metadata on every sweep, so the trim is self-correcting and needs no running total. In a multi-replica deployment a Postgres advisory lock ensures exactly one app pod runs the sweep at a time.

This mode is suitable for long-running streams where recent data is most valuable and you want zero rejection of incoming traffic.