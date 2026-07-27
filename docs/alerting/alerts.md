# Alerts

Alerts run BQL queries on a schedule and trigger actions on hits. A background ticker (default 60 seconds, configurable from **Admin &rarr; Settings &rarr; Query &amp; Alerting &rarr; Alert evaluation interval**) evaluates all enabled alerts using a cursor-based approach on the ingest timestamp. Each alert tracks `last_evaluated_at`, so no logs are missed across restarts. Changing the interval takes effect on the next tick, with no restart required.

Re-enabling a previously disabled alert resets its cursor to a few minutes before now rather than resuming from its old, potentially stale value — this avoids a large cold-storage catch-up scan across the disabled window, at the cost of not retroactively evaluating logs that arrived while the alert was disabled.

## Alert Configuration

| Field | Description |
|-------|-------------|
| Name | Display name for the alert. Supports `{{field}}` templates, resolved from the first matching result |
| Description | Free-text context shown in the UI and included in webhook payloads |
| Query | BQL query to evaluate |
| Type | `event` (per-match) or `compound` (threshold-based) |
| Severity | Severity label carried through to actions |
| Labels | Tags for organization and filtering (e.g. `sigma:high`, `product:windows`) |
| References | External links for context (e.g. MITRE ATT&CK URLs) |
| Throttle | Suppression window in seconds, optionally per-value via a **throttle field** (e.g. throttle per `src_ip` rather than globally) |
| Actions | One or more actions to run on a hit (see below) |

An alert has no single "webhook URL" field. Actions are defined once and attached to any number of alerts.

## Actions

| Action | What it does |
|--------|--------------|
| **Webhook** | POSTs the alert payload to an HTTP endpoint. See [Webhooks](../api/webhooks.md) for the payload schema and configuration |
| **Email** | Sends an email via the configured SMTP settings |
| **Fractal** | Writes the alert result back into a fractal as new log events, so detections are themselves searchable and can feed other alerts |
| **Dictionary** | Upserts matched values into a [dictionary](../features/dictionaries.md), building a live watchlist (e.g. accumulating suspicious IPs for later enrichment) |

Actions are managed from the fractal's **Alerts** tab and can be attached to multiple alerts.

## Auto-Projection

Alert queries that consist only of filter conditions (no `table()`, `groupby()`, or other pipeline commands) are automatically optimized. Instead of reading all columns from ClickHouse, Bifract projects only:

- `timestamp` and `log_id` (always included)
- The specific fields referenced in the query's WHERE conditions
- The alert's throttle field, if configured
- Any `{{field}}` placeholders in the alert name template

This significantly reduces ClickHouse disk I/O for alerts that filter on a small number of fields, which is common with Sigma rules imported via [Alert Feeds](alert-feeds.md). A typical Sigma rule referencing 3-5 fields avoids reading dozens of unused columns on every evaluation tick.

**What this means for actions:** Webhook payloads, fractal actions, and dictionary actions will only contain the projected fields, not the full log. The `log_id` is always present so the original log can be retrieved. If an action needs additional fields, add an explicit `table()` to the alert query:

```
image=/powershell/i | table(image, user, commandline, timestamp, log_id)
```

**When auto-projection is skipped:** Queries that contain any pipeline command (`table()`, `groupby()`, `multi()`, `match()`, etc.) or field assignments (`:=`) are never modified. Regular user search queries are also unaffected.

## Settings

| Setting | Default | Description |
|---------|---------|-------------|
| Alert evaluation interval | `60s` | How often the alert ticker runs, minimum 60s. |
| Alert query timeout | `5s` | Maximum runtime for a single alert query. Alerts that exceed it are disabled automatically, with the reason recorded on the alert. |

Both live under **Admin &rarr; Settings &rarr; Query &amp; Alerting** (admin only). Neither is an environment variable: they are stored in PostgreSQL and take effect without a restart.
