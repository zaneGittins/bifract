# Alerts

Alerts run BQL queries on a schedule and trigger actions on hits. A background ticker (default 30 seconds, `BIFRACT_ALERT_EVAL_INTERVAL`) evaluates all enabled alerts using a cursor-based approach on the ingest timestamp. Each alert tracks `last_evaluated_at`, so no logs are missed across restarts.

Admins manage alerts from the **Alerts** page within a fractal.

- **Create/edit alerts**: Define a BQL query, schedule interval, and webhook destination.
- **Alert types**: `event` alerts fire once per matching log. `compound` alerts fire when the result count crosses a threshold.
- **Import alerts**: Bulk import alerts from YAML via the import dialog.
- **Execution history**: View past alert runs and their results from the alert detail page.
- **Bulk operations**: Filter alerts by severity or label, then bulk enable or disable the filtered set.

## Alert Configuration

| Field | Description |
|-------|-------------|
| Name | Display name for the alert |
| Query | BQL query to evaluate |
| Type | `event` (per-match) or `compound` (threshold-based) |
| Webhook URL | Destination for alert notifications |
| Labels | Tags for organization and filtering (e.g. `sigma:high`, `product:windows`) |
| References | External links for context (e.g. MITRE ATT&CK URLs) |

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

This opt-out is per-alert and disables auto-projection entirely, giving full control over which fields are returned.

**When auto-projection is skipped:** Queries that contain any pipeline command (`table()`, `groupby()`, `multi()`, `match()`, etc.) or field assignments (`:=`) are never modified. Regular user search queries are also unaffected.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BIFRACT_ALERT_EVAL_INTERVAL` | `30s` | How often the alert ticker runs |
