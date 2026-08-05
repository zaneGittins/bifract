# Alerts

```
GET    /api/v1/alerts
POST   /api/v1/alerts
GET    /api/v1/alerts/{id}
PUT    /api/v1/alerts/{id}
DELETE /api/v1/alerts/{id}
POST   /api/v1/alerts/import              Import alerts from YAML
POST   /api/v1/alerts/batch-toggle        Enable/disable many alerts at once
POST   /api/v1/alerts/{id}/duplicate
GET    /api/v1/alerts/{id}/executions     Firing history
```

## Actions

Actions are defined once and attached to alerts. Each type has its own CRUD collection.

```
GET|POST         /api/v1/webhooks               (see Webhooks)
GET|PUT|DELETE   /api/v1/webhooks/{id}
POST             /api/v1/webhooks/{id}/test

GET|POST         /api/v1/email-actions
GET|PUT|DELETE   /api/v1/email-actions/{id}
POST             /api/v1/email-actions/{id}/test
GET|POST         /api/v1/smtp-settings

GET|POST         /api/v1/fractal-actions
GET|PUT|DELETE   /api/v1/fractal-actions/{id}

GET|POST         /api/v1/dictionary-actions
GET|PUT|DELETE   /api/v1/dictionary-actions/{id}
```

## Feeds

```
GET|POST         /api/v1/feeds
GET|PUT|DELETE   /api/v1/feeds/{id}
POST             /api/v1/feeds/{id}/sync
GET              /api/v1/feeds/{id}/alerts
POST             /api/v1/feeds/{id}/alerts/enable-all
POST             /api/v1/feeds/{id}/alerts/disable-all
GET              /api/v1/alerts/feed                 Feed-sourced alerts (paginated)
POST             /api/v1/alerts/feed/batch-toggle
POST             /api/v1/alerts/{id}/toggle-feed
```

`GET /api/v1/alerts/feed` filters, sorts and pages in Postgres. Query parameters:

| Param | Values |
| --- | --- |
| `search` | matches name, description, rule path or query |
| `status` | `all`, `enabled`, `disabled` |
| `feed_id` | `all` or a feed UUID |
| `severity` | `all` or a Sigma level (`critical`...`informational`) |
| `label` | `all` or an exact label |
| `sort` / `dir` | `name`, `severity`, `exec_time`, `last_triggered` / `asc`, `desc` |
| `limit` / `offset` | page size (max 500, default 25) and row offset |
| `facets` | `1` to also return label/feed dropdown options and the unfiltered total |

Rows are a trimmed projection (no `query_string` or `references`); fetch
`GET /api/v1/alerts/{id}` for the full alert.

`POST /api/v1/alerts/feed/batch-toggle` accepts either `alert_ids` (max 5000) or a
`filter` object using the same fields as above, which toggles every matching alert
in the current scope.
