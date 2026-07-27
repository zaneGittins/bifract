# Dashboards

```
GET    /api/v1/dashboards
POST   /api/v1/dashboards
GET    /api/v1/dashboards/{id}
PUT    /api/v1/dashboards/{id}
DELETE /api/v1/dashboards/{id}

POST   /api/v1/dashboards/{id}/widgets
PUT    /api/v1/dashboards/{id}/widgets/{widget_id}
PUT    /api/v1/dashboards/{id}/widgets/{widget_id}/layout
DELETE /api/v1/dashboards/{id}/widgets/{widget_id}
```

## Execution

```
POST   /api/v1/dashboards/{id}/execute                       Run every widget
POST   /api/v1/dashboards/{id}/widgets/{widget_id}/execute   Run one widget
PUT    /api/v1/dashboards/{id}/variables
PUT    /api/v1/dashboards/{id}/refresh-interval
```

## Collaboration

```
GET    /api/v1/dashboards/{id}/events      SSE stream of live edits
GET    /api/v1/dashboards/{id}/presence
POST   /api/v1/dashboards/{id}/presence
```

## Import & Export

```
GET    /api/v1/dashboards/{id}/export      YAML
POST   /api/v1/dashboards/import
```

## Shared Links

Read-only, no-auth public links serving cached results. Disabled globally until an admin enables them.

```
GET    /api/v1/dashboards/{id}/shared-links
POST   /api/v1/dashboards/{id}/shared-links
DELETE /api/v1/dashboards/{id}/shared-links/{link_id}
```
