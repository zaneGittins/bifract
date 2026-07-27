# Fractals

```
GET    /api/v1/fractals
POST   /api/v1/fractals           (admin)
GET    /api/v1/fractals/{id}
PUT    /api/v1/fractals/{id}      (admin)
DELETE /api/v1/fractals/{id}      (admin)
POST   /api/v1/fractals/{id}/select
GET    /api/v1/fractals/{id}/stats
POST   /api/v1/fractals/stats/refresh  (admin)
```

## Lifecycle

```
PUT    /api/v1/fractals/{id}/retention          (admin)  Hot-store retention in days
PUT    /api/v1/fractals/{id}/archive-retention  (admin)  Iceberg archive retention
PUT    /api/v1/fractals/{id}/disk-quota         (admin)  Quota bytes + action
```

## Permissions

```
GET    /api/v1/fractals/{id}/permissions
POST   /api/v1/fractals/{id}/permissions            Grant a user or group a role
PUT    /api/v1/fractals/{id}/permissions/{permId}
DELETE /api/v1/fractals/{id}/permissions/{permId}
```

## Keys

```
GET|POST         /api/v1/fractals/{id}/api-keys
GET|PUT|DELETE   /api/v1/fractals/{id}/api-keys/{keyId}
POST             /api/v1/fractals/{id}/api-keys/{keyId}/toggle

GET|POST         /api/v1/fractals/{id}/ingest-tokens
GET|PUT|DELETE   /api/v1/fractals/{id}/ingest-tokens/{tokenId}
POST             /api/v1/fractals/{id}/ingest-tokens/{tokenId}/toggle
```

## Prisms

Prisms are views across several fractals and expose a parallel set of routes.

```
GET|POST         /api/v1/prisms
GET|PUT|DELETE   /api/v1/prisms/{id}
POST             /api/v1/prisms/{id}/select
POST             /api/v1/prisms/{id}/members
DELETE           /api/v1/prisms/{id}/members/{fractalID}
GET|POST         /api/v1/prisms/{id}/permissions
PUT|DELETE       /api/v1/prisms/{id}/permissions/{permId}
GET|POST         /api/v1/prisms/{id}/api-keys
GET|PUT|DELETE   /api/v1/prisms/{id}/api-keys/{keyId}
POST             /api/v1/prisms/{id}/api-keys/{keyId}/toggle
```

## Groups

```
GET|POST         /api/v1/groups                          (admin)
GET|PUT|DELETE   /api/v1/groups/{id}                     (admin)
GET|POST         /api/v1/groups/{id}/members             (admin)
DELETE           /api/v1/groups/{id}/members/{username}   (admin)
```
