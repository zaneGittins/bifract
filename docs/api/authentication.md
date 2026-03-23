# Authentication

Most endpoints require authentication via session cookie or API key.

## Session (browser)

```
POST /api/v1/auth/login
Content-Type: application/json

{"username": "admin", "password": "changeme"}
```

Sets an `HttpOnly` session cookie valid for 24 hours.

```
POST /api/v1/auth/logout
```

## API Key

Include the key in one of two ways:

```
Authorization: Bearer bifract_<key>
X-API-Key: bifract_<key>
```

API keys are scoped to a specific fractal and have granular permissions. See [API Keys](../administration/ingest-tokens.md) for details.

**Create API key:**

```
POST /api/v1/fractals/{id}/api-keys  (fractal admin)
```

```json
{
  "name": "CI Pipeline",
  "description": "Read-only query access",
  "expires_at": "2026-06-01T00:00:00Z",
  "permissions": {
    "query": true,
    "comment": false,
    "alert_manage": false,
    "notebook": false,
    "dashboard": false
  }
}
```

Omitted permissions default to: `query: true`, `comment: true`, others `false`.

**Update API key permissions:**

```
PUT /api/v1/fractals/{id}/api-keys/{keyId}  (fractal admin)
```

```json
{
  "permissions": {
    "query": true,
    "comment": true,
    "alert_manage": true,
    "notebook": true,
    "dashboard": true
  }
}
```

Only known permission keys with boolean values are accepted. Unknown keys return `400 Bad Request`.
