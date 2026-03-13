# API Keys

There are two types of API keys: generic keys and ingest tokens.

Ingest tokens authenticate external log senders and are scoped to a single fractal. They use the prefix `bifract_ingest_`.

Admins manage generic API keys from the **API Keys** tab within a fractal's settings. See the [API Reference](../api/authentication.md) for endpoint details.

- **Create key**: Provide a name, optional description, expiration, and permissions. The full key value is shown only once at creation. Store it securely.
- **Toggle key**: Enable or disable a key without deleting it.
- **Delete key**: Permanently revoke a key.

## Permissions

Each API key has granular permissions that control what it can access. Follow the principle of least privilege and only enable what is needed.

| Permission | Default | Description |
|-----------|---------|-------------|
| `query` | Enabled | Execute BQL queries against logs |
| `comment` | Enabled | Create and manage comments on logs |
| `alert_manage` | Disabled | Create, update, and delete alerts |
| `notebook` | Disabled | Create, edit, and manage notebooks |
| `dashboard` | Disabled | Create, edit, and manage dashboards |

Permissions are set during key creation and can be updated via the API. The overview table shows each key's active permissions at a glance.

When a permission is disabled, the API key receives a `403 Forbidden` response for that operation. Permissions are enforced server-side regardless of how the key is used.

For details on using API keys for ingestion, see the [API Reference](../api/ingestion.md).
