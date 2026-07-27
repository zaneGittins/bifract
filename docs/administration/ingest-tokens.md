# API Keys

There are two types of API keys: generic keys and ingest tokens.

Ingest tokens authenticate external log senders and are scoped to a single fractal. They use the prefix `bifract_ingest_` and are managed from the fractal's **Ingest** tab. Every fractal gets a default ingest token when it is created.

Generic API keys authenticate programmatic access to the query, comment, alert, notebook, and dashboard APIs. They use the prefix `bifract_` and are managed from **[Fractal] > Manage > Access > API Keys** (or **[Prism] > Manage > Access** for prism-scoped keys). See the [API Reference](../api/authentication.md) for endpoint details.

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

For details on using API keys for ingestion, see the [API Reference](../api/ingestion.md).
