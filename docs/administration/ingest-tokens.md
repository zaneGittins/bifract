# API Keys

There are two types of API keys: generic keys and ingest tokens.

Ingest tokens authenticate external log senders and are scoped to a single fractal. They use the prefix `bifract_ingest_` and are managed from the fractal's **Ingest** tab. Every fractal gets a default ingest token when it is created.

Generic API keys authenticate programmatic access to the query, comment, alert, notebook, and dashboard APIs. They use the prefix `bifract_` and are managed from **[Fractal] > Manage > Access > API Keys** (or **[Prism] > Manage > Access** for prism-scoped keys). Every issued key, whatever its scope, is also listed under **Admin > API Keys**. See the [API Reference](../api/authentication.md) for endpoint details.

- **Create key**: Provide a name, optional description, expiration, and the role the key holds. The full key value is shown only once at creation. Store it securely.
- **Toggle key**: Enable or disable a key without deleting it.
- **Delete key**: Permanently revoke a key.

## Roles

A key is authorized exactly like a person: it holds one role on the fractal or prism it was issued for. Follow the principle of least privilege and issue the lowest role that does the job.

| Role | Description |
|------|-------------|
| `viewer` | Read logs, comments, alerts, notebooks, and dashboards |
| `analyst` | Everything a viewer can do, plus create and edit them |
| `admin` | Everything an analyst can do, plus manage the scope itself |

## Instance-wide keys

A tenant admin key administers the whole instance, so it belongs to no fractal or prism and holds no scope role. It uses the prefix `bifract_admin_`, must carry an expiry, and is issued only from **Admin > API Keys**. Because it is not bound to a scope, a request made with one names the fractal or prism it means with the `X-Bifract-Scope` header, the same way a browser session does.

Scoped and instance-wide are exclusive: a key is one or the other, and neither can be converted into the other after it is issued.

For details on using API keys for ingestion, see the [API Reference](../api/ingestion.md).
