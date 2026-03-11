# API Keys

There are two types of API keys: generic keys and ingest tokens.

Ingest tokens authenticate external log senders and are scoped to a single fractal. They use the prefix `bifract_ingest_`.

Admins manage generic API keys from the **API Keys** tab within a fractal's settings. Generic API keys can be used to access all parts of the system, see the [API Reference](../api.md) for more info.

- **Create key**: Provide a name and optional description. The full key value is shown only once at creation. Store it securely.
- **Toggle key**: Enable or disable a key without deleting it.
- **Delete key**: Permanently revoke a key.

For details on using API keys for ingestion, see the [API Reference](../api.md#log-ingestion).
