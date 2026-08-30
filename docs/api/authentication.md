# Authentication

Bifract accepts three credentials. Which one you use depends on what is calling.

## API key

A program calls Bifract with an API key, sent as a bearer token:

```
Authorization: Bearer bifract_<key>
```

`X-API-Key: bifract_<key>` is accepted as an alternative header.

A key belongs to one fractal or one prism and carries the role its creator granted it, so
a key made for a reporting job cannot reach another team's data. An instance-wide key is
the exception: it administers the whole instance, belongs to no scope, carries the
`bifract_admin_` prefix, and always expires. Create and revoke keys under
**Admin > API Keys**, or through the API itself; see [All Operations](reference.md) for
the shape of those requests, and [API Keys](../administration/ingest-tokens.md) for what
the roles mean.

## Ingest token

Log shippers use a separate credential with a `bifract_ingest_` prefix. It is accepted only
on the ingestion endpoints and is scoped to the single fractal its logs land in, so a
compromised shipper cannot read anything back. See [Sending Logs](ingestion.md).

## Session

The web UI signs in with a username and password and receives an `HttpOnly` cookie valid
for 24 hours. Sessions exist for the browser: prefer an API key for anything scripted, so
the credential can be scoped, rotated, and revoked without touching a user account.

## Choosing a scope

A scoped key is already bound to its fractal or prism, so it needs nothing further. A
session is not, because one browser session moves between fractals, and neither is an
instance-wide key. Requests made with either say which scope they mean:

```
X-Bifract-Scope: fractal:<fractal-id>
X-Bifract-Scope: prism:<prism-id>
X-Bifract-Scope: none
```

The header is authorized on every request, never trusted on its own: asking for a scope you
cannot reach answers `403`, not the data.

`none` states that the caller holds no scope, which is not the same as omitting the header.
Omitting it falls back to the session's last selected scope, so a page that holds no scope
must send `none` rather than be answered from a scope it is not on.

## When authentication fails

Failures answer with the standard error envelope, and the `code` is what a program should
branch on. The message is for a human and may be reworded.

```json
{"success": false, "error": "Invalid username or password", "code": "unauthenticated"}
```

`unauthenticated` means the credential is missing, malformed, or wrong. `forbidden` means it
was understood but does not grant what the operation needs.
