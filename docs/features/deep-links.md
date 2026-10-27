# Deep Links

A deep link is a URL that opens Bifract with a BQL query loaded and run. Use one wherever a tool can build a string (an EDR rule, a SOAR playbook, a webhook payload, a runbook) so an alert lands the analyst on the exact search.

## The URL

```
https://bifract.example.com/go/search?q=<query>&fractal=<name>&from=-24h
```

Everything is plain and percent-encoded, so a link is readable and hand-editable.

| Parameter | Required | Value |
|---|---|---|
| `q` | yes (or `q64`) | The BQL query, percent-encoded |
| `q64` | | The query in base64 or base64url, instead of `q` |
| `fractal` | | Fractal name or id. Defaults to your default fractal, or your only one |
| `prism` | | Prism name or id, instead of `fractal` |
| `from` | | Start of the window. Default `-24h` |
| `to` | | End of the window. Default `now` |
| `var.<name>` | | Value for `@<name>` in the query |

### Time

`from` and `to` accept:

- **Relative**: `-24h`, `now-24h`, `-90m`, `-3w`. Units `s`, `m`, `h`, `d`, `w`.
- **Absolute**: RFC 3339 (`2026-08-01T00:00:00Z`) or a Unix timestamp in seconds or milliseconds.
- **`all`**: `from=all` searches all retained data.

A relative window ending at `now` stays relative, so the link means the same thing whenever it is opened. Anything else is pinned to absolute times, so an incident link keeps pointing at that incident.

### Variables

Pass each [BQL variable](../bql/basics.md) as its own parameter. Variables not in the query are ignored.

```
/go/search?q=computer_name%3D%40host%20%7C%20head(50)&var.host=web-01
```

## Examples

PowerShell with encoded commands, last 24 hours:

```
/go/search?q=image%3D~powershell%20%7C%20commandline%3D~encodedcommand
```

A provenance graph for one process, in a named fractal:

```
/go/search?q=pgr(start%3D%22%7B1a2b-3c4d%7D%22)%20%7C%20pgraph()&fractal=endpoints
```

A fixed incident window:

```
/go/search?q=computer_name%3D%22web-01%22&from=2026-08-01T14:00:00Z&to=2026-08-01T18:00:00Z
```

## Copy a link from the Query page

1. Run a query on the **Query** page.
2. Click **Share**, then **Copy share link**.

The copied link is a `/go/search` URL like the ones above. The address bar also tracks your search: each new query is a browser history entry, so Back re-runs the previous query.

## Behavior

| Case | What happens |
|---|---|
| No session | The browser goes to login, then on to the link |
| Fractal or prism you cannot access | Reported the same as one that does not exist, so links cannot enumerate fractals |
| Malformed link | An error page names the problem: missing `q`, bad `from`, both `fractal` and `prism`, or no `fractal` when you can see several and none is default |
| Reload | The link stays in the address bar; refreshing re-runs it |

## Limits

Queries up to 4000 characters, variable values up to 512, and at most 32 variables. Use a saved query or notebook for anything larger.
