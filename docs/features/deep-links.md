# Deep Links

A deep link is a URL that opens Bifract with a specific BQL query already loaded and executed. Any tool that can build a string can build one: an EDR detection rule, a SOAR playbook, a webhook payload, a wiki page, a runbook.

The point is to remove the copy-and-paste step between a detection firing and an analyst looking at the surrounding data. Instead of "search Bifract for the process GUID in this alert", the alert carries a link that lands on exactly that view.

## The URL

```
https://bifract.example.com/go/search?q=<query>&fractal=<name>&from=-24h
```

Everything is plain and percent-encoded, so a link is readable and hand-editable. There is no base64 to construct and no internal identifier to look up.

## Parameters

| Parameter | Required | Value |
|---|---|---|
| `q` | yes | The BQL query, percent-encoded |
| `q64` | | The BQL query in base64 or base64url, as an alternative to `q` |
| `fractal` | | Fractal name or id. Defaults to your default fractal |
| `prism` | | Prism name or id, instead of `fractal` |
| `from` | | Start of the window. Defaults to `-24h` |
| `to` | | End of the window. Defaults to `now` |
| `var.<name>` | | Value bound to `@<name>` in the query |

### Time

`from` and `to` accept three forms:

- **Relative**: `-24h`, `now-24h`, `-90m`, `-3w`. Units are `m`, `h`, `d`, `w`.
- **Absolute**: RFC 3339 (`2026-08-01T00:00:00Z`) or a Unix timestamp in seconds or milliseconds.
- **`all`**: `from=all` searches all retained data.

A relative window ending at `now` stays relative, so the link means the same thing whenever it is opened. Anything else is pinned to absolute timestamps, so a link to a specific incident keeps pointing at that incident.

### Variables

Values for [BQL variables](../bql/basics.md) are passed one parameter each, which every template system can emit:

```
/go/search?q=host%3D%22%40host%22%20%7C%20head%2050&var.host=web-01
```

Only variables that appear in the query are bound. The rest are ignored.

## Examples

Recent PowerShell with encoded commands, last 24 hours:

```
/go/search?q=process_name%3D%22powershell.exe%22%20AND%20cmdline~%22-enc%22
```

A process tree rooted at a specific GUID, in a named fractal:

```
/go/search?q=pgr(start%3D%22%7B1a2b-3c4d%7D%22)%20%7C%20pgraph()&fractal=endpoints
```

A fixed incident window:

```
/go/search?q=host%3D%22web-01%22&from=2026-08-01T14:00:00Z&to=2026-08-01T18:00:00Z
```

## Behavior

**Authentication.** Deep links are authenticated. Opening one without a session sends the browser to the login page and then on to the link, so an expired session costs a password and not the link itself.

**Access.** Fractal and prism names resolve within what the signed-in user can see. A name the user has no access to is reported the same way as a name that does not exist, so links cannot be used to enumerate fractals.

**Errors.** A malformed link renders an error page naming the problem rather than dropping the user on an empty search screen. The common causes are a missing `q`, an unparseable `from`, and an ambiguous `fractal` when the account can see more than one.

**Reload.** After the query runs, the link stays in the address bar. Refreshing re-runs it, and the URL can be copied straight out of the browser.

## Sharing from the UI

Everything above also describes what the Query tab hands out. **Share -> Copy share link** builds a `/go/search` URL for whatever you are looking at, so a pasted link is one a colleague can read and edit rather than an opaque blob.

The address bar tracks the search as you work. Each query you run becomes its own entry in browser history, so Back returns to the previous query and re-runs it, and Forward moves on again. Re-running the same query does not add an entry. Leaving the Query tab drops the query from the URL.

Two details worth knowing when you share what is in your address bar:

- A relative window stays relative. A link on `-24h` means "the last 24 hours" whenever it is opened, not the 24 hours you were looking at.
- An absolute window stays absolute, so a link to a specific incident keeps pointing at that incident.

## Limits

The query is capped at 4000 characters, variable values at 512, and a link may carry at most 32 variables. Anything larger belongs in a saved query or a notebook rather than in a URL.
