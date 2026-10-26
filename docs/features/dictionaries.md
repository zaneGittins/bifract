# Lookups

Lookups are per-fractal tables for enriching log data at query time. They appear on the fractal's **Lookups** page and BQL refers to one by name as `dict=`. Define a lookup with key-value columns, then use `match()` in BQL queries to join it onto your results.

## Creating a Lookup

Open **Lookups** in the fractal's sidebar. Click **+ New Lookup** and provide:

- **Name** - must be a valid identifier (alphanumeric and underscores)
- **Key column** - the primary lookup column name
- **Description** (optional)
- **Global** (optional) - makes the lookup available to all fractals

## Managing Data

The editor provides a spreadsheet-like interface:

- **Add columns** using the `+` button in the table header
- **Mark columns as keys** to enable lookups on non-primary columns (required for `match()`)
- **Edit cells** by double-clicking; press Enter to save, Escape to cancel
- **Import CSV** via drag-and-drop or file picker. New columns in the CSV are added automatically
- **Reload** forces ClickHouse to refresh its cached copy immediately. Adding, editing, and deleting rows already refresh it

## Filling a Lookup from a URL

A lookup can fetch its rows from a URL and refresh them on a schedule, so a published list such as a threat feed or a tool inventory stays current without re-importing it. When creating a lookup, an administrator sets **Rows** to **Fetched from a URL**. On an existing lookup, **Fetch from URL** replaces its rows the same way.

| Setting | Meaning |
|---------|---------|
| URL | An `http` or `https` address. The server fetches it, so it needs outbound access |
| Key column | The column in the file holding the key. For a one-value-per-line file it names the column the values land in |
| Format | CSV, TSV, JSON array of objects, NDJSON, or one value per line. **Detect** reads it from the file. Gzip is unpacked either way |
| Other columns to keep | Comma-separated file columns, each of which must exist. Empty keeps them all |
| Split key cells on | Splits one key cell into a row per value, each carrying the record's other columns |
| Keys are globs | Patterns lists only: converts `*` and `?` wildcards to expressions. See [Glob keys](#glob-keys) |
| Refresh every | 15 minutes to a week in the UI, 5 minutes to 7 days through the API. Default one hour |

Each refresh replaces every row with what the file holds, in the file's order, which on a Patterns list decides which expression answers first. An unchanged file is detected and skipped.

- A file that can't be fetched, holds no usable rows, or names a missing or reserved column changes nothing: the existing rows stay and the error shows on the lookup. A failed refresh is retried after 5 minutes, doubling with each further failure up to the refresh interval.
- A load that fails after it started keeps the old rows alongside what it loaded, and the next refresh loads again. Changing the key column empties the lookup until a load succeeds; other columns marked as keys stay keys.
- Records whose key is empty or invalid for the lookup's kind are skipped and counted.
- A load that takes longer than 45 seconds continues in the background after the page returns.

The rows of a URL lookup are read-only: editing, importing, and alert dictionary actions are refused, since the next refresh would undo them. Columns can still be marked as keys. **Detach** stops the refreshes and makes the rows editable again.

| Who | Can |
|-----|-----|
| Administrator | Set, change, or detach a source |
| Analyst | **Refresh now**, which fetches immediately, at most once a minute |

By default a source can reach only public addresses; turn on **Lookup sources on the internal network** under **Settings > Security** to allow private and loopback addresses. Cloud metadata addresses are always refused, and HTTP proxy settings are not used. A file may be at most 64 MiB, expand to at most 512 MiB, and hold at most 2 million rows, or 100,000 for a Patterns lookup, since every pattern is compiled on every ClickHouse node.

### Glob keys

A glob converts to an expression that matches ignoring case. `*` matches any run of characters and `?` any one, both across path separators. A glob without a `\` or `/` matches the last segment of a path:

| Glob | Matches |
|------|---------|
| `aeroadmin.exe` | `C:\Tools\AeroAdmin.exe`, `aeroadmin.exe` |
| `tigervnc*.exe` | `C:\bin\tigervnc-1.12.exe` |
| `C:\Users\*\AppData\Local\RemSupp\*` | anything under any user's `RemSupp` folder |

A glob of only wildcards would match everything and is skipped.

For example, the [LOLRMM](https://lolrmm.io) tool list keeps several comma-separated install paths per tool. A Patterns lookup named `lolrmm` over `https://raw.githubusercontent.com/magicsword-io/LOLRMM/main/website/public/api/rmm_tools.csv`, keyed on `InstallationPaths`, keeping `Name,Description`, split on `,`, with glob keys, flags processes running from a known remote-management tool's path:

```
* | match(dict="lolrmm", field=image, column=InstallationPaths, include=[Name,Description], require=true)
```

## Using Lookups in Queries

Use `match()` to enrich query results. See [Enrichment](../bql/enrichment.md) for full syntax.

```
* | match(dict="threat_intel", field=src_ip, column=ip, include=[threat_score,category])
```

The `column` parameter must reference a column marked as a key in the editor.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `dict` | Yes | Lookup name |
| `field` | Yes | Log field to look up |
| `column` | Yes | Key column to match against |
| `include` | Yes | Columns to add to results |
| `strict` | No | If `true`, filter out non-matching rows (default: `false`) |

## Scope

- Lookups are scoped to a **fractal** or **prism**
- **Global** lists are visible across all fractals and prisms
- Requires **Analyst** role to create or modify; **Viewer** can read

## Limitations

- All columns are stored as strings
- The `column` parameter in `match()` must be marked as a key in the editor
- Rows with a blank key are ignored: they would otherwise match every log whose lookup field is absent
