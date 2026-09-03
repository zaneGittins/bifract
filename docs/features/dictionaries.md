# Context

Context lists are per-fractal lookup tables for enriching log data at query time. They appear under the **Context** tab and BQL refers to one by name as `dict=`. Define a list with key-value columns, then use `match()` in BQL queries to join it onto your results.

## Creating a Context List

Navigate to **Context** within a fractal. Click **+ New List** and provide:

- **Name** - must be a valid identifier (alphanumeric and underscores)
- **Key column** - the primary lookup column name
- **Description** (optional)
- **Global** (optional) - makes the list available to all fractals

## Managing Data

The editor provides a spreadsheet-like interface:

- **Add columns** using the `+` button in the table header
- **Mark columns as keys** to enable lookups on non-primary columns (required for `match()`)
- **Edit cells** by double-clicking; press Enter to save, Escape to cancel
- **Import CSV** via drag-and-drop or file picker. New columns in the CSV are added automatically
- **Reload** forces ClickHouse to refresh its cached copy immediately. Adding, editing, and deleting rows already refresh it

## Using Context Lists in Queries

Use `match()` to enrich query results. See [Enrichment](../bql/enrichment.md) for full syntax.

```
* | match(dict="threat_intel", field=src_ip, column=ip, include=[threat_score,category])
```

The `column` parameter must reference a column marked as a key in the editor.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `dict` | Yes | Context list name |
| `field` | Yes | Log field to look up |
| `column` | Yes | Key column to match against |
| `include` | Yes | Columns to add to results |
| `strict` | No | If `true`, filter out non-matching rows (default: `false`) |

## Scope

- Context lists are scoped to a **fractal** or **prism**
- **Global** lists are visible across all fractals and prisms
- Requires **Analyst** role to create or modify; **Viewer** can read

## Limitations

- All columns are stored as strings
- The `column` parameter in `match()` must be marked as a key in the editor
- Rows with a blank key are ignored: they would otherwise match every log whose lookup field is absent
