# Dictionaries

Dictionaries are per-fractal lookup tables for enriching log data at query time. Define a dictionary with key-value columns, then use `match()` in BQL queries to join dictionary data onto your results.

## Creating a Dictionary

Navigate to **Dictionaries** within a fractal. Click **Create** and provide:

- **Name** - must be a valid identifier (alphanumeric and underscores)
- **Key column** - the primary lookup column name
- **Description** (optional)
- **Global** (optional) - makes the dictionary available to all fractals

## Managing Data

The dictionary editor provides a spreadsheet-like interface:

- **Add columns** using the `+` button in the table header
- **Mark columns as keys** to enable lookups on non-primary columns (required for `match()`)
- **Edit cells** by double-clicking; press Enter to save, Escape to cancel
- **Import CSV** via drag-and-drop or file picker. New columns in the CSV are added automatically
- **Reload** forces ClickHouse to refresh its dictionary cache immediately

## Using Dictionaries in Queries

Use `match()` to enrich query results with dictionary data. See [Enrichment](../bql/enrichment.md) for full syntax.

```
* | match(dict="threat_intel", field=src_ip, column=ip, include=[threat_score,category])
```

The `column` parameter must reference a column marked as a key in the dictionary editor.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `dict` | Yes | Dictionary name |
| `field` | Yes | Log field to look up |
| `column` | Yes | Dictionary key column to match against |
| `include` | Yes | Columns to add to results |
| `strict` | No | If `true`, filter out non-matching rows (default: `false`) |

## Scope

- Dictionaries are scoped to a **fractal** or **prism**
- **Global** dictionaries are visible across all fractals and prisms
- Requires **Analyst** role to create or modify; **Viewer** can read

## Limitations

- All columns are stored as strings
- The `column` parameter in `match()` must be marked as a key in the dictionary editor
- ClickHouse caches dictionary data for 5 minutes; use **Reload** to force a refresh
