# Enrichment

## match()

Enrich log events with data from a dictionary. Each matching log row gets additional columns from the dictionary lookup.

```
* | match(dict="threat_intel", field=src_ip, column=ip, include=[threat_score,category])
```

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `dict`    | Yes      | Name of the dictionary (created in the Dictionaries page) |
| `field`   | Yes      | Log field to use as the lookup key |
| `column`  | Yes      | Dictionary column to match against |
| `include` | Yes      | Dictionary columns to add to results: `include=[col1,col2]` |
| `strict`  | No       | When `true`, only return rows that have a match in the dictionary. Default: `false` |

When `strict=false` (the default), non-matching rows are kept with empty strings for the included columns. When `strict=true`, non-matching rows are filtered out entirely.

### Examples

Enrich logs with threat intelligence data:

```
* | match(dict="threat_intel", field=src_ip, column=ip, include=[threat_score,category])
```

Only keep logs that match the dictionary (strict mode):

```
* | match(dict="threat_intel", field=src_ip, column=ip, include=[threat_score,category], strict=true)
```

Combine with other pipeline stages:

```
* | match(dict="asset_inventory", field=hostname, column=name, include=[owner,department])
  | groupBy(department) | count()
```

## comment()

Filter logs to only those that have comments. Optionally narrow by tag labels or keyword search in comment text.

```
* | comment()
```

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `tags`    | No       | One or more tag labels (OR logic, case sensitive). Comma-separated. |
| `keyword` | No       | Search term matched against comment text (case insensitive) |

With no arguments, returns all logs that have at least one comment.

### Tag Filtering

```
* | comment(tags=security)
* | comment(tags=security,critical)
```

Multiple tags use OR logic. This matches logs with comments tagged `security` OR `critical`.

### Keyword Filtering

```
* | comment(keyword="timeout")
```

Matches logs with comments containing "timeout" (case insensitive).

### Combined

```
* | comment(keyword="error", tags=security)
```

Keyword AND at least one matching tag.

### Pipeline Usage

`comment()` can be combined with other pipeline commands:

```
* | comment(tags=incident) | groupby(src_ip) | count()
* | comment() | table(timestamp, raw_log, src_ip)
```
