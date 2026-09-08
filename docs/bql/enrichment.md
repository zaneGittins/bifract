# Enrichment

## match()

Enrich log events with data from a dictionary. Each matching log row gets additional columns from the dictionary lookup.

```
* | match(dict="threat_intel", field=src_ip, column=ip, include=[threat_score,category])
```

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `dict`    | Yes      | Name of the context list (created in the Context tab) |
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
  | groupBy(department, function=count())
```

## model_lookup()

Enrich rows with the baseline an analytics [model](../features/models.md) has built, so a query can score each event against learned history.

```
* | model_lookup(model="rare_parent_child", key=[parent_image, image])
```

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `model` | Yes | Name of an active model in this fractal |
| `key` | Yes | Log fields matched against the model's key, in order |
| `strict` | No | `true` (default) returns only rows the model scored; `false` keeps the rest with empty enrichment |

The key shape depends on the model type:

| Model type | `key=` |
|---|---|
| `rarity` | `[partition_key, value_key]` |
| `first_seen` | `[entity]` |
| `volume_baseline` | `[entity]` |
| `beacon`, `long_connection` | `[src_ip, dst_ip, dst_port]` |

Enrichment columns can be filtered and aggregated like any other field.

### Position in the pipeline

Placement relative to an aggregation (`groupby`, stats functions, [`chain()`](chain.md)) decides what gets enriched:

- `model_lookup()` before the aggregation enriches rows first. Model columns can then be group keys, aggregation inputs, row filters ahead of the aggregation, and step conditions inside `chain()`.
- `model_lookup()` after the aggregation enriches the aggregated results instead, so the key fields must be among the group columns.

Count events by whether the model has seen the user before:

```
* | model_lookup(model="known_users", key=[user]) | groupby(is_new)
```

Sequence a first-ever-seen user straight into process execution:

```
* | model_lookup(model="known_users", key=[user]) | chain(user, within=10m) {
  is_new="1";
  bifract_category="process_creation"
}
```

### strict

By default a row the model never scored is dropped. The model's key set is pushed into the log scan, so the query reads only the logs that can match rather than every log in the range and discarding the rest after the join.

`strict=false` keeps unscored rows with their enrichment columns at ClickHouse's type defaults (`0` for a score, empty for a date). Those defaults compare like real values, so a threshold that looks for something small matches every unscored row:

```
* | model_lookup(model="rare_images", key=[computer_name, image], strict=false) | percent < 0.1
```

That query returns every log the model has no entry for. Use `strict=false` only to see which rows went unscored, and test the enrichment column for emptiness rather than thresholding it.

A model scores forward from its creation (plus whatever its backfill covered), so under the default a query over a window older than the model returns nothing.

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
* | comment(tags=incident) | groupby(src_ip, function=count())
* | comment() | table(timestamp, norm_log, src_ip)
```

## tlsh()

Filter logs to those whose fuzzy-hash digest is similar to one or more known digests. TLSH survives recompilation and repacking, so it matches variants of a file that a SHA-256 comparison would miss.

```
event_id=1 | tlsh(field=tlsh, dict="known_bad", threshold=30)
```

### Parameters

| Parameter   | Required | Description |
|-------------|----------|-------------|
| `field`     | Yes      | Log field holding the TLSH digest. Must be a stored field, not one produced by an earlier command. |
| `hash`      | One of   | Literal digest to compare against. Comma-separate for several. |
| `dict`      | One of   | Dictionary whose key column holds the digests to compare against. |
| `threshold` | No       | Maximum TLSH distance. Defaults to 30. |

Supply either `hash` or `dict`, not both.

Distance runs from 0 (identical) upward. The convention is `<=30` for similar and `<=50` for loosely similar; the maximum accepted is 200.

Digests are accepted as 70 hex characters, with or without the `T1` version prefix, in either case. Producers differ: the Go implementation behind Velociraptor's `tlsh_hash()` writes bare lowercase, while MalwareBazaar publishes the uppercase prefixed form.

### Requires a TLSH index

`tlsh()` reads a **tlsh analytics model** on the same field, which indexes the distinct digests present in the fractal. Without one the query fails and names the model to create.

The index is what makes this affordable. Digests repeat heavily across rows, so the comparison runs over the number of distinct digests rather than the number of rows, which is why the filter works over billions of logs. The model's view admits only well-formed digests, so the empty and truncated values that producers emit for inputs under 50 bytes never enter the index. That matters: two empty digests compare at distance 0, which would otherwise match everything.

Create the model under Analytics Models, then run a backfill to cover existing history.

In a prism, every member fractal needs its own TLSH index on that field. A partially indexed prism fails rather than silently returning matches from only the indexed members.

### Added columns

| Column          | Description |
|-----------------|-------------|
| `tlsh_distance` | Distance to the closest matching needle |
| `tlsh_match`    | The needle that matched: the dictionary key, or the literal digest |

```
event_id=1 | tlsh(field=tlsh, dict="known_bad") | sort(tlsh_distance)
```

### Position in the pipeline

`tlsh()` filters log rows, so it must come before `groupby()` or any aggregation. Placing it after one is an error rather than a silent re-interpretation as a filter on the pre-aggregation rows.

```
event_id=1 | tlsh(field=tlsh, dict="known_bad") | groupby(computer_name, function=count())
```

### Cost

Work scales with distinct digests multiplied by needle count. Aim to keep that product under roughly 10^9 for interactive hunting and 10^8 for alerting, which means curated, family-representative digest lists rather than corpus dumps.

Three limits are enforced, each with an error naming the numbers involved:

| Limit | Value |
|-------|-------|
| Distinct digests in the index | 500,000 |
| Digests in a `dict` | 50,000 |
| Distinct digests x needles | 2 x 10^9 |

`tlsh()` works in alerts as well as in search. The index probe runs on each evaluation, which is cheap because it reads the model rather than the log table.
