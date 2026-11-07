# Enrichment

Enrichment commands add context to log rows at query time: lookup columns (`match()`), model baselines (`modelLookup()`), analyst comments (`comment()`), fuzzy-hash similarity (`tlsh()`) and row filters on process context (`pgrHasAncestor()`, `pgrHasNetwork()`). For GeoIP, see [`lookupIP()`](field-operations.md#geoip-enrichment).

## match()

Adds columns from a [lookup](../features/dictionaries.md) to each row whose field value matches a key.

```
* | match(dict="threat_intel", field=src_ip, column=ip, include=[threat_score,category])
```

| Parameter | Required | Description |
|-----------|----------|-------------|
| `dict` | Yes | Lookup name, as shown on the **Lookups** page |
| `field` | Yes | Log field to look up |
| `column` | Yes | Lookup key column to match against |
| `include` | Yes | Lookup columns to add: `include=[col1,col2]` |
| `require` | No | `true` drops rows with no match. Default `false`: unmatched rows are kept with empty columns. |

Keep only rows found in the lookup:

```
* | match(dict="threat_intel", field=src_ip, column=ip, include=[threat_score], require=true)
```

Including the key column itself reports membership: it holds the field value on a hit and is empty on a miss.

### Lookup kinds

A lookup's kind is chosen when it is created and decides how keys are compared. The query is written the same way for all three.

| Kind | Keys hold | A value matches |
|------|-----------|-----------------|
| Values | names, hashes, indicators | the key, exactly. **Ignore case** on the lookup makes it case-insensitive. |
| Networks | IPv4 and IPv6 CIDR ranges | the narrowest range containing the address. A value that is not an IP misses. |
| Patterns | regular expressions | the first expression, in row order, that matches. Case-sensitive unless the expression starts with `(?i)`. |

On a Patterns lookup, put specific expressions above general ones: with `mimikatz` above `\.exe$`, `mimikatz.exe` matches `mimikatz`; reversed, it matches `\.exe$`. Lookups filled from a URL can use [glob keys](../features/dictionaries.md#glob-keys), which ignore case.

```
* | match(dict="corp_ranges", field=src_ip, column=network, include=[site,owner])
```

## modelLookup()

Adds the baseline an analytics [model](../features/models.md) has learned to each row, so a query can score events against history.

```
* | modelLookup(model="rare_parent_child", key=[parent_image, image]) | percent < 1
```

| Parameter | Required | Description |
|-----------|----------|-------------|
| `model` | Yes | Name of a model in this fractal |
| `key` | Yes | Log fields matched against the model's key, in order. The count must match the model |
| `require` | No | Default `true`: only rows the model scored. `false` keeps the rest with empty enrichment. |

| Model type | `key=` | Columns added |
|------------|--------|---------------|
| `rarity` | `[group, value]`, or `[value]` with no group ([scoring](../features/models.md#how-rarity-is-scored)) | `percent`, `coverage`, `model_count`, `model_total` |
| `first_seen` | `[groups..., value]`, or `[value]` with no group ([what is_new means](../features/models.md#what-new-means-for-first-last-seen)) | `first_seen`, `last_seen`, `event_count`, `is_new` |
| `volume_baseline` | `[entity]` ([scoring](../features/models.md#how-volume-is-scored)) | `z_score`, `baseline_median`, `latest_count`, `mad`, `n_buckets`, `latest_bucket` |
| `beacon` | `[src_ip, dst_ip, dst_port]` | `beacon_score`, `conn_count`, `prevalence` and per-signal scores |
| `long_connection` | `[src_ip, dst_ip, dst_port]` | `longconn_score`, `total_duration`, `conn_count`, `prevalence` and per-signal scores |

Use the added columns like any other field: filter, sort, compute from them and pass them to other commands.

```
event_id=22 | modelLookup(model="dns_first_seen", key=[query]) | age := dateDiff("second", first_seen, timestamp) | table(query, first_seen, age)
```

Only one `modelLookup()` that adds columns per query, and it cannot be combined with `join()`.

!!! tip "Filter on model columns right after the lookup"
    With the default `require=true`, a filter that reads only model columns, such as `is_new="1"` or `percent < 1`, narrows the model's keys before the logs are read, so the scan touches only logs with a matching key. A filter that also reads a log field, or comes after `head()`, runs on the joined rows instead.

### Position in the pipeline

- **Before an aggregation** (`groupby()`, stats, [`chain()`](chain.md)): rows are enriched first, so model columns can be group keys, aggregation inputs and chain step conditions.
- **After an aggregation:** the aggregated rows are enriched, so the key fields must be group columns.

```
* | modelLookup(model="known_users", key=[user]) | groupby(is_new)
```

!!! warning "require=false and thresholds"
    Unscored rows get type defaults (`0` for a score), which compare like real values. `require=false | percent < 0.1` returns every row the model has never seen. To find unscored rows, use [`NOT modelLookup()`](#keep-the-rows-a-model-does-not-score).

A model scores from its creation forward, plus any backfill, so with the default `require=true` a query over older data returns nothing.

### Keep the rows a model does not score

Written as a condition, `modelLookup()` tests whether the model scores a row's key and adds no columns. Negate it to keep the rows it does not score:

```
event_id=22 | NOT modelLookup(model="dns_first_seen", key=[query])
* | NOT modelLookup(model="known_users", key=[user]) OR user="admin"
```

`require=` does not apply to the condition form. A condition can sit beside a `modelLookup()` that adds columns, including one for another model.

## comment()

Keeps only logs that have analyst comments, optionally narrowed by tag or text.

```
* | comment(tags=[security,critical], keyword="timeout")
```

| Parameter | Required | Description |
|-----------|----------|-------------|
| `tags` | No | Tag labels, case-sensitive. A log matches if any comment has any listed tag. |
| `keyword` | No | Substring of the comment text, case-insensitive |

With no arguments, `comment()` returns every commented log. Tags and keyword together must both match. At most 50,000 commented logs are considered per query.

## tlsh()

Keeps logs whose TLSH fuzzy-hash digest is close to a known digest. TLSH survives recompiling and repacking, so it finds variants that an exact SHA-256 match misses.

```
event_id=1 | tlsh(field=tlsh, dict="known_bad", threshold=30) | sort(tlsh_distance)
```

| Parameter | Required | Description |
|-----------|----------|-------------|
| `field` | Yes | Stored log field holding the digest (not a computed one) |
| `hash` | One of | Digest(s) to compare against: `hash=[d1,d2]` |
| `dict` | One of | Lookup whose key column holds the digests |
| `threshold` | No | Maximum distance, 0 to 200. Default 30. |

Distance 0 is identical; up to 30 is similar, up to 50 loosely similar. Digests are 70 hex characters, with or without the `T1` prefix, in any case.

Adds `tlsh_distance` (distance to the closest needle) and `tlsh_match` (that needle). Only one `tlsh()` per query, and it must come before any aggregation.

To bring other lookup columns onto a hit, key `match()` on `tlsh_match`, not on the log field: the log's digest is only similar to the lookup key, never equal.

```
event_id=1
| tlsh(field=tlsh, dict="known_bad")
| match(dict="known_bad", field=tlsh_match, column=digest, include=[name])
```

### TLSH index

`tlsh()` reads a **TLSH Index** model on the same field, which stores the distinct digests in the fractal. Create one under **Analytics** and run **Backfill** for existing data. Without an index the query fails and names the model to create.

In a prism, a member with no index is skipped and the results show a `partial coverage` notice naming it.

| Limit | Value |
|-------|-------|
| Distinct digests in the index | 500,000 |
| Digests in a `dict` | 50,000 |
| Distinct digests x needles | 2,000,000,000 |

Cost grows with distinct digests times needles, so prefer curated, family-representative digest lists over full corpus dumps. `tlsh()` also works in alert rules.

## pgrHasAncestor()

Keeps rows whose process has a matching image among its parents, using the [provenance tables](../features/provenance-graph.md) instead of a `join()` or `chain()`. It works on any event with a `process_guid`, and needs **Collect baselines** on: only processes logged while it is on are recorded, and any others never match, so `NOT` keeps them.

```
bifract_category=network_connect image=~rundll32 | pgrHasAncestor("winword.exe")
bifract_category=file_write | pgrHasAncestor("powershell.exe", depth=3)
bifract_category=process_creation image=~whoami | pgrHasAncestor("C:\\Windows\\System32\\cmd.exe")
bifract_category=process_creation image=~whoami | NOT pgrHasAncestor("services.exe")
```

| Parameter | Required | Description |
|-----------|----------|-------------|
| value | Yes | File name, full path or `/regex/` of the ancestor's image |
| `depth` | No | Levels to walk up, default 5, max 8. `depth=1` is the direct parent. |

Each process is found by its `process_guid`, so the time range only decides which rows are tested: an ancestor counts whenever it started, within lineage retention (730 days by default, set by `BIFRACT_PROC_LINEAGE_TTL_DAYS`). The row's process and each process on the way up need a `process_creation` event with `parent_image`, and `parent_process_guid` to walk past the first level.

A value with a slash or backslash is a whole path, not part of one, and backslashes are doubled inside quotes. Names and paths ignore ASCII case, and a regex ignores all case.

It combines with `AND`, `OR` and `NOT` like `cidr()`, and works in `chain()` steps. It must come before any aggregation or `chain()`, and anything later is an error, as is using it in a `let` binding or assigning `process_guid` in the same query. A query can spend 40 lookups, counting each filter once: a walk costs 15 at the default depth and 36 at depth 8, and a shallow walk costs 5.

!!! note "Keeping it fast"
    The cost follows the rows in the time range and how many distinct processes they hold, not retention, so narrow the rows first: `bifract_category=network_connect image=~rundll32 | pgrHasAncestor("winword.exe")` is far cheaper than `pgrHasAncestor("winword.exe")` alone. Plain filters in the same query do this wherever they are written, unless `head()`, `tail()` or `dedup()` stands between them, or the filter is inside an `OR`, a `chain()` step or a case branch. Keep it behind a filter in Event alerts.

!!! warning "In alerts"
    An alert sees what had arrived when it ran. An ancestor whose process creation arrives after the alert has read the event is not matched, and under `NOT` the alert fires when it should not.

It is not available in archive search or models, and rule tests cannot evaluate it.

## pgrHasNetwork()

Keeps rows whose process made a matching network connection, whenever it happened, up to 730 days after its last ingest. It works like `pgrHasAncestor()`, with the same placement rules, alert behaviour and limits, and costs 5 of a query's 40 lookups.

```
bifract_category=process_creation image=~notepad | pgrHasNetwork(external=true)
bifract_category=file_write | pgrHasNetwork("203.0.113.7")
bifract_category=process_creation image=~svchost | NOT pgrHasNetwork()
```

| Parameter | Required | Description |
|-----------|----------|-------------|
| value | No | Public IP, internal `/24` such as `10.1.2.0/24`, `internal` for internal IPv6, or `/regex/`. With no value, any connection matches. |
| `external` | No | `true` leaves out internal ranges, CGNAT (`100.64.0.0/10`), `0.0.0.0/8`, multicast, reserved and broadcast addresses, for IPv6 anything outside global unicast (`2000::/3`), including IPv4-mapped addresses, and anything that is not a plain IP. |

A value compares exactly as the address was logged, and a regex is as written. Documentation and benchmark ranges such as `198.18.0.0/15` still count as external. The connection event must carry `image`, `process_guid` and `dst_ip`, or it is not recorded.
