# Models

Analytics **Models** turn a BQL query into a continuously-maintained detection baseline. Each model captures matching logs as they are ingested, summarizes them into a compact table, and can raise an alert when something deviates. Models are **fractal-scoped** and live under the fractal's **Models** tab.

## Model Types

| Type | Answers | Shape |
|------|---------|-------|
| **Rarity** | How unusual is a value within its group? | Partition key (group by), value key, min sample size |
| **First / Last Seen** | When was an entity first and last observed? | One or more key fields |
| **Volume Baseline** | Does an entity's volume deviate from its own history? | Entity fields, time bucket (hour/day), min history |
| **TLSH Index** | Which fuzzy-hash digests exist here? | One digest field |
| **Beacon** | Is this pair talking on a suspiciously regular interval? | `src_ip`, `dst_ip`, `dst_port` |
| **Long Connection** | Is this pair holding an unusually long-lived session? | `src_ip`, `dst_ip`, `dst_port` |

Rarity's **min sample size** is a floor on how many times a value must have been seen before the model scores it at all, not a cap on what it collects. At 1 every value is scored. Raise it and the rarest values, which are the ones a rarity model exists to surface, stop being scored at all and are then dropped by `modelLookup`'s default `require=true`.

Volume Baseline scores the latest **complete** time bucket against the entity's own median using a modified z-score (3.5 is the standard cutoff); the current incomplete bucket is excluded.

TLSH Index is not a detection on its own. It indexes the distinct fuzzy-hash digests in a field, which is what [`tlsh()`](../bql/enrichment.md#tlsh) probes instead of scanning every row.

Beacon and Long Connection are network models. They maintain rolling per-connection state and score it on a schedule, applying a prevalence modifier so a pattern seen across many hosts scores lower than the same pattern on one.

## Using a Model in Queries

Any model can be read from BQL with `modelLookup()`, which joins the model's baseline onto matching rows so you can filter or aggregate on it:

```
* | modelLookup(model="rare_parent_child", key=[parent_image, image])
```

See [Enrichment](../bql/enrichment.md#modellookup) for the key shape each model type expects.

## Building a Model

The editor is a split panel:

- **Left - source query.** Write a BQL filter to narrow which logs feed the model, and use `regex()` to pull fields out of `norm_log` (the normalized event text) or a specific field. Run it against a time range to preview matching logs and the fields you extracted. `raw_log` cannot be an extraction source: it is only retained for 7 days, while model state is long-lived.

  The source accepts any BQL that keeps one row per log, so a filter can consult a [dictionary](dictionaries.md) (`match()`), a CIDR range, a regex or a list. What it cannot accept is a query that changes the row set: an aggregation, `sort`/`head`/`dedup`, anything that bounds the rows it returns (`limit`, a chart's `limit=`), or a command that reads outside the window a cycle covers (`join`, `chain`, `model_lookup`, `tlsh`, `pgr`). The editor names the reason inline.

  A key can be a log field, a `regex(... as=)` extraction, or a column the query computes: a `match(include=[...])` lookup, a `lowercase()` rewrite, anything you name with `as=`. The model's scan projects it. Two exceptions: a generated name (anything starting with `_`) is refused because you did not choose it, so name it with `as=`; and a network model cannot key on a computed column at all, because its state is one flat aggregate over the log table with nowhere to compute one.

  A computed key follows whatever computed it. Keying on a dictionary lookup means the key is whatever the list said **when that cycle ran**: rename a list entry and rows read afterwards carry the new name while state already built keeps the old one, so the same tool appears as two entities and the new one looks brand new. Key on the stored value instead when that matters.
- **Right - shape and alert.** Pick the model type, map its keys to extracted or base fields, and optionally attach an alert.

Models capture new logs from the moment they are created. They do **not** retroactively process history until you seed it (see below).

## Seeding History (Backfill)

From a model's **Data** view, seed historical data over a chosen window (24h, 7d, 30d, or 90d). Progress is shown per-day and can be cancelled; a failed or cancelled backfill can be resumed from where it stopped without double-counting. Seeding is terminal once complete - to re-seed, edit the model (which resets its data).

## Alerts

Each model has an alert mode:

- **Collect data only** - the model runs silently; view its data anytime.
- **Paused** (recommended default) - the alert is created but does not fire until enabled.
- **Active** - the alert fires when its threshold is exceeded.

Thresholds depend on the model type (confidence and max % for Rarity, z-score for Volume Baseline, new-entities-only for First/Last Seen). Toggle the mode from the listing or the data viewer. See [Alerts](../alerting/alerts.md) for actions and feeds.

## Viewing Results

The **Data** view shows the model's output table with sorting, search, and pagination, a stats panel (top partitions, anomalous entity counts, first/last seen ranges), and a **Configuration** tab summarizing the filters, extractions, shape, and alert.

## Import / Export

Models export to YAML for version control or sharing between fractals and deployments, and can be re-imported from the listing.
