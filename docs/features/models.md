# Analytics Models

Analytics **Models** turn a BQL query into a continuously-maintained detection baseline. Each model captures matching logs as they are ingested, summarizes them into a compact table, and can raise an alert when something deviates. Models are **fractal-scoped** and live under the fractal's **Analytics** page.

## Model Types

| Type | Answers | Shape |
|------|---------|-------|
| **Rarity** | How unusual is a value within its group? | Partition key (group by), value key, min days seen |
| **First / Last Seen** | When was an entity first and last observed? | One or more key fields |
| **Volume Baseline** | Does an entity's volume deviate from its own history? | Entity fields, time bucket (hour/day), min history |
| **TLSH Index** | Which fuzzy-hash digests exist here? | One digest field |
| **Beacon** | Is this pair talking on a suspiciously regular interval? | `src_ip`, `dst_ip`, `dst_port` |
| **Long Connection** | Is this pair holding an unusually long-lived session? | `src_ip`, `dst_ip`, `dst_port` |

### How rarity is scored

Rarity counts **days**, not events: a value seen 10,000 times on one day counts once, so a burst cannot make itself look normal. For each value in a partition:

| Output | Meaning |
|---|---|
| `model_count` | Days the value was seen |
| `model_total` | Days the partition was seen with any value |
| `percent` | `model_count / model_total`, the share of the partition's days the value appeared on |
| `confidence` | Good-Turing coverage of the partition: 1 minus (values seen on only one day / total value-days). Near 1 means the partition rarely produces a new value; low means new values are routine there |

The alert fires on a value whose `percent` is below the share threshold while `confidence` is above its threshold. Port 22 appearing once on a host that used ports 80, 443 and 8080 every day for 30 days scores `percent` 3.3 and `confidence` 0.99; on a host that touches a new port most days, `confidence` stays low and new ports do not alert.

The share threshold also sets the learning period: a value seen on one day can only fall below it once the partition has more than 100 / threshold days of history (10% needs more than 10 days). **Min days seen** is a floor on `model_count` before a value is scored; leave it at 1, since first sightings are what the model finds. Volume Baseline stores its **min history** in the same field, where it counts buckets of history, and 7 is the sensible default there.

Volume Baseline scores the latest **complete** time bucket against the entity's own median using a modified z-score (3.5 is the standard cutoff); the current incomplete bucket is excluded.

### What "new" means for First / Last Seen

`first_seen` and `last_seen` are event times. `is_new`, which the alert fires on, is about the model instead: it is `1` when the model first recorded the entity within the last hour. A log that arrives late, or a dataset replayed with old timestamps, still counts as new if the model has not seen its entity before. History seeded by a backfill never counts as new, so seeding does not flood the alert; run the backfill before activating the alert, or entities that only appear in history may fire once before the backfill reaches them. An alert catching up on a backlog counts everything recorded since the start of the window it is evaluating. The **Data** view shows when the model recorded each entity as `recorded_at` in the row details (empty for seeded history).

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

  The source accepts any BQL that keeps one row per log, so a filter can consult a [lookup](dictionaries.md) (`match()`), a CIDR range, a regex or a list. What it cannot accept is a query that changes the row set: an aggregation, `sort`/`head`/`dedup`, anything that bounds the rows it returns (`limit`, a chart's `limit=`), or a command that reads outside the window a cycle covers (`join`, `chain`, `modelLookup`, `tlsh`, `pgr`). The editor names the reason inline.

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

Thresholds depend on the model type (confidence and max share of days for Rarity, z-score for Volume Baseline, new-entities-only for First/Last Seen). Toggle the mode from the listing or the data viewer. See [Alerts](../alerting/alerts.md) for actions and feeds.

## Viewing Results

The **Data** view shows the model's output table with sorting, search, and pagination, a stats panel (top partitions, anomalous entity counts, first/last seen ranges), and a **Configuration** tab summarizing the filters, extractions, shape, and alert.

## Import / Export

Models export to YAML for version control or sharing between fractals and deployments, and can be re-imported from the listing.
