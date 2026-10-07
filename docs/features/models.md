# Analytics Models

Analytics **Models** turn a BQL query into a continuously-maintained detection baseline. Each model captures matching logs as they are ingested, summarizes them into a compact table, and can raise an alert when something deviates. Models are **fractal-scoped** and live under the fractal's **Analytics** page.

## Model Types

| Type | Answers | Shape |
|------|---------|-------|
| **Rarity** | How unusual is a value within its group? | Group by (e.g. host), value (e.g. port), min history |
| **First / Last Seen** | When was an entity first and last observed? | One or more key fields |
| **Volume Baseline** | Does an entity's volume deviate from its own history? | Entity fields, time bucket (hour/day), min history |
| **TLSH Index** | Which fuzzy-hash digests exist here? | One digest field |
| **Beacon** | Is this pair talking on a suspiciously regular interval? | `src_ip`, `dst_ip`, `dst_port` |
| **Long Connection** | Is this pair holding an unusually long-lived session? | `src_ip`, `dst_ip`, `dst_port` |

### How rarity is scored

Rarity counts **days**, not events: a value seen 10,000 times on one day counts once, so a burst cannot make itself look normal. For each value in a group (the "Group by" field):

| Output | Meaning |
|---|---|
| `model_count` | Days the value was seen |
| `model_total` | Days the group was seen with any value |
| `percent` | `model_count / model_total`, the share of the group's days the value appeared on |
| `confidence` | Good-Turing coverage of the group: 1 minus (values seen on only one day / total value-days). Near 1 means the group rarely produces a new value; low means new values are routine there |

The alert fires on a value whose `percent` is below the share threshold while `confidence` is above its threshold. Port 22 appearing once on a host that used ports 80, 443 and 8080 every day for 30 days scores `percent` 3.3 and `confidence` 0.99; on a host that touches a new port most days, `confidence` stays low and new ports do not alert.

**Min history** is the learning period: the days a group must have been seen (`model_total`) before any of its values can alert. It defaults to 14, two weeks of normal. The share threshold adds its own floor, since a value seen on one day only falls below it once the group has more than 100 / threshold days (10% needs more than 10), so the effective learning period is the longer of the two. A value is never held back for being new: first sightings are what the model finds.

### How volume is scored

Volume Baseline scores the latest **complete** time bucket (`latest_bucket`, yesterday for a daily model) against the entity's own history; the current incomplete bucket is excluded. History runs from the entity's first bucket in the window (90 days for daily, 30 days for hourly) up to, but not including, the scored bucket. Buckets with no events count as zero, so a quiet entity's bursts and drops show, and an empty scored bucket scores as 0 events.

| Output | Meaning |
|---|---|
| `latest_count` | Events in `latest_bucket` (0 when it was empty) |
| `baseline_median` | Median count per bucket over the history |
| `mad` | Median absolute deviation of the history |
| `n_buckets` | Buckets of history, empty ones included. An entity is scored once this reaches **min history** (default 7) |
| `z_score` | Modified z-score, `0.6745 * (latest_count - median) / mad`. 3.5 is the standard cutoff |

When `mad` is 0 (most buckets equal the median), `z_score` falls back to `(latest_count - median) / (1.253314 * mean absolute deviation)`. When the history is perfectly flat (every bucket the same), any change scores `z_score` 1000000, or -1000000 for a drop, shown as "flat history, any change". Seasonality (weekday or hour-of-day baselines) is not modeled.

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

A new model opens on a gallery of templates over the normalized schema: new programs per host (First / Last Seen on `computer_name`, `image`), rare child of Office apps (Rarity of `image` per `parent_image`), new outbound ports per host (Rarity of `dst_port` per `computer_name`), network volume spike per host (hourly Volume Baseline on `computer_name`), and beaconing. A template fills the query, shape and thresholds and scores the result; **Start blank** skips it.

The **Scores** tab estimates what the model would produce over a window: the last 1, 7 or 30 days, or a **custom range** of up to 90 days that can end in the past, so a model over older data can be judged on it. The threshold controls above the distribution recount "would flag" as they move, without another scan.

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

## Model Health

The listing shows one state per model, the worst that applies, with the reason on hover:

| State | Meaning |
|---|---|
| **Error** | The model failed to build |
| **Not updating** / **Not started** / **Behind** | State maintenance is failing, never took the model over, or lags the logs |
| **Rebuilding** | Its tables are being created |
| **Backfill n%** | History is being seeded |
| **Stale** | The newest event the model recorded is more than 2 days old (3 hours for an hourly Volume Baseline, which otherwise scores every entity against an empty bucket) |
| **Learning k/n** | Too little history for scores to mean much. Rarity needs its min history in days per group, or more than 100 / share threshold days if that is longer, Volume Baseline its min history in buckets, First / Last Seen 7 days, network models one full window |
| **Healthy** | Recording and scoring normally |

**Findings** counts what the model surfaced over the last 7 days: entities first recorded per day (First / Last Seen) or value pairs first seen per day (Rarity), each with a daily sparkline; entities above the z threshold in the latest bucket (Volume Baseline); pairs at or above the score threshold (network models). **Last alert** is when the linked alert last fired. Both are read from the model's own tables, never raw logs, and cached for two minutes.

## Viewing Results

The **Data** view opens on **Findings**: the rows the model's alert would raise, most unusual first. **All rows** lists everything the model scored, with sorting, search, and pagination.

| Type | A finding is | Ordered by |
|---|---|---|
| Rarity | A value meeting the alert's confidence and share-of-days thresholds | Share of days, then confidence |
| Volume Baseline | An entity whose `z_score` is above the alert's threshold | `z_score`, highest first |
| First / Last Seen | An entity first recorded by the model in the last 7 days | Newest first |
| Beacon, Long Connection | A pair whose score is above the alert's threshold | Score, highest first |

A rarity model with no thresholds has no findings, and a TLSH Index has no alert, so it shows only its rows. The same rows are available from the API as `GET /models/{id}/data?view=findings`.

A summary line above the table counts the findings and, for rarity and first/last seen models, what was new this week, with a 30-day sparkline of new values per day. A **Why** column and the row details state the facts behind each row (days seen, group confidence, latest against typical volume, connection regularity); a fact that names the row's activity opens it in search. The row details keep every stored column under **All columns**.

## Import / Export

Models export to YAML for version control or sharing between fractals and deployments, and can be re-imported from the listing.
