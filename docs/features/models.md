# Models

Analytics **Models** turn a BQL query into a continuously-maintained detection baseline. Each model captures matching logs as they are ingested, summarizes them into a compact table, and can raise an alert when something deviates. Models are **fractal-scoped** and live under the fractal's **Models** tab.

## Model Types

| Type | Answers | Shape |
|------|---------|-------|
| **Rarity** | How unusual is a value within its group? | Partition key (group by), value key, min sample size |
| **First / Last Seen** | When was an entity first and last observed? | One or more key fields |
| **Volume Baseline** | Does an entity's volume deviate from its own history? | Entity fields, time bucket (hour/day), min history |

Volume Baseline scores the latest **complete** time bucket against the entity's own median using a modified z-score (3.5 is the standard cutoff); the current incomplete bucket is excluded.

## Building a Model

The editor is a split panel:

- **Left - source query.** Write a BQL filter to narrow which logs feed the model, and use `regex()` to pull fields out of the raw log. Run it against a time range to preview matching logs and the fields you extracted.
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