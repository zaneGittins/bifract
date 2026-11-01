# Display & Sorting

These commands choose which columns appear, in what order, and how many rows come back.

## Table

```
* | table(timestamp, image, user, limit=5)
```

Shows only the listed columns, in that order. `limit=` caps the rows. Fields can be expressions (`table(user, len(commandline))`) or aggregates:

```
* | table(user, sum(bytes), avg(response_time))
```

After an aggregation, list its output columns by name (`_count`, `_sum`, or your `as=` name):

```
* | groupBy(image) | table(image, _count)
```

!!! note "Filters after an aggregation"
    After `groupBy()` or another aggregate, only its group keys and output columns exist. Filtering on any other name is an error that lists the columns that do exist. Move log-field filters before the aggregation.

### Built-in columns

Every event has these columns besides its own fields. They can be selected, sorted and matched like any field.

| Column | Meaning |
|---|---|
| `timestamp` | When the event happened, parsed from the log |
| `ingest_timestamp` | When Bifract received it |
| `log_id` | The event's identifier |
| `fractal_id` | The fractal the event belongs to |
| `norm_log` | The normalized event text |
| `normalizer` | The normalizer that shaped the event, as `name@version` |

```
* | table(timestamp, ingest_timestamp, image)
```

`timestamp`, `ingest_timestamp`, `log_id` and `fractal_id` scope every search, so no command can write an output under those names; pick another name, such as `as=event_time`. `norm_log` and `normalizer` can be replaced by an assignment or `eval()` for the rest of the query.

## Sort

```
* | sort(bytes, order=desc)
```

Default order is ascending. The order can also be given positionally (`sort(bytes, desc)`), and the field can be an expression (`sort(len(commandline), desc)`).

## Limit

```
* | limit(100)
```

Returns at most N rows.

## Head and tail

```
* | head(50)
```

`head(n)` returns the earliest N events and `tail(n)` the latest, by `timestamp`. Default N is 200. After an aggregation they take the first or last N rows of the current order instead.

## Dedup

```
level=error | dedup(host, service) | table(host, service, message)
```

Keeps one row for each distinct combination of the listed fields. After an aggregation, dedup only on its output columns.
