# Display & Sorting

## Table

Select specific columns to display:

```
* | table(timestamp, image, user)
* | groupBy(image, function=count()) | table(image, _count)
```

Aggregations are projected under their generated alias, not the function name: `count()` produces `_count`, `sum(bytes)` produces `sum_bytes`. Reference that alias in `table()`, `sort()`, and post-aggregation filters.

Note the difference between the two ways to combine `groupBy` with `count()`:

```
* | groupBy(image, function=count())   # one row per image, with its _count
* | groupBy(image) | count()           # a single row: how many distinct images
```

Use `function=` when you want a value per group and need the grouped field downstream in `table()` or `sort()`. A trailing `| count()` counts the rows the groupBy produced, so the result is a single total and the grouped field is no longer in scope.

Aggregation functions can be used inline in `table()`:

```
* | table(user, sum(bytes), avg(response_time))
```

Limit number of rows:
```
* | table(timestamp, image, user, limit=5)
```

## Sort

```
* | sort(timestamp, order=asc)
* | sort(bytes, order=desc)
```

Default direction is ascending.

## Limit

```
* | limit(100)
```

## Filtering on Aggregated Results

Filter on computed or aggregated fields after a pipeline stage:

```
* | groupBy(image, function=count()) | _count > 100
* | groupBy(user, function=sum(bytes)) | sum_bytes >= 1000000
```

You can also add bare string or regex filters after the initial pipeline to further narrow results:

```
event_id=1 | "powershell"
* | /error.*timeout/i
```

## Dedup

Deduplicate results by one or more fields, keeping the first occurrence:

```
* | dedup(user)
* | dedup(src_ip, dst_ip)
level=error | dedup(host, service) | table(host, service, message)
```

## Head / Tail

Return the first or last N events (default: 200):

```
* | head(50)
* | tail(50)
```
