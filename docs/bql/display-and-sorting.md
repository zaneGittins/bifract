# Display & Sorting

## Table

Select specific columns to display:

```
* | table(timestamp, image, user)
* | groupBy(image, function=count()) | table(image, _count)
```

Aggregations are projected under a fixed generated alias, not the function name and not the field name: `count()` produces `_count`, `sum(bytes)` produces `_sum`, `avg(x)` produces `_avg`, `min`/`max` produce `_min`/`_max`. Reference that alias in `table()`, `sort()`, and post-aggregation filters, or set your own with `as=`.

!!! warning "Referencing a wrong alias fails silently"
    An unknown name is treated as a log field, not an error. `| sum_bytes > 1000` compiles to a filter on a `sum_bytes` log field that does not exist, so the query runs and returns nothing useful. Use `_sum`, or name the column yourself: `function=sum(bytes, as=sum_bytes)`.

Note the difference between the two ways to combine `groupBy` with `count()`:

```
// one row per image, with its _count
* | groupBy(image, function=count())

// a single row: how many distinct images
* | groupBy(image) | count()
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
* | groupBy(user, function=sum(bytes)) | _sum >= 1000000
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
