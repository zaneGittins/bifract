# Aggregation

## Count

```
* | count()
event_id=1 | count()
```

## Sum / Avg / Max / Min

```
* | sum(bytes)
* | avg(response_time)
* | max(bytes)
* | min(response_time)
```

## Percentile and Standard Deviation

```
* | percentile(response_time)
* | stdDev(response_time)
```

Returns p50, p75, p99 for `percentile()`.

## Median and MAD

```
* | median(response_time)
* | mad(response_time)
* | groupBy(host) | mad(latency) | _mad > 50
```

`median()` returns the median value (`_median`). `mad()` returns both the median (`_median`) and the median absolute deviation (`_mad`), a robust measure of variability resistant to outliers. Unlike standard deviation, MAD is not skewed by extreme values, making it ideal for anomaly detection on noisy data.

## Select First / Last

Return the value from the earliest or latest event in each group:

```
* | groupBy(user) | selectFirst(timestamp)
* | groupBy(user) | selectLast(status)
```

## Multiple Aggregations with Multi

```
* | multi(count(), avg(response_time), sum(bytes))
```

### Count with Named Parameters

```
* | groupby(user) | multi(count(field=event_id, distinct=true, as=unique_events))
* | groupby(user) | multi(count(field=event_id, as=total))
```

Use `distinct=true` for unique counts (`uniqExact`), and `as=` to name the output column.

### Collect (groupArray)

```
* | groupby(user) | multi(collect(image))
```

Collects all values of a field into an array per group.

### Top (Frequency Distribution)

```
* | groupby(user) | multi(top(field=event_id, percent=true, as=top_events))
```

Shows the top values with their frequency. Use `percent=true` to show percentages.

## Skewness and Kurtosis

```
* | skewness(response_time)
* | kurtosis(response_time)
```

`skewness()` returns the population skewness (`_skewness`). `kurtosis()` returns the population excess kurtosis (`_kurtosis`). Both work on chained aggregation outputs:

```
* | groupby(host) | count() | skewness(_count)
* | groupby(host) | count() | kurtosis(_count)
```

## Frequency

Build a frequency table with count, percentage, and cumulative percentage:

```
* | frequency(event_name)
* | frequency(status_code)
```

Returns `value`, `_count`, `_percentage`, and `_cumulative_pct` columns, sorted by count descending.

## IQR (Interquartile Range)

```
* | iqr(response_time)
* | groupby(host) | count() | iqr(_count)
```

Returns `_q1` (25th percentile), `_q3` (75th percentile), and `_iqr` (Q3 - Q1).

## Head/Tail (Pareto Analysis)

Segment values into "head" and "tail" groups based on cumulative percentage (80/20 rule):

```
* | headTail(event_name)
* | headTail(hostname, threshold=90)
```

Returns `value`, `_count`, `_percentage`, `_cumulative_pct`, and `_segment` (head or tail). Default threshold is 80%.

## Modified Z-Score

Per-row modified z-score using median and MAD, robust to outliers:

```
* | modifiedZScore(response_time)
```

Returns `_median`, `_mad`, `_modified_z` for each row. Formula: `0.6745 * (x - median) / MAD`.

## MAD Outlier Detection

Modified z-score with an outlier flag:

```
* | madOutlier(response_time)
* | madOutlier(latency, threshold=2.5)
```

Returns `_median`, `_mad`, `_modified_z`, and `_is_outlier` (1 if `|_modified_z| > threshold`). Default threshold: 3.5. Filter outliers with:

```
* | madOutlier(response_time) | _is_outlier = 1
```

## Analyze Fields

Compute statistics across all or selected fields:

```
* | analyzeFields()
* | analyzeFields(response_time, bytes, limit=100000)
```

Returns per-field stats: `field_name`, `_events`, `_distinct_vals`, `_mean`, `_min`, `_max`, `_stdev`. Default scan limit: 50,000 rows (max: 200,000, use `limit=max` for maximum).

## Group By

```
* | groupBy(image)
* | groupBy(image, user)
* | groupBy(image) | count()
* | groupBy(user) | sum(bytes)
```

`groupBy()` automatically adds a `_count` if no aggregation is specified.

### Distinct Count with groupBy

```
* | groupBy(computer, function=count(field=user, distinct=true))
```

### Stats with groupBy

```
* | groupBy(computer, function=multi(count(computer), count(user,distinct=true), sum(bytes)))
```
