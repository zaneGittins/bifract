# Visualizations

## Pie Chart

```
* | groupBy(status) | piechart()
* | groupBy(image, function=count()) | piechart(limit=5)
```

## Bar Chart

```
* | groupBy(user, function=count()) | barchart()
* | groupBy(status) | barchart(limit=10)
```

## Graph (Relationship View)

```
* | table(process_guid, parent_process_guid) | graph(child=process_guid, parent=parent_process_guid)
* | graph(child=process_guid, parent=parent_process_guid, limit=200)
```

Both `child=` and `parent=` are required. Max limit is 500.

## Single Value

Display a single aggregate statistic as a large number. Requires an aggregation that produces a single row.

```
* | count() | singleval()
* | avg(response_time) | singleval(label="Avg Response Time")
* | groupBy(computer_name) | count() | singleval(label="Unique Computers")
```

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `label`   | No       | Text displayed below the value. Defaults to the aggregation field name. |

## Histogram

Distribute a numeric field into equal-width bins:

```
* | histogram(response_time)
* | histogram(bytes, buckets=30)
```

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `field`   | Yes      | Numeric field to build distribution for |
| `buckets` | No       | Number of equal-width bins (default: 20, max: 200) |

## Heatmap

Render a 2D density heatmap with aggregated values:

```
* | heatmap(x=src_ip, y=dst_port, value=count())
* | heatmap(x=user, y=action, value=sum(bytes), limit=20)
```

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `x`       | Yes      | Field for the X axis |
| `y`       | Yes      | Field for the Y axis |
| `value`   | No       | Aggregation function (default: `count()`) |
| `limit`   | No       | Max distinct values per axis (default: 50, max: 200) |

## Time Chart

Render a time series line chart. Buckets events into time intervals and applies an aggregation function.

```
* | timechart(span=5m, function=count())
* | timechart(span=1h, function=avg(response_time))
```

Combine with `groupBy()` for multi-series charts (one line per group):

```
* | groupBy(status) | timechart(span=5m, function=count())
```

### Parameters

| Parameter  | Required | Description |
|------------|----------|-------------|
| `span`     | No       | Bucket interval. Supports `s`, `m`, `h`, `d`, `w`. Default: `5m`. |
| `function` | No       | Aggregation function to apply per bucket: `count()`, `sum(field)`, `avg(field)`, `max(field)`, `min(field)`. Default: `count()`. |
