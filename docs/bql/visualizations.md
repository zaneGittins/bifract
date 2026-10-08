# Visualizations

## Pie Chart

```
* | groupBy(status) | piechart()
* | groupBy(image, function=count()) | piechart(render=5)
```

## Bar Chart

```
* | groupBy(user, function=count()) | barchart()
* | groupBy(status) | barchart(render=10)
```

## Graph (Relationship View)

```
* | table(process_guid, parent_process_guid) | graph(child=process_guid, parent=parent_process_guid)
* | graph(child=process_guid, parent=parent_process_guid, render=200)
```

Both `child=` and `parent=` are required. Default limit 100, max 500.

## Mesh (Network View)

Renders a force-directed graph of source-to-destination relationships. Where `graph()` draws a hierarchy, `mesh()` draws a many-to-many network, which suits traffic, authentication, and peer-to-peer data.

```
* | mesh(src=src_ip, dst=dst_ip)
* | mesh(src=src_ip, dst=dst_ip, weight=_count, directed=true, render=300)
* | mesh(src=user, dst=computer_name, color=department, labels=user)
```

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `src` | Yes | Field for source nodes |
| `dst` | Yes | Field for destination nodes |
| `weight` | No | Field controlling edge thickness (default: `_count`) |
| `size` | No | Field summed per node to control node size (default: `_count`, falling back to node degree) |
| `color` | No | Coloring mode. Default `auto`: by IP subnet when nodes look like IPs, else by degree. Also `subnet` (or `subnet/16`), `degree`, `role`, or a field name (top-8 palette) |
| `labels` | No | Comma-separated fields to display as node labels |
| `directed` | No | Draw src-to-dst arrows (default: `false`) |
| `limit` | No | Max edges to render (default: 100, max: 500) |

## Single Value

Display a single aggregate statistic as a large number. Requires an aggregation that produces a single row.

```
* | count() | singleval()
* | avg(response_time) | singleval(title="Avg Response Time")
* | groupBy(computer_name) | count() | singleval(title="Unique Computers")
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

## Box Plot

Summarize a numeric field's distribution, optionally one box per group:

```
* | len(commandline) | boxplot(_len, by=image)
* | groupby(computer_name) | boxplot(_count)
* | boxplot(duration, fence=3)
```

The box spans the first to third quartile (Q1 to Q3) with a line at the median and a diamond at the mean. Values past a fence (`Q1 - fence * IQR`, `Q3 + fence * IQR`) are outliers and drawn as points. Each whisker ends at the most extreme value inside its fence. The fences are in the result table, ready to use as alert thresholds.

Quartiles are approximate (0.1% rank error) so the chart stays fast on billions of rows. Up to `outliers` of the most extreme values are kept at each end of each box. If that many lie past a fence, more may exist, and the tooltip says so.

### Parameters

| Parameter  | Required | Description |
|------------|----------|-------------|
| `field`    | Yes      | Numeric field to summarize |
| `by`       | No       | One box per value of this field, most frequent first |
| `fence`    | No       | IQR multiplier for the outlier fences (default: 1.5) |
| `limit`    | No       | Max groups (default: 20, max: 100) |
| `outliers` | No       | Extreme values kept at each end per group (default: 10, max: 100) |

## Scatter Plot

Plot two numeric fields against each other, one point per row:

```
* | groupby(computer_name) | multi(sum(orig_bytes, as=sent), sum(resp_bytes, as=received)) | scatter(x=received, y=sent, label=computer_name)
* | scatter(x=duration, y=orig_bytes)
```

After an aggregation each point is a group, which is the usual way to use it: hosts that break a trend the rest follow stand out. Before an aggregation each point is an event, taken from the newest events that carry both values. Axes switch to a log scale when positive values span three or more orders of magnitude.

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `x`       | Yes      | Numeric field for the X axis |
| `y`       | Yes      | Numeric field for the Y axis |
| `label`   | No       | Field that names each point in the tooltip |
| `limit`   | No       | Max points (default: 5000, max: 50000) |

## Heatmap

Render a 2D density heatmap with aggregated values:

```
* | heatmap(x=src_ip, y=dst_port, value=count())
* | heatmap(x=user, y=action, value=sum(bytes))
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

## Render caps vs row limits

`render=` is how many marks a chart draws; `limit=` bounds the rows the query
returns. A chart with a large result set usually wants both.

```
* | groupBy(src_ip) | mesh(src=src_ip, dst=dst_ip, render=300, limit=5000)
```
