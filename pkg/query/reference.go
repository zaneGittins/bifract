package query

import (
	"encoding/json"
	"net/http"
)

type FunctionDoc struct {
	Name        string   `json:"name"`
	Category    string   `json:"category"`
	Description string   `json:"description"`
	Syntax      string   `json:"syntax"`
	Parameters  []Param  `json:"parameters"`
	Examples    []string `json:"examples"`
}

type Param struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Required    bool   `json:"required"`
	Description string `json:"description"`
}

type OperatorDoc struct {
	Operator    string   `json:"operator"`
	Description string   `json:"description"`
	Examples    []string `json:"examples"`
}

type ReferenceResponse struct {
	Functions []FunctionDoc `json:"functions"`
	Operators []OperatorDoc `json:"operators"`
}

// HandleReference returns documentation for all supported query functions
func (h *QueryHandler) HandleReference(w http.ResponseWriter, r *http.Request) {
	response := ReferenceResponse{
		Functions: []FunctionDoc{
			{
				Name:        "groupby",
				Category:    "Aggregation",
				Description: "Groups results by one or more fields, similar to SQL GROUP BY",
				Syntax:      "| groupby(field1, field2, ...)",
				Parameters: []Param{
					{Name: "fields", Type: "string", Required: true, Description: "One or more field names to group by"},
				},
				Examples: []string{
					"level=error | groupby(host)",
					"| groupby(level, service)",
					"status!=200 | groupby(method, path)",
				},
			},
			{
				Name:        "analyzeFields",
				Category:    "Aggregation",
				Description: "Computes per-field statistics: event count, distinct values, and numeric stats (min, max, mean, stdev). Useful for data exploration and threat hunting.",
				Syntax:      "| analyzeFields(field1, field2, ..., limit=N)",
				Parameters: []Param{
					{Name: "fields", Type: "string", Required: false, Description: "Fields to analyze (default: all fields)"},
					{Name: "limit", Type: "int", Required: false, Description: "Max rows to scan (default: 50000, max: 200000)"},
				},
				Examples: []string{
					"service=webapp | analyzeFields()",
					"* | analyzeFields(user, image)",
					"* | analyzeFields() | _events < 10",
					"event_id=4625 | analyzeFields(limit=100000)",
				},
			},
			{
				Name:        "table",
				Category:    "Display",
				Description: "Selects specific fields to display in results, similar to SQL SELECT",
				Syntax:      "| table(field1, field2, ...)",
				Parameters: []Param{
					{Name: "fields", Type: "string", Required: true, Description: "One or more field names to include in results"},
				},
				Examples: []string{
					"| table(timestamp, level, message)",
					"level=error | table(host, service, error_code)",
					"| table(timestamp, user_id, action)",
				},
			},
			{
				Name:        "count",
				Category:    "Aggregation",
				Description: "Counts the number of events or occurrences",
				Syntax:      "| count()",
				Parameters:  []Param{},
				Examples: []string{
					"level=error | count()",
					"| groupby(host) | count()",
					"status=500 | groupby(service) | count()",
				},
			},
			{
				Name:        "sum",
				Category:    "Aggregation",
				Description: "Calculates the sum of a numeric field",
				Syntax:      "| sum(field)",
				Parameters: []Param{
					{Name: "field", Type: "numeric", Required: true, Description: "The field to sum"},
				},
				Examples: []string{
					"| sum(bytes)",
					"| groupby(host) | sum(response_time)",
					"status=200 | sum(request_size)",
				},
			},
			{
				Name:        "avg",
				Category:    "Aggregation",
				Description: "Calculates the average of a numeric field",
				Syntax:      "| avg(field)",
				Parameters: []Param{
					{Name: "field", Type: "numeric", Required: true, Description: "The field to average"},
				},
				Examples: []string{
					"| avg(response_time)",
					"| groupby(service) | avg(latency)",
					"level=info | avg(memory_usage)",
				},
			},
			{
				Name:        "sort",
				Category:    "Ordering",
				Description: "Sorts results by a field in ascending or descending order",
				Syntax:      "| sort(field [, order])",
				Parameters: []Param{
					{Name: "field", Type: "string", Required: true, Description: "The field to sort by"},
					{Name: "order", Type: "string", Required: false, Description: "Sort order: 'asc' (default) or 'desc'"},
				},
				Examples: []string{
					"| sort(timestamp)",
					"| sort(count, desc)",
					"| groupby(host) | count() | sort(count, desc)",
				},
			},
			{
				Name:        "limit",
				Category:    "Limiting",
				Description: "Limits the number of results returned",
				Syntax:      "| limit(n)",
				Parameters: []Param{
					{Name: "n", Type: "number", Required: true, Description: "Maximum number of results to return"},
				},
				Examples: []string{
					"| limit(100)",
					"level=error | sort(timestamp, desc) | limit(10)",
					"| groupby(user) | count() | sort(count, desc) | limit(5)",
				},
			},
			{
				Name:        "selectfirst",
				Category:    "Aggregation",
				Description: "Selects the value from the first (earliest) event in each group",
				Syntax:      "| selectfirst(field)",
				Parameters: []Param{
					{Name: "field", Type: "string", Required: true, Description: "The field to select from the first event"},
				},
				Examples: []string{
					"| groupby(user) | selectfirst(event_id)",
					"| groupby(host) | selectfirst(timestamp)",
					"error=true | groupby(service) | selectfirst(error_message)",
				},
			},
			{
				Name:        "selectlast",
				Category:    "Aggregation",
				Description: "Selects the value from the last (latest) event in each group",
				Syntax:      "| selectlast(field)",
				Parameters: []Param{
					{Name: "field", Type: "string", Required: true, Description: "The field to select from the last event"},
				},
				Examples: []string{
					"| groupby(user) | selectlast(event_id)",
					"| groupby(host) | selectlast(timestamp)",
					"status=500 | groupby(service) | selectlast(error_code)",
				},
			},
			{
				Name:        "chain",
				Category:    "Detection",
				Description: "Detects sequential event patterns sharing common field(s) using ClickHouse sequenceMatch. Supports multiple grouping fields and multi-condition steps.",
				Syntax:      "| chain(field1, field2, ..., within=DURATION) { step1; step2; ... }",
				Parameters: []Param{
					{Name: "field", Type: "string", Required: true, Description: "One or more grouping fields - events are partitioned by these fields"},
					{Name: "within", Type: "duration", Required: false, Description: "Max time between consecutive steps (e.g., 5m, 1h, 1d)"},
				},
				Examples: []string{
					"| chain(user) { event_id=4624; event_id=4688 }",
					"| chain(user, within=5m) { event_id=4624; event_id=1 | image=/powershell/i }",
					"event_source=Security | chain(user, within=1h) { event_id=4624; event_id=4672; event_id=4688 }",
					"| chain(user, computer, within=1d) { event_id=4624; event_id=4688 }",
					"| chain(user, within=1d) { event_id=1 | image=/explorer/i; event_id=1 | image=/powershell/i | command_line=/-nop/i; event_id=3 | image=/powershell/i }",
				},
			},
			{
				Name:        "join",
				Category:    "Enrichment",
				Description: "Correlates results with a subquery by joining on a shared field. The subquery runs with the same time range and fractal isolation. Supports inner and left joins.",
				Syntax:      "| join(key, type=inner|left, max=N, include=[fields]) { subquery }",
				Parameters: []Param{
					{Name: "key", Type: "string", Required: true, Description: "Field to join on (must exist in both outer query and subquery results)"},
					{Name: "type", Type: "string", Required: false, Description: "Join type: inner (default) or left"},
					{Name: "max", Type: "number", Required: false, Description: "Max rows the subquery can return (default: 10000, hard max: 100000)"},
					{Name: "include", Type: "array", Required: false, Description: "Fields to include from subquery results (default: all, prefixed with _join_)"},
				},
				Examples: []string{
					`action="denied" | join(src_ip) { action="login" | groupby(src_ip) | count() }`,
					`* | join(user, type=left, include=[department,role]) { * | groupby(user) | selectFirst(department) | selectFirst(role) }`,
					`action="login_failed" | join(user, max=5000) { action="login_success" | groupby(user) | count() }`,
				},
			},
			{
				Name:        "case",
				Category:    "Transformation",
				Description: "Conditional field assignment using case statements",
				Syntax:      "| case { condition | field:=\"value\"; * | field:=\"default\"; }",
				Parameters: []Param{
					{Name: "conditions", Type: "expression", Required: true, Description: "Case conditions with field assignments"},
				},
				Examples: []string{
					"| case { level=/error/i | priority:=\"high\"; * | priority:=\"normal\"; }",
					"| case { status>=400 | category:=\"error\"; status>=200 | category:=\"success\"; }",
					"| case { user=/admin/i | is_admin:=true; * | is_admin:=false; }",
				},
			},
			{
				Name:        "head",
				Category:    "Limiting",
				Description: "Returns the first N events (earliest by timestamp)",
				Syntax:      "| head(n)",
				Parameters: []Param{
					{Name: "n", Type: "number", Required: false, Description: "Number of events to return (default: 200)"},
				},
				Examples: []string{
					"| head(10)",
					"level=error | head(50)",
					"| head()",
				},
			},
			{
				Name:        "tail",
				Category:    "Limiting",
				Description: "Returns the last N events (latest by timestamp)",
				Syntax:      "| tail(n)",
				Parameters: []Param{
					{Name: "n", Type: "number", Required: false, Description: "Number of events to return (default: 200)"},
				},
				Examples: []string{
					"| tail(10)",
					"status=500 | tail(25)",
					"| tail()",
				},
			},
			{
				Name:        "min",
				Category:    "Aggregation",
				Description: "Finds the minimum value of a numeric field",
				Syntax:      "| min(field)",
				Parameters: []Param{
					{Name: "field", Type: "numeric", Required: true, Description: "The field to find minimum value for"},
				},
				Examples: []string{
					"| min(response_time)",
					"| groupby(service) | min(latency)",
					"status=200 | min(bytes)",
				},
			},
			{
				Name:        "max",
				Category:    "Aggregation",
				Description: "Finds the maximum value of a numeric field",
				Syntax:      "| max(field)",
				Parameters: []Param{
					{Name: "field", Type: "numeric", Required: true, Description: "The field to find maximum value for"},
				},
				Examples: []string{
					"| max(response_time)",
					"| groupby(service) | max(memory_usage)",
					"level=error | max(error_count)",
				},
			},
			{
				Name:        "multi",
				Category:    "Aggregation",
				Description: "Computes multiple aggregate statistics. Supports count (with distinct/alias), avg, sum, max, min, stddev, percentile, median, mad, skewness, kurtosis, iqr, selectfirst, selectlast, collect, and top.",
				Syntax:      "| multi(function1, function2, ...)",
				Parameters: []Param{
					{Name: "functions", Type: "string", Required: true, Description: "One or more aggregate functions: count(), avg(), sum(), max(), min(), stddev(), percentile(), median(), mad(), skewness(), kurtosis(), iqr(), selectfirst(), selectlast(), collect(), top()"},
				},
				Examples: []string{
					"| multi(count(), avg(response_time))",
					"| groupby(service) | multi(count(), min(latency), max(latency))",
					"| multi(sum(bytes), count())",
					"| groupby(user) | multi(count(field=event_id, distinct=true, as=unique_events), count(field=event_id, as=total))",
					"| groupby(user) | multi(collect(image))",
					"| groupby(user) | multi(top(field=event_id, percent=true, as=top_events))",
				},
			},
			{
				Name:        "hash",
				Category:    "Transformation",
				Description: "Creates a hash key from one or more fields using cityHash64",
				Syntax:      "| hash(field1, field2, ...)",
				Parameters: []Param{
					{Name: "field", Type: "string", Required: true, Description: "One or more fields to hash"},
					{Name: "as", Type: "string", Required: false, Description: "Alias for the hash column (default: hash_key)"},
				},
				Examples: []string{
					"| hash(user)",
					"| hash(field=user, computer)",
					"| hash(user, event_id, as=composite_key)",
				},
			},
			{
				Name:        "singleval",
				Category:    "Visualization",
				Description: "Displays a single aggregate value as a large number. Requires an aggregation function without groupBy.",
				Syntax:      `| singleval(label="Label")`,
				Parameters: []Param{
					{Name: "label", Type: "string", Required: false, Description: "Display label for the value"},
				},
				Examples: []string{
					"* | count() | singleval()",
					`* | avg(response_time) | singleval(label="Avg Response")`,
					`* | sum(bytes) | singleval(label="Total Bytes")`,
				},
			},
			{
				Name:        "timechart",
				Category:    "Visualization",
				Description: "Creates a time series line chart with bucketed data. Supports multiple series with groupBy.",
				Syntax:      "| timechart(span=5m, function=count())",
				Parameters: []Param{
					{Name: "span", Type: "string", Required: true, Description: "Time bucket size (e.g., 1s, 5m, 15m, 1h, 1d, 1w)"},
					{Name: "function", Type: "string", Required: true, Description: "Aggregation function: count(), sum(field), avg(field), max(field), min(field)"},
				},
				Examples: []string{
					"* | timechart(span=5m, function=count())",
					"* | timechart(span=1h, function=avg(latency))",
					"* | groupBy(status) | timechart(span=5m, function=count())",
				},
			},
			{
				Name:        "match",
				Category:    "Enrichment",
				Description: "Enriches log events by looking up values in a dictionary. Adds the specified columns from the dictionary to each log row. With strict=true, only rows that have a matching key in the dictionary are returned.",
				Syntax:      `| match(dict="name", field=logfield, column=keycolumn, include=[col1,col2], strict=false)`,
				Parameters: []Param{
					{Name: "dict", Type: "string", Required: true, Description: "Name of the dictionary defined in the fractal"},
					{Name: "field", Type: "string", Required: true, Description: "Log field whose value is used as the lookup key"},
					{Name: "column", Type: "string", Required: true, Description: "Primary key column in the dictionary (documents intent; implicit in the lookup)"},
					{Name: "include", Type: "array", Required: true, Description: "Comma-separated list of dictionary columns to add to each row"},
					{Name: "strict", Type: "bool", Required: false, Description: "When true, only rows with a matching key in the dictionary are returned (default: false)"},
				},
				Examples: []string{
					`| match(dict="threat_intel", field=src_ip, column=ip, include=[threat_score,category], strict=false)`,
					`| match(dict="users", field=user_id, column=id, include=[username,department], strict=true)`,
					`| match(dict="geo", field=ip, column=ip, include=[country,city])`,
				},
			},
			{
				Name:        "lookupIP",
				Category:    "Enrichment",
				Description: "Enriches logs with GeoIP and ASN data from MaxMind GeoLite2 databases. Requires MAXMIND_LICENSE_KEY and MAXMIND_ACCOUNT_ID environment variables.",
				Syntax:      "| lookupIP(field=ip_field, include=[country,city,asn,as_org])",
				Parameters: []Param{
					{Name: "field", Type: "string", Required: true, Description: "Field containing the IP address to look up"},
					{Name: "include", Type: "array", Required: true, Description: "Columns to include: country, city, subdivision, continent, timezone, latitude, longitude, postal_code, asn, as_org"},
				},
				Examples: []string{
					`| lookupIP(field=src_ip, include=[country,city])`,
					`| lookupIP(field=client_ip, include=[asn,as_org,country])`,
					`| lookupIP(field=src_ip, include=[country,city]) | groupby(country)`,
				},
			},
			{
				Name:        "graphWorld",
				Category:    "Visualization",
				Description: "Renders data points on an interactive world map with zoom and density clustering. Points cluster together at low zoom levels and split apart as you zoom in. Works with latitude/longitude fields, typically from lookupIP().",
				Syntax:      "| graphWorld(lat=field, lon=field, label=field, limit=N)",
				Parameters: []Param{
					{Name: "lat", Type: "string", Required: false, Description: "Latitude field (default: latitude)"},
					{Name: "lon", Type: "string", Required: false, Description: "Longitude field (default: longitude)"},
					{Name: "label", Type: "string", Required: false, Description: "Field to display as marker label in popups"},
					{Name: "limit", Type: "number", Required: false, Description: "Maximum number of points to render (default: 5000, max: 50000)"},
				},
				Examples: []string{
					`| lookupIP(field=src_ip, include=[latitude,longitude,country]) | graphWorld(label=country)`,
					`| lookupIP(field=src_ip, include=[latitude,longitude,city,asn]) | graphWorld(label=city)`,
					`| graphWorld(lat=geo_lat, lon=geo_lon)`,
				},
			},
			{
				Name:        "percentile",
				Category:    "Aggregation",
				Description: "Calculates the specified percentile of a numeric field",
				Syntax:      "| percentile(field, p)",
				Parameters: []Param{
					{Name: "field", Type: "numeric", Required: true, Description: "The field to calculate percentile for"},
					{Name: "p", Type: "number", Required: false, Description: "Percentile value 0-100 (default: 95)"},
				},
				Examples: []string{
					"| percentile(response_time)",
					"| groupby(service) | percentile(latency, 99)",
					"| percentile(duration, 50)",
				},
			},
			{
				Name:        "stddev",
				Category:    "Aggregation",
				Description: "Calculates the standard deviation of a numeric field",
				Syntax:      "| stddev(field)",
				Parameters: []Param{
					{Name: "field", Type: "numeric", Required: true, Description: "The field to calculate standard deviation for"},
				},
				Examples: []string{
					"| stddev(response_time)",
					"| groupby(service) | stddev(latency)",
				},
			},
			{
				Name:        "median",
				Category:    "Aggregation",
				Description: "Calculates the median (50th percentile) of a numeric field using an approximate t-digest algorithm, efficient on large datasets.",
				Syntax:      "| median(field)",
				Parameters: []Param{
					{Name: "field", Type: "numeric", Required: true, Description: "The field to calculate the median for"},
				},
				Examples: []string{
					"| median(response_time)",
					"| groupby(service) | median(latency)",
				},
			},
			{
				Name:        "mad",
				Category:    "Aggregation",
				Description: "Calculates the median absolute deviation of a numeric field, a robust measure of variability resistant to outliers. Also returns the median. Use with groupBy and comparison operators for anomaly detection.",
				Syntax:      "| mad(field)",
				Parameters: []Param{
					{Name: "field", Type: "numeric", Required: true, Description: "The field to calculate MAD for"},
				},
				Examples: []string{
					"| mad(response_time)",
					"| groupby(host) | mad(latency) | _mad > 50",
					"| groupby(service) | multi(mad(response_time), avg(response_time))",
				},
			},
			{
				Name:        "skewness",
				Category:    "Aggregation",
				Description: "Calculates the population skewness of a numeric field. Positive values indicate a right-skewed distribution, negative values indicate left-skewed. Alias: skew.",
				Syntax:      "| skewness(field)",
				Parameters: []Param{
					{Name: "field", Type: "numeric", Required: true, Description: "The field to calculate skewness for"},
				},
				Examples: []string{
					"| skewness(response_time)",
					"| groupby(host) | multi(skewness(latency), kurtosis(latency))",
				},
			},
			{
				Name:        "kurtosis",
				Category:    "Aggregation",
				Description: "Calculates the population excess kurtosis of a numeric field. Higher values indicate heavier tails (more outliers). Alias: kurt.",
				Syntax:      "| kurtosis(field)",
				Parameters: []Param{
					{Name: "field", Type: "numeric", Required: true, Description: "The field to calculate kurtosis for"},
				},
				Examples: []string{
					"| kurtosis(response_time)",
					"| groupby(host) | multi(skewness(latency), kurtosis(latency))",
				},
			},
			{
				Name:        "frequency",
				Category:    "Aggregation",
				Description: "Produces a frequency table for the given field, showing each unique value with its count, percentage of total, and cumulative percentage. Results are sorted by count descending.",
				Syntax:      "| frequency(field)",
				Parameters: []Param{
					{Name: "field", Type: "string", Required: true, Description: "The field to analyze"},
				},
				Examples: []string{
					"| frequency(event_name)",
					"| frequency(status_code) | _percentage > 1.0",
				},
			},
			{
				Name:        "modifiedZScore",
				Category:    "Detection",
				Description: "Computes the modified z-score for each row using median and MAD (median absolute deviation). Robust outlier detection resistant to skewed distributions. Formula: 0.6745 * (x - median) / MAD. Adds _median, _mad, and _modified_z columns. Aliases: modifiedz, mzscore.",
				Syntax:      "| modifiedZScore(field)",
				Parameters: []Param{
					{Name: "field", Type: "numeric", Required: true, Description: "The numeric field to compute modified z-scores for"},
				},
				Examples: []string{
					"| modifiedZScore(response_time)",
					"| groupby(user) | count() | modifiedZScore(_count)",
					"| modifiedZScore(latency) | _modified_z > 3.5",
					"| groupby(user) | count() | modifiedZScore(_count) | sort(_modified_z, desc)",
				},
			},
			{
				Name:        "madOutlier",
				Category:    "Detection",
				Description: "Computes modified z-score and flags outliers exceeding the threshold. Returns _median, _mad, _modified_z, and _is_outlier (1/0). Default threshold: 3.5. Alias: outlier.",
				Syntax:      "| madOutlier(field, threshold)",
				Parameters: []Param{
					{Name: "field", Type: "numeric", Required: true, Description: "The numeric field to detect outliers in"},
					{Name: "threshold", Type: "number", Required: false, Description: "Modified z-score threshold for outlier flag (default: 3.5)"},
				},
				Examples: []string{
					"| groupby(user) | count() | madOutlier(_count)",
					"| groupby(user) | count() | madOutlier(_count, 3.5) | _is_outlier = 1",
					"| madOutlier(latency, threshold=2.5)",
				},
			},
			{
				Name:        "iqr",
				Category:    "Aggregation",
				Description: "Calculates the interquartile range (IQR) of a numeric field. Returns Q1 (25th percentile), Q3 (75th percentile), and IQR (Q3-Q1). Useful for robust spread measurement and outlier detection.",
				Syntax:      "| iqr(field)",
				Parameters: []Param{
					{Name: "field", Type: "numeric", Required: true, Description: "The numeric field to calculate IQR for"},
				},
				Examples: []string{
					"| iqr(response_time)",
					"| groupby(service) | multi(iqr(latency), median(latency))",
					"| groupby(host) | count() | iqr(_count)",
				},
			},
			{
				Name:        "headTail",
				Category:    "Aggregation",
				Description: "Frequency analysis with Pareto segmentation. Groups by field, counts occurrences, computes percentage and cumulative percentage, and labels each value as 'head' or 'tail' based on cumulative percentage threshold. Default threshold: 80 (80/20 rule).",
				Syntax:      "| headTail(field, threshold)",
				Parameters: []Param{
					{Name: "field", Type: "string", Required: true, Description: "The field to analyze"},
					{Name: "threshold", Type: "number", Required: false, Description: "Cumulative percentage threshold for head/tail split (default: 80)"},
				},
				Examples: []string{
					"| headTail(event_name)",
					"| headTail(user, 90)",
					"| headTail(src_ip) | _segment = head",
				},
			},
			{
				Name:        "regex",
				Category:    "Extraction",
				Description: "Extracts values from a field using a regex pattern with capture groups. Named captures (?<name>...) are extracted to individual fields.",
				Syntax:      `| regex("pattern", field=raw_log) or | regex(field=name, regex="pattern")`,
				Parameters: []Param{
					{Name: "pattern", Type: "regex", Required: true, Description: "Regex pattern with capture groups. Use (?<name>...) for named captures."},
					{Name: "field", Type: "string", Required: false, Description: "Field to extract from (default: raw_log)"},
					{Name: "regex", Type: "string", Required: false, Description: "Alternative way to specify the regex pattern"},
				},
				Examples: []string{
					`| regex("(\\d+\\.\\d+\\.\\d+\\.\\d+)", field=raw_log)`,
					`| regex("user=(\\w+)", field=message)`,
					`| regex(field=image, regex="(.+)\\\\(?<executable_name>.*\\.exe)")`,
				},
			},
			{
				Name:        "replace",
				Category:    "Transformation",
				Description: "Replaces text matching a regex pattern in a field",
				Syntax:      `| replace("pattern", "replacement", field, as=output)`,
				Parameters: []Param{
					{Name: "pattern", Type: "regex", Required: true, Description: "Regex pattern to match"},
					{Name: "replacement", Type: "string", Required: true, Description: "Replacement string"},
					{Name: "field", Type: "string", Required: false, Description: "Field to replace in (default: raw_log)"},
					{Name: "as", Type: "string", Required: false, Description: "Output field name"},
				},
				Examples: []string{
					`| replace("password=\\S+", "password=***", raw_log)`,
					`| replace("\\d{4}-\\d{4}", "XXXX-XXXX", message, as=redacted)`,
				},
			},
			{
				Name:        "concat",
				Category:    "Transformation",
				Description: "Concatenates multiple fields into a single field",
				Syntax:      "| concat([field1, field2, ...], as=output)",
				Parameters: []Param{
					{Name: "fields", Type: "array", Required: true, Description: "Fields to concatenate"},
					{Name: "as", Type: "string", Required: false, Description: "Output field name"},
				},
				Examples: []string{
					"| concat([user, host], as=user_host)",
					"| concat([method, path], as=request)",
				},
			},
			{
				Name:        "lowercase",
				Category:    "Transformation",
				Description: "Converts a field's values to lowercase",
				Syntax:      "| lowercase(field)",
				Parameters: []Param{
					{Name: "field", Type: "string", Required: true, Description: "The field to convert to lowercase"},
				},
				Examples: []string{
					"| lowercase(hostname)",
					"| lowercase(user) | groupby(user)",
				},
			},
			{
				Name:        "uppercase",
				Category:    "Transformation",
				Description: "Converts a field's values to uppercase",
				Syntax:      "| uppercase(field)",
				Parameters: []Param{
					{Name: "field", Type: "string", Required: true, Description: "The field to convert to uppercase"},
				},
				Examples: []string{
					"| uppercase(level)",
					"| uppercase(status) | groupby(status)",
				},
			},
			{
				Name:        "len",
				Category:    "Transformation",
				Description: "Returns the string length of a field's value as _len",
				Syntax:      "| len(field)",
				Parameters: []Param{
					{Name: "field", Type: "string", Required: true, Description: "The field to measure the length of"},
				},
				Examples: []string{
					"| len(message)",
					"| len(program_name) | _len > 10",
					"| len(user) | sort(_len, desc)",
				},
			},
			{
				Name:        "levenshtein",
				Category:    "Transformation",
				Description: "Calculates the Damerau-Levenshtein edit distance between two fields or values as _distance",
				Syntax:      "| levenshtein(field1, field2)",
				Parameters: []Param{
					{Name: "s1", Type: "string", Required: true, Description: "First field or quoted string"},
					{Name: "s2", Type: "string", Required: true, Description: "Second field or quoted string"},
				},
				Examples: []string{
					`| levenshtein(user, "admin")`,
					"| levenshtein(src_host, dst_host)",
					`| levenshtein(process_name, "svchost.exe") | _distance < 3`,
				},
			},
			{
				Name:        "base64Decode",
				Category:    "Transformation",
				Description: "Decodes a base64-encoded field value as _decoded. Returns empty string on invalid input.",
				Syntax:      "| base64Decode(field)",
				Parameters: []Param{
					{Name: "field", Type: "string", Required: true, Description: "The base64-encoded field to decode"},
				},
				Examples: []string{
					"| base64Decode(payload)",
					"| base64Decode(encoded_command) | _decoded=/powershell/i",
					"| base64Decode(data) | table(data, _decoded)",
				},
			},
			{
				Name:        "dedup",
				Category:    "Filtering",
				Description: "Deduplicates results by one or more fields, keeping the first occurrence (earliest by timestamp)",
				Syntax:      "| dedup(field1, field2, ...)",
				Parameters: []Param{
					{Name: "fields", Type: "string", Required: true, Description: "One or more fields to deduplicate by"},
				},
				Examples: []string{
					"| dedup(user)",
					"| dedup(src_ip, dst_ip)",
					"level=error | dedup(host, service) | table(host, service, message)",
				},
			},
			{
				Name:        "cidr",
				Category:    "Filtering",
				Description: "Filters events where an IP field is within a CIDR range. Use !cidr() to exclude a range.",
				Syntax:      `| cidr(field, "range")`,
				Parameters: []Param{
					{Name: "field", Type: "string", Required: true, Description: "The IP address field to check"},
					{Name: "range", Type: "string", Required: true, Description: "CIDR range (e.g., 10.0.0.0/8, 192.168.1.0/24)"},
				},
				Examples: []string{
					`| cidr(src_ip, "10.0.0.0/8")`,
					`| cidr(dst_ip, "192.168.1.0/24")`,
					`| !cidr(src_ip, "10.0.0.0/8")`,
				},
			},
			{
				Name:        "split",
				Category:    "Transformation",
				Description: "Splits a field by a delimiter and returns the Nth element (1-indexed) as _split",
				Syntax:      `| split(field, "delimiter", index)`,
				Parameters: []Param{
					{Name: "field", Type: "string", Required: true, Description: "The field to split"},
					{Name: "delimiter", Type: "string", Required: true, Description: "The delimiter string"},
					{Name: "index", Type: "number", Required: true, Description: "1-based index of the element to extract"},
				},
				Examples: []string{
					`| split(image, "\\", -1)`,
					`| split(path, "/", 2)`,
					`| split(email, "@", 2) | groupby(_split) | count()`,
				},
			},
			{
				Name:        "substr",
				Category:    "Transformation",
				Description: "Extracts a substring from a field as _substr",
				Syntax:      "| substr(field, start, length)",
				Parameters: []Param{
					{Name: "field", Type: "string", Required: true, Description: "The field to extract from"},
					{Name: "start", Type: "number", Required: true, Description: "Starting position (1-based)"},
					{Name: "length", Type: "number", Required: false, Description: "Number of characters to extract (default: rest of string)"},
				},
				Examples: []string{
					"| substr(message, 1, 50)",
					"| substr(hash, 1, 8)",
					"| substr(path, 5) | table(path, _substr)",
				},
			},
			{
				Name:        "urldecode",
				Category:    "Transformation",
				Description: "Decodes a URL-encoded field as _urldecoded",
				Syntax:      "| urldecode(field)",
				Parameters: []Param{
					{Name: "field", Type: "string", Required: true, Description: "The URL-encoded field to decode"},
				},
				Examples: []string{
					"| urldecode(request_uri)",
					"| urldecode(query_string) | _urldecoded=/script/i",
					"| urldecode(path) | table(path, _urldecoded)",
				},
			},
			{
				Name:        "coalesce",
				Category:    "Transformation",
				Description: "Returns the first non-empty value from a list of fields as _coalesced",
				Syntax:      "| coalesce(field1, field2, ...)",
				Parameters: []Param{
					{Name: "fields", Type: "string", Required: true, Description: "Two or more fields to check in order"},
				},
				Examples: []string{
					"| coalesce(user, username, account_name)",
					"| coalesce(src_ip, client_ip) | groupby(_coalesced) | count()",
					"| coalesce(display_name, email, user_id) | table(_coalesced)",
				},
			},
			{
				Name:        "sprintf",
				Category:    "Transformation",
				Description: "Formats fields into a string using printf-style format specifiers, output as _sprintf or custom alias",
				Syntax:      `| sprintf(format_string, field1, field2, ..., as=alias)`,
				Parameters: []Param{
					{Name: "format", Type: "string", Required: true, Description: "Printf-style format string with %s, %d, etc."},
					{Name: "fields", Type: "string", Required: false, Description: "Fields to substitute into the format string"},
					{Name: "as", Type: "string", Required: false, Description: "Output field name (default: _sprintf)"},
				},
				Examples: []string{
					`| sprintf("%s - %s", username, action, as=user_action)`,
					`| sprintf("https://%s:%d/%s", hostname, port, path, as=full_url)`,
					`| sprintf("%s@%s", user, domain) | groupby(_sprintf) | count()`,
				},
			},
			{
				Name:        "eval",
				Category:    "Transformation",
				Description: "Creates computed fields using mathematical expressions",
				Syntax:      `| eval("field = expression")`,
				Parameters: []Param{
					{Name: "expression", Type: "string", Required: true, Description: "Assignment expression (e.g., score = latency * priority)"},
				},
				Examples: []string{
					`| eval("score = bytes + priority")`,
					`| eval("rate = requests / duration")`,
				},
			},
			{
				Name:        "comment",
				Category:    "Filtering",
				Description: "Filters logs to only those with comments. Optionally filter by tag labels or keyword search in comment text.",
				Syntax:      "| comment(tags=tag1,tag2, keyword=\"text\")",
				Parameters: []Param{
					{Name: "tags", Type: "string", Required: false, Description: "One or more tag labels to filter by (OR logic). Case sensitive."},
					{Name: "keyword", Type: "string", Required: false, Description: "Search term matched against comment text (case insensitive)"},
				},
				Examples: []string{
					"* | comment()",
					"* | comment(tags=security)",
					"* | comment(tags=security,critical)",
					`* | comment(keyword="timeout")`,
					`* | comment(keyword="error", tags=security)`,
					"* | comment(tags=incident) | groupby(src_ip) | count()",
				},
			},
			{
				Name:        "in",
				Category:    "Filtering",
				Description: "Filters events where a field matches any value in a list",
				Syntax:      "| in(field, values=[val1, val2, ...])",
				Parameters: []Param{
					{Name: "field", Type: "string", Required: true, Description: "The field to check"},
					{Name: "values", Type: "array", Required: true, Description: "List of values to match against"},
				},
				Examples: []string{
					"| in(level, values=[error, fatal, critical])",
					"| in(status, values=[404, 500, 503])",
				},
			},
			{
				Name:        "strftime",
				Category:    "Transformation",
				Description: "Extracts or formats time components from a timestamp as _time or custom alias",
				Syntax:      `| strftime(format, field=timestamp, timezone=UTC, as=_time)`,
				Parameters: []Param{
					{Name: "format", Type: "string", Required: true, Description: "strftime format string (%H, %b, %a, %Y, %d, etc.)"},
					{Name: "field", Type: "string", Required: false, Description: "Field to format (default: timestamp)"},
					{Name: "timezone", Type: "string", Required: false, Description: "Timezone (default: UTC)"},
					{Name: "as", Type: "string", Required: false, Description: "Output field name (default: _time)"},
				},
				Examples: []string{
					`| strftime("%H", as=_hour)`,
					`| strftime("%a", as=_day)`,
					`| strftime("%Y-%m-%d", timezone="US/Eastern", as=_date)`,
				},
			},
			{
				Name:        "now",
				Category:    "Transformation",
				Description: "Returns the current timestamp as a computed field",
				Syntax:      "| now(outputField)",
				Parameters: []Param{
					{Name: "outputField", Type: "string", Required: false, Description: "Output field name (default: current_time)"},
				},
				Examples: []string{
					"| now()",
					"| now(snapshot_time)",
				},
			},
			{
				Name:        "bucket",
				Category:    "Aggregation",
				Description: "Groups events into time buckets with an aggregation function",
				Syntax:      "| bucket(span=1h, function=count())",
				Parameters: []Param{
					{Name: "span", Type: "string", Required: true, Description: "Time bucket size (e.g., 1s, 5m, 1h, 1d)"},
					{Name: "function", Type: "string", Required: true, Description: "Aggregation function: count(), sum(field)"},
				},
				Examples: []string{
					"| bucket(span=1h, function=count())",
					"| bucket(span=5m, function=sum(bytes))",
				},
			},
			{
				Name:        "piechart",
				Category:    "Visualization",
				Description: "Renders results as a pie chart. Requires groupby with aggregation.",
				Syntax:      "| piechart(limit=N)",
				Parameters: []Param{
					{Name: "limit", Type: "number", Required: false, Description: "Maximum number of slices"},
				},
				Examples: []string{
					"| groupby(level) | count() | piechart()",
					"| groupby(service) | count() | piechart(limit=10)",
				},
			},
			{
				Name:        "barchart",
				Category:    "Visualization",
				Description: "Renders results as a bar chart. Requires groupby with aggregation.",
				Syntax:      "| barchart(limit=N)",
				Parameters: []Param{
					{Name: "limit", Type: "number", Required: false, Description: "Maximum number of bars"},
				},
				Examples: []string{
					"| groupby(status) | count() | barchart()",
					"| groupby(host) | sum(bytes) | barchart(limit=15)",
				},
			},
			{
				Name:        "graph",
				Category:    "Visualization",
				Description: "Renders results as a network/relationship graph",
				Syntax:      "| graph(child=field, parent=field, labels=field1,field2)",
				Parameters: []Param{
					{Name: "child", Type: "string", Required: true, Description: "Field for child nodes"},
					{Name: "parent", Type: "string", Required: true, Description: "Field for parent nodes"},
					{Name: "labels", Type: "string", Required: false, Description: "Comma-separated fields to display as node labels (default: all non-key fields)"},
					{Name: "limit", Type: "number", Required: false, Description: "Maximum results to render (default: 100, max: 500)"},
				},
				Examples: []string{
					"| graph(child=process, parent=parent_process)",
					"| graph(child=process_guid, parent=parent_process_guid, labels=image)",
					"| graph(child=process_guid, parent=parent_process_guid, labels=image,computer_name)",
				},
			},
			{
				Name:        "bfs",
				Category:    "Traversal",
				Description: "Breadth-first search traversal of parent-child relationships in log data. Starts from a specific node and discovers all connected nodes level by level. Always returns child and parent fields. Use include= to add extra fields. Pairs well with graph() for visualization.",
				Syntax:      `| bfs(child=field, parent=field, start="value", depth=N, include=[field1,field2])`,
				Parameters: []Param{
					{Name: "child", Type: "string", Required: true, Description: "Field that uniquely identifies each node (e.g., process_guid)"},
					{Name: "parent", Type: "string", Required: true, Description: "Field that references the parent node (e.g., parent_process_guid)"},
					{Name: "start", Type: "string", Required: true, Description: "Value of the child field for the starting node"},
					{Name: "depth", Type: "number", Required: false, Description: "Maximum traversal depth (default: 10, max: 50)"},
					{Name: "include", Type: "array", Required: false, Description: "Additional fields to extract from logs (e.g., include=[image,command_line]). Child and parent fields are always included."},
				},
				Examples: []string{
					`event_id=1 | bfs(child=process_guid, parent=parent_process_guid, start="{GUID}")`,
					`event_id=1 | bfs(child=process_guid, parent=parent_process_guid, start="{GUID}", include=image)`,
					`event_id=1 | bfs(child=process_guid, parent=parent_process_guid, start="{GUID}", include=[image,command_line]) | graph(child=process_guid, parent=parent_process_guid, labels=image)`,
					`event_id=1 | bfs(child=process_guid, parent=parent_process_guid, start="{GUID}", depth=5) | table(process_guid, image, _depth)`,
				},
			},
			{
				Name:        "dfs",
				Category:    "Traversal",
				Description: "Depth-first search traversal of parent-child relationships in log data. Starts from a specific node and follows each chain to its maximum depth before backtracking. Always returns child and parent fields. Use include= to add extra fields. Results are ordered by traversal path.",
				Syntax:      `| dfs(child=field, parent=field, start="value", depth=N, include=[field1,field2])`,
				Parameters: []Param{
					{Name: "child", Type: "string", Required: true, Description: "Field that uniquely identifies each node (e.g., process_guid)"},
					{Name: "parent", Type: "string", Required: true, Description: "Field that references the parent node (e.g., parent_process_guid)"},
					{Name: "start", Type: "string", Required: true, Description: "Value of the child field for the starting node"},
					{Name: "depth", Type: "number", Required: false, Description: "Maximum traversal depth (default: 10, max: 50)"},
					{Name: "include", Type: "array", Required: false, Description: "Additional fields to extract from logs (e.g., include=[image,command_line]). Child and parent fields are always included."},
				},
				Examples: []string{
					`event_id=1 | dfs(child=process_guid, parent=parent_process_guid, start="{GUID}")`,
					`event_id=1 | dfs(child=process_guid, parent=parent_process_guid, start="{GUID}", include=[image,command_line])`,
					`event_id=1 | dfs(child=process_guid, parent=parent_process_guid, start="{GUID}", depth=3) | graph(child=process_guid, parent=parent_process_guid, labels=image)`,
					`event_id=1 | dfs(child=process_guid, parent=parent_process_guid, start="{GUID}") | table(process_guid, image, _depth, _path)`,
				},
			},
			{
				Name:        "collect",
				Category:    "Aggregation",
				Description: "Collects all values of a field into an array per group (groupArray)",
				Syntax:      "| multi(collect(field))",
				Parameters: []Param{
					{Name: "field", Type: "string", Required: true, Description: "The field to collect values from"},
				},
				Examples: []string{
					"* | groupby(user) | multi(collect(image))",
					"* | groupby(host) | multi(collect(service))",
				},
			},
			{
				Name:        "top",
				Category:    "Aggregation",
				Description: "Shows the top values of a field with their frequency",
				Syntax:      "| multi(top(field=name, percent=bool, as=alias))",
				Parameters: []Param{
					{Name: "field", Type: "string", Required: true, Description: "The field to find top values for"},
					{Name: "percent", Type: "bool", Required: false, Description: "Show as percentage (default: false)"},
					{Name: "as", Type: "string", Required: false, Description: "Output field alias"},
				},
				Examples: []string{
					"* | groupby(user) | multi(top(field=event_id, percent=true, as=top_events))",
					"* | groupby(host) | multi(top(field=service))",
				},
			},
			{
				Name:        "histogram",
				Category:    "Visualization",
				Description: "Distributes a numeric field into equal-width bins and renders a histogram chart",
				Syntax:      "| histogram(field, buckets=N)",
				Parameters: []Param{
					{Name: "field", Type: "numeric", Required: true, Description: "The numeric field to build distribution for"},
					{Name: "buckets", Type: "number", Required: false, Description: "Number of equal-width bins (default: 20)"},
				},
				Examples: []string{
					"* | histogram(response_time)",
					"* | histogram(bytes, buckets=30)",
				},
			},
			{
				Name:        "heatmap",
				Category:    "Visualization",
				Description: "Renders a 2D density heatmap with aggregated values",
				Syntax:      "| heatmap(x=field, y=field, value=agg(), limit=N)",
				Parameters: []Param{
					{Name: "x", Type: "string", Required: true, Description: "Field for the X axis"},
					{Name: "y", Type: "string", Required: true, Description: "Field for the Y axis"},
					{Name: "value", Type: "string", Required: false, Description: "Aggregation function (default: count())"},
					{Name: "limit", Type: "number", Required: false, Description: "Max distinct values per axis (default: 50)"},
				},
				Examples: []string{
					"* | heatmap(x=src_ip, y=dst_port, value=count())",
					"* | heatmap(x=user, y=action, value=sum(bytes), limit=20)",
				},
			},
		},
		Operators: []OperatorDoc{
			{
				Operator:    "=",
				Description: "Exact match - field equals value",
				Examples: []string{
					"level=error",
					"status=200",
					"user_id=12345",
				},
			},
			{
				Operator:    "!=",
				Description: "Not equal - field does not equal value",
				Examples: []string{
					"level!=debug",
					"status!=200",
					"environment!=production",
				},
			},
			{
				Operator:    "~",
				Description: "Regular expression match - case insensitive",
				Examples: []string{
					"message~/error/i",
					"host~/web-[0-9]+/",
					"path~/api\\/v1/",
				},
			},
			{
				Operator:    ">",
				Description: "Greater than - for numeric comparisons",
				Examples: []string{
					"bytes>1000",
					"response_time>500",
					"status_code>400",
				},
			},
			{
				Operator:    "<",
				Description: "Less than - for numeric comparisons",
				Examples: []string{
					"bytes<100",
					"duration<50",
					"error_count<5",
				},
			},
			{
				Operator:    ">=",
				Description: "Greater than or equal to",
				Examples: []string{
					"status>=400",
					"latency>=100",
					"users>=1000",
				},
			},
			{
				Operator:    "<=",
				Description: "Less than or equal to",
				Examples: []string{
					"status<=299",
					"memory<=1024",
					"connections<=100",
				},
			},
			{
				Operator:    "AND",
				Description: "Logical AND - both conditions must be true within a filter stage. Use | to chain multiple filter stages.",
				Examples: []string{
					"level=error AND service=api",
					"status=500 AND response_time>1000",
					"event_id=1 AND image=/powershell/i | NOT user=/system/i",
				},
			},
			{
				Operator:    "OR",
				Description: "Logical OR - at least one condition must be true within a filter stage. Use | to chain multiple filter stages.",
				Examples: []string{
					"level=error OR level=fatal",
					"status=404 OR status=500",
					"event_id=1 | status=error OR status=critical | NOT image=/svchost/i",
				},
			},
			{
				Operator:    "NOT",
				Description: "Logical NOT - negates a condition",
				Examples: []string{
					"NOT level=debug",
					"NOT status=200",
					"service=api AND NOT environment=test",
				},
			},
			{
				Operator:    ":=",
				Description: "Assignment operator for computed fields and mathematical expressions. Supports parenthesized expressions, division, and references to aggregated fields.",
				Examples: []string{
					"field:=value | result:=1+2",
					"| score:=latency*priority",
					"| total:=requests+errors",
					"| groupby(user) | multi(count(field=event_id, distinct=true, as=unique), count(field=event_id, as=total)) | confidence := ((total - unique) / total) * 0.95",
				},
			},
			{
				Operator:    "/pattern/i",
				Description: "Regex pattern matching (case-insensitive with /i flag)",
				Examples: []string{
					"message=/error/i",
					"host=/web-[0-9]+/",
					"path=/\\/api\\//i",
				},
			},
			{
				Operator:    "\"string\"",
				Description: "Bare string search - searches raw log content",
				Examples: []string{
					"\"error occurred\"",
					"\"failed to connect\"",
					"\"user logged in\"",
				},
			},
			{
				Operator:    "/regex/",
				Description: "Bare regex search - searches raw log content",
				Examples: []string{
					"/error/i",
					"/[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}/",
					"/failed|error|exception/i",
				},
			},
			{
				Operator:    "|",
				Description: "Pipeline operator - chains filter stages and functions. Each stage receives the output of the previous. Filters can be chained together or followed by aggregation functions.",
				Examples: []string{
					"level=error | count()",
					"event_id=1 AND image=/powershell/i | NOT user=/system/i | groupby(host) | count()",
					"event_id=1 | status=error OR status=critical | NOT image=/svchost/i",
				},
			},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
