// Query autocomplete
const Autocomplete = {
    suggestions: [
        { keyword: 'groupBy(', desc: 'Group results by field' },
        { keyword: 'table([', desc: 'Display specific fields' },
        { keyword: 'sort(', desc: 'Sort results' },
        { keyword: 'limit(', desc: 'Limit number of results' },
        { keyword: 'AND', desc: 'Logical AND operator' },
        { keyword: 'OR', desc: 'Logical OR operator' },
        { keyword: 'NOT', desc: 'Logical NOT operator' },
        { keyword: 'count()', desc: 'Count results' },
        { keyword: 'sum(', desc: 'Sum numeric field' },
        { keyword: 'avg(', desc: 'Average numeric field' },
        { keyword: 'median(', desc: 'Median value of numeric field' },
        { keyword: 'mad(', desc: 'Median absolute deviation' },
        { keyword: 'chain(', desc: 'Detect sequential event patterns' },
        { keyword: 'dedup(', desc: 'Deduplicate by fields' },
        { keyword: 'cidr(', desc: 'Filter by CIDR range' },
        { keyword: 'split(', desc: 'Split field by delimiter' },
        { keyword: 'base64Decode(', desc: 'Decode base64 field' },
        { keyword: 'len(', desc: 'String length of field' },
        { keyword: 'coalesce(', desc: 'First non-empty field' },
        { keyword: 'levenshtein(', desc: 'Edit distance between values' },
        { keyword: 'substr(', desc: 'Extract substring' },
        { keyword: 'urldecode(', desc: 'URL-decode field' },
        { keyword: 'hash(', desc: 'Hash fields with cityHash64' },
        { keyword: 'comment(', desc: 'Filter commented logs' },
        { keyword: 'modifiedZScore(', desc: 'Modified z-score outlier detection' },
        { keyword: 'madOutlier(', desc: 'Outlier detection with threshold' },
        { keyword: 'iqr(', desc: 'Interquartile range' },
        { keyword: 'headTail(', desc: 'Pareto head/tail segmentation' },
        { keyword: 'frequency(', desc: 'Frequency table with percentages' },
        { keyword: 'skewness(', desc: 'Population skewness' },
        { keyword: 'kurtosis(', desc: 'Population excess kurtosis' },
        { keyword: 'max(', desc: 'Maximum value of field' },
        { keyword: 'min(', desc: 'Minimum value of field' },
        { keyword: 'percentile(', desc: 'Percentile values (p50, p75, p99)' },
        { keyword: 'stdDev(', desc: 'Standard deviation of field' },
        { keyword: 'selectFirst(', desc: 'Value from earliest event' },
        { keyword: 'selectLast(', desc: 'Value from latest event' },
        { keyword: 'collect(', desc: 'Collect values into array' },
        { keyword: 'top(', desc: 'Top values with frequency' },
        { keyword: 'head(', desc: 'First N events' },
        { keyword: 'tail(', desc: 'Last N events' },
        { keyword: 'regex(', desc: 'Extract with regex pattern' },
        { keyword: 'replace(', desc: 'Regex find and replace' },
        { keyword: 'concat(', desc: 'Concatenate fields' },
        { keyword: 'lowercase(', desc: 'Convert field to lowercase' },
        { keyword: 'uppercase(', desc: 'Convert field to uppercase' },
        { keyword: 'eval(', desc: 'Compute field assignment' },
        { keyword: 'in(', desc: 'Filter by value list' },
        { keyword: 'multi(', desc: 'Multiple aggregations' },
        { keyword: 'strftime(', desc: 'Extract time component' },
        { keyword: 'now(', desc: 'Current timestamp' },
        { keyword: 'bucket(', desc: 'Time bucketing' },
        { keyword: 'case', desc: 'Conditional field assignment' },
        { keyword: 'piechart(', desc: 'Pie chart visualization' },
        { keyword: 'barchart(', desc: 'Bar chart visualization' },
        { keyword: 'graph(', desc: 'Relationship graph visualization' },
        { keyword: 'singleval(', desc: 'Single value display' },
        { keyword: 'timechart(', desc: 'Time series chart' },
        { keyword: 'match(', desc: 'Dictionary lookup enrichment' },
        { keyword: 'bfs(', desc: 'Breadth-first graph traversal' },
        { keyword: 'dfs(', desc: 'Depth-first graph traversal' },
        { keyword: 'analyzeFields(', desc: 'Analyze field statistics' },
        { keyword: 'sprintf(', desc: 'Format string with fields' },
        { keyword: 'histogram(', desc: 'Numeric distribution histogram' },
        { keyword: 'heatmap(', desc: '2D density heatmap' },
        { keyword: 'lookupIP(', desc: 'GeoIP/ASN enrichment (MaxMind)' },
        { keyword: 'graphWorld(', desc: 'World map visualization' },
        { keyword: 'join(', desc: 'Join with subquery results' }
    ],
    selectedIndex: -1,

    // Function signatures for ? hint popup
    functionHints: {
        'groupby': {
            name: 'groupBy',
            signature: 'groupBy(field, ..., limit=N, distinct=bool)',
            args: [
                { name: 'field', desc: 'Field(s) to group by', required: true },
                { name: 'limit', desc: 'Max groups to return', required: false },
                { name: 'distinct', desc: 'Count distinct values (true/false)', required: false },
            ],
            example: 'groupBy(hostname, limit=20)',
        },
        'table': {
            name: 'table',
            signature: 'table([field1, field2, ...])',
            args: [
                { name: 'fields', desc: 'Fields to display (comma-separated in brackets)', required: true },
                { name: 'limit', desc: 'Max rows to return', required: false },
            ],
            example: 'table([hostname, level, message])',
        },
        'sort': {
            name: 'sort',
            signature: 'sort(field, order=asc|desc)',
            args: [
                { name: 'field', desc: 'Field to sort by', required: true },
                { name: 'order', desc: 'Sort direction: asc or desc', required: false },
            ],
            example: 'sort(timestamp, order=desc)',
        },
        'limit': {
            name: 'limit',
            signature: 'limit(n)',
            args: [
                { name: 'n', desc: 'Maximum number of results', required: true },
            ],
            example: 'limit(100)',
        },
        'count': {
            name: 'count',
            signature: 'count()',
            args: [],
            example: 'count()',
        },
        'sum': {
            name: 'sum',
            signature: 'sum(field)',
            args: [
                { name: 'field', desc: 'Numeric field to sum', required: true },
            ],
            example: 'sum(bytes)',
        },
        'avg': {
            name: 'avg',
            signature: 'avg(field)',
            args: [
                { name: 'field', desc: 'Numeric field to average', required: true },
            ],
            example: 'avg(response_time)',
        },
        'max': {
            name: 'max',
            signature: 'max(field)',
            args: [
                { name: 'field', desc: 'Field to find maximum value', required: true },
            ],
            example: 'max(response_time)',
        },
        'min': {
            name: 'min',
            signature: 'min(field)',
            args: [
                { name: 'field', desc: 'Field to find minimum value', required: true },
            ],
            example: 'min(response_time)',
        },
        'percentile': {
            name: 'percentile',
            signature: 'percentile(field)',
            args: [
                { name: 'field', desc: 'Numeric field (returns p50, p75, p99)', required: true },
            ],
            example: 'percentile(response_time)',
        },
        'stddev': {
            name: 'stdDev',
            signature: 'stdDev(field)',
            args: [
                { name: 'field', desc: 'Numeric field for standard deviation', required: true },
            ],
            example: 'stdDev(response_time)',
        },
        'median': {
            name: 'median',
            signature: 'median(field)',
            args: [
                { name: 'field', desc: 'Numeric field for median value', required: true },
            ],
            example: 'median(response_time)',
        },
        'mad': {
            name: 'mad',
            signature: 'mad(field)',
            args: [
                { name: 'field', desc: 'Numeric field for median absolute deviation', required: true },
            ],
            example: 'mad(response_time)',
        },
        'skewness': {
            name: 'skewness',
            signature: 'skewness(field)',
            args: [
                { name: 'field', desc: 'Numeric field for population skewness', required: true },
            ],
            example: 'skewness(response_time)',
        },
        'kurtosis': {
            name: 'kurtosis',
            signature: 'kurtosis(field)',
            args: [
                { name: 'field', desc: 'Numeric field for population excess kurtosis', required: true },
            ],
            example: 'kurtosis(response_time)',
        },
        'frequency': {
            name: 'frequency',
            signature: 'frequency(field)',
            args: [
                { name: 'field', desc: 'Field to produce frequency table for', required: true },
            ],
            example: 'frequency(event_name)',
        },
        'modifiedzscore': {
            name: 'modifiedZScore',
            signature: 'modifiedZScore(field)',
            args: [
                { name: 'field', desc: 'Numeric field for modified z-score computation', required: true },
            ],
            example: 'modifiedZScore(response_time)',
        },
        'madoutlier': {
            name: 'madOutlier',
            signature: 'madOutlier(field, threshold)',
            args: [
                { name: 'field', desc: 'Numeric field for outlier detection', required: true },
                { name: 'threshold', desc: 'Modified z-score threshold (default: 3.5)', required: false },
            ],
            example: 'madOutlier(latency, 3.5)',
        },
        'iqr': {
            name: 'iqr',
            signature: 'iqr(field)',
            args: [
                { name: 'field', desc: 'Numeric field for interquartile range', required: true },
            ],
            example: 'iqr(response_time)',
        },
        'headtail': {
            name: 'headTail',
            signature: 'headTail(field, threshold)',
            args: [
                { name: 'field', desc: 'Field for Pareto analysis', required: true },
                { name: 'threshold', desc: 'Cumulative % threshold for head/tail (default: 80)', required: false },
            ],
            example: 'headTail(event_name)',
        },
        'selectfirst': {
            name: 'selectFirst',
            signature: 'selectFirst(field)',
            args: [
                { name: 'field', desc: 'Field value from earliest event in group', required: true },
            ],
            example: 'selectFirst(status)',
        },
        'selectlast': {
            name: 'selectLast',
            signature: 'selectLast(field)',
            args: [
                { name: 'field', desc: 'Field value from latest event in group', required: true },
            ],
            example: 'selectLast(status)',
        },
        'collect': {
            name: 'collect',
            signature: 'collect(field)',
            args: [
                { name: 'field', desc: 'Field to collect into array', required: true },
            ],
            example: 'collect(hostname)',
        },
        'top': {
            name: 'top',
            signature: 'top(field, percent=bool, as=alias)',
            args: [
                { name: 'field', desc: 'Field to find top values', required: true },
                { name: 'percent', desc: 'Show as percentage (true/false)', required: false },
                { name: 'as', desc: 'Output field alias', required: false },
            ],
            example: 'top(hostname)',
        },
        'head': {
            name: 'head',
            signature: 'head(n)',
            args: [
                { name: 'n', desc: 'First N events (default: 200)', required: false },
            ],
            example: 'head(50)',
        },
        'tail': {
            name: 'tail',
            signature: 'tail(n)',
            args: [
                { name: 'n', desc: 'Last N events (default: 200)', required: false },
            ],
            example: 'tail(50)',
        },
        'regex': {
            name: 'regex',
            signature: 'regex(pattern, field=raw_log)',
            args: [
                { name: 'pattern', desc: 'Regex pattern (supports named captures)', required: true },
                { name: 'field', desc: 'Field to match against (default: raw_log)', required: false },
            ],
            example: 'regex("(?<ip>\\d+\\.\\d+\\.\\d+\\.\\d+)", field=raw_log)',
        },
        'replace': {
            name: 'replace',
            signature: 'replace(pattern, replacement, field=raw_log, as=output)',
            args: [
                { name: 'pattern', desc: 'Regex pattern to match', required: true },
                { name: 'replacement', desc: 'Replacement string', required: true },
                { name: 'field', desc: 'Field to apply to (default: raw_log)', required: false },
                { name: 'as', desc: 'Output field name', required: false },
            ],
            example: 'replace("\\d+", "***", field=raw_log, as=redacted)',
        },
        'concat': {
            name: 'concat',
            signature: 'concat([field1, field2, ...], as=output)',
            args: [
                { name: 'fields', desc: 'Fields to concatenate (comma-separated in brackets)', required: true },
                { name: 'as', desc: 'Output field name (default: _concat)', required: false },
            ],
            example: 'concat([hostname, level], as=combined)',
        },
        'lowercase': {
            name: 'lowercase',
            signature: 'lowercase(field, as=output)',
            args: [
                { name: 'field', desc: 'Field to convert to lowercase', required: true },
                { name: 'as', desc: 'Output field name (default: same field)', required: false },
            ],
            example: 'lowercase(hostname)',
        },
        'uppercase': {
            name: 'uppercase',
            signature: 'uppercase(field, as=output)',
            args: [
                { name: 'field', desc: 'Field to convert to uppercase', required: true },
                { name: 'as', desc: 'Output field name (default: same field)', required: false },
            ],
            example: 'uppercase(level)',
        },
        'eval': {
            name: 'eval',
            signature: 'eval(field = expression)',
            args: [
                { name: 'field = expr', desc: 'Assign computed value (supports +, * operators)', required: true },
            ],
            example: 'eval(total = bytes_in + bytes_out)',
        },
        'in': {
            name: 'in',
            signature: 'in(field, [v1, v2, ...])',
            args: [
                { name: 'field', desc: 'Field to filter on', required: true },
                { name: 'values', desc: 'List of values to match', required: true },
            ],
            example: 'in(level, [error, critical])',
        },
        'hash': {
            name: 'hash',
            signature: 'hash(field1, field2, ..., as=alias)',
            args: [
                { name: 'fields', desc: 'Field(s) to hash together', required: true },
                { name: 'as', desc: 'Output field name (default: hash_key)', required: false },
            ],
            example: 'hash(hostname, process_id, as=session_key)',
        },
        'now': {
            name: 'now',
            signature: 'now(as=output)',
            args: [
                { name: 'as', desc: 'Output field name (default: _now)', required: false },
            ],
            example: 'now(as=current_time)',
        },
        'bucket': {
            name: 'bucket',
            signature: 'bucket(span=duration, function=agg())',
            args: [
                { name: 'span', desc: 'Time bucket size (e.g. 1h, 5m, 1d)', required: true },
                { name: 'function', desc: 'Aggregation function (e.g. count(), sum(field))', required: true },
            ],
            example: 'bucket(span=1h, function=count())',
        },
        'case': {
            name: 'case',
            signature: 'case { condition | result ; ... ; * | default }',
            args: [
                { name: 'condition', desc: 'Filter condition for each branch', required: true },
                { name: 'result', desc: 'Value or field assignment when matched', required: true },
                { name: '*', desc: 'Default/catch-all branch', required: false },
            ],
            example: 'case { level="error" | "bad" ; * | "ok" }',
        },
        'multi': {
            name: 'multi',
            signature: 'multi(agg1(), agg2(), ...)',
            args: [
                { name: 'functions', desc: 'Aggregate functions: count(), sum(), avg(), max(), min(), percentile(), stdDev(), median(), mad(), skewness(), kurtosis(), iqr(), selectFirst(), selectLast(), collect(), top()', required: true },
            ],
            example: 'multi(count(), avg(response_time), max(bytes))',
        },
        'chain': {
            name: 'chain',
            signature: 'chain(field, ..., within=duration) { step1 ; step2 ; ... }',
            args: [
                { name: 'field', desc: 'Identity field(s). Single field groups directly. Multiple fields are treated as aliases for the same entity (e.g. user, source_user, target_user).', required: true },
                { name: 'within', desc: 'Max time window between steps (e.g. 5m)', required: false },
                { name: '{ steps }', desc: 'Sequential conditions separated by ;', required: true },
            ],
            example: 'chain(user, source_user, target_user, within=1d) { event_id=1 | image=/powershell/i ; event_id=10 ; event_id=4625 }',
        },
        'match': {
            name: 'match',
            signature: 'match(dict=name, field=logfield, column=key, include=[cols], strict=bool)',
            args: [
                { name: 'dict', desc: 'Dictionary name', required: true },
                { name: 'field', desc: 'Log field to look up', required: true },
                { name: 'column', desc: 'Dictionary key column', required: true },
                { name: 'include', desc: 'Columns to include from dictionary', required: true },
                { name: 'strict', desc: 'Only keep matching rows (default: false)', required: false },
            ],
            example: 'match(dict=assets, field=ip, column=address, include=[owner, location])',
        },
        'lookupIP': {
            name: 'lookupIP',
            signature: 'lookupIP(field=ip_field, include=[country,city,asn,as_org])',
            args: [
                { name: 'field', desc: 'Field containing the IP address', required: true },
                { name: 'include', desc: 'Columns: country, city, subdivision, continent, timezone, latitude, longitude, postal_code, asn, as_org', required: true },
            ],
            example: 'lookupIP(field=src_ip, include=[country,city,asn])',
        },
        'lookupip': { ref: 'lookupIP' },
        'geoip': { ref: 'lookupIP' },
        'graphWorld': {
            name: 'graphWorld',
            signature: 'graphWorld(lat=field, lon=field, label=field, limit=N)',
            args: [
                { name: 'lat', desc: 'Latitude field (default: latitude)', required: false },
                { name: 'lon', desc: 'Longitude field (default: longitude)', required: false },
                { name: 'label', desc: 'Field to use as marker label', required: false },
                { name: 'limit', desc: 'Max number of points (default: 5000)', required: false },
            ],
            example: 'lookupIP(field=src_ip, include=[latitude,longitude,country]) | graphWorld(label=country)',
        },
        'graphworld': { ref: 'graphWorld' },
        'worldmap': { ref: 'graphWorld' },
        'comment': {
            name: 'comment',
            signature: 'comment()',
            args: [],
            example: 'comment()',
        },
        'piechart': {
            name: 'piechart',
            signature: 'piechart(limit=N)',
            args: [
                { name: 'limit', desc: 'Max slices to display (default: 10)', required: false },
            ],
            example: 'groupBy(level) | count() | piechart(limit=5)',
        },
        'barchart': {
            name: 'barchart',
            signature: 'barchart(limit=N)',
            args: [
                { name: 'limit', desc: 'Max bars to display (default: 10)', required: false },
            ],
            example: 'groupBy(hostname) | count() | barchart()',
        },
        'graph': {
            name: 'graph',
            signature: 'graph(child=field, parent=field, labels=[fields], limit=N)',
            args: [
                { name: 'child', desc: 'Child node field', required: true },
                { name: 'parent', desc: 'Parent node field', required: true },
                { name: 'labels', desc: 'Fields to show in node labels', required: false },
                { name: 'limit', desc: 'Max nodes (default: 100, max: 500)', required: false },
            ],
            example: 'graph(child=process_guid, parent=parent_guid, labels=[name])',
        },
        'singleval': {
            name: 'singleval',
            signature: 'singleval(label="Label")',
            args: [
                { name: 'label', desc: 'Display label for the value', required: false },
            ],
            example: 'count() | singleval(label="Total Events")',
        },
        'timechart': {
            name: 'timechart',
            signature: 'timechart(span=duration, function=agg())',
            args: [
                { name: 'span', desc: 'Time bucket size (default: 1h)', required: false },
                { name: 'function', desc: 'Aggregation: count(), sum(), avg(), max(), min()', required: false },
            ],
            example: 'timechart(span=5m, function=count())',
        },
        'analyzefields': {
            name: 'analyzeFields',
            signature: 'analyzeFields(field1, field2, ..., limit=N)',
            args: [
                { name: 'fields', desc: 'Fields to analyze (default: all)', required: false },
                { name: 'limit', desc: 'Max rows to scan (default: 50000)', required: false },
            ],
            example: 'service=webapp | analyzeFields()',
        },
        'bfs': {
            name: 'bfs',
            signature: 'bfs(child=field, parent=field, start=value, depth=N, include=[fields])',
            args: [
                { name: 'child', desc: 'Child node field', required: true },
                { name: 'parent', desc: 'Parent node field', required: true },
                { name: 'start', desc: 'Starting node value', required: true },
                { name: 'depth', desc: 'Max traversal depth (default: 10)', required: false },
                { name: 'include', desc: 'Extra fields to include', required: false },
            ],
            example: 'bfs(child=id, parent=parent_id, start="root", depth=5)',
        },
        'dfs': {
            name: 'dfs',
            signature: 'dfs(child=field, parent=field, start=value, depth=N, include=[fields])',
            args: [
                { name: 'child', desc: 'Child node field', required: true },
                { name: 'parent', desc: 'Parent node field', required: true },
                { name: 'start', desc: 'Starting node value', required: true },
                { name: 'depth', desc: 'Max traversal depth (default: 10)', required: false },
                { name: 'include', desc: 'Extra fields to include', required: false },
            ],
            example: 'dfs(child=id, parent=parent_id, start="root", depth=5)',
        },
        'len': {
            name: 'len',
            signature: 'len(field)',
            args: [
                { name: 'field', desc: 'Field to measure string length of', required: true },
            ],
            example: 'len(message) | _len > 100',
        },
        'levenshtein': {
            name: 'levenshtein',
            signature: 'levenshtein(field1, field2)',
            args: [
                { name: 's1', desc: 'First field or quoted string', required: true },
                { name: 's2', desc: 'Second field or quoted string', required: true },
            ],
            example: 'levenshtein(process_name, "svchost.exe") | _distance < 3',
        },
        'base64decode': {
            name: 'base64Decode',
            signature: 'base64Decode(field)',
            args: [
                { name: 'field', desc: 'Base64-encoded field to decode', required: true },
            ],
            example: 'base64Decode(encoded_command) | _decoded=/powershell/i',
        },
        'dedup': {
            name: 'dedup',
            signature: 'dedup(field1, field2, ...)',
            args: [
                { name: 'fields', desc: 'Fields to deduplicate by (keeps first occurrence)', required: true },
            ],
            example: 'dedup(src_ip, dst_ip)',
        },
        'cidr': {
            name: 'cidr',
            signature: 'cidr(field, "range")',
            args: [
                { name: 'field', desc: 'IP address field', required: true },
                { name: 'range', desc: 'CIDR range (e.g. 10.0.0.0/8)', required: true },
            ],
            example: 'cidr(src_ip, "10.0.0.0/8")',
        },
        'split': {
            name: 'split',
            signature: 'split(field, "delimiter", index)',
            args: [
                { name: 'field', desc: 'Field to split', required: true },
                { name: 'delimiter', desc: 'Delimiter string', required: true },
                { name: 'index', desc: '1-based index (-1 for last element)', required: true },
            ],
            example: 'split(image, "\\\\", -1)',
        },
        'substr': {
            name: 'substr',
            signature: 'substr(field, start, length)',
            args: [
                { name: 'field', desc: 'Field to extract from', required: true },
                { name: 'start', desc: 'Starting position (1-based)', required: true },
                { name: 'length', desc: 'Number of characters (default: rest of string)', required: false },
            ],
            example: 'substr(message, 1, 50)',
        },
        'urldecode': {
            name: 'urldecode',
            signature: 'urldecode(field)',
            args: [
                { name: 'field', desc: 'URL-encoded field to decode', required: true },
            ],
            example: 'urldecode(request_uri)',
        },
        'coalesce': {
            name: 'coalesce',
            signature: 'coalesce(field1, field2, ...)',
            args: [
                { name: 'fields', desc: 'Fields to check in order (returns first non-empty)', required: true },
            ],
            example: 'coalesce(user, username, account_name)',
        },
        'sprintf': {
            name: 'sprintf',
            signature: 'sprintf(format, field1, field2, ..., as=alias)',
            args: [
                { name: 'format', desc: 'Printf-style format string (%s, %d, etc.)', required: true },
                { name: 'fields', desc: 'Fields to substitute into the format string', required: false },
                { name: 'as', desc: 'Output field name (default: _sprintf)', required: false },
            ],
            example: 'sprintf("%s - %s", username, action, as=user_action)',
        },
        'strftime': {
            name: 'strftime',
            signature: 'strftime(format, field=timestamp, timezone=UTC, as=_time)',
            args: [
                { name: 'format', desc: 'strftime format (%H, %b, %a, %Y, %d, etc.)', required: true },
                { name: 'field', desc: 'Field to format (default: timestamp)', required: false },
                { name: 'timezone', desc: 'Timezone (default: UTC)', required: false },
                { name: 'as', desc: 'Output field name (default: _time)', required: false },
            ],
            example: 'strftime("%H", as=_hour)',
        },
        'histogram': {
            name: 'histogram',
            signature: 'histogram(field, buckets=N)',
            args: [
                { name: 'field', desc: 'Numeric field to build distribution for', required: true },
                { name: 'buckets', desc: 'Number of equal-width bins (default: 20)', required: false },
            ],
            example: 'histogram(response_time, buckets=30)',
        },
        'join': {
            name: 'join',
            signature: 'join(key, type=inner|left, max=N, include=[fields]) { subquery }',
            args: [
                { name: 'key', desc: 'Field to join on (must exist in both queries)', required: true },
                { name: 'type', desc: 'Join type: inner (default) or left', required: false },
                { name: 'max', desc: 'Max subquery rows (default: 10000, max: 100000)', required: false },
                { name: 'include', desc: 'Fields to include from subquery (default: all)', required: false },
                { name: '{ subquery }', desc: 'BQL subquery in curly braces', required: true },
            ],
            example: 'action="denied" | join(src_ip) { action="login" | groupby(src_ip) | count() }',
        },
        'heatmap': {
            name: 'heatmap',
            signature: 'heatmap(x=field, y=field, value=agg(), limit=N)',
            args: [
                { name: 'x', desc: 'Field for X axis', required: true },
                { name: 'y', desc: 'Field for Y axis', required: true },
                { name: 'value', desc: 'Aggregation function (default: count())', required: false },
                { name: 'limit', desc: 'Max distinct values per axis (default: 50)', required: false },
            ],
            example: 'heatmap(x=src_ip, y=dst_port, value=count())',
        },
    },

    // State for function hints popup
    _hintVisible: false,
    _hintAnchor: null, // the textarea that triggered the hint

    init() {
        const input = document.getElementById('queryInput');
        const autocompleteDiv = document.getElementById('autocomplete');

        if (!input || !autocompleteDiv) return;

        input.addEventListener('input', () => {
            this.show();
        });
        input.addEventListener('keydown', (e) => this.handleKeyDown(e));

        // Document-wide listeners for function hints (works on all query textareas)
        document.addEventListener('input', (e) => {
            if (e.target.tagName === 'TEXTAREA' && this._isQueryTextarea(e.target)) {
                this._checkHintTrigger(e.target);
            }
        });
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this._hintVisible) {
                this.hideHint();
            }
        });

        // Close on click outside
        document.addEventListener('click', (e) => {
            const hintPopup = document.getElementById('functionHint');
            if (!input.contains(e.target) && !autocompleteDiv.contains(e.target)) {
                this.hide();
            }
            if (hintPopup && !hintPopup.contains(e.target) &&
                (!this._hintAnchor || !this._hintAnchor.contains(e.target))) {
                this.hideHint();
            }
        });
    },

    // Check if a textarea is a query editor (main, alert, notebook, dashboard)
    _isQueryTextarea(el) {
        // Main query input or alert editor
        if (el.classList.contains('search-input')) return true;
        // Notebook/dashboard editors: textarea inside .query-input-wrapper or with wie-q- id
        if (el.closest('.query-input-wrapper')) return true;
        if (el.id && el.id.startsWith('wie-q-')) return true;
        return false;
    },

    // Find what function the cursor is currently inside of
    _getEnclosingFunction(text, cursorPos) {
        const before = text.substring(0, cursorPos);
        // Walk backwards through the text to find the nearest unmatched open paren
        let depth = 0;
        for (let i = before.length - 1; i >= 0; i--) {
            const ch = before[i];
            if (ch === ')') {
                depth++;
            } else if (ch === '(') {
                if (depth === 0) {
                    // Found unmatched open paren, extract function name before it
                    const preceding = before.substring(0, i);
                    const match = preceding.match(/([a-zA-Z_]\w*)$/);
                    if (match) {
                        return match[1];
                    }
                    return null;
                }
                depth--;
            }
        }
        return null;
    },

    // Check if a ? at the given position is inside a quoted string
    _isInsideString(text, pos) {
        let inSingle = false;
        let inDouble = false;
        for (let i = 0; i < pos; i++) {
            const ch = text[i];
            if (ch === '"' && !inSingle) inDouble = !inDouble;
            else if (ch === "'" && !inDouble) inSingle = !inSingle;
        }
        return inSingle || inDouble;
    },

    // React to input: show hint if there's a ? trigger, hide if it's gone
    _checkHintTrigger(textarea) {
        if (!textarea) return;

        const cursorPos = textarea.selectionStart;
        const text = textarea.value;
        const charBefore = cursorPos > 0 ? text[cursorPos - 1] : '';

        // If the char right before cursor is ?, check if it's a valid trigger
        if (charBefore === '?') {
            // Must not be inside a quoted string
            if (this._isInsideString(text, cursorPos - 1)) {
                this.hideHint();
                return;
            }

            // Must be inside a function's parens
            // Check from position before the ? (so _getEnclosingFunction sees the paren context)
            const funcName = this._getEnclosingFunction(text, cursorPos - 1);
            if (funcName) {
                let hint = this.functionHints[funcName.toLowerCase()];
                if (hint && hint.ref) {
                    hint = this.functionHints[hint.ref];
                }
                if (hint && hint.signature) {
                    this._showHint(hint, textarea);
                    return;
                }
            }
        }

        // No valid ? trigger at cursor -- hide if visible
        if (this._hintVisible) {
            this.hideHint();
        }
    },

    _showHint(hint, textarea) {
        let popup = document.getElementById('functionHint');
        if (!popup) {
            popup = document.createElement('div');
            popup.id = 'functionHint';
            popup.className = 'function-hint';
            document.body.appendChild(popup);
        }

        let html = '<div class="fn-hint-header">';
        html += '<span class="fn-hint-signature">' + this._escapeHtml(hint.signature) + '</span>';
        html += '<span class="fn-hint-dismiss" title="Dismiss">&times;</span>';
        html += '</div>';

        if (hint.args.length > 0) {
            html += '<div class="fn-hint-args">';
            for (const arg of hint.args) {
                const reqClass = arg.required ? 'fn-arg-required' : 'fn-arg-optional';
                const reqLabel = arg.required ? '' : '?';
                html += '<div class="fn-hint-arg">';
                html += '<span class="fn-arg-name ' + reqClass + '">' + this._escapeHtml(arg.name) + reqLabel + '</span>';
                html += '<span class="fn-arg-desc">' + this._escapeHtml(arg.desc) + '</span>';
                html += '</div>';
            }
            html += '</div>';
        }

        html += '<div class="fn-hint-example">' + this._escapeHtml(hint.example) + '</div>';

        popup.innerHTML = html;

        // Position below the textarea
        const rect = textarea.getBoundingClientRect();
        popup.style.top = (rect.bottom + 4) + 'px';
        popup.style.left = rect.left + 'px';
        popup.style.maxWidth = Math.min(480, rect.width) + 'px';
        popup.style.display = 'block';
        this._hintVisible = true;
        this._hintAnchor = textarea;

        // Bind dismiss button
        const dismissBtn = popup.querySelector('.fn-hint-dismiss');
        if (dismissBtn) {
            dismissBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.hideHint();
            });
        }
    },

    hideHint() {
        const popup = document.getElementById('functionHint');
        if (popup) {
            popup.style.display = 'none';
        }
        this._hintVisible = false;
    },

    _escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    },

    // Get field names from current results via FieldStats
    _getFieldNames() {
        if (window.FieldStats && FieldStats.stats) {
            return Object.keys(FieldStats.stats);
        }
        return [];
    },

    // Get top values for a field from FieldStats
    _getFieldValues(fieldName) {
        if (window.FieldStats && FieldStats.stats && FieldStats.stats[fieldName]) {
            return FieldStats.stats[fieldName].topValues.map(([val, count]) => ({
                value: val,
                count: count
            }));
        }
        return [];
    },

    // Detect if cursor is right after field= or field!= and return the field name and partial value
    _getFieldValueContext(textBeforeCursor) {
        // Match quoted partial: fieldName="partial (no closing quote yet)
        const quotedMatch = textBeforeCursor.match(/([a-zA-Z_][\w.]*)(!?=)"([^"]*)$/);
        if (quotedMatch) {
            return { field: quotedMatch[1], partial: quotedMatch[3], quoted: true };
        }
        // Match unquoted: fieldName=partial
        const match = textBeforeCursor.match(/([a-zA-Z_][\w.]*)(!?=)([^"\s]*)$/);
        if (match) {
            return { field: match[1], partial: match[3], quoted: false };
        }
        return null;
    },

    // Detect if cursor is inside function parens and return the partial token being typed
    _getFunctionArgContext(textBeforeCursor) {
        // Check if we're inside function parens by looking for unmatched (
        let depth = 0;
        let parenPos = -1;
        for (let i = textBeforeCursor.length - 1; i >= 0; i--) {
            const ch = textBeforeCursor[i];
            if (ch === ')') depth++;
            else if (ch === '(') {
                if (depth === 0) { parenPos = i; break; }
                depth--;
            }
        }
        if (parenPos < 0) return null;

        // Extract the token being typed (after last separator: comma, [, space, =)
        const afterParen = textBeforeCursor.substring(parenPos + 1);
        const tokenMatch = afterParen.match(/(?:.*[\[,\s=])?\s*([a-zA-Z_][\w.]*)$/);
        if (tokenMatch && tokenMatch[1]) {
            return { partial: tokenMatch[1] };
        }
        return null;
    },

    // Compute the best Tab-completion match based on current cursor context.
    // Returns { keyword, mode } or null if no match.
    _getBestMatch() {
        const input = document.getElementById('queryInput');
        if (!input) return null;

        const value = input.value;
        const cursorPos = input.selectionStart;
        const textBeforeCursor = value.substring(0, cursorPos);

        // Priority 1: Field value after = operator
        const valueCtx = this._getFieldValueContext(textBeforeCursor);
        if (valueCtx) {
            const topValues = this._getFieldValues(valueCtx.field);
            if (topValues.length > 0) {
                const partial = valueCtx.partial.toLowerCase();
                const filtered = partial.length > 0
                    ? topValues.filter(v => v.value.toLowerCase().includes(partial))
                    : topValues;
                if (filtered.length > 0) {
                    return { keyword: filtered[0].value, mode: 'value' };
                }
            }
        }

        // Priority 2: Field name inside function args (3+ chars)
        const argCtx = this._getFunctionArgContext(textBeforeCursor);
        if (argCtx && argCtx.partial.length >= 3) {
            const fieldNames = this._getFieldNames();
            const partial = argCtx.partial.toLowerCase();
            const match = fieldNames.find(f => f.toLowerCase().includes(partial));
            if (match) {
                return { keyword: match, mode: 'field' };
            }
        }

        // Priority 3: Bare field name (3+ chars, not after = operator)
        const lastWord = textBeforeCursor.split(/\s/).pop();
        if (lastWord && lastWord.length >= 3 && !lastWord.includes('=')) {
            const fieldNames = this._getFieldNames();
            const partial = lastWord.toLowerCase();
            const fieldMatch = fieldNames.find(f => f.toLowerCase().startsWith(partial));
            if (fieldMatch) {
                return { keyword: fieldMatch, mode: 'field' };
            }
        }

        // Priority 4: Keyword (3+ chars)
        if (!lastWord || lastWord.length < 3 || lastWord.includes('=')) return null;

        const match = this.suggestions.find(s =>
            s.keyword.toLowerCase().startsWith(lastWord.toLowerCase())
        );
        if (match) {
            return { keyword: match.keyword, mode: 'keyword' };
        }

        return null;
    },

    show() {
        // No dropdown -- Tab completion only
    },

    hide() {
        // No dropdown to hide
    },

    handleKeyDown(e) {
        // Handle Escape to close hint
        if (e.key === 'Escape' && this._hintVisible) {
            this.hideHint();
        }

        if (e.key === 'Tab') {
            const best = this._getBestMatch();
            if (best) {
                e.preventDefault();
                e._autocompleteHandled = true;
                this._applyCompletion(best.keyword, best.mode);
            }
        }
    },

    _applyCompletion(keyword, mode) {
        const input = document.getElementById('queryInput');
        if (!input) return;

        const value = input.value;
        const cursorPos = input.selectionStart;
        const textBeforeCursor = value.substring(0, cursorPos);
        const textAfterCursor = value.substring(cursorPos);

        let newBefore;

        if (mode === 'value') {
            const escaped = keyword.replace(/"/g, '\\"');
            const quotedCtx = textBeforeCursor.match(/([a-zA-Z_][\w.]*!?=)"[^"]*$/);
            if (quotedCtx) {
                newBefore = textBeforeCursor.replace(/"[^"]*$/, '"' + escaped + '"');
            } else {
                newBefore = textBeforeCursor.replace(/([!=]=?)[^"\s]*$/, '$1"' + escaped + '"');
            }
        } else if (mode === 'field') {
            newBefore = textBeforeCursor.replace(/[a-zA-Z_][\w.]*$/, keyword);
        } else {
            // Keyword mode: replace the partial word in-place, no extra space
            newBefore = textBeforeCursor.replace(/[a-zA-Z_][\w.]*$/, keyword);
        }

        const newValue = newBefore + textAfterCursor;
        input.value = newValue;
        input.setSelectionRange(newBefore.length, newBefore.length);
        input.focus();

        if (window.SyntaxHighlight) {
            SyntaxHighlight.update();
        }
    }
};

// Make globally available
window.Autocomplete = Autocomplete;
