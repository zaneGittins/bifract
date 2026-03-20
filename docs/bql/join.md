# Join (Subquery Correlation)

Correlate results from two independent queries by joining on a shared field. The subquery runs with the same time range and fractal isolation as the outer query.

## Syntax

```
| join(key, type=inner|left, max=N, include=[field1,field2]) { subquery }
```

**Parameters:**
- `key` (required) - Field to join on. Must exist in both the outer query results and the subquery results.
- `type` (optional) - Join type: `inner` (default) keeps only matching rows, `left` keeps all outer rows with NULLs for non-matches.
- `max` (optional) - Maximum rows the subquery can return (default: 10000, hard max: 100000). Safety guardrail for memory.
- `include` (optional) - Specific fields to include from the subquery. If omitted, all subquery fields are included with a `_join_` prefix.

**Block syntax:**
- The subquery inside `{ }` is a full BQL query (filters, pipes, commands).
- Nested joins are not supported.
- The subquery inherits the same fractal and time range constraints.

## Examples

Find IPs that were denied but also had successful logins:
```
action="denied" | join(src_ip) {
  action="login_success" | groupby(src_ip, function=count())
}
```

Enrich events with user metadata using a left join:
```
* | join(user, type=left, include=[department,role]) {
  * | groupby(user) | selectFirst(department) | selectFirst(role)
}
```

Correlate login failures with successes by user:
```
action="login_failed" | join(user) {
  action="login_success" | groupby(user, function=count())
}
```

Join with a custom max limit:
```
* | join(src_ip, max=50000) {
  * | groupby(src_ip, function=count())
} | sort(_join__count, desc)
```

Combine with other pipeline commands:
```
event_id=1 | join(user) {
  event_id=4624 | groupby(user, function=count())
} | sort(timestamp, desc) | limit(100)
```

## How It Works

1. The outer query runs normally and produces results.
2. The subquery is parsed and translated independently with the same security context (fractal ID, time range).
3. ClickHouse performs a hash join between the two result sets on the join key.
4. Subquery fields are prefixed with `_join_` to avoid name collisions (unless `include` is specified, in which case only those fields are added with the prefix).

## Limitations

- **No nested joins** - the subquery cannot itself contain a `join()`.
- **Single join key** - only one field can be used as the join key.
- **Max subquery rows** - capped at 100,000 to prevent memory issues. Use `max=` to adjust within this limit.
- **Join types** - only `inner` and `left` are supported.
