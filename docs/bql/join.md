# Join (Subquery Correlation)

`join()` correlates the outer query with a subquery on one shared field, for example denied connections from IPs that also logged in successfully. The subquery runs with the same time range and fractal as the outer query.

## Syntax

```
| join(key, type=inner|left, max=N, include=[field1,field2]) { subquery }
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `key` (required) | | Field to join on. The subquery must output it, so aggregate with `groupby(key)`, not a bare `count()`. |
| `type` | `inner` | `inner` keeps only outer rows with a match. `left` keeps every outer row; non-matches get empty values (`0` for counts). |
| `max` | 10000 | Most rows the subquery may return. Capped at 100000. |
| `include` | all | Subquery columns to bring back. Each must exist in the subquery output. |

The block `{ }` holds a full BQL query. Subquery columns come back prefixed with `_join_`, so `_count` becomes `_join__count`. The join key is matched on and not repeated.

## Examples

IPs that were denied and also logged in successfully, with their login count:

```
action="denied" | join(src_ip) {
  action="login_success" | groupby(src_ip)
}
```

Add each user's department and role to every event, keeping users with no match:

```
* | join(user, type=left, include=[department,role]) {
  * | groupby(user, function=[selectFirst(department, as=department), selectFirst(role, as=role)])
}
```

Sort by a joined column, with a larger subquery cap:

```
* | join(src_ip, max=50000) {
  * | groupby(src_ip)
} | sort(_join__count, desc)
```

## Limitations

- **One key, one join.** A second key, or a second `join()`, is an error.
- **No nested joins.** The subquery cannot contain `join()`.
- **No `modelLookup()`** in the same query as `join()`.
- **Aggregation runs first.** An aggregation after `join()` runs on the outer rows before the join, and is rejected if it drops the key. To count only rows another query matches, filter with a [result-set binding](basics.md#result-sets) instead: `let &keys := <subquery>; ... | in(field, &keys) | count()`.
- **Only** `inner` **and** `left` join types exist.
