# Chain (Sequential Event Detection)

Find entities that produced a sequence of events: a user who signed in and then launched PowerShell, a process that called out to the internet and then to an internal host.

## Syntax

```
chain(field, ..., within=DURATION, order=BOOL) { step1; step2; step3 }
```

| Parameter | Description |
|-----------|-------------|
| `field` (required) | The identity the steps share. Several fields are aliases for one entity: an event joins a group if *any* of them holds that entity's value. |
| `within` | Longest gap between consecutive steps, in `s`, `m`, `h` or `d`. Without it, the steps only have to fall inside the query's time range. |
| `order` | Whether the steps must happen in the written order. Defaults to `true`; `order=false` asks only that each step happened, and cannot be combined with `within`. |

**Block syntax:**

- Steps are separated by `;`, conditions within a step by `|` (AND). At least two steps are required.
- A step is a row condition, so it takes what a filter takes, `in()`, `cidr()` and `comment()` included. Anything that projects or aggregates (`regex()`, `groupby()`, `sort()`, `match()`) is rejected.

**Returns:** the identity field, named `_entity` when several were given, and `chain_count`, how many times the full sequence occurred.

## Examples

A login followed by PowerShell:

```
chain(user) {
  event_id=4624;
  event_id=1 | image=/powershell/i
}
```

The same actor across differently named fields, within a day:

```
chain(user, source_user, target_user, within=1d) {
  event_id=1 | image=/powershell/i;
  event_id=10;
  event_id=4625
}
```

A process that reached the internet and then an internal host. `!cidr()` is also true of a missing address, so the first step requires a value:

```
bifract_category="network_connect" | chain(process_guid, within=5m) {
  dst_ip!="" | !cidr(dst_ip, "10.0.0.0/8") | !cidr(dst_ip, "192.168.0.0/16") | !cidr(dst_ip, "172.16.0.0/12");
  cidr(dst_ip, "10.0.0.0/8") OR cidr(dst_ip, "192.168.0.0/16") OR cidr(dst_ip, "172.16.0.0/12")
}
```

Order does not matter here, only that the user did both:

```
chain(user, order=false) {
  event_id=4625;
  event_id=4672
}
```

Filters narrow what is chained, and the results pipe on like any other rows:

```
event_source=Security | chain(user, within=1h) {
  event_id=4624;
  event_id=4688
} | limit(10)
```

## How It Works

- Rows group by identity. With several fields, a row is expanded into one row per identity value it carries, so an event lands in every group it belongs to.
- Ordered chains match the steps as a sequence over event timestamps at millisecond precision, so same-second events still sequence correctly.
- Unordered chains only require that each step occurred. `chain_count` is then the smallest per-step count: the number of complete sets.
- Every result carries a hidden `_chain_ts`, the timestamps of one matching sequence in step order, so drilldowns and alert evidence can fetch the exact events without running the query again.
