# Chain (Sequential Event Detection)

`chain()` finds entities that produced a sequence of events, such as a user who signed in and then launched PowerShell. Use it when the order of events, not just their presence, is the signal.

## Syntax

```
chain(field, ..., within=DURATION, sequence=strict|any) { step1; step2; ... }
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `field` (required) | | Identity the steps share. Several fields are treated as aliases for one entity: an event joins a group if any of them holds that value. |
| `within` | none | Longest gap between consecutive steps (`s`, `m`, `h`, `d`, `w`). Without it, steps only have to fall inside the query's time range. |
| `sequence` | `strict` | `strict`: steps must happen in the written order. `any`: each step must happen, in any order. `any` cannot be combined with `within`. |

**Steps:**

- Separate steps with `;` and conditions within a step with `|` (AND). `OR` works inside a step. At least two steps are required.
- A step takes anything a filter takes, including `in()`, `cidr()` and `comment()`.
- A step may compute a per-row value (`len()`, `match()`, `lookupIP()`) and filter on it in the same step: `len(commandline) | _len > 500`. The value is not visible to other steps.
- Commands that reshape or aggregate (`groupby()`, `sort()`, `table()`, assignments) are rejected, and so is a step with no condition.
- A step can test [`modelLookup()`](enrichment.md#modellookup) columns when `modelLookup()` comes before `chain()`.

**Returns:** one row per entity with the identity field (named `_entity` when several fields were given) and `chain_count`, the number of times the full sequence occurred. Rows are sorted by `chain_count`, highest first.

## Examples

A login followed by PowerShell:

```
chain(user) {
  event_id=4624;
  event_id=1 | image=/powershell/i
}
```

The same actor across differently named fields, each step within a day of the last:

```
chain(user, source_user, target_user, within=1d) {
  event_id=1 | image=/powershell/i;
  event_id=10;
  event_id=4625
}
```

A process that reached the internet and then an internal host. `!cidr()` is also true when the field is missing, so the first step requires a value:

```
bifract_category="network_connect" | chain(process_guid, within=5m) {
  dst_ip!="" | !cidr(dst_ip, "10.0.0.0/8") | !cidr(dst_ip, "192.168.0.0/16") | !cidr(dst_ip, "172.16.0.0/12");
  cidr(dst_ip, "10.0.0.0/8") OR cidr(dst_ip, "192.168.0.0/16") OR cidr(dst_ip, "172.16.0.0/12")
}
```

Both events, in any order:

```
chain(user, sequence=any) {
  event_id=4625;
  event_id=4672
}
```

Count the users who completed the sequence (a stats command after `chain()` aggregates its one-row-per-entity output):

```
chain(user, within=1h) {
  event_id=4624;
  event_id=4688
} | count()
```

## How it works

- Rows are grouped by identity. With several fields, each row is expanded into one row per non-empty identity value, so an event lands in every group it belongs to.
- Strict chains are matched over event timestamps at millisecond precision, so same-second events still sequence correctly.
- With `sequence=any`, `chain_count` is the smallest per-step count: the number of complete sets.
- Each result carries a hidden `_chain_ts`, the timestamps of one matching sequence, so drilldowns and alert evidence fetch the exact events without rerunning the query.
