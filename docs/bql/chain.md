# Chain (Sequential Event Detection)

Detect sequences of events that share a common identity. Useful for threat detection, behavioral analysis, and identifying multi-step patterns like lateral movement.

## Syntax

```
chain(field1, field2, ..., within=DURATION) { step1; step2; step3 }
```

**Parameters:**
- `field` (required) - One or more identity fields. With a single field, events are grouped by that field directly. With multiple fields, they are treated as aliases for the same entity (e.g., `user`, `source_user`, `target_user`). An event matches an entity if *any* of the listed fields contain that entity's value.
- `within` (optional) - Maximum time between consecutive steps. Supports `s` (seconds), `m` (minutes), `h` (hours), `d` (days). If omitted, steps just need to occur in order within the query's time range.

**Block syntax:**
- Steps are separated by `;`
- Within a step, use `|` to combine multiple conditions (AND)
- Each step supports the same condition syntax as filters: `field=value`, `field!=value`, `field=/regex/i`, `field>N`, etc.
- Regex alternation inside conditions is supported: `image=/powershell|cmd|whoami/i`

**Returns:** The grouping field (or `_entity` when using multiple identity fields) and `chain_count` (number of times the full sequence occurred).

## Examples

Find users who logged in and then launched PowerShell:
```
chain(user) {
  event_id=4624;
  event_id=1 | image=/powershell/i
}
```

Detect lateral movement pattern within 5 minutes:
```
chain(user, within=5m) {
  event_id=4624;
  event_id=1 | image=/explorer/i;
  event_id=1 | image=/powershell/i
}
```

Cross-field identity correlation (lateral movement across field names):
```
chain(user, source_user, target_user, within=1d) {
  event_id=1 | image=/powershell/i;
  event_id=10;
  event_id=4625
}
```
When multiple fields are provided, an event is included in an entity's group if *any* of the fields (`user`, `source_user`, or `target_user`) match that entity. This enables detection of patterns where the same actor appears under different field names across event types.

Multi-condition steps with pipes:
```
chain(user, within=1d) {
  event_id=1 | image=/explorer/i;
  event_id=1 | image=/powershell/i | command_line=/-nop/i;
  event_id=3 | image=/powershell/i
}
```

Filter to a specific source first, then find chains:
```
event_source=Security | chain(user, within=1h) {
  event_id=4624;
  event_id=4672;
  event_id=4688
}
```

Chain results can be piped to other commands:
```
chain(user) { event_id=4624; event_id=4688 } | limit(10)
```
