# Chain (Sequential Event Detection)

Detect sequences of events that share a common field. Useful for threat detection, behavioral analysis, and identifying multi-step patterns.

## Syntax

```
chain(field1, field2, ..., within=DURATION) { step1; step2; step3 }
```

**Parameters:**
- `field` (required) - One or more grouping fields. Events are partitioned by these fields and the sequence is checked within each partition. Multiple fields create a composite grouping key.
- `within` (optional) - Maximum time between consecutive steps. Supports `s` (seconds), `m` (minutes), `h` (hours), `d` (days). If omitted, steps just need to occur in order within the query's time range.

**Block syntax:**
- Steps are separated by `;`
- Within a step, use `|` to combine multiple conditions (AND)
- Each step supports the same condition syntax as filters: `field=value`, `field!=value`, `field=/regex/i`, `field>N`, etc.

**Returns:** The grouping field(s) and `chain_count` (number of times the full sequence occurred).

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

Group by multiple fields (user and computer):
```
chain(user, computer, within=1d) {
  event_id=4624;
  event_id=4688
}
```

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
