# Examples

### Find all PowerShell executions grouped by user

```
image=/powershell/i | groupBy(user) | count() | sort(_count, order=desc)
```

### Top 10 users by data transferred

```
* | groupBy(user) | sum(bytes) | sort(_sum, order=desc) | limit(10)
```

### HTTP errors in the last time window

```
status_code>=400 | groupBy(status_code) | count() | barchart()
```

### Classify events by severity

```
* | case {
  status_code>=500 | severity := "critical" ;
  status_code>=400 | severity := "warning" ;
  * | severity := "info"
} | groupBy(severity) | count()
```

### Process ancestry graph

```
event_id=1 | table(process_guid, parent_process_guid) | graph(child=process_guid, parent=parent_process_guid)
```

### Trace a process tree from a specific process

```
event_id=1
| bfs(child=process_guid, parent=parent_process_guid, start="{63047898-81ee-6860-5202-000000002502}")
| graph(child=process_guid, parent=parent_process_guid, labels=image)
```

### Total event count as a single value

```
* | count() | singleval(label="Total Events")
```

### Request volume over time by status

```
* | groupBy(status_code) | timechart(span=5m, function=count())
```

### Detect login-then-PowerShell pattern per user

```
chain(user, within=1h) {
  event_id=4624;
  event_id=1 | image=/powershell/i
}
```

### Detect lateral movement chain within a day

```
event_source=Security | chain(user, within=1d) {
  event_id=4624;
  event_id=1 | image=/explorer.exe/i;
  event_id=1 | image=/powershell.exe/i
} | limit(20)
```
