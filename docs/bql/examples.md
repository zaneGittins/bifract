# Examples

Common queries to copy and adapt. Field names depend on your normalizers.

## PowerShell executions by user

```
image=/powershell/i | groupBy(user) | sort(_count, order=desc)
```

## Top 10 users by bytes transferred

```
* | groupBy(user, function=sum(bytes)) | sort(_sum, order=desc) | limit(10)
```

## HTTP errors by status code

```
status_code>=400 | groupBy(status_code) | barchart()
```

## Classify events by severity

```
* | case {
  status_code>=500 | severity := "critical" ;
  status_code>=400 | severity := "warning" ;
  * | severity := "info"
} | groupBy(severity)
```

## Request volume over time by status

```
* | timechart(span=5m, groupby(status_code))
```

## Total event count as a single value

```
* | count() | singleval(title="Total Events")
```

## Process ancestry graph

```
event_id=1 | graph(child=process_guid, parent=parent_process_guid)
```

## Process tree from one process

```
ptg(start="{63047898-81ee-6860-5202-000000002502}") | pgraph()
```

## Explorer followed by PowerShell for the same user

```
chain(user, within=1h) {
  event_id=1 | image=/explorer/i;
  event_id=1 | image=/powershell/i;
}
```
