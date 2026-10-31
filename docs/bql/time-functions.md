# Time Functions

Use these to format timestamps, add the current time, or bucket events over time.

## strftime

Formats a time as text, by default the event `timestamp` into `_time`:

```
* | strftime("%H", as=_hour) | groupBy(_hour)
```

| Parameter | Description |
|---|---|
| format | Format string, required (first argument) |
| `field=` | Time field to format. Default `timestamp`. Other fields may hold any common time format |
| `timezone=` | IANA zone, e.g. `US/Eastern`. Default `UTC` |
| `as=` | Output column. Default `_time` |

Format specifiers (ClickHouse `formatDateTime`):

| Specifier | Description | Example |
|---|---|---|
| `%Y` | 4-digit year | 2026 |
| `%m` | Month (01 to 12) | 03 |
| `%d` | Day of month (01 to 31) | 04 |
| `%H` | Hour (00 to 23) | 14 |
| `%i` | Minute (00 to 59) | 30 |
| `%S` | Second (00 to 59) | 05 |
| `%a` | Abbreviated weekday | Wed |
| `%W` | Full weekday | Wednesday |
| `%b` | Abbreviated month | Mar |
| `%M` | Full month name | March |
| `%F` | Date, same as `%Y-%m-%d` | 2026-03-04 |
| `%T` | Time, same as `%H:%i:%S` | 14:30:05 |
| `%R` | Hour and minute, same as `%H:%i` | 14:30 |

The strftime spellings `%A` and `%B` are accepted as `%W` and `%M`.

!!! warning
    `%M` is the month name, not the minute. Use `%i` for minutes.

## now

```
* | now(checked_at)
```

Adds the current time as a column, `_now` unless you name it.

## timechart

Buckets events over time and draws a time chart:

```
* | timechart(span=5m, sum(bytes))
```

`span=` is a number plus `s`, `m`, `h`, `d` or `w` (default `1h`). The aggregate defaults to `count()`. See [Visualizations](visualizations.md) for all options.
