# Time Functions

Use these to compute with times and durations, bucket events by time, pick out the hour or weekday, and format timestamps.

## Times and durations

`timestamp` and `ingest_timestamp` are times, and so are `now()`, `bin()` and anything computed from them. A duration is a whole number and a unit:

| Unit | Meaning | Example |
|---|---|---|
| `s` | seconds | `90s` |
| `m` | minutes | `5m` |
| `h` | hours | `1h` |
| `d` | days | `7d` |
| `w` | weeks | `2w` |

Times and durations combine the way you would expect:

| Expression | Result | Example |
|---|---|---|
| time `-` time | duration | `timestamp - process_start` |
| time `+` or `-` duration | time | `timestamp - 1h` |
| duration `+` or `-` duration | duration | `1h + 30m` |
| duration `*` or `/` number | duration | `gap / 2` |
| duration `/` duration | number | `gap / 1m`, the gap in minutes |
| time compared with a time | condition | `timestamp > now() - 7d` |
| duration compared with a duration | condition | `gap > 5m` |

```
* | age := now() - timestamp
* | (timestamp - process_start) < 1m
* | gap := timestamp - ingest_timestamp | gap > 5m
* | recent := timestamp > now() - 7d
```

- **A bare number next to a time or a duration is an error**, since `30` could be seconds or minutes. Write `30s` or `30m`.
- **A stored field next to a time is read as a time**, the way `dateDiff()` reads one: `2026-03-04 11:00:00`, ISO 8601, or epoch seconds, milliseconds, microseconds or nanoseconds. A value that is no time gives an empty result rather than an error.
- **A quoted time in an expression is checked**: `recent := timestamp > "2026-03-04 12:00:00"` works, and `recent := timestamp > "yesterday"` is an error.
- **A stored duration is its number of seconds**, so it averages, sums and sorts as a number: `groupBy(host, function=avg(gap))`. A filter on a field reads a duration value as seconds too, so `duration > 2m` keeps a Zeek connection that lasted 150 seconds.
- **In a filter, arithmetic goes in brackets or starts the filter**, as elsewhere in BQL: `(timestamp - process_start) < 1m` or `now() - timestamp < 7d`.
- `min()` and `max()` of a time are a time. A numeric aggregate such as `avg()` or `sum()` of a time is an error; take a duration first, such as `now() - timestamp`.

## bin()

The start of the time bucket a time falls in. Use it to group by time and keep the result as a table, which `timechart()` does not:

```
* | groupBy([bin(timestamp, 5m), host])
```

More than twenty failed logons from one address within ten minutes:

```
event_id=4625 | groupBy([bin(timestamp, 10m), src_ip]) | _count > 20
```

| Parameter | Description |
|---|---|
| field | A time, required (first argument). A log field holding a time is parsed |
| span | Bucket size, a duration such as `5m`, `1h` or `1d`, required (second argument) |
| `timezone=` | IANA zone the buckets snap to, e.g. `Europe/Berlin`. Default: your time zone |

The column is named after the call, `bin_timestamp_5m` above. To choose the name, assign it first: `minute := bin(timestamp, 1m) | groupBy(minute)`.

Buckets follow your time zone, as `timechart()` does, so a day starts at your midnight, including on the days daylight saving time starts or ends. Weeks start on Sunday. Alerts, API keys and other runs with no viewer use UTC unless `timezone=` names a zone.

## hourOfDay() and dayOfWeek()

`hourOfDay(field)` is the hour a time falls in, `0` to `23`. `dayOfWeek(field)` is the day it falls on, `1` (Monday) to `7` (Sunday). Logons outside business hours:

```
event_id=4624 | hourOfDay(timestamp) < 6 OR dayOfWeek(timestamp) >= 6
```

Both follow your time zone unless `timezone=` names one, as `bin()` does. For an alert, name the zone the business hours are in:

```
event_id=4624 | hourOfDay(timestamp, timezone="America/New_York") >= 20
```

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
    `%M` is the month name, not the minute. Use `%i` for minutes. To filter or group by the hour or weekday, `hourOfDay()` and `dayOfWeek()` avoid format codes altogether.

## now

```
* | now(checked_at)
```

Adds the current time as a column, `_now` unless you name it. Inside an expression, `now()` is the current time:

```
* | age := now() - timestamp
```

## timechart

Buckets events over time and draws a time chart:

```
* | timechart(span=5m, sum(bytes))
```

`span=` is a number plus `s`, `m`, `h`, `d` or `w` (default `1h`). The aggregate defaults to `count()`. See [Visualizations](visualizations.md) for all options.
