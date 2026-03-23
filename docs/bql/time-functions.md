# Time Functions

## strftime

Extract or format time components from a timestamp as `_time` (or custom alias):

```
* | strftime("%H", as=_hour)
* | strftime("%b", as=_month)
* | strftime("%a", as=_day)
* | strftime("%Y", as=_year)
* | strftime("%d", as=_dayInMonth)
* | strftime("%Y-%m-%d", timezone="US/Eastern", as=_date)
* | strftime("%H", field=created_at)
```

Common format specifiers:

| Specifier | Description | Example |
|-----------|-------------|---------|
| `%Y` | 4-digit year | 2026 |
| `%m` | Month (01-12) | 03 |
| `%d` | Day (01-31) | 04 |
| `%H` | Hour (00-23) | 14 |
| `%M` | Minute (00-59) | 30 |
| `%S` | Second (00-59) | 05 |
| `%a` | Abbreviated day | Mon |
| `%b` | Abbreviated month | Mar |

## Current Time

```
* | now()
* | now(current_time)
```

## Time Bucketing

```
* | bucket(span=1h, function=count())
* | bucket(span=5m, function=sum(bytes))
```

Supported spans: `s` (second), `m` (minute), `h` (hour), `d` (day), `w` (week).

