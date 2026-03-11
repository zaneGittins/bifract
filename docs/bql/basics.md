# Basics & Filtering

BQL is Bifract's query language for searching and analyzing logs. It uses a pipeline model where results are filtered and transformed through a series of commands separated by `|`.

## Basics

```
filter | command() | command()
```

- Start with a filter expression (or `*` to match all logs)
- Chain commands with `|`
- All queries run against the selected time range and fractal

## Filtering

### Match all logs

```
*
```

### Field equality

```
event_id=1
| status=error
```

### Regex match

```
image=/powershell/i
| command_line=/cmd\.exe/
```

Append `i` for case-insensitive matching.

### Negative match

```
image!=/powershell/
status!=200
```

### Wildcard value

Match any non-empty value for a field:

```
user=*
```

### Comparison operators

```
status_code>=500
bytes>1000
response_time<200
```

### In (value list)

Filter by a set of values:

```
* | in(status, "200,301,404")
```

### CIDR range

Filter by IP address range:

```
* | cidr(src_ip, "10.0.0.0/8")
* | cidr(dst_ip, "192.168.1.0/24")
* | !cidr(src_ip, "10.0.0.0/8")
```

Use `!cidr()` to exclude a range.

### Bare string search

Searches `raw_log` for a substring or pattern.

```
"authentication failed"
/failed.*login/i
```

### Boolean logic

```
event_id=1 AND image=/powershell/i
status=error OR status=critical
NOT image=/svchost/
```

AND has higher precedence than OR. Use parentheses to group:

```
(status=error OR status=critical) AND user=admin
```

Implicit AND: multiple conditions without an operator are ANDed together.

```
event_id=1 image=/powershell/i
```

## Variables

Notebooks and dashboards support variables that act as placeholders in queries. Define variables in the variables bar, then reference them with `@` in any query:

```
user=@target_user AND image=@process
```

When the query runs, `@target_user` and `@process` are replaced with the values set in the variables bar. Variables default to `*` if no value is set. This lets you reuse the same notebook or dashboard across different investigations by changing variable values instead of editing every query.
