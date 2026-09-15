# Basics & Filtering

BQL is Bifract's query language for searching and analyzing logs. It uses a pipeline model where results are filtered and transformed through a series of commands separated by `|`.

## Basics

```
filter | command() | command()
```

- Start with a filter expression (or `*` to match all logs)
- Chain commands with `|`
- All queries run against the selected time range and fractal

### Comments

A line beginning with `//` is a comment and is stripped before the query runs. Comments must
be on their own line; `//` after query text on the same line is not a comment.

```
// failed logons, busiest source first
event_id=4625 | groupBy(src_ip, function=count()) | sort(_count, order=desc)
```

## Filtering

### Match all logs

```
*
```

### Field equality

```
event_id=1
status=error
```

### Regex match

```
image=/powershell/i
| commandline=/cmd\.exe/
```

Append `i` for case-insensitive matching.

### Contains-any (`=~`)

Case-insensitive substring match against a comma-separated list of terms. Faster than equivalent regex for multi-term searches: it uses SIMD multi-pattern search internally and gains additional speed from text indexes when present.

```
image=~powershell,pwsh,cmd
commandline=~encodedcommand,bypass,hidden
parent_image=~wscript,cscript,mshta
```

Single-term form also works:

```
image=~mimikatz
```

### Starts-with-any (`=^`)

Case-insensitive prefix match against a comma-separated list of terms.

```
image=^mimikatz,impacket
commandline=^"powershell -enc"
src_ip=^192.168,10.
```

### Ends-with-any (`=$`)

Case-insensitive suffix match against a comma-separated list of terms.

```
image=$exe,dll,bat
image=$powershell.exe,cmd.exe
```

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

### Compare two fields (`field()`)

The right-hand side of a comparison is normally a literal: `src_port=dst_port` matches rows whose `src_port` is the text "dst_port". Wrap it in `field()` to compare against another field's value instead.

```
src_port = field(dst_port)
src_bytes > field(dst_bytes)
user != field(process_owner)
```

Valid with `=`, `!=`, `>`, `<`, `>=` and `<=`. `=` and `!=` compare the two as text; the ordering operators compare them numerically. A row missing either field never matches `=` and always matches `!=`.

Both fields are read per row, so the comparison cannot prune granules through a skip index. Put a time range or another selective filter in front of it.

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

Searches `norm_log` (the canonical normalized event text) for a substring or pattern. `norm_log` carries an n-gram text index, so these searches prune granules rather than scanning every row.

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

Condition functions are operands like any other, so they group and negate the same way:

```
cidr(dst_ip, "10.0.0.0/8") OR cidr(dst_ip, "192.168.0.0/16")
!in(status, "200,301") AND user=admin
```

## Variables

Search, notebooks, and dashboards support variables that act as placeholders in queries. Define variables in the variables bar, then reference them with `@` in any query:

```
user=@target_user AND image=@process
```

When the query runs, `@target_user` and `@process` are replaced with the values set in the variables bar. Variables default to `*` if no value is set, so a notebook or dashboard is reused across investigations by changing values instead of editing every query.

A variable is supplied from outside the query. A name the query defines for itself is a
[binding](#bindings-let), written `&name`.

## Bindings (`let`)

A `let` statement names an expression, a filter, or a pipeline so a query states it once and
uses it in several places. Unlike an `@variable`, which a dashboard or notebook supplies, a
binding belongs to the query that declares it.

Statements come before the query, separated by `;`. A binding is made with `:=`, the assignment operator, and a reference carries the `&` sigil:

```
let &lolbin := lower(image) =~ "rundll32.exe","regsvr32.exe","mshta.exe";
let &officey := lower(parent_image) =~ "winword.exe","excel.exe";

* | &lolbin AND &officey | table(computer_name, user, image, commandline)
```

A binding holds anything the expression grammar accepts, plus the string, number and regex
literals BQL already has, so it can be a value, a computed field, or a whole filter. A literal
binding stands wherever a value goes; a computed one is used on its own:

| Binding | Used as |
|---|---|
| `let &n := 500;` | `len(commandline) > &n` |
| `let &cmdlen := len(commandline);` | `&cmdlen > 500 AND &cmdlen < 4000`, `table(&cmdlen)` |
| `let &lolbin := lower(image) =~ "mshta.exe";` | `&lolbin`, `NOT &lolbin`, `&lolbin AND user="bob"` |
| `let &user := "CORP\\rpatel";` | `user=&user`, `user =~ &user,"other"`, `in(user, &user)` |
| `let &enc := /-enc(odedcommand)?\s/;` | `commandline=&enc` |

The `&` is part of the name, so a binding never collides with a log field, and a misspelled
reference is an error rather than a field lookup that quietly matches nothing. A binding may
use one declared before it; referencing itself or a later one is an error.

A column a command produces is named after the binding, so `table(&cmdlen)` returns a column
called `cmdlen`.

### Result sets

A binding whose value is a pipeline names a set of rows. Use it in `in()` or as a `join()` block:

```
let &admins  := user_type="admin" | groupby(user);
let &servers := role="server"     | groupby(computer_name);

event_id="4624"
  | in(user, &admins)
  | in(computer_name, &servers)
  | groupby(user, computer_name) | count()
```

`join()` takes no nested joins, so two set memberships against two subqueries can only be
written this way.

The column tested is the one named after the field, which is how a `join()` block names its key.
A binding returning exactly one column needs no name match; one returning several unrelated
columns is an error naming them.

A result-set binding is scoped exactly like the query around it: same fractal, same time range,
and it may build on a binding declared before it. Read it in more than one place and the query
builds it once and reads it twice, rather than running the same subquery again.

## Boolean parameters

Most switches are off by default and turned on: `strict=true`, `distinct=true`,
`percent=true`, `directed=true`.

`pgr()` is the exception. Its `reconnect=` and `diffuse=` are on by default,
because a provenance graph without them is the narrower answer, so you write
`reconnect=false` or `diffuse=false` to turn them off.

Anything a parameter does not recognise as a yes or a no leaves the default
alone, so a typo cannot silently flip a switch.
