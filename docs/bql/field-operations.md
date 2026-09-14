# Field Operations

## Field Assignment

Assign computed values to new fields using `:=`:

```
severity := "high"
score := bytes * 2
label := status
tag := substr(lower(image), 1, 10)
```

The right-hand side is a full expression: functions nest, arithmetic follows normal precedence, and parentheses group.

```
* | mb := round(bytes / 1048576, 2)
* | user_host := concat(user, "@", hostname)
* | zone := if(cidr(src_ip, "10.0.0.0/8"), "internal", "external")
```

After an aggregation, an assignment references the computed aliases:

```
* | groupby(user) | multi(count(field=event_id, distinct=true, as=unique), count(field=event_id, as=total)) | confidence := ((total - unique) / total) * 0.95
```

### Types

A field has no declared type, so it coerces to whatever an expression needs: `bytes * 2` works whether or not `bytes` carries a type hint. What is rejected is an operand that is definitely text.

```
score := bytes * 2              ok
name  := concat(user, "@", domain)   ok
name  := user + "@" + domain    error: + expects numbers, use concat()
```

A missing field reads as empty text, so `length()` of one is 0 and `isEmpty()` is true.

### Named arguments

Every function accepts its parameter names. Positional arguments come first, named ones after, and a parameter cannot be given both ways.

```
substr(commandline, 1, 50)
substr(field=commandline, start=1, length=50)
substr(commandline, length=50, start=1)
```

### Functions

| Category | Functions |
|---|---|
| Text | `lower`, `upper`, `length`, `substring`, `concat`, `coalesce`, `splitAt`, `replaceRegex`, `trim`, `base64Decode`, `urlDecode`, `hash`, `toString` |
| Numbers | `abs`, `floor`, `ceil`, `round`, `editDistance`, `toNumber` |
| Network | `isPrivateIP`, `isIPv4`, `isIPv6`, `ipPrefix`, `cidr` |
| Time | `dateDiff` |
| Conditions | `isEmpty`, `startsWith`, `endsWith`, `contains`, `if` |

`true` and `false` are boolean literals, so a condition can be compared to one:

```
event_id=3 | isPrivateIP(dst_ip) = false
event_id=3 | NOT isPrivateIP(dst_ip)
```

`isPrivateIP()` covers RFC1918, loopback, link-local, CGNAT and the IPv6 equivalents, replacing a chain of `!cidr()` calls. A value that is not an address is not private.

`ipPrefix(field, bits)` returns the enclosing network as `network/bits`. Group by it to find which subnets are active, which a membership test cannot answer:

```
event_id=3 | groupby(ipPrefix(src_ip, 24), function=count(dst_port, unique=true)) | _count > 50
```

`dateDiff("unit", start, end)` counts whole units between two times. Either side may be a log field holding a time in any of the usual shapes; a row whose value cannot be parsed yields no result rather than failing the query.

```
* | age := dateDiff("second", first_seen, last_seen)
* | dateDiff("hour", process_start, timestamp) > 24
```

Where a pipeline command of the same name exists (`len`, `substr`, `concat`, `hash`, `coalesce`, `base64Decode`, `urlDecode`, `levenshtein`), the name means the same thing in either position. As a stage it binds its documented output column; inside an expression it returns a value.

```
* | len(commandline) | _len > 500
* | n := len(commandline) | n > 500
```

An unknown function name is an error, never a field reference, so a typo cannot quietly match nothing.

### Expressions as filters

An expression that reads as a condition can be a filter on its own, with no assignment:

```
* | lower(image) = "cmd.exe"
* | len(commandline) > 500
* | startsWith(image, "C:\\Windows")
* | !contains(commandline, "-enc")
```

An expression that produces a value rather than a condition is an error, so `| lower(image)` on its own is rejected instead of filtering on whatever that string became.

Names shared with a pipeline command keep their command meaning unless a comparison follows, so `| len(commandline)` still binds `_len` and `cidr(a) OR cidr(b)` still behaves as it always has.

```
* | len(commandline) | _len > 500      the command, binding _len
* | len(commandline) > 500             the expression, as a filter
```

Filtering on an expression computes it per row, so it cannot prune through a skip index. Put a time range or a selective indexed filter in front of it.

### Expressions in command arguments

A field position accepts an expression, so a derived value can be grouped, sorted, projected or aggregated without materialising it first:

```
* | groupby(lower(user))
* | groupby(substr(image, 1, 10), function=count())
* | sort(len(commandline), order=desc)
* | table(user, len(commandline))
* | dedup(lower(user))
* | groupby(user, function=sum(len(commandline)))
```

The output column is named after the expression, so `groupby(lower(user))` produces `lower_user`, addressable downstream like any other column.

Bracket lists stay argument syntax rather than array values, so `table([a,b,c])`, `concat([a,b], as=x)` and `in(f, values=[...])` are unchanged.

### Eval

Alternative syntax for field assignments inside a pipeline. The quoted text is the same expression language:

```
* | eval("score = bytes + priority")
```

## Hash

Create a hash key from one or more fields:

```
* | hash(user)
* | hash(field=user, computer)
* | hash(user, event_id, as=composite_key)
```

Uses `cityHash64` internally. Useful for creating composite keys for dictionary lookups.

## Case Statements

Evaluate conditional branches in order. Each branch is written as `condition | commands`, branches are separated by `;`, and `*` is the default branch:

```
case {
  status=200 | result := "ok" ;
  status=404 | result := "not found" ;
  * | result := "other"
}
```

### Conditions

A branch condition accepts any filter operator (`=`, `!=`, `=~` contains-any, `=^` starts-with, `=$` ends-with, `>`, `<`, `>=`, `<=`, and `=/regex/`) as well as `in(...)` and `cidr(...)`:

```
case {
  image=~powershell,pwsh     | tool := "shell" ;
  cidr(src_ip, "10.0.0.0/8") | zone := "internal" ;
  * | tool := "other"
}
```

### Multiple commands per branch

A branch can run several pipe commands. Field assignments and per-row transforms (`regex`, `eval`, `lowercase`, and others) apply only to rows matching the branch:

```
case {
  level=error | sev := "high" | regex(field=norm_log, pattern="code=(?<code>[0-9]+)") ;
  * | sev := "low"
}
```

### Aggregations per branch

Aggregations inside a branch compile to single-pass conditional aggregates (ClickHouse `-If` combinators), producing one column per branch aggregation in a single scan:

```
case {
  image=~powershell | count() | sum(bytes) ;
  image=~explorer   | count() ;
  * | count()
}
```

Structural commands (`groupby`, `sort`, `limit`, `join`, `chain`, window functions, and charts) cannot be used inside a branch. Place them after the `case` (for example, `case { ... } | groupby(tool)`).

## String Operations

### Regex Extraction

```
* | regex("(\d+\.\d+\.\d+\.\d+)", field=norm_log)
```

`field=` defaults to `norm_log`, the canonical normalized event text. `raw_log` is not addressable from BQL: it is a demoted, 7-day troubleshooting column stored in a separate table.

Named captures extract to individual fields:

```
* | regex(field=image, regex="(.+)\\\\(?<executable_name>.*\\.exe)")
```

This creates a field called `executable_name` from the named capture group. Both `(?<name>...)` and `(?P<name>...)` are accepted. Each named group becomes its own field; a pattern with no named group and no `as=` produces the array field `regex_match` instead.

### Replace

```
* | replace("password=\S+", "password=***", norm_log)
```

### Concat

```
* | concat([user, host], as=user_host)
```

### Lowercase

```
* | lowercase(user)
```

### Uppercase

```
* | uppercase(user)
```

### Length

Returns the string length of a field as `_len`:

```
* | len(program_name)
* | len(program_name) | _len > 10
* | len(message) | sort(_len, desc)
```

### Log Size

Returns the byte size of a log as `_size`. With no argument it measures the whole normalized event (`norm_log`); pass a field to size that column instead. Useful for diagnosing log growth by summing or aggregating sizes:

```
* | logSize() | sort(_size, desc)
* | logSize() | groupby(computer_name, function=sum(_size))
* | logSize(message, as=_msgsize) | _msgsize > 4096
```

Sizes are computed at query time via ClickHouse `byteSize()` (estimated uncompressed bytes), so they work retroactively on all logs with no extra storage.

### Levenshtein Distance

Calculates the Damerau-Levenshtein edit distance between two fields as `_distance`:

```
* | levenshtein(src_host, dst_host)
* | levenshtein(user, expected_user) | _distance < 3
```

Both arguments are resolved as log fields. To compare against a fixed string, materialize it first with an assignment:

```
* | baseline := "svchost.exe" | levenshtein(process_name, baseline) | _distance < 3
```

Useful for detecting typosquatting, lookalike process names, or fuzzy matching.

### Base64 Decode

Decodes a base64-encoded field as `_decoded`. Returns empty string on invalid input:

```
* | base64Decode(payload)
* | base64Decode(encoded_command) | _decoded=/powershell/i
* | base64Decode(data) | table(data, _decoded)
```

### Split

Splits a field by a delimiter and returns the Nth element (1-indexed) as `_split`:

```
* | split(path, "/", 2)
* | split(email, "@", 2) | groupby(_split, function=count())
```

The index must be a positive integer.

### Substring

Extracts a substring from a field as `_substr`:

```
* | substr(message, 1, 50)
* | substr(hash, 1, 8)
* | substr(path, 5)
```

### URL Decode

Decodes a URL-encoded field as `_urldecoded`:

```
* | urldecode(request_uri)
* | urldecode(query_string) | _urldecoded=/script/i
```

### Coalesce

Returns the first non-empty value from a list of fields as `_coalesced`:

```
* | coalesce(user, username, account_name)
* | coalesce(src_ip, client_ip) | groupby(_coalesced, function=count())
```

### Sprintf

Formats fields into a string using printf-style format specifiers as `_sprintf`:

```
* | sprintf("%s - %s", username, action, as=user_action)
* | sprintf("https://%s:%d/%s", hostname, port, path, as=full_url)
* | sprintf("%s@%s", user, domain) | groupby(_sprintf, function=count())
```

Supports `%s` (string), `%d` (integer), `%f` (float), and other standard format specifiers. Use `as=` to set a custom output field name.

## GeoIP Enrichment

### lookupIP

Enriches logs with geolocation and ASN data from MaxMind GeoLite2 databases. Requires `MAXMIND_LICENSE_KEY` and `MAXMIND_ACCOUNT_ID` environment variables to be configured.

```
* | lookupIP(field=src_ip, include=[country,city])
* | lookupIP(field=client_ip, include=[asn,as_org,country])
* | lookupIP(field=src_ip, include=[country,city]) | groupby(country, function=count())
```

**Parameters:**
- `field` (required): The log field containing the IP address
- `include` (required): Columns to retrieve from the GeoIP databases

**Available columns:**

| Column | Source | Type | Description |
|--------|--------|------|-------------|
| country | City DB | string | Country name |
| city | City DB | string | City name |
| subdivision | City DB | string | State/province |
| continent | City DB | string | Continent name |
| timezone | City DB | string | IANA timezone |
| latitude | City DB | float | Geographic latitude |
| longitude | City DB | float | Geographic longitude |
| postal_code | City DB | string | Postal/ZIP code |
| asn | ASN DB | integer | Autonomous System Number |
| as_org | ASN DB | string | AS organization name |

**Setup:**

Add these environment variables to your `.env` file (or pass them to the container):

```
MAXMIND_LICENSE_KEY=your_license_key
MAXMIND_ACCOUNT_ID=your_account_id
```

Obtain a free license key at [maxmind.com](https://www.maxmind.com/en/geolite2/signup). The databases are downloaded automatically on startup and refreshed daily.

## World Map Visualization

### graphWorld

Renders data points on an interactive world map. Points with geographic proximity are clustered together at low zoom levels and split apart as you zoom in. Works in search, notebooks, and dashboards.

```
* | lookupIP(field=src_ip, include=[latitude,longitude,country]) | graphWorld(label=country)
* | lookupIP(field=src_ip, include=[latitude,longitude,city,asn]) | graphWorld(label=city)
* | graphWorld(lat=geo_lat, lon=geo_lon, limit=10000)
```

**Parameters:**
- `lat` (optional): Latitude field name (default: `latitude`)
- `lon` (optional): Longitude field name (default: `longitude`)
- `label` (optional): Field to display as marker label in popups
- `limit` (optional): Maximum number of points (default: 5000, max: 50000)

The map supports zoom, pan, and click-to-expand clusters. Individual markers show a popup with the label, coordinates, and additional fields from the result row.
