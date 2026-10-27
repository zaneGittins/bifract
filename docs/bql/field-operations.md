# Field Operations

Field operations compute new values per row: assignments, expression functions, conditional `case` blocks, and transform commands such as `regex()` or `split()`.

## Field assignment

`name := expression` adds a field. The right side is a full expression: functions nest, arithmetic follows normal precedence, and parentheses group.

```
* | zone := if(cidr(src_ip, "10.0.0.0/8"), "internal", "external")
```

After an aggregation, assignments can use its output columns:

```
* | groupBy(user) | multi(count(field=hostname, distinct=true, as=hosts), count(as=events)) | per_host := events / hosts
```

- **Types:** fields have no declared type and convert to what an expression needs, so `bytes * 2` works. An operand that is definitely text is rejected: `user + "@" + domain` is an error, use `concat()`.
- **Missing fields** read as empty text: `length()` is 0 and `isEmpty()` is true.
- **Named arguments:** every function accepts its parameter names after the positional ones, e.g. `substr(commandline, length=50, start=1)`.
- **Unknown names** are an error, never a field reference, so a typo cannot quietly match nothing.

### Eval

`eval()` is an alternative spelling of an assignment:

```
* | eval("score = bytes + priority")
```

## Expression functions

Use these inside `:=`, `eval()`, filters and command arguments.

| Function | Returns |
|---|---|
| `lower(field)`, `upper(field)` | Lowercased or uppercased text. Aliases `lowercase`, `uppercase` |
| `length(field)` | Character count. Alias `len` |
| `substring(field, start, length)` | Part of the text, counting from 1; `length` is optional. Alias `substr` |
| `concat(a, b, ...)` | The values joined, at least two |
| `coalesce(a, b, ...)` | The first non-empty value |
| `splitAt(field, delimiter, index)` | The Nth part after splitting, counting from 1 |
| `replaceRegex(field, pattern, replacement)` | The text with every regex match replaced |
| `trim(field)` | The text without leading and trailing whitespace |
| `base64Decode(field)`, `urlDecode(field)` | Decoded text; empty when base64 is invalid |
| `hash(a, ...)` | A hex hash (cityHash64) of the values |
| `toString(value)`, `toNumber(value)` | The value as text, or as a number (empty if not numeric) |
| `abs(value)`, `floor(value)`, `ceil(value)` | Absolute value, rounded down, rounded up |
| `round(value, digits)` | Rounded to `digits` decimal places, or to a whole number |
| `editDistance(a, b)` | Damerau-Levenshtein distance. Alias `levenshtein` |
| `entropy(field)` | Shannon entropy in bits per character |
| `isIPv4(field)`, `isIPv6(field)` | True for a valid address of that version |
| `isPrivateIP(field)` | True for RFC1918, loopback, link-local, CGNAT and IPv6 equivalents. Non-addresses are not private |
| `ipPrefix(field, bits)` | The enclosing network as `network/bits`, empty for a non-address |
| `cidr(field, range)` | True when the address is in the range |
| `dateDiff(unit, start, end)` | Whole units between two times: `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year`. Unparseable times give no result |
| `isEmpty(field)` | True when missing or empty |
| `startsWith(field, prefix)`, `endsWith(field, suffix)` | True on a match, case-sensitive |
| `contains(field, text)` | True on a match, case-insensitive |
| `if(condition, then, else)` | `then` or `else`. For more outcomes use `case` |

```
* | mb := round(bytes / 1048576, 2)
event_id=3 | groupBy(ipPrefix(src_ip, 24), function=count(dst_port, unique=true)) | _count > 50
* | dateDiff("hour", process_start, timestamp) > 24
```

`true` and `false` are literals, so `isPrivateIP(dst_ip) = false` and `NOT isPrivateIP(dst_ip)` are the same filter.

### Expressions as filters

An expression that yields a condition is a filter on its own:

```
* | startsWith(image, "C:\\Windows") AND len(commandline) > 500
```

An expression that yields a value, such as `| lower(image)` alone, is an error. Expression filters are computed per row and cannot use a skip index, so put a selective filter in front.

A name that is also a command (`len`, `entropy`, `substr`, `concat`, `hash`, `coalesce`, `base64Decode`, `urlDecode`, `levenshtein`) runs as the command unless a comparison follows:

```
* | len(commandline) | _len > 500    the command, which adds _len
* | len(commandline) > 500           an expression filter
```

### Expressions in command arguments

Any field argument accepts an expression, so a derived value can be grouped, sorted or aggregated directly:

```
* | groupBy(lower(user), function=sum(len(commandline)))
```

The column is named after the expression: `groupBy(lower(user))` produces `lower_user`.

## Case

`case` evaluates branches in order and runs the first that matches. Each branch is `condition | commands`, branches end with `;`, and `*` is the default:

```
* | case {
  image=~powershell,pwsh     | tool := "shell" ;
  cidr(src_ip, "10.0.0.0/8") | tool := "internal" ;
  * | tool := "other"
} | groupBy(tool)
```

- Conditions accept any filter, including `in()` and `cidr()`.
- A branch can run several commands, e.g. `level=error | sev := "high" | regex(...)`. They apply only to rows matching that branch.
- Aggregates in a branch (`image=~powershell | count() | sum(bytes)`) are computed per branch in a single scan.
- `groupBy`, `sort`, `limit`, `head`, `tail`, `dedup`, `table`, `join`, `chain`, outlier commands and charts cannot run inside a branch. Put them after the `case`.

## Transform commands

Each command adds one column, named as shown unless you pass `as=`.

| Command | Output | Description |
|---|---|---|
| `lowercase(field)`, `uppercase(field)` | the field itself | Rewrites the field in place. A second argument names a new column instead: `lowercase(user, user_lc)` |
| `len(field)` | `_len` | Character count |
| `entropy(field)` | `_entropy` | Shannon entropy, see [below](#entropy) |
| `logSize(field)` | `_size` | Estimated byte size; no field means the whole event (`norm_log`) |
| `levenshtein(a, b)` | `_distance` | Damerau-Levenshtein edit distance. A quoted argument is a literal string |
| `base64Decode(field)` | `_decoded` | Decoded text; empty when invalid |
| `urldecode(field)` | `_urldecoded` | URL-decoded text |
| `split(field, "delimiter", index)` | `_split` | The Nth part, counting from 1. A negative index counts from the end |
| `substr(field, start, length)` | `_substr` | Part of the text, counting from 1; `length` is optional |
| `coalesce(a, b, ...)` | `_coalesced` | The first non-empty value of two or more fields |
| `concat([a, b, ...])` | `_concat` | The values joined |
| `hash(a, ...)` | `_hash` | A hex cityHash64 of the values, useful as a composite lookup key |
| `sprintf(format, a, ...)` | `_sprintf` | printf-style formatting: `%s` text, `%d` integer, `%f` float |
| `replace(field, pattern, replacement)` | the field itself | Replaces every regex match in place, or into `as=` |
| `regex(pattern, field=...)` | named groups | Extracts with a regex, see [below](#regex) |

```
* | levenshtein(process_name, "svchost.exe") | _distance < 3
* | split(email, "@", 2) | groupBy(_split)
* | sprintf("%s@%s", user, domain, as=account)
* | replace(norm_log, "password=\\S+", "password=***")
* | logSize() | groupBy(computer_name, function=sum(_size))
```

!!! warning "Backslashes in quoted strings"
    Inside `"..."` a backslash escapes the next character, so write `"\\d+"` for the regex `\d+`. A single backslash is dropped silently: `"\d+"` becomes `d+`. A `/.../` regex literal needs no doubling.

### Regex

```
* | regex(/(?<user>[^\\]+)$/, field=account)
```

`field=` defaults to `norm_log`. Each named group, `(?<name>...)` or `(?P<name>...)`, becomes its own field. Without named groups, `as=name` stores the first capture group; with neither, the column `_regex` holds an array of every capture group. The pattern must contain at least one capture group.

### Entropy

```
* | entropy(query) | _entropy > 3.5
```

Entropy is 0 for empty or repeated text and rises as characters get more varied. Random or encoded text scores high: DGA domains, base64, DNS tunnel labels. It also grows with length, so instead of one cutoff, plot it against length and look for points above the trend:

```
query=* | entropy(query) | len(query) | groupBy(query)
| multi(max(_entropy, as=h), max(_len, as=n))
| scatter(x=n, y=h, label=query, line=trend)
```

Measure the part that varies, such as a subdomain label or an encoded argument, not the whole string.

## GeoIP enrichment

`lookupIP()` adds location and network columns for an IP address field. Alias `geoip`.

```
* | lookupIP(field=src_ip, include=[country,city]) | groupBy(country)
```

Both `field=` and `include=` are required. `include=` takes any of:

| Column | Type | Description |
|---|---|---|
| `country` | string | Country name |
| `city` | string | City name |
| `subdivision` | string | State or province |
| `continent` | string | Continent name |
| `timezone` | string | IANA timezone |
| `latitude`, `longitude` | float | Coordinates |
| `postal_code` | string | Postal code |
| `asn` | integer | Autonomous System Number |
| `as_org` | string | AS organization name |

!!! note "Setup"
    `lookupIP()` needs MaxMind GeoLite2. Get a free license key at [maxmind.com](https://www.maxmind.com/en/geolite2/signup), then set `MAXMIND_LICENSE_KEY` and `MAXMIND_ACCOUNT_ID` in `.env` (or the container environment). The databases download on startup and refresh daily. Without them the query fails with an error naming these variables.

## World map

`graphWorld()` plots rows on a zoomable world map, clustering nearby points. Alias `worldmap`.

```
* | lookupIP(field=src_ip, include=[latitude,longitude,country]) | graphWorld(label=country)
```

| Parameter | Description |
|---|---|
| `lat` | Latitude field. Default `latitude` |
| `lon` | Longitude field. Default `longitude` |
| `label` | Field shown in each marker's popup |
| `render` | Maximum points drawn. Default 5000, max 50000 |
| `limit` | Maximum rows the query returns |
