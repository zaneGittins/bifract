# Field Operations

## Field Assignment

Assign computed values to new fields using `:=`:

```
severity := "high"
score := bytes * 2
sum := field1 + 5
label := status
```

Supports complex math with parentheses and division. When used after aggregations, references the computed aliases:

```
* | groupby(user) | multi(count(field=event_id, distinct=true, as=unique), count(field=event_id, as=total)) | confidence := ((total - unique) / total) * 0.95
```

### Eval

Alternative syntax for field assignments inside a pipeline:

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

Conditionally assign field values:

```
case {
  status=200 | result := "ok" ;
  status=404 | result := "not found" ;
  * | result := "other"
}
```

Conditions support `=`, `!=`, `>`, `<`, and regex patterns:

```
case {
  user=/admin/i | role := "admin" ;
  bytes>1000000 | size := "large" ;
  * | size := "small"
}
```

## String Operations

### Regex Extraction

```
* | regex("(\d+\.\d+\.\d+\.\d+)", field=raw_log)
```

Named captures extract to individual fields:

```
* | regex(field=image, regex="(.+)\\\\(?<executable_name>.*\\.exe)")
```

This creates a field called `executable_name` from the named capture group.

### Replace

```
* | replace("password=\S+", "password=***", raw_log)
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

### Levenshtein Distance

Calculates the Damerau-Levenshtein edit distance between two fields or values as `_distance`:

```
* | levenshtein(user, "admin")
* | levenshtein(src_host, dst_host)
* | levenshtein(process_name, "svchost.exe") | _distance < 3
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
* | split(image, "\\", -1)
* | split(path, "/", 2)
* | split(email, "@", 2) | groupby(_split) | count()
```

Use index `-1` to get the last element.

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
* | coalesce(src_ip, client_ip) | groupby(_coalesced) | count()
```

### Sprintf

Formats fields into a string using printf-style format specifiers as `_sprintf`:

```
* | sprintf("%s - %s", username, action, as=user_action)
* | sprintf("https://%s:%d/%s", hostname, port, path, as=full_url)
* | sprintf("%s@%s", user, domain) | groupby(_sprintf) | count()
```

Supports `%s` (string), `%d` (integer), `%f` (float), and other standard format specifiers. Use `as=` to set a custom output field name.

## GeoIP Enrichment

### lookupIP

Enriches logs with geolocation and ASN data from MaxMind GeoLite2 databases. Requires `MAXMIND_LICENSE_KEY` and `MAXMIND_ACCOUNT_ID` environment variables to be configured.

```
* | lookupIP(field=src_ip, include=[country,city])
* | lookupIP(field=client_ip, include=[asn,as_org,country])
* | lookupIP(field=src_ip, include=[country,city]) | groupby(country) | count()
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
