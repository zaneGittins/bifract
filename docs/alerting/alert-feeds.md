# Alert Feeds

Alert feeds sync detection rules from Git repositories. Feeds support both native Bifract YAML alert definitions and Sigma rules, which are automatically translated and normalized.

Admins manage feeds from **Alerts > Feeds** within a fractal.

## How Feeds Work

1. Bifract clones the configured Git repository on each sync cycle.
2. YAML files in the specified path are parsed as Sigma rules or Bifract alert definitions.
3. New rules are created as alerts. Changed rules are updated. Removed rules are deleted.
4. A normalizer (explicit or default) maps Sigma field names to your log schema.
5. The rule's `logsource.category` becomes a `bifract_category` prefilter, so the detection only runs against the event type it was written for.

Each feed alert is linked to its source file. Editing a feed alert creates a manual copy, leaving the original feed-managed version intact.

Every sync also catalogs the rules it did **not** import and why, which is what
[ATT&CK Coverage](attack-coverage.md) uses to turn a gap into a decision.

## Log Source Scoping

A Sigma rule declares the event type it targets in its `logsource` block. Bifract translates
that into a `bifract_category` equality placed ahead of the detection logic:

```yaml
logsource:
  category: process_creation
```

becomes

```
bifract_category=process_creation AND ( ...detection... )
```

Without this scoping a rule's detection runs against every event type in the fractal. A rule
matching on `Hashes` for a `create_stream_hash` event would also fire on every process
creation, because Sysmon event 1 carries an IMPHASH too.

Sigma category names are used as-is, so **your normalizer should emit Sigma's category
taxonomy**. A handful of names are aliased onto Bifract's canonical vocabulary:

| Sigma `logsource.category` | `bifract_category` |
|---|---|
| `network_connection` | `network_connect` |
| `file_event`, `file_create` | `file_write` |
| `create_remote_thread` | `remote_thread` |
| `dns_query_unfiltered` | `dns_query` |
| `registry_add`, `registry_set`, `registry_delete`, `registry_rename` | `registry_event` |

Registry is collapsed because Sigma splits it four ways (Sysmon 12/13/14) while the rules
themselves carry the discriminator (`EventType: SetValue`, `DeleteValue`, ...) inside their
detection block, so almost no precision is lost and normalizers only need one category.

Everything else passes through unchanged (`process_creation`, `dns_query`, `process_access`,
`image_load`, `ps_script`, `pipe_created`, and so on).

!!! warning "Rules for event types you do not collect will not match"
    Scoping means a `registry_set` rule matches nothing unless your normalizer emits
    `bifract_category=registry_event` (the alias the four Sigma registry categories collapse
    onto, per the table above). This is intentional: such rules previously appeared to work
    while matching unrelated events on field-name collisions alone. If a rule you expect to
    fire is silent, confirm your normalizer sets the category its `logsource` declares.

Rules with no `logsource.category` (Windows Security log rules, for example) are not
prefiltered. They scope themselves through `EventID` inside their own detection block.

## How Sigma Values Match

Bifract matches Sigma values the way the Sigma specification defines them:

- **Case is ignored.** `OriginalFileName: Cmd.Exe` matches `cmd.exe` and `CMD.EXE`.
- **`*` matches any run of characters and `?` matches exactly one**, in plain values and
  inside `contains`, `startswith` and `endswith`. `Image: '*\cmd.exe'` matches any path
  ending in `\cmd.exe`, and `CommandLine|contains: 'http*://'` matches `http://` and
  `https://` anywhere in the command line.
- **A backslash escapes a wildcard or another backslash**: `\*` is a literal asterisk,
  `\?` a literal question mark, `\\` a single backslash. A backslash before anything else
  is an ordinary character, so Windows paths need no escaping. A path separator followed
  by a wildcard must be written `\\*`: `C:\Windows\\*` matches everything under
  `C:\Windows\`, while `C:\Windows\*` matches only the literal text `C:\Windows*`.
- **`Field: null`** matches events where the field is absent or empty, and a value of
  `'*'` matches events where it has any non-empty value.

Each value becomes the cheapest query that matches it exactly:

| Value | Query | Notes |
|-------|-------|-------|
| No letters (`EventID: 4688`) | Equality | Can use the field's skip index |
| Letters, exact match (`OriginalFileName: Cmd.Exe`) | `in(lower(field), values=[...])` | Values with non-ASCII letters use a regular expression instead |
| Wildcard at one end only | Prefix, suffix or substring match | |
| Wildcard in the middle, or `?` | Case-insensitive regular expression | |

### Supported modifiers

| Modifier | Meaning |
|---|---|
| `contains`, `startswith`, `endswith` | The value appears anywhere in, at the start of, or at the end of the field |
| `all` | Every listed value must match, rather than any one of them |
| `re` | The value is a regular expression, matched case-sensitively |
| `re\|i`, `re\|m`, `re\|s` | Regular expression flags: case-insensitive, `^`/`$` match at line breaks, `.` matches a line break |

A rule that uses any other modifier is not imported, because dropping the modifier would
deploy a different detection than the one written. This includes `base64`,
`base64offset`, `cidr`, `windash`, `exists`, `gt`, `gte`, `lt`, `lte`, `fieldref`,
`expand`, `utf16le`, `utf16be`, `utf16`, `wide` and `cased`. Conditions that use
aggregation (`| count() by ...`) and combinations such as `contains|endswith` are
refused the same way. Each refused rule is catalogued with the field, selection and
modifier that stopped it, and appears in [ATT&CK Coverage](attack-coverage.md#closing-a-gap)
as `cannot be translated to BQL`.

## Feed Configuration

| Field | Description |
|-------|-------------|
| Name | Display name for the feed |
| Repository URL | Git HTTPS URL (e.g. `https://github.com/SigmaHQ/sigma`) |
| Branch | Git branch to sync from (default: `main`) |
| Path | Subdirectory within the repo containing rules (e.g. `rules/windows`) |
| Auth Token | Personal access token for private repositories (encrypted at rest) |
| Normalizer | Field mapping normalizer for Sigma rule translation |
| Sync Schedule | `hourly`, `daily`, `weekly`, `monthly`, or `never` |
| Min Severity | Minimum Sigma severity level to import (`informational`, `low`, `medium`, `high`, `critical`) |
| Min Status | Minimum Sigma maturity status to import (`unsupported`, `deprecated`, `experimental`, `test`, `stable`) |

## Severity Hierarchy

Rules below the configured minimum severity are skipped during sync.

| Level | Order |
|-------|-------|
| informational | 1 (lowest) |
| low | 2 |
| medium | 3 |
| high | 4 |
| critical | 5 (highest) |

## Status Hierarchy

Rules below the configured minimum status are skipped during sync.

| Status | Order |
|--------|-------|
| unsupported | 1 (lowest) |
| deprecated | 2 |
| experimental | 3 |
| test | 4 |
| stable | 5 (highest) |

## Recommended Community Feeds

These public Sigma rule repositories work well as starting points. Add them from **Alerts > Feeds**.

| Name | Repository URL | Path | Min Severity | Min Status | Schedule |
|------|---------------|------|-------------|------------|----------|
| SigmaHQ Windows | `https://github.com/SigmaHQ/sigma` | `rules/windows` | high | stable | daily |
| Hayabusa Sysmon | `https://github.com/Yamato-Security/hayabusa-rules` | `sigma/sysmon` | medium | stable | daily |
