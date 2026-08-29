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

These public Sigma rule repositories work well as starting points. Add them from **Alerts > Alert Feeds**.

| Name | Repository URL | Path | Min Severity | Min Status | Schedule |
|------|---------------|------|-------------|------------|----------|
| SigmaHQ Windows | `https://github.com/SigmaHQ/sigma` | `rules/windows` | high | stable | daily |
| Hayabusa Sysmon | `https://github.com/Yamato-Security/hayabusa-rules` | `sigma/sysmon` | medium | stable | daily |
