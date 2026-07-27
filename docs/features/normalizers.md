# Normalizers

Normalizers transform field names and extract timestamps during log ingestion. They consolidate inconsistent naming conventions from different log sources into canonical field names.

## Creating a Normalizer

Admins manage normalizers from the top-level **Normalizers** tab.

- **Name** and optional **Description**
- **Transforms** - ordered list of field name transformations
- **Field mappings** - map one or more source field names to a target name
- **Timestamp fields** - custom field names and their Go time format strings

## Transforms

Transforms modify field names in order. The following are available:

| Transform | Example |
|-----------|---------|
| `flatten_leaf` | `user.profile.name` &rarr; `name` |
| `snake_case` | `UserID` &rarr; `user_id` |
| `camelCase` | `user_id` &rarr; `userId` |
| `PascalCase` | `user_id` &rarr; `UserId` |
| `dedot` | `user.profile.name` &rarr; `user_profile_name` |
| `lowercase` | `EventID` &rarr; `eventid` |
| `uppercase` | `event_id` &rarr; `EVENT_ID` |

Order matters. Some transforms conflict (e.g., `flatten_leaf` and `dedot` cannot be combined).

## Field Mappings

Map multiple source names to a single target:

| Sources | Target |
|---------|--------|
| `userId`, `user_id`, `uid` | `user_id` |
| `srcIP`, `src_ip`, `source_address` | `src_ip` |

This is useful when ingesting logs from different vendors that use different field names for the same concept.

## Event Categories

`bifract_category` is the canonical taxonomy field. It is what routes events into the
provenance graph's backing tables and what [Sigma rules](../alerting/alert-feeds.md) are
scoped against, so a normalizer handling endpoint telemetry should set it.

Derive it with a value mapping from whatever your source uses to identify the event type:

```yaml
value_mappings:
  - from_field: event_id
    to_field: bifract_category
    map:
      "1": process_creation
      "3": network_connect
      "8": remote_thread
      "10": process_access
      "11": file_write
      "22": dns_query
```

Value mappings run *after* field mappings, so `from_field` must name the target field, not
the original source name.

Use **Sigma's `logsource.category` vocabulary** for these values, since Sigma rules are
prefiltered by category. The exception is registry: emit `registry_event` for all registry
activity, and Bifract maps Sigma's four registry categories onto it.

### Common categories

Only the first group builds [provenance graph](provenance-graph.md) edges. The rest are
searchable and usable by Sigma rules like any other category.

| Category | Sysmon event | Builds graph edges |
|---|---|---|
| `process_creation` | 1 | yes |
| `network_connect` | 3 | yes |
| `file_write` | 11 | yes |
| `dns_query` | 22 | yes |
| `remote_thread` | 8 | yes |
| `process_access` | 10 | yes |
| `registry_event` | 12, 13, 14 | no |
| `image_load` | 7 | no |
| `ps_script` | PowerShell/Operational 4104 | no |
| `pipe_created` | 17, 18 | no |
| `driver_load` | 6 | no |
| `create_stream_hash` | 15 | no |
| `file_delete` | 23, 26 | no |

Categories not listed here still work: an unrecognised category is used verbatim, so any
Sigma `logsource.category` is supported as long as your normalizer emits the same string.

## Timestamp Fields

Define custom timestamp field names and their formats. During ingestion, Bifract checks for timestamps in this order:

1. Per-token timestamp fields (if configured on the ingest token)
2. Normalizer timestamp fields
3. Global timestamp settings
4. Common field name fallbacks

Formats use Go time layout syntax (e.g., `2006-01-02T15:04:05Z07:00` for RFC3339).

## Assigning Normalizers

Normalizers are assigned to **ingest tokens**, not applied globally. Each token can reference one normalizer. When logs arrive via that token, the normalizer is applied during parsing.

[Alert feeds](../alerting/alert-feeds.md) can also reference a normalizer. When a feed syncs detection rules, the normalizer is applied to field names in the imported alerts so they match your ingested data.

One normalizer can be marked as the **default**, which is used for internal log sources.

!!! warning
    The default normalizer cannot be deleted.
