# Normalizers

Normalizers transform field names and extract timestamps during log ingestion. They consolidate inconsistent naming conventions from different log sources into canonical field names.

## Creating a Normalizer

Admins manage normalizers from **Settings > Normalizers**.

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
