# Schema Fields

The **Schema** tab (admin-only) controls which log attributes get a dedicated ClickHouse type hint and, optionally, a secondary index for faster filtering. Bifract stores logs in a single `logs` table with attributes in a JSON column; promoting a frequently-filtered field gives it its own sub-column so equality and token lookups skip far more data.

Open it from the top nav **Schema** tab. It is project-wide (applies across all fractals), not per-fractal.

## Project Defaults

Bifract ships with a set of built-in, type-hinted fields (for example `computer_name`, `src_ip`, `user`). These are always present and cannot be removed.

## Observed Fields

The Schema tab lists the field names Bifract has actually seen in your logs, with how often each appears and a recommended index type. This is the normal way to promote a field: find it in the list and **Reserve** it, rather than typing the name from memory.

Fields you never filter on can be **ignored**, which hides them from the list without changing how they are stored or queried.

## Adding a Field

Use **Add field** to reserve an attribute that has not appeared in your logs yet. Provide:

- **Field name** - letters, digits, and underscores; must start with a letter or underscore.
- **Skip index** - one of:
    - **No index** (default) - applies the type hint (a dedicated sub-column) with no secondary index. This alone is a large speedup for most fields, and costs the least at ingest.
    - **Set** - for low-cardinality values, under roughly a few hundred distinct values (status, severity, region).
    - **Bloom filter** - for high-cardinality values (IDs, hashes, hostnames).

Leave the index off unless you actually filter on the field: every index is work done on every ingested row.

!!! note "Indexing is forward-only"
    A new field applies to **newly ingested logs only**. Existing logs are not retroactively indexed. New fields show an **Indexing** badge that flips to **Active** once ClickHouse finishes applying the schema change.

Removing a custom field stops queries from using its index. The underlying ClickHouse type hint and index remain until the next schema reset.

## Import / Export

Custom fields can be exported to and imported from YAML for version control or moving between deployments. Importing **replaces** all current custom fields with the file's contents; log data is unaffected.

## Schema Reset

!!! danger "Destructive"
    Reset drops and rebuilds the `logs` table schema and **deletes all log data across every fractal**. It is the only way to retroactively change index hints for existing data. The action requires typing `DELETE ALL LOG DATA` to confirm.

    With the [Iceberg archive](backup-restore.md#iceberg-archive) enabled, the archived copy is untouched by a reset, so log data can be restored afterwards. Without it, a reset is unrecoverable.
