# Schema Fields

The **Schema** tab (admin-only) controls which log attributes get a dedicated ClickHouse index for faster filtering. Bifract stores logs in a single `log` table with attributes in a JSON column; promoting a frequently-filtered field to a schema field gives it a type hint and a secondary index so equality and token lookups skip far more data.

Open it from the top nav **Schema** tab. It is project-wide (applies across all fractals), not per-fractal.

## Project Defaults

Bifract ships with a set of built-in, type-hinted fields (for example `computer_name`, `src_ip`, `user`). These are always indexed and cannot be removed.

## Custom Fields

Add a field to accelerate attributes specific to your environment:

- **Field Name** - the attribute as it appears in your logs.
- **Index Type**:
    - **Bloom Filter** - best for high-cardinality values (IDs, hashes, hostnames).
    - **Set** - best for low-cardinality values (status, severity, region).

!!! note "Indexing is forward-only"
    A new field applies to **newly ingested logs only**. Existing logs are not retroactively indexed. New fields show an **Indexing** badge that flips to **Active** once ClickHouse finishes applying the schema change.

Removing a custom field stops queries from using its index. The underlying ClickHouse type hint and index remain until the next schema reset.

## Import / Export

Custom fields can be exported to and imported from YAML for version control or moving between deployments. Importing **replaces** all current custom fields with the file's contents; log data is unaffected.

## Schema Reset

!!! danger "Destructive"
    Reset drops and rebuilds the `log` table schema and **deletes all log data across every fractal**. It is the only way to retroactively change index hints for existing data. The action requires typing `DELETE ALL LOG DATA` to confirm.