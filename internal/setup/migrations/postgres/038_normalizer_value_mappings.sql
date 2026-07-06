-- Additive value-lookup (derived-field) transform for normalizers. value_mappings
-- stores an array of {from_field, to_field, map, default} entries that derive a new
-- field from an existing field's value (e.g. Sysmon event_id -> category) without
-- removing the source. Applied at ingest, after field mappings, so the derived field
-- is stored in ClickHouse and the Iceberg spool. Additive and non-destructive.
ALTER TABLE normalizers ADD COLUMN IF NOT EXISTS value_mappings JSONB NOT NULL DEFAULT '[]';
