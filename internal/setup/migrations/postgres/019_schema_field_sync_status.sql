-- Track background ClickHouse reconcile state for custom schema fields.
-- Adding a custom field now persists immediately and reconciles the ClickHouse
-- type hint + skip index in the background (the ALTER can block for minutes on
-- large datasets). These columns surface that progress in the UI.
-- Existing rows default to 'active' because they were reconciled on prior boot.
ALTER TABLE clickhouse_schema_fields ADD COLUMN IF NOT EXISTS sync_status VARCHAR(16) NOT NULL DEFAULT 'active';
ALTER TABLE clickhouse_schema_fields ADD COLUMN IF NOT EXISTS sync_error  TEXT         NOT NULL DEFAULT '';
