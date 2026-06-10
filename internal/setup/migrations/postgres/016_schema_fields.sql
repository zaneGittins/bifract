-- User-defined custom ClickHouse schema fields.
-- Project default fields (src_ip, user, event_id, etc.) are hardcoded in the
-- binary (pkg/schemafields/defaults.go) and are not stored here.
-- Rows in this table represent additive user customizations only.
CREATE TABLE IF NOT EXISTS clickhouse_schema_fields (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    field_name  VARCHAR(255) NOT NULL UNIQUE,
    index_type  VARCHAR(32)  NOT NULL DEFAULT 'bloom_filter',
    created_by  VARCHAR(255) NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
