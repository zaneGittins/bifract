-- Detected max_dynamic_paths overflow.
--
-- ClickHouse gives each JSON path its own sub-column only up to
-- max_dynamic_paths (1024). Past that, new paths spill into shared storage and
-- queries touching them stop pruning and start scanning every row. The field
-- keeps working, so nothing fails loudly: it just gets quietly and permanently
-- slow, which is the worst possible failure mode to leave invisible.
--
-- Finding which paths have spilled requires reading the JSON column
-- (JSONSharedDataPaths), roughly ten times the cost of the flat norm_log sample
-- the schema tab uses, and it scales with path count rather than row count, so
-- it gets more expensive exactly as it becomes more relevant. It is therefore
-- computed by a background monitor on one advisory-locked replica and cached
-- here, rather than recomputed per page load on every replica.
CREATE TABLE IF NOT EXISTS schema_field_overflow (
    field_name  VARCHAR(255) PRIMARY KEY,
    rows_seen   BIGINT       NOT NULL DEFAULT 0,
    detected_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
