-- DEAD: stamped but never executed since the ingest-partition reset. Kept for numbering (next: 016) and TestUpgradeFromV002.
-- Process-lineage skeleton for ptg() (Process Tree Graph). One row per process-create
-- event, ordered by (fractal_id, process_guid) so ptg() traversal hops are primary-key
-- point lookups instead of full-table scans over logs (which OOMs dfs/bfs at scale).
--
-- Self-sufficient and compact (no raw_log/norm_log/fields bulk): the process tree renders
-- from these columns alone even after the source logs tier/expire, so it is retained on a
-- long, DFIR-driven TTL decoupled from logs. The app applies BIFRACT_PROC_LINEAGE_TTL_DAYS
-- via ALTER ... MODIFY TTL at startup; 365d here is the default.
--
-- ReplacingMergeTree dedups on re-ingestion / iceberg replay (process_guid is globally
-- unique per process). New-logs-only: the MV populates from process-create events ingested
-- after this migration; no backfill of pre-existing data.
--
-- process_guid / parent_process_guid type hints and the process_guid bloom index are NOT
-- added here (no destructive MODIFY COLUMN fields JSON): ReconcileSchemaFields applies them
-- additively at every app startup from schemafields.ProjectDefaultFields.
CREATE TABLE IF NOT EXISTS proc_lineage (
    fractal_id    LowCardinality(String),
    timestamp     DateTime64(3),
    log_id        String,
    process_guid  String,
    parent_guid   String,
    image         LowCardinality(String),
    parent_image  LowCardinality(String),
    commandline   String,
    computer_name LowCardinality(String),
    INDEX idx_parent_guid parent_guid TYPE bloom_filter(0.001) GRANULARITY 1
) ENGINE = ReplacingMergeTree(timestamp)
ORDER BY (fractal_id, process_guid)
TTL toDateTime(timestamp) + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

-- Populates proc_lineage from process-create events, keyed on the normalized
-- bifract_category taxonomy field (Phase A; bifract_ prefix avoids collision with
-- source-set "category"). ::String is safe whether or not the type hint is applied yet.
CREATE MATERIALIZED VIEW IF NOT EXISTS proc_lineage_mv TO proc_lineage
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    fractal_id,
    timestamp,
    log_id,
    fields.process_guid::String        AS process_guid,
    fields.parent_process_guid::String AS parent_guid,
    fields.image::String               AS image,
    fields.parent_image::String        AS parent_image,
    fields.commandline::String         AS commandline,
    fields.computer_name::String       AS computer_name
FROM logs
WHERE fields.bifract_category = 'process_creation' AND fields.process_guid != '';
