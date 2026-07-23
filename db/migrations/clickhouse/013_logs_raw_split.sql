-- Split raw_log out of the logs table into its own daily-partitioned logs_raw table.
-- The old design kept raw_log as a column with a 7-day column TTL; a column TTL cannot
-- drop whole parts, so every part crossing the 7-day boundary was rewritten to purge the
-- largest column, consuming 10-20% of all merge work at scale. logs_raw uses a whole-row
-- TTL with ttl_only_drop_parts, so expiry is a metadata-only DROP PARTITION with zero
-- merge rewrites, and the main logs merges get cheaper for no longer carrying the column.
--
-- No backfill: the pre-existing 7-day raw_log window is not copied into logs_raw (it could
-- be enormous and would block the migration). The troubleshooting window repopulates from
-- new ingest; the Iceberg archive retains the historical raw_log copy where enabled.
CREATE TABLE IF NOT EXISTS logs_raw (
    timestamp   DateTime64(3),
    fractal_id  LowCardinality(String) DEFAULT '',
    log_id      String,
    raw_log     String CODEC(ZSTD(3)),
    INDEX log_id_bloom log_id TYPE bloom_filter(0.001) GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY (fractal_id, toDate(timestamp))
ORDER BY (timestamp, log_id)
TTL toDateTime(timestamp) + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

-- Stop the hot-table MV projecting raw_log BEFORE dropping the column from logs, or the
-- MV would reference a missing column and fail every insert. MODIFY QUERY edits in place;
-- DROP + re-CREATE would drop inserts landing in the gap (silently missed alerts).
ALTER TABLE logs_hot_mv MODIFY QUERY
SELECT
    timestamp,
    log_id,
    fields,
    fractal_id,
    ingest_timestamp,
    norm_log,
    normalizer
FROM logs;

-- Reclaim the column. DROP COLUMN is a metadata change plus a mutation that deletes only
-- this column's files per part; it does not rewrite the other columns.
ALTER TABLE logs DROP COLUMN IF EXISTS raw_log;
ALTER TABLE logs_hot DROP COLUMN IF EXISTS raw_log;
