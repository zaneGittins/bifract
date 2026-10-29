-- Per-field presence rollup for the schema tab. Definitions match db/init-clickhouse.sql.
--
-- No backfill here: the schema sweep reconciles every closed day against logs
-- (pkg/storage/field_paths.go), recounting days the view never saw at low priority in
-- bounded slices, rather than one unbounded scan of all history inside a migration.
CREATE TABLE IF NOT EXISTS logs_field_paths (
    fractal_id LowCardinality(String),
    ingest_day Date,
    path       String,
    cnt        UInt64
) ENGINE = SummingMergeTree(cnt)
PARTITION BY (fractal_id, ingest_day)
ORDER BY (fractal_id, ingest_day, path)
SETTINGS index_granularity = 256, parts_to_delay_insert = 3000, parts_to_throw_insert = 10000;

CREATE MATERIALIZED VIEW IF NOT EXISTS logs_field_paths_mv TO logs_field_paths
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    fractal_id,
    toDate(ingest_timestamp) AS ingest_day,
    path,
    count() AS cnt
FROM logs
ARRAY JOIN arrayPushFront(arrayMap(kv -> kv.1, arrayFilter(kv -> kv.1 != '' AND kv.2 != '' AND length(kv.1) <= 255, JSONExtractKeysAndValues(norm_log, 'String'))), '') AS path
GROUP BY fractal_id, ingest_day, path
ORDER BY path = '' DESC, cnt DESC
LIMIT 1024 BY fractal_id, ingest_day;
