-- Create database if not exists
CREATE DATABASE IF NOT EXISTS logs;

-- Use the logs database
USE logs;

-- Create the main logs table with fractal isolation support
CREATE TABLE IF NOT EXISTS logs (
    timestamp DateTime64(3),
    raw_log String CODEC(ZSTD(3)),
    log_id String,
    fields JSON(
        max_dynamic_paths=1024,
        `computer_name`      String,
        `user`               String,
        `src_ip`             String,
        `dst_ip`             String,
        `src_port`           String,
        `dst_port`           String,
        `commandline`        String,
        `hash`               String,
        `event_id`           String,
        `image`              String,
        `parent_image`       String,
        `call_chain`         String,
        `operation`          String,
        `artifact`           String,
        `query`              String,
        `original_file_name` String
    ),
    fractal_id LowCardinality(String) DEFAULT '',
    ingest_timestamp DateTime64(3) DEFAULT now64(3),
    INDEX raw_log_inverted raw_log TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(raw_log)),
    INDEX log_id_bloom log_id TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX ingest_ts_minmax ingest_timestamp TYPE minmax GRANULARITY 1,
    -- Skip indexes on normalized fields. Defined inline so all new parts are indexed
    -- on insert without requiring MATERIALIZE INDEX. Direct sub-column references
    -- (no CAST) are required — ClickHouse's skip index optimizer does not match
    -- CAST/function expressions against bloom filter or set indexes.
    INDEX idx_src_ip             fields.src_ip             TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_dst_ip             fields.dst_ip             TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_computer_name      fields.computer_name      TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_user               fields.user               TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_hash               fields.hash               TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_image              fields.image              TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_parent_image       fields.parent_image       TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_original_file_name fields.original_file_name TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_query              fields.query              TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_event_id           fields.event_id           TYPE set(256)           GRANULARITY 1,
    INDEX idx_operation          fields.operation          TYPE set(256)            GRANULARITY 1,
    INDEX idx_artifact           fields.artifact           TYPE set(64)            GRANULARITY 1,
    INDEX idx_src_port           fields.src_port           TYPE set(4096)          GRANULARITY 1,
    INDEX idx_dst_port           fields.dst_port           TYPE set(4096)          GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY (fractal_id, toDate(timestamp))
ORDER BY (timestamp, log_id)
SETTINGS index_granularity = 8192;

-- Defensive: idempotent ADD COLUMN / ADD INDEX for existing installs that predate
-- inline definitions. IF NOT EXISTS means these are safe no-ops on fresh installs.
ALTER TABLE logs ADD INDEX IF NOT EXISTS ingest_ts_minmax       ingest_timestamp      TYPE minmax           GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_src_ip             fields.src_ip         TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_dst_ip             fields.dst_ip         TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_computer_name      fields.computer_name  TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_user               fields.user           TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_hash               fields.hash           TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_image              fields.image          TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_parent_image       fields.parent_image   TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_original_file_name fields.original_file_name TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_query              fields.query          TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_event_id           fields.event_id       TYPE set(256)           GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_operation          fields.operation      TYPE set(256)            GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_artifact           fields.artifact       TYPE set(64)            GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_src_port           fields.src_port       TYPE set(4096)          GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_dst_port           fields.dst_port       TYPE set(4096)          GRANULARITY 1;

-- Retire the deprecated field_tokens column and its text index. Equality now resolves against
-- the JSON sub-column directly (see pkg/parser), so field_tokens is no longer written or queried.
-- These DROPs live here, not only in a numbered migration, because the server applies this file
-- on every boot but does NOT run the bifract-setup migrations, so this is the only path that
-- reaches server/k8s installs. IF EXISTS makes them idempotent no-ops on fresh installs and on
-- every later boot. On a cluster, Initialize propagates each ALTER to every shard (ON CLUSTER or
-- per-shard) so all shards converge. Inserts stay safe meanwhile because the column had a
-- DEFAULT and is no longer written. DROP INDEX must precede DROP COLUMN. DROP COLUMN schedules a
-- background mutation to reclaim disk.
ALTER TABLE logs DROP INDEX IF EXISTS field_tokens_text;
ALTER TABLE logs DROP COLUMN IF EXISTS field_tokens;
ALTER TABLE logs_distributed DROP COLUMN IF EXISTS field_tokens;

-- Pre-aggregated per-minute counts per fractal for fast landing-page histograms.
-- Querying this instead of raw logs reduces the recent-logs histogram from a
-- 200M-row scan to ~1440 rows for a 24-hour window.
CREATE TABLE IF NOT EXISTS logs_histogram (
    fractal_id LowCardinality(String),
    minute     DateTime,
    cnt        UInt64
) ENGINE = SummingMergeTree(cnt)
ORDER BY (fractal_id, minute)
SETTINGS index_granularity = 256;

-- Feeds logs_histogram from every insert into the local logs table.
-- The MV writes to the local logs_histogram. The distributed table handles cross-shard reads.
CREATE MATERIALIZED VIEW IF NOT EXISTS logs_histogram_mv TO logs_histogram AS
SELECT
    fractal_id,
    toStartOfMinute(timestamp) AS minute,
    count() AS cnt
FROM logs
GROUP BY fractal_id, minute;
