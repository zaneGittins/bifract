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
    -- Character n-gram full-text index on lower(raw_log). Indexes the lowercased
    -- expression (not the column) so the translator can route case-insensitive
    -- substring/regex search to match(lower(raw_log), ...) and prune granules.
    -- The n-gram tokenizer (unlike whole-word splitByNonAlpha) accelerates
    -- arbitrary substring and regex matches. Single raw_log copy: only the index
    -- stores the lowercased form, not a duplicate column.
    INDEX raw_log_ngram_lc lower(raw_log) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
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

-- Replace the legacy word-tokenized raw_log index (splitByNonAlpha) with a
-- character n-gram index on lower(raw_log). The old index could only match whole
-- tokens, so substring/regex search ("test" matching "testing") fell back to a
-- full scan; the n-gram index prunes granules for those. DROP/ADD INDEX are
-- metadata-only and instant, so this is safe to run at startup. Existing parts are
-- NOT indexed until MATERIALIZE INDEX runs; the bifract app submits that backfill
-- asynchronously at startup (alter_sync=0) so it never blocks boot. To backfill
-- manually: ALTER TABLE logs MATERIALIZE INDEX raw_log_ngram_lc;
ALTER TABLE logs DROP INDEX IF EXISTS raw_log_inverted;
ALTER TABLE logs ADD INDEX IF NOT EXISTS raw_log_ngram_lc lower(raw_log) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1;

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

-- Hot table for the alert engine: 2-hour rolling window ordered by ingest_timestamp.
-- Alert queries with UseIngestTimestamp=true route here when cursor is < 110 min old,
-- giving a primary-key range scan instead of a full day-partition scan on logs.
-- No fractal_id in PARTITION BY: keeps active partitions to ~24 for a 2h window.
-- fractal_id leads ORDER BY for efficient per-fractal ingest_timestamp range scans.
-- No skip indexes: ORDER BY covers the alert query pattern; indexes on 2h data waste writes.
-- TTL is a safety net only; the StartHotTableCleaner goroutine is the primary mechanism.
CREATE TABLE IF NOT EXISTS logs_hot (
    timestamp        DateTime64(3),
    raw_log          String CODEC(ZSTD(3)),
    log_id           String,
    fields           JSON(
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
    fractal_id       LowCardinality(String) DEFAULT '',
    ingest_timestamp DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
PARTITION BY toStartOfFiveMinutes(ingest_timestamp)
ORDER BY (fractal_id, ingest_timestamp, log_id)
TTL toDateTime(ingest_timestamp) + INTERVAL 4 HOUR DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

-- Feeds logs_hot from every insert into the local logs table.
-- The MV writes to local logs_hot on each shard — it fires per-shard when distributed
-- writes land on local logs, so the distributed layer is never in the write path.
CREATE MATERIALIZED VIEW IF NOT EXISTS logs_hot_mv TO logs_hot AS
SELECT
    timestamp,
    raw_log,
    log_id,
    fields,
    fractal_id,
    ingest_timestamp
FROM logs;
