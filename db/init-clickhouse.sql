-- Create database if not exists
CREATE DATABASE IF NOT EXISTS logs;

-- Use the logs database
USE logs;

-- Create the main logs table with fractal isolation support
CREATE TABLE IF NOT EXISTS logs (
    timestamp DateTime64(3),
    raw_log String CODEC(ZSTD(3)),
    log_id String,
    fields JSON(max_dynamic_paths=1024),
    fractal_id LowCardinality(String) DEFAULT '',
    ingest_timestamp DateTime64(3) DEFAULT now64(3),
    -- Inverted index: lower() preprocessor enables hasToken() to auto-lower search terms,
    -- providing index-accelerated granule pruning for both case-sensitive and
    -- case-insensitive regex queries via hasToken pre-filters.
    INDEX raw_log_inverted raw_log TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(raw_log)),
    INDEX log_id_bloom log_id TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX ingest_ts_minmax ingest_timestamp TYPE minmax GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY toDate(timestamp)
ORDER BY (fractal_id, timestamp, log_id)
SETTINGS index_granularity = 8192;

-- Defensive: add ingest_timestamp minmax index for existing installs.
-- Alert queries filter on ingest_timestamp which is not in the primary key;
-- without this index ClickHouse scans every granule in the table.
ALTER TABLE logs ADD INDEX IF NOT EXISTS ingest_ts_minmax ingest_timestamp TYPE minmax GRANULARITY 1;

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
-- The MV writes to the local logs_histogram; the distributed table handles cross-shard reads.
CREATE MATERIALIZED VIEW IF NOT EXISTS logs_histogram_mv TO logs_histogram AS
SELECT
    fractal_id,
    toStartOfMinute(timestamp) AS minute,
    count() AS cnt
FROM logs
GROUP BY fractal_id, minute;
