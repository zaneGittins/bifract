-- Migration 004: logs_hot hot table and materialized view for alert engine scaling.
--
-- logs_hot holds the last ~2 hours of logs ordered by (fractal_id, ingest_timestamp)
-- so alert queries with UseIngestTimestamp=true get a primary-key range scan
-- instead of a full day-partition scan on the main logs table.
--
-- logs_hot_distributed is NOT created here — it requires the cluster name, which is
-- only available to the running server. Initialize() creates it at runtime.
-- K8s deployments receive this schema via Initialize()'s per-shard upgrade path on
-- restart; this file is for Docker "bifract upgrade" runs only.
--
-- All statements are idempotent (IF NOT EXISTS).

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
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS logs_hot_mv TO logs_hot AS
SELECT
    timestamp,
    raw_log,
    log_id,
    fields,
    fractal_id,
    ingest_timestamp
FROM logs;
