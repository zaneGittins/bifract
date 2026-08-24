-- DEAD: stamped but never executed since the ingest-partition reset. Kept for numbering (next: 016) and TestUpgradeFromV002.
-- Raise part-count thresholds on logs_histogram. logs_histogram_mv aggregates only within
-- each incoming insert block (SummingMergeTree reconciles duplicate (fractal_id, minute)
-- keys on merge, not on insert), so every insert batch lands its own small part. Under
-- bursty ingest, part creation can outpace background merges and hit the parts_to_throw_insert
-- default of 3000, rejecting inserts into logs_distributed until merges catch up; this surfaces
-- as a distribution queue backlog that climbs and drains on its own as merges recover. The
-- table stays tiny (tens of MB), so tolerating far more parts before throttling/rejecting is
-- low-risk. Fresh installs get these settings directly from init-clickhouse.sql.
--
-- logs_histogram and its MV were added to init-clickhouse.sql without a migration, so an
-- install predating that (v0.0.2) has neither and the ALTER below would fail. Create them
-- first; both are no-ops where they already exist.
CREATE TABLE IF NOT EXISTS logs_histogram (
    fractal_id LowCardinality(String),
    minute     DateTime,
    cnt        UInt64
) ENGINE = SummingMergeTree(cnt)
ORDER BY (fractal_id, minute)
SETTINGS index_granularity = 256;

CREATE MATERIALIZED VIEW IF NOT EXISTS logs_histogram_mv TO logs_histogram
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    fractal_id,
    toStartOfMinute(timestamp) AS minute,
    count() AS cnt
FROM logs
GROUP BY fractal_id, minute;

ALTER TABLE logs_histogram MODIFY SETTING parts_to_delay_insert = 3000, parts_to_throw_insert = 10000;
