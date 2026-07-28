-- Raise part-count thresholds on logs_histogram. logs_histogram_mv aggregates only within
-- each incoming insert block (SummingMergeTree reconciles duplicate (fractal_id, minute)
-- keys on merge, not on insert), so every insert batch lands its own small part. Under
-- bursty ingest, part creation can outpace background merges and hit the parts_to_throw_insert
-- default of 3000, rejecting inserts into logs_distributed until merges catch up; this surfaces
-- as a distribution queue backlog that climbs and drains on its own as merges recover. The
-- table stays tiny (tens of MB), so tolerating far more parts before throttling/rejecting is
-- low-risk. Fresh installs get these settings directly from init-clickhouse.sql.
ALTER TABLE logs_histogram MODIFY SETTING parts_to_delay_insert = 3000, parts_to_throw_insert = 10000;
