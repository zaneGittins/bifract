-- DEAD: stamped but never executed since the ingest-partition reset. Kept for numbering (next: 016) and TestUpgradeFromV002.
-- Extend process-provenance retention to 730 days. The original defaults (proc_lineage 365d
-- in migration 007, proc_freq 180d in migration 008) are too short for DFIR / CTF datasets
-- whose events can be a year or more old: on ingest, a row whose timestamp + old-TTL is
-- already in the past is dropped by the next merge, so ptg()/pgr() never see it.
--
-- materialize_ttl_after_modify = 0 makes this a metadata-only change: it does NOT launch a
-- part-rewriting mutation across the (potentially large) proc_lineage table. New and merged
-- parts adopt 730d; existing parts keep their prior TTL until they merge. That is the right
-- trade-off here since the extended window only ever RETAINS more data, and re-ingested data
-- lands in fresh parts with the new TTL. Fresh installs get 730d directly from
-- init-clickhouse.sql. Operators can still override at startup via
-- BIFRACT_PROC_LINEAGE_TTL_DAYS / BIFRACT_PROC_FREQ_TTL_DAYS.
ALTER TABLE proc_lineage MODIFY TTL toDateTime(timestamp) + INTERVAL 730 DAY SETTINGS materialize_ttl_after_modify = 0;
ALTER TABLE proc_freq MODIFY TTL day + INTERVAL 730 DAY SETTINGS materialize_ttl_after_modify = 0;
