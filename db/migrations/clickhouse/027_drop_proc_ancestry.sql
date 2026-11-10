-- The ancestry filters and their builder were removed (see 024). Drops what earlier builds of
-- 024 created; on any other install every statement is a no-op.
DROP TABLE IF EXISTS proc_ancestry_distributed SYNC;
DROP TABLE IF EXISTS proc_ancestry_watermark_distributed SYNC;
DROP TABLE IF EXISTS proc_ancestry SYNC;
DROP TABLE IF EXISTS proc_ancestry_watermark SYNC;
ALTER TABLE proc_lineage DROP INDEX IF EXISTS idx_ingest_ts;
