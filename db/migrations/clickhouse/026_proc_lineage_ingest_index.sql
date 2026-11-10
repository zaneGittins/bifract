-- Indexed proc_lineage by ingest time for the ancestry builder, removed before release. A no-op
-- drop so the numbering holds; 027 covers installs that ran the earlier version.
ALTER TABLE proc_lineage DROP INDEX IF EXISTS idx_ingest_ts;
