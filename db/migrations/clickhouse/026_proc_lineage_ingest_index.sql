-- 024 added idx_ingest_ts, which covers only parts written after it, so the ancestry builder's
-- ingest-time filters read every older part in full. Indexes them; a background mutation.
ALTER TABLE proc_lineage MATERIALIZE INDEX idx_ingest_ts;
