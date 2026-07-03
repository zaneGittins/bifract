-- Add norm_log: a flat, indefinitely-retained serialization of the normalized fields.
-- The log-detail lookup and BQL text search read norm_log instead of reconstructing the
-- JSON fields column (which opens ~1000+ per-path subcolumn files per part). A point-read
-- of one ZSTD String column is subsecond. Populated by ClickHouse via DEFAULT
-- toString(fields), so on existing data it is computed-on-read (correct, but slow) until
-- MATERIALIZE COLUMN physically writes it -- run that in the background at your leisure:
--   ALTER TABLE logs MATERIALIZE COLUMN norm_log;  (optionally IN PARTITION ...)
ALTER TABLE logs ADD COLUMN IF NOT EXISTS norm_log String DEFAULT toString(fields) CODEC(ZSTD(3));

-- Per-log "name@version" of the normalizer that produced fields, for traceability.
ALTER TABLE logs ADD COLUMN IF NOT EXISTS normalizer LowCardinality(String) DEFAULT '';

-- raw_log is demoted to the true pre-normalization original, kept only for a 7-day
-- troubleshooting window. WARNING on non-wiped installs: this begins reclaiming raw_log
-- for rows older than 7 days on the next merge.
ALTER TABLE logs MODIFY COLUMN raw_log String CODEC(ZSTD(3)) TTL toDateTime(timestamp) + INTERVAL 7 DAY;

-- Move the full-text n-gram index from raw_log to lower(norm_log). DROP/ADD INDEX are
-- metadata-only and instant. Existing parts are NOT indexed until MATERIALIZE INDEX runs;
-- the bifract app submits that backfill asynchronously at startup (alter_sync=0). Note:
-- until both the column and index are materialized, free-text search over pre-migration
-- data falls back to a scan. To backfill manually:
--   ALTER TABLE logs MATERIALIZE INDEX norm_log_ngram_lc;
ALTER TABLE logs DROP INDEX IF EXISTS raw_log_ngram_lc;
ALTER TABLE logs ADD INDEX IF NOT EXISTS norm_log_ngram_lc lower(norm_log) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1;
