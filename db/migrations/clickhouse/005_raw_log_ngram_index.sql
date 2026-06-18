-- Replace the legacy word-tokenized raw_log full-text index (splitByNonAlpha) with
-- a character n-gram text index on lower(raw_log). The n-gram tokenizer accelerates
-- case-insensitive substring and regex search, which the query translator routes to
-- match(lower(raw_log), ...). The old whole-word index could not prune granules for
-- substring/regex matches (e.g. "test" inside "testing"), forcing full scans.
--
-- DROP/ADD INDEX are metadata-only and instant. Existing parts are NOT indexed until
-- MATERIALIZE INDEX runs; the bifract app submits that backfill asynchronously at
-- startup (guarded by a Postgres advisory lock, alter_sync=0) so it never blocks
-- boot. To backfill manually instead:
--   ALTER TABLE logs MATERIALIZE INDEX raw_log_ngram_lc;
ALTER TABLE logs DROP INDEX IF EXISTS raw_log_inverted;
ALTER TABLE logs ADD INDEX IF NOT EXISTS raw_log_ngram_lc lower(raw_log) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1;
