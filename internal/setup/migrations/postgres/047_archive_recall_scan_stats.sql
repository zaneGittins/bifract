-- Recall scan cost: what ClickHouse actually read from object storage to answer
-- a search. Without it a timed-out Recall is indistinguishable from one that was
-- simply too broad, and there is no way to tell a well-pruned query from a full
-- window scan.
ALTER TABLE archive_search_jobs ADD COLUMN IF NOT EXISTS read_rows  BIGINT NOT NULL DEFAULT 0;
ALTER TABLE archive_search_jobs ADD COLUMN IF NOT EXISTS read_bytes BIGINT NOT NULL DEFAULT 0;
