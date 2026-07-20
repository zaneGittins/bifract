-- Chunked, resumable restore.
--
-- A restore used to run as a single unbounded INSERT ... SELECT covering the
-- whole requested window. That had no resumability (a crash or cancel mid-insert
-- left partial data and no way to continue), and it filtered on event timestamp
-- while the Iceberg archive partitions on ingest_date, so every restore scanned
-- the fractal's entire archive regardless of how narrow the window was.
--
-- Restores now execute as one chunk per ingest day. cursor_ts records the next
-- unprocessed chunk boundary so a re-run picks up where the previous attempt
-- stopped; chunks_total/chunks_done drive an exact progress bar without polling
-- ClickHouse for a row count.
--
-- Note on semantics: from_ts/to_ts are now interpreted as an INGEST-time window
-- (matching the archive's partition axis and Recall's UseIngestTimestamp), not
-- an event-time window. Existing rows are historical records of completed jobs,
-- so no backfill is needed or possible.
ALTER TABLE archive_restore_jobs ADD COLUMN IF NOT EXISTS cursor_ts    TIMESTAMPTZ;
ALTER TABLE archive_restore_jobs ADD COLUMN IF NOT EXISTS chunks_total INTEGER NOT NULL DEFAULT 0;
ALTER TABLE archive_restore_jobs ADD COLUMN IF NOT EXISTS chunks_done  INTEGER NOT NULL DEFAULT 0;
