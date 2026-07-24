-- Lifecycle counters for the maintain pass: expired archive partitions dropped
-- (retention) and unreferenced files reclaimed (orphan cleanup), so the admin UI
-- can show storage being returned rather than only compaction throughput.
ALTER TABLE archive_maintain_status ADD COLUMN IF NOT EXISTS retention_tables INTEGER NOT NULL DEFAULT 0;
ALTER TABLE archive_maintain_status ADD COLUMN IF NOT EXISTS retention_files  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE archive_maintain_status ADD COLUMN IF NOT EXISTS orphans_deleted  INTEGER NOT NULL DEFAULT 0;

ALTER TABLE archive_maintain_history ADD COLUMN IF NOT EXISTS retention_tables INTEGER NOT NULL DEFAULT 0;
ALTER TABLE archive_maintain_history ADD COLUMN IF NOT EXISTS retention_files  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE archive_maintain_history ADD COLUMN IF NOT EXISTS orphans_deleted  INTEGER NOT NULL DEFAULT 0;

-- Orphan cleanup lists every file under each table location, so it runs on its
-- own (much longer) cadence rather than once per maintain pass. This is the
-- claim stamp that keeps it to one sweep per interval across restarts.
ALTER TABLE archive_maintain_status ADD COLUMN IF NOT EXISTS last_orphan_sweep_at TIMESTAMPTZ;
