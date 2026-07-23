-- Restore into a dedicated fractal. A restore replays an archived window back
-- into the logs table; targeting a separate, no-retention fractal keeps the
-- restored data from being deleted again by the source fractal's retention pass,
-- and isolates it (own RBAC, own teardown via DROP PARTITION).

-- Where a restore job's rows land. NULL means "same as fractal_id" (a classic
-- self-restore); a value routes the archived rows to a different fractal.
ALTER TABLE archive_restore_jobs ADD COLUMN IF NOT EXISTS target_fractal_id TEXT;

-- Provenance for a fractal created as a restore target: which fractal's archive
-- it was restored from, and the ingest window. Display + audit only; the log
-- rows themselves only carry the new fractal_id. ON DELETE SET NULL so deleting
-- the source fractal drops the back-link rather than cascading into the
-- investigation workspace.
ALTER TABLE fractals ADD COLUMN IF NOT EXISTS restored_from_fractal_id UUID REFERENCES fractals(id) ON DELETE SET NULL;
ALTER TABLE fractals ADD COLUMN IF NOT EXISTS restored_from_ts TIMESTAMPTZ;
ALTER TABLE fractals ADD COLUMN IF NOT EXISTS restored_to_ts   TIMESTAMPTZ;
