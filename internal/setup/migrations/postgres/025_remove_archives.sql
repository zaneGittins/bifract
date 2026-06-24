-- Remove the archive export/restore subsystem, superseded by ClickHouse cold
-- storage tiering (data stays queryable in place; no export/restore round-trip).
DROP TABLE IF EXISTS archives;
DROP TABLE IF EXISTS archive_groups;

ALTER TABLE fractals DROP COLUMN IF EXISTS archive_schedule;
ALTER TABLE fractals DROP COLUMN IF EXISTS max_archives;
ALTER TABLE fractals DROP COLUMN IF EXISTS archive_split;
