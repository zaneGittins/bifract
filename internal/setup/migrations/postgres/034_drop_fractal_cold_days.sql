-- Native ClickHouse cold tiering has been replaced by the Iceberg archive, so
-- the per-fractal cold_days threshold is obsolete. Drop the column. Safe: no Go
-- code references it, and the hot-window bound is now retention_days alone.
ALTER TABLE fractals DROP COLUMN IF EXISTS cold_days;
