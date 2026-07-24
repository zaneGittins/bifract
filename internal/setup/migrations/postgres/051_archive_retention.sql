-- Per-fractal Iceberg archive retention. NULL (the default, including for every
-- existing fractal) means keep forever, so an upgrade never starts expiring
-- archived data on its own; an admin opts in per fractal.
ALTER TABLE fractals ADD COLUMN IF NOT EXISTS archive_retention_days INTEGER DEFAULT NULL;
