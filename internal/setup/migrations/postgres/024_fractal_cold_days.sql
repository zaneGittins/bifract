-- Per-fractal cold-storage age threshold. Logs older than cold_days are tiered
-- to the cold object-storage volume (queryable in place). NULL = never tier.
ALTER TABLE fractals ADD COLUMN IF NOT EXISTS cold_days INTEGER DEFAULT NULL;
