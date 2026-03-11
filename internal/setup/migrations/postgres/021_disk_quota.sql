-- Add per-fractal disk quota settings
ALTER TABLE fractals ADD COLUMN IF NOT EXISTS disk_quota_bytes BIGINT DEFAULT NULL;
ALTER TABLE fractals ADD COLUMN IF NOT EXISTS disk_quota_action VARCHAR(10) DEFAULT 'reject';
