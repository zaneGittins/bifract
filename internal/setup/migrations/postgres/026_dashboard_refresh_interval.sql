-- Add server-side auto-refresh cadence to dashboards.
-- Seconds: 0 = off/manual (default), -1 = auto (executor derives from time range), >0 = fixed interval.
ALTER TABLE dashboards ADD COLUMN IF NOT EXISTS refresh_interval INTEGER NOT NULL DEFAULT 0;
