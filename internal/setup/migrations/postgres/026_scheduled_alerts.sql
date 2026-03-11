-- Add columns for scheduled alert type (cron-based evaluation with configurable query window)
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS schedule_cron VARCHAR(100) DEFAULT NULL;
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS query_window_seconds INTEGER DEFAULT NULL;
