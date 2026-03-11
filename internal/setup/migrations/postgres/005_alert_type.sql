-- Add alert type column for categorizing alerts
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS alert_type VARCHAR(50) NOT NULL DEFAULT 'event';
