-- Add window_duration column for compound alerts (tumbling window evaluation)
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS window_duration INTEGER DEFAULT NULL;
