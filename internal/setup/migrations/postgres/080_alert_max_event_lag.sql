-- Ignore matches whose event time trails their ingest time by more than this many
-- seconds (0 = off). Alerts evaluate on ingest_timestamp, so a reconnected agent
-- flushing a week of buffered events would otherwise alert on all of it.
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS max_event_lag_seconds INTEGER NOT NULL DEFAULT 0;
