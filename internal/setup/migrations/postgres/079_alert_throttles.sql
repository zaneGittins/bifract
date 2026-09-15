-- Open throttle windows, one row per (alert, suppression key). The engine used to
-- hold these in process memory, which released every window on restart and was
-- invisible to whichever replica held the evaluation lock next.
CREATE TABLE IF NOT EXISTS alert_throttles (
    alert_id     UUID NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
    throttle_key VARCHAR(255) NOT NULL,
    expires_at   TIMESTAMP NOT NULL,
    PRIMARY KEY (alert_id, throttle_key)
);
CREATE INDEX IF NOT EXISTS idx_alert_throttles_expires ON alert_throttles(expires_at);

-- A throttle now suppresses per key instead of per batch, so an execution can be
-- partly delivered: log_count stays every row matched, suppressed_count is how
-- many of those were withheld.
ALTER TABLE alert_executions ADD COLUMN IF NOT EXISTS suppressed_count INTEGER NOT NULL DEFAULT 0;
