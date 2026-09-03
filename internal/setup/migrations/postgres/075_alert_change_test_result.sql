-- The last test run of a proposal, kept so reloading the review does not forget it.
-- test_result_hash records the content and tests it ran against; a mismatch means stale.
ALTER TABLE alert_change_requests ADD COLUMN IF NOT EXISTS test_result JSONB;
ALTER TABLE alert_change_requests ADD COLUMN IF NOT EXISTS test_result_hash CHAR(64) NOT NULL DEFAULT '';
