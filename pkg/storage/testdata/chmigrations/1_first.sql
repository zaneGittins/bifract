-- Synthetic migration for the runner tests.
CREATE TABLE IF NOT EXISTS t1 (x UInt8) ENGINE = MergeTree() ORDER BY x;

-- A statement the runner must skip: it is neither DDL nor a write.
SELECT 1;

ALTER TABLE t1 ADD COLUMN IF NOT EXISTS y String;
