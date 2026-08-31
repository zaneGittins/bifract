-- Locking a notebook freezes it as the record of an investigation: no edits and
-- no re-running queries, so a reader weeks later sees the rows the author saw.
--
-- A workflow control, not tamper evidence. Unlocking is allowed and the lock is
-- an ordinary column anyone with database access can clear.
ALTER TABLE notebooks ADD COLUMN IF NOT EXISTS locked_at TIMESTAMP;
ALTER TABLE notebooks ADD COLUMN IF NOT EXISTS locked_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL;
