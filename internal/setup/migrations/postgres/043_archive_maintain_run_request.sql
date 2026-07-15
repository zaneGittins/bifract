-- "Run now" for archive maintenance: the admin UI records an on-demand request
-- on the single archive_maintain_status row, and the always-on maintain-loop
-- (Docker container / k8s replicas:1 Deployment) atomically claims and clears
-- it on its next short poll, then runs a compaction+expiry pass out of the
-- normal schedule. NULL run_requested_at = no pending request; run_requested_by
-- records the requesting admin for the audit trail and UI.
ALTER TABLE archive_maintain_status
    ADD COLUMN IF NOT EXISTS run_requested_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS run_requested_by TEXT;
