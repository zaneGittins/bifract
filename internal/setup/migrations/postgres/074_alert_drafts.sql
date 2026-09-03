-- Drafts: a proposal that has not been submitted yet.
--
-- An alert takes real time to write (a query iterated for an hour, a test corpus
-- assembled one event at a time), and until now all of it lived in the browser until
-- Save. A draft is the same row a proposal uses, with status 'draft', visible only to
-- its author. The review gate decides what finishing a draft means: apply it, or open
-- it for review.
ALTER TABLE alert_change_requests DROP CONSTRAINT IF EXISTS alert_change_requests_status_check;
ALTER TABLE alert_change_requests ADD CONSTRAINT alert_change_requests_status_check
    CHECK (status IN ('draft', 'open', 'changes_requested', 'merged', 'discarded'));

-- One draft per author per alert; a new alert's draft has no alert yet, so it is
-- addressed by its own id instead.
CREATE UNIQUE INDEX IF NOT EXISTS idx_alert_cr_draft_per_alert
    ON alert_change_requests(created_by, alert_id) WHERE status = 'draft' AND alert_id IS NOT NULL;
