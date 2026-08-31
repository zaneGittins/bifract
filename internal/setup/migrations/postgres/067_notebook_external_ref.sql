-- A link to the case this investigation belongs to in whatever system owns
-- cases here: TheHive, Jira, ServiceNow, LimaCharlie.
--
-- Deliberately a link and not a lifecycle. Bifract does not track status,
-- assignee, severity or verdict: two systems claiming to know whether something
-- is closed is worse than one, and the notebook's job is the analysis, not the
-- ticket.
ALTER TABLE notebooks ADD COLUMN IF NOT EXISTS external_ref_url   TEXT;
ALTER TABLE notebooks ADD COLUMN IF NOT EXISTS external_ref_label VARCHAR(120);
