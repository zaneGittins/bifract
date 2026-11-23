-- An actions proposal adds and removes actions across many alerts at once. It names
-- its alerts in content, so alert_id stays null.
ALTER TABLE alert_change_requests DROP CONSTRAINT IF EXISTS alert_change_requests_kind_check;
ALTER TABLE alert_change_requests ADD CONSTRAINT alert_change_requests_kind_check
    CHECK (kind IN ('create', 'update', 'delete', 'actions'));
