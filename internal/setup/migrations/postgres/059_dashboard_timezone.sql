-- IANA zone a dashboard's calendar-aligned time buckets snap to.
--
-- On the dashboard rather than the viewer because widget results are one
-- cached blob in dashboard_widgets.last_results, broadcast over SSE and served
-- to unauthenticated share links that never run BQL. A per-viewer zone cannot
-- reach that SQL. UTC keeps existing dashboards bucketing exactly as before.
ALTER TABLE dashboards ADD COLUMN IF NOT EXISTS timezone VARCHAR(64) NOT NULL DEFAULT 'UTC';
