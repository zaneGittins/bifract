-- IANA zone a notebook's calendar-aligned time buckets snap to.
--
-- On the notebook rather than the viewer for the same reason as dashboards
-- (059): a query section's results are one cached blob in
-- notebook_sections.last_results, broadcast over SSE to everyone reading the
-- notebook. Bucketing in whoever last pressed play's personal zone made the
-- same shared investigation show different day boundaries per reader.
-- UTC keeps existing notebooks bucketing exactly as before.
ALTER TABLE notebooks ADD COLUMN IF NOT EXISTS timezone VARCHAR(64) NOT NULL DEFAULT 'UTC';
