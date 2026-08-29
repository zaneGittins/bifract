-- When the thing a section describes actually happened, in UTC.
--
-- Distinct from created_at (when it was added to the notebook), which can
-- trail the event by days. Sections are ordered by order_index, so a notebook
-- can be read as a document but not as a chronology; event_time is what lets
-- the same sections be sorted into an investigation timeline.
--
-- NULL means the section has no single point in time (a note, an AI summary),
-- and those sort to the end of a chronological view rather than to 1970.
ALTER TABLE notebook_sections ADD COLUMN IF NOT EXISTS event_time TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_notebook_sections_event_time ON notebook_sections(notebook_id, event_time);
