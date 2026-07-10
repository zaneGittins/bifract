-- Persist the query time range with saved queries, and the relative range
-- components with query history, so re-running either restores the exact range
-- (absolute or relative) it was last run with.
--
-- saved_queries had no time range at all; query_history stored time_range/
-- custom_start/custom_end but not the relative n + unit, so a relative range
-- could not round-trip.
ALTER TABLE saved_queries ADD COLUMN IF NOT EXISTS time_range    VARCHAR(32);
ALTER TABLE saved_queries ADD COLUMN IF NOT EXISTS custom_start  TIMESTAMP;
ALTER TABLE saved_queries ADD COLUMN IF NOT EXISTS custom_end    TIMESTAMP;
ALTER TABLE saved_queries ADD COLUMN IF NOT EXISTS relative_n    INTEGER;
ALTER TABLE saved_queries ADD COLUMN IF NOT EXISTS relative_unit VARCHAR(16);

ALTER TABLE query_history ADD COLUMN IF NOT EXISTS relative_n    INTEGER;
ALTER TABLE query_history ADD COLUMN IF NOT EXISTS relative_unit VARCHAR(16);
