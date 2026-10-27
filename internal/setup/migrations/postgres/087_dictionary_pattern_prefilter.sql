-- Whether ClickHouse compiled a pattern list's expressions for multiMatchAny, and
-- a counter every write bumps so a probe cannot vouch for rows it did not see.
ALTER TABLE dictionaries ADD COLUMN IF NOT EXISTS pattern_prefilter BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE dictionaries ADD COLUMN IF NOT EXISTS pattern_rev BIGINT NOT NULL DEFAULT 0;
