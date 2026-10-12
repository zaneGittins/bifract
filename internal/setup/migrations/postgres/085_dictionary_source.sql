-- A lookup filled from a URL, and the outcome of its last refresh.
ALTER TABLE dictionaries ADD COLUMN IF NOT EXISTS source JSONB;
ALTER TABLE dictionaries ADD COLUMN IF NOT EXISTS source_status JSONB;
