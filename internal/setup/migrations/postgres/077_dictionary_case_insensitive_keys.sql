-- Case-insensitive dictionary keys. When set, the ClickHouse dictionary objects are
-- built over lower(key) and match() lowercases the looked-up value, so a lookup hits
-- regardless of casing. Off keeps the existing exact-byte behaviour.
ALTER TABLE dictionaries ADD COLUMN IF NOT EXISTS case_insensitive_keys BOOLEAN NOT NULL DEFAULT false;
