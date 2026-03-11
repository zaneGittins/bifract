ALTER TABLE dictionaries ADD COLUMN IF NOT EXISTS is_global BOOLEAN NOT NULL DEFAULT false;
CREATE INDEX IF NOT EXISTS idx_dictionaries_global ON dictionaries(is_global) WHERE is_global = true;
