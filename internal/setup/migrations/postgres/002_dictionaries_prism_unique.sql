-- Ensure prism-scoped dictionary names are unique per prism.
CREATE UNIQUE INDEX IF NOT EXISTS idx_dictionaries_prism_name ON dictionaries(prism_id, name) WHERE prism_id IS NOT NULL;
