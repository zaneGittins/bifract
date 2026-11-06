ALTER TABLE dataset_imports ADD COLUMN IF NOT EXISTS warning TEXT NOT NULL DEFAULT '';

ALTER TABLE dataset_imports DROP CONSTRAINT IF EXISTS dataset_imports_status_check;
ALTER TABLE dataset_imports ADD CONSTRAINT dataset_imports_status_check
    CHECK (status IN ('held', 'pending', 'downloading', 'importing', 'scoring', 'succeeded', 'failed'));

DROP INDEX IF EXISTS idx_dataset_imports_open;
CREATE INDEX IF NOT EXISTS idx_dataset_imports_open
    ON dataset_imports (updated_at) WHERE status IN ('pending', 'downloading', 'importing', 'scoring');
