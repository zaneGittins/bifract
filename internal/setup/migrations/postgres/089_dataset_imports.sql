ALTER TABLE fractals ADD COLUMN IF NOT EXISTS default_range_start TIMESTAMPTZ;
ALTER TABLE fractals ADD COLUMN IF NOT EXISTS default_range_end   TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS dataset_imports (
    id           BIGSERIAL PRIMARY KEY,
    dataset_id   TEXT NOT NULL,
    fractal_id   UUID NOT NULL UNIQUE REFERENCES fractals(id) ON DELETE CASCADE,
    url          TEXT NOT NULL,
    sha256       TEXT NOT NULL,
    bytes_total  BIGINT NOT NULL,
    logs_total   BIGINT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'downloading', 'importing', 'succeeded', 'failed')),
    bytes_done   BIGINT NOT NULL DEFAULT 0,
    logs_done    BIGINT NOT NULL DEFAULT 0,
    error        TEXT NOT NULL DEFAULT '',
    requested_by TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dataset_imports_open
    ON dataset_imports (updated_at) WHERE status IN ('pending', 'downloading', 'importing');
