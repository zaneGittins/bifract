CREATE TABLE IF NOT EXISTS archives (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fractal_id UUID NOT NULL REFERENCES fractals(id) ON DELETE CASCADE,
    filename VARCHAR(255) NOT NULL,
    storage_type VARCHAR(20) NOT NULL DEFAULT 'disk',
    storage_path TEXT NOT NULL,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    log_count BIGINT NOT NULL DEFAULT 0,
    time_range_start TIMESTAMP,
    time_range_end TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'in_progress',
    error_message TEXT,
    created_by VARCHAR(50) NOT NULL REFERENCES users(username),
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE archives ADD COLUMN IF NOT EXISTS storage_type VARCHAR(20) NOT NULL DEFAULT 'disk';
ALTER TABLE archives ADD COLUMN IF NOT EXISTS storage_path TEXT NOT NULL DEFAULT '';
ALTER TABLE archives ADD COLUMN IF NOT EXISTS size_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE archives ADD COLUMN IF NOT EXISTS log_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE archives ADD COLUMN IF NOT EXISTS time_range_start TIMESTAMP;
ALTER TABLE archives ADD COLUMN IF NOT EXISTS time_range_end TIMESTAMP;
ALTER TABLE archives ADD COLUMN IF NOT EXISTS status VARCHAR(20) NOT NULL DEFAULT 'in_progress';
ALTER TABLE archives ADD COLUMN IF NOT EXISTS error_message TEXT;

CREATE INDEX IF NOT EXISTS idx_archives_fractal_id ON archives(fractal_id);
CREATE INDEX IF NOT EXISTS idx_archives_status ON archives(status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_archives_active_operation ON archives(fractal_id) WHERE status IN ('in_progress', 'restoring');
