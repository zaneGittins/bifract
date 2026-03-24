CREATE TABLE IF NOT EXISTS archive_groups (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fractal_id UUID NOT NULL REFERENCES fractals(id) ON DELETE CASCADE,
    split_granularity VARCHAR(10) NOT NULL DEFAULT 'none',
    status VARCHAR(20) NOT NULL DEFAULT 'in_progress',
    error_message TEXT,
    total_log_count BIGINT NOT NULL DEFAULT 0,
    total_size_bytes BIGINT NOT NULL DEFAULT 0,
    archive_count INTEGER NOT NULL DEFAULT 0,
    completed_count INTEGER NOT NULL DEFAULT 0,
    archive_type VARCHAR(20) NOT NULL DEFAULT 'adhoc',
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_archive_groups_fractal_id ON archive_groups(fractal_id);

ALTER TABLE archives ADD COLUMN IF NOT EXISTS group_id UUID REFERENCES archive_groups(id) ON DELETE CASCADE;
ALTER TABLE archives ADD COLUMN IF NOT EXISTS period_label VARCHAR(30);
CREATE INDEX IF NOT EXISTS idx_archives_group_id ON archives(group_id);

ALTER TABLE fractals ADD COLUMN IF NOT EXISTS archive_split VARCHAR(10) NOT NULL DEFAULT 'none';
