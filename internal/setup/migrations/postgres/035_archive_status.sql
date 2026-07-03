-- Archive heartbeat/status: a single row the bifract-archiver updates so the
-- admin UI can show last-commit time, whether the sidecar is alive, the archived
-- fractal count, and the total object-storage footprint. Server reads it; the
-- archiver is the sole writer. Empty/zero until archiving is enabled.
CREATE TABLE IF NOT EXISTS archive_status (
    id             SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_commit_at TIMESTAMPTZ,
    fractal_count  INTEGER  NOT NULL DEFAULT 0,
    total_bytes    BIGINT   NOT NULL DEFAULT 0,
    total_records  BIGINT   NOT NULL DEFAULT 0
);
INSERT INTO archive_status (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
