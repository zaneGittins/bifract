-- Async restore jobs: the admin UI (System -> Archive) enqueues one row per
-- (fractal, window) and the bifract-archiver run process claims and executes it,
-- streaming progress back. A restore/reconcile replays Iceberg data into the
-- ClickHouse logs table. Claiming uses FOR UPDATE SKIP LOCKED so the N archiver
-- sidecars never double-run the same job.
CREATE TABLE IF NOT EXISTS archive_restore_jobs (
    id            BIGSERIAL PRIMARY KEY,
    batch_id      UUID NOT NULL,
    fractal_id    TEXT NOT NULL,
    mode          TEXT NOT NULL DEFAULT 'restore' CHECK (mode IN ('restore', 'reconcile')),
    from_ts       TIMESTAMPTZ NOT NULL,
    to_ts         TIMESTAMPTZ NOT NULL,
    dedup         BOOLEAN NOT NULL DEFAULT TRUE,
    status        TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'canceled')),
    target_rows   BIGINT NOT NULL DEFAULT 0,
    rows_restored BIGINT NOT NULL DEFAULT 0,
    error         TEXT,
    requested_by  TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Claim path: pending jobs oldest-first.
CREATE INDEX IF NOT EXISTS idx_archive_restore_jobs_pending
    ON archive_restore_jobs (created_at) WHERE status = 'pending';
-- Listing path: newest-first for the UI.
CREATE INDEX IF NOT EXISTS idx_archive_restore_jobs_created
    ON archive_restore_jobs (created_at DESC);
