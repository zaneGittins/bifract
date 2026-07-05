-- Async Recall search jobs: the per-fractal Recall tab enqueues one row per
-- search and the bifract-archiver run process claims and executes it, reading
-- the fractal's Iceberg archive directly through ClickHouse iceberg*() table
-- functions (BQL translated in iceberg source mode, filtered on ingest_timestamp).
-- Claiming uses FOR UPDATE SKIP LOCKED so the N archiver sidecars never
-- double-run the same job. Results are bounded (query LIMIT) and stored as
-- compressed JSONB; a cleanup pass ages out the payload (24h) and row (14d) so
-- Postgres never accumulates archive result blobs. ClickHouse stores nothing:
-- Recall only reads Iceberg.
CREATE TABLE IF NOT EXISTS archive_search_jobs (
    id            BIGSERIAL PRIMARY KEY,
    fractal_id    TEXT NOT NULL,
    query         TEXT NOT NULL,
    from_ts       TIMESTAMPTZ NOT NULL,
    to_ts         TIMESTAMPTZ NOT NULL,
    max_rows      INTEGER NOT NULL DEFAULT 1000,
    status        TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'canceled')),
    row_count     BIGINT NOT NULL DEFAULT 0,
    is_aggregated BOOLEAN NOT NULL DEFAULT FALSE,
    limit_hit     BOOLEAN NOT NULL DEFAULT FALSE,
    field_order   JSONB,
    results       JSONB,
    error         TEXT,
    requested_by  TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Claim path: pending jobs oldest-first.
CREATE INDEX IF NOT EXISTS idx_archive_search_jobs_pending
    ON archive_search_jobs (created_at) WHERE status = 'pending';
-- Listing/reattach path: newest-first per fractal for the Recall tab.
CREATE INDEX IF NOT EXISTS idx_archive_search_jobs_fractal
    ON archive_search_jobs (fractal_id, created_at DESC);
