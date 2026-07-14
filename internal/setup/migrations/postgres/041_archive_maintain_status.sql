-- Archive maintenance status (single row): the bifract-archiver `maintain`
-- CronJob updates this after each pass, so the admin UI can show whether
-- compaction is running, keeping pace, or falling behind without requiring
-- kubectl logs. Distinct from archive_status (the always-on archiver's
-- liveness heartbeat): this is a periodic batch job's last-run summary, a
-- different freshness signal. Server reads it; the maintain CronJob is the
-- sole writer.
CREATE TABLE IF NOT EXISTS archive_maintain_status (
    id              SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_run_at     TIMESTAMPTZ,
    duration_ms     BIGINT  NOT NULL DEFAULT 0,
    tables_seen     INTEGER NOT NULL DEFAULT 0,
    compacted       INTEGER NOT NULL DEFAULT 0,
    groups_failed   INTEGER NOT NULL DEFAULT 0,
    expired         INTEGER NOT NULL DEFAULT 0,
    candidate_bytes BIGINT  NOT NULL DEFAULT 0,
    compacted_bytes BIGINT  NOT NULL DEFAULT 0
);
INSERT INTO archive_maintain_status (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
