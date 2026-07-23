-- Archive completeness: per (fractal, ingest day) row counts in the hot store vs
-- the Iceberg archive, so a gap is visible instead of silent.
--
-- The archive is fed by a pod-local spool. If an ingest pod is replaced while it
-- still holds un-archived data (rolling update, eviction, node failure) that data
-- reaches ClickHouse but never reaches Iceberg, and nothing today notices: the
-- reconcile path only heals the hot store FROM the archive, never the reverse.
-- This table is the detector. ch_count > ice_count on a sealed day is a gap.
CREATE TABLE IF NOT EXISTS archive_completeness (
    fractal_id  TEXT NOT NULL,
    ingest_day  DATE NOT NULL,
    ch_count    BIGINT NOT NULL DEFAULT 0,
    ice_count   BIGINT NOT NULL DEFAULT 0,
    checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (fractal_id, ingest_day)
);
CREATE INDEX IF NOT EXISTS idx_archive_completeness_day
    ON archive_completeness (ingest_day DESC);
-- Partial index over gap rows only: the admin panel's default view and any
-- alerting both ask "is anything missing", which stays cheap as history grows.
CREATE INDEX IF NOT EXISTS idx_archive_completeness_gaps
    ON archive_completeness (ingest_day DESC) WHERE ch_count > ice_count;
