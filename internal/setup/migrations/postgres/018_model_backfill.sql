-- One-time historical backfill state for analytics models.
-- The live materialized view is forward-only; these columns track a bounded,
-- chunked backfill that seeds a model from historical logs.
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS backfill_status     VARCHAR(20) NOT NULL DEFAULT 'none'; -- none|running|completed|failed|cancelled
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS backfill_window     VARCHAR(10) NOT NULL DEFAULT '';      -- 24h|7d|30d|90d
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS backfill_total      INT         NOT NULL DEFAULT 0;       -- total day-chunks
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS backfill_done       INT         NOT NULL DEFAULT 0;       -- completed day-chunks
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS backfill_anchor     TIMESTAMP;                            -- ingest_timestamp dedup boundary vs the MV
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS backfill_started_at TIMESTAMP;
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS backfill_error      TEXT        NOT NULL DEFAULT '';
