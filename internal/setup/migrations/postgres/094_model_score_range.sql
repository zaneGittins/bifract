-- A network model's Score range runs as a background job. queued_at identifies a
-- run: its progress and result writes match on it, so a cancelled or replaced
-- run writes nothing more.
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS score_range_status    VARCHAR(20) NOT NULL DEFAULT 'none';
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS score_range_from      TIMESTAMPTZ;
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS score_range_to        TIMESTAMPTZ;
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS score_range_queued_at TIMESTAMPTZ;
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS score_range_phase     VARCHAR(20) NOT NULL DEFAULT '';
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS score_range_read      BIGINT      NOT NULL DEFAULT 0;
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS score_range_total     BIGINT      NOT NULL DEFAULT 0;
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS score_range_result    JSONB;
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS score_range_error     TEXT        NOT NULL DEFAULT '';
