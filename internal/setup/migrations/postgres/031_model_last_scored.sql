-- Scheduled (network) analytics models (beacon / long_connection) are scored by a
-- background engine on a per-model cadence. last_scored_at is that cursor: the
-- scorer skips a model until now() - last_scored_at exceeds its rescore interval.
-- Nullable so a never-scored model is due immediately. Additive and non-destructive.
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS last_scored_at TIMESTAMPTZ;
