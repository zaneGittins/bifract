-- Add prism support to alert_feeds
ALTER TABLE alert_feeds ADD COLUMN IF NOT EXISTS prism_id UUID REFERENCES prisms(id) ON DELETE CASCADE;
ALTER TABLE alert_feeds ALTER COLUMN fractal_id DROP NOT NULL;
ALTER TABLE alert_feeds DROP CONSTRAINT IF EXISTS alert_feeds_name_fractal_id_key;
ALTER TABLE alert_feeds DROP CONSTRAINT IF EXISTS alert_feeds_scope_check;
ALTER TABLE alert_feeds ADD CONSTRAINT alert_feeds_scope_check CHECK (
    (fractal_id IS NOT NULL AND prism_id IS NULL) OR
    (fractal_id IS NULL AND prism_id IS NOT NULL)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_alert_feeds_name_scope ON alert_feeds (name, COALESCE(fractal_id, prism_id));
CREATE INDEX IF NOT EXISTS idx_alert_feeds_prism_id ON alert_feeds(prism_id);
