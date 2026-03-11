-- Alert Feeds: detection-as-code via git repo sync
CREATE TABLE IF NOT EXISTS alert_feeds (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT DEFAULT '',
    repo_url TEXT NOT NULL,
    branch VARCHAR(255) DEFAULT 'main',
    path VARCHAR(1024) DEFAULT '',
    auth_token TEXT DEFAULT '',
    normalizer_id UUID REFERENCES normalizers(id) ON DELETE SET NULL,
    sync_schedule VARCHAR(50) NOT NULL DEFAULT 'daily',
    enabled BOOLEAN DEFAULT true,
    fractal_id UUID NOT NULL REFERENCES fractals(id) ON DELETE CASCADE,
    last_synced_at TIMESTAMP,
    last_sync_status TEXT DEFAULT '',
    last_sync_rule_count INTEGER DEFAULT 0,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(name, fractal_id)
);

CREATE INDEX IF NOT EXISTS idx_alert_feeds_fractal_id ON alert_feeds(fractal_id);
CREATE INDEX IF NOT EXISTS idx_alert_feeds_enabled ON alert_feeds(enabled) WHERE enabled = true;

DROP TRIGGER IF EXISTS update_alert_feeds_updated_at ON alert_feeds;
CREATE TRIGGER update_alert_feeds_updated_at BEFORE UPDATE ON alert_feeds
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Add feed columns to alerts table
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS feed_id UUID REFERENCES alert_feeds(id) ON DELETE CASCADE;
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS feed_rule_path TEXT DEFAULT '';
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS feed_rule_hash TEXT DEFAULT '';

-- Replace the single UNIQUE(name) with scoped indexes
ALTER TABLE alerts DROP CONSTRAINT IF EXISTS alerts_name_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_alerts_name_manual ON alerts(name) WHERE feed_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_alerts_feed_rule ON alerts(feed_id, feed_rule_path) WHERE feed_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_alerts_feed_id ON alerts(feed_id) WHERE feed_id IS NOT NULL;
