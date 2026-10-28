-- Manual alert names are unique per fractal or prism rather than instance-wide, so
-- the same rule can be added to several, as feeds already allow.
DROP INDEX IF EXISTS idx_alerts_name_manual;
CREATE UNIQUE INDEX IF NOT EXISTS idx_alerts_name_manual_scope ON alerts (name, COALESCE(fractal_id, prism_id)) WHERE feed_id IS NULL;
