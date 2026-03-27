-- Add prism support to saved_queries (mirrors alerts/notebooks/dashboards/dictionaries pattern)
ALTER TABLE saved_queries ADD COLUMN IF NOT EXISTS prism_id UUID REFERENCES prisms(id) ON DELETE CASCADE;
ALTER TABLE saved_queries ALTER COLUMN fractal_id DROP NOT NULL;
CREATE INDEX IF NOT EXISTS idx_saved_queries_prism_id ON saved_queries(prism_id) WHERE prism_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_saved_queries_prism_name ON saved_queries(prism_id, name) WHERE prism_id IS NOT NULL;
