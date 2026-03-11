CREATE TABLE IF NOT EXISTS saved_queries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    query_text TEXT NOT NULL,
    tags TEXT[] DEFAULT '{}',
    fractal_id UUID NOT NULL REFERENCES fractals(id) ON DELETE CASCADE,
    created_by VARCHAR(50) NOT NULL REFERENCES users(username) ON DELETE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(fractal_id, name)
);

CREATE INDEX IF NOT EXISTS idx_saved_queries_fractal_id ON saved_queries(fractal_id);
CREATE INDEX IF NOT EXISTS idx_saved_queries_tags ON saved_queries USING GIN(tags);
CREATE INDEX IF NOT EXISTS idx_saved_queries_created_at ON saved_queries(created_at DESC);

DROP TRIGGER IF EXISTS update_saved_queries_updated_at ON saved_queries;
CREATE TRIGGER update_saved_queries_updated_at BEFORE UPDATE ON saved_queries
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
