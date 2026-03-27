-- Add prism_id to api_keys for prism-scoped API keys
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS prism_id UUID REFERENCES prisms(id) ON DELETE CASCADE;
ALTER TABLE api_keys ALTER COLUMN fractal_id DROP NOT NULL;
CREATE INDEX IF NOT EXISTS idx_api_keys_prism_id ON api_keys(prism_id) WHERE prism_id IS NOT NULL;

-- Exactly one of fractal_id or prism_id must be set
ALTER TABLE api_keys DROP CONSTRAINT IF EXISTS api_keys_scope_check;
ALTER TABLE api_keys ADD CONSTRAINT api_keys_scope_check CHECK (
    (fractal_id IS NOT NULL AND prism_id IS NULL) OR
    (fractal_id IS NULL AND prism_id IS NOT NULL)
);
