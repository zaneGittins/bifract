-- Named Normalizers: configurable transformation pipelines for log ingestion
CREATE TABLE IF NOT EXISTS normalizers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT DEFAULT '',
    transforms JSONB NOT NULL DEFAULT '[]',
    field_mappings JSONB NOT NULL DEFAULT '[]',
    is_default BOOLEAN NOT NULL DEFAULT false,
    created_by VARCHAR(50) NOT NULL REFERENCES users(username) ON DELETE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_normalizers_default_unique
    ON normalizers(is_default) WHERE is_default = true;
CREATE INDEX IF NOT EXISTS idx_normalizers_name ON normalizers(name);

DROP TRIGGER IF EXISTS update_normalizers_updated_at ON normalizers;
CREATE TRIGGER update_normalizers_updated_at BEFORE UPDATE ON normalizers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Seed the Bifract Default normalizer
INSERT INTO normalizers (name, description, transforms, field_mappings, is_default, created_by)
VALUES (
    'Bifract Default',
    'Flattens nested JSON, converts to snake_case, and lowercases all field names. Includes standard field mappings for common log sources.',
    '["flatten_leaf", "snake_case", "lowercase"]',
    '[
        {"sources": ["computer","host","@host","host_name"], "target": "computer_name"},
        {"sources": ["username","userprincipalname","target_user_name"], "target": "user"},
        {"sources": ["source_ip","orig_h","client_ip"], "target": "src_ip"},
        {"sources": ["destination_ip","resp_h","server_ip"], "target": "dst_ip"},
        {"sources": ["source_port","orig_p","client_port"], "target": "src_port"},
        {"sources": ["destination_port","resp_p","server_port"], "target": "dst_port"},
        {"sources": ["command_line"], "target": "commandline"},
        {"sources": ["parent_command_line"], "target": "parent_commandline"},
        {"sources": ["hashes"], "target": "hash"}
    ]',
    true,
    'admin'
) ON CONFLICT (name) DO NOTHING;

-- Add normalizer_id column to ingest_tokens
ALTER TABLE ingest_tokens ADD COLUMN IF NOT EXISTS normalizer_id UUID REFERENCES normalizers(id) ON DELETE SET NULL;

-- Migrate existing tokens: normalize=true -> default normalizer, normalize=false -> NULL
UPDATE ingest_tokens
SET normalizer_id = (SELECT id FROM normalizers WHERE is_default = true LIMIT 1)
WHERE normalize = true AND normalizer_id IS NULL;
