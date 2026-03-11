CREATE TABLE IF NOT EXISTS chat_instructions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fractal_id UUID NOT NULL REFERENCES fractals(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    is_default BOOLEAN NOT NULL DEFAULT false,
    created_by VARCHAR(50) NOT NULL REFERENCES users(username) ON DELETE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_instructions_fractal_id ON chat_instructions(fractal_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_instructions_default_unique
    ON chat_instructions(fractal_id, is_default) WHERE is_default = true;

DROP TRIGGER IF EXISTS update_chat_instructions_updated_at ON chat_instructions;
CREATE TRIGGER update_chat_instructions_updated_at BEFORE UPDATE ON chat_instructions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

ALTER TABLE chat_conversations ADD COLUMN IF NOT EXISTS instruction_id UUID REFERENCES chat_instructions(id) ON DELETE SET NULL;
