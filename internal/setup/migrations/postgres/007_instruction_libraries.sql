-- Migration 007: Instruction Libraries
-- Replaces flat chat_instructions with hierarchical Library > Pages model.

-- Create instruction_libraries table
CREATE TABLE IF NOT EXISTS instruction_libraries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    is_default BOOLEAN NOT NULL DEFAULT false,
    fractal_id UUID REFERENCES fractals(id) ON DELETE CASCADE,
    prism_id UUID REFERENCES prisms(id) ON DELETE CASCADE,
    source VARCHAR(20) NOT NULL DEFAULT 'manual',
    repo_url TEXT NOT NULL DEFAULT '',
    branch VARCHAR(255) NOT NULL DEFAULT 'main',
    path TEXT NOT NULL DEFAULT '',
    auth_token TEXT NOT NULL DEFAULT '',
    sync_schedule VARCHAR(50) NOT NULL DEFAULT 'never',
    last_synced_at TIMESTAMP,
    last_sync_status TEXT NOT NULL DEFAULT '',
    last_sync_page_count INTEGER NOT NULL DEFAULT 0,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT il_scope CHECK (
        (fractal_id IS NOT NULL AND prism_id IS NULL) OR
        (fractal_id IS NULL AND prism_id IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_il_default_fractal ON instruction_libraries(fractal_id) WHERE is_default = true;
CREATE UNIQUE INDEX IF NOT EXISTS idx_il_default_prism ON instruction_libraries(prism_id) WHERE is_default = true;
CREATE INDEX IF NOT EXISTS idx_il_fractal ON instruction_libraries(fractal_id);
CREATE INDEX IF NOT EXISTS idx_il_prism ON instruction_libraries(prism_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_il_name_scope ON instruction_libraries(name, COALESCE(fractal_id, prism_id));

DROP TRIGGER IF EXISTS update_instruction_libraries_updated_at ON instruction_libraries;
CREATE TRIGGER update_instruction_libraries_updated_at BEFORE UPDATE ON instruction_libraries
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create instruction_pages table
CREATE TABLE IF NOT EXISTS instruction_pages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    library_id UUID NOT NULL REFERENCES instruction_libraries(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    content TEXT NOT NULL DEFAULT '',
    always_include BOOLEAN NOT NULL DEFAULT false,
    sort_order INTEGER NOT NULL DEFAULT 0,
    source_path TEXT NOT NULL DEFAULT '',
    source_hash TEXT NOT NULL DEFAULT '',
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT ip_unique_name UNIQUE (library_id, name)
);

CREATE INDEX IF NOT EXISTS idx_ip_library ON instruction_pages(library_id);

DROP TRIGGER IF EXISTS update_instruction_pages_updated_at ON instruction_pages;
CREATE TRIGGER update_instruction_pages_updated_at BEFORE UPDATE ON instruction_pages
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create conversation_libraries join table
CREATE TABLE IF NOT EXISTS conversation_libraries (
    conversation_id UUID NOT NULL REFERENCES chat_conversations(id) ON DELETE CASCADE,
    library_id UUID NOT NULL REFERENCES instruction_libraries(id) ON DELETE CASCADE,
    PRIMARY KEY (conversation_id, library_id)
);

CREATE INDEX IF NOT EXISTS idx_cl_conversation ON conversation_libraries(conversation_id);
CREATE INDEX IF NOT EXISTS idx_cl_library ON conversation_libraries(library_id);

-- Migrate existing chat_instructions to instruction_libraries + instruction_pages
INSERT INTO instruction_libraries (id, name, description, is_default, fractal_id, source, created_by, created_at, updated_at)
SELECT id, name, '', is_default, fractal_id, 'manual', created_by, created_at, updated_at
FROM chat_instructions
ON CONFLICT DO NOTHING;

INSERT INTO instruction_pages (library_id, name, description, content, always_include, created_by, created_at, updated_at)
SELECT id, name, '', content, true, created_by, created_at, updated_at
FROM chat_instructions
ON CONFLICT DO NOTHING;

-- Migrate conversation instruction associations to join table
INSERT INTO conversation_libraries (conversation_id, library_id)
SELECT id, instruction_id FROM chat_conversations WHERE instruction_id IS NOT NULL
ON CONFLICT DO NOTHING;

-- Drop the old FK column (data has been migrated to conversation_libraries)
ALTER TABLE chat_conversations DROP COLUMN IF EXISTS instruction_id;
