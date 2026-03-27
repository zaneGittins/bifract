-- Add prism support to comments and chat_conversations
ALTER TABLE comments ADD COLUMN IF NOT EXISTS prism_id UUID REFERENCES prisms(id) ON DELETE CASCADE;
ALTER TABLE comments ALTER COLUMN fractal_id DROP NOT NULL;
CREATE INDEX IF NOT EXISTS idx_comments_prism_id ON comments(prism_id) WHERE prism_id IS NOT NULL;

ALTER TABLE chat_conversations ADD COLUMN IF NOT EXISTS prism_id UUID REFERENCES prisms(id) ON DELETE CASCADE;
ALTER TABLE chat_conversations ALTER COLUMN fractal_id DROP NOT NULL;
CREATE INDEX IF NOT EXISTS idx_chat_conversations_prism_id ON chat_conversations(prism_id) WHERE prism_id IS NOT NULL;

-- Comments: prism_id controls visibility scope; fractal_id tracks which fractal the log lives in.
-- Both can be set (prism comment on a log from a member fractal).
ALTER TABLE comments DROP CONSTRAINT IF EXISTS comments_scope_check;
ALTER TABLE comments ADD CONSTRAINT comments_scope_check CHECK (
    fractal_id IS NOT NULL OR prism_id IS NOT NULL
);

-- Chat conversations: exactly one of fractal_id/prism_id must be set.
ALTER TABLE chat_conversations DROP CONSTRAINT IF EXISTS chat_conversations_scope_check;
ALTER TABLE chat_conversations ADD CONSTRAINT chat_conversations_scope_check CHECK (
    (fractal_id IS NOT NULL AND prism_id IS NULL) OR
    (fractal_id IS NULL AND prism_id IS NOT NULL)
);
