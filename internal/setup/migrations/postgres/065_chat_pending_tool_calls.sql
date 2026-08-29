-- Tool calls a chat user must approve before they run. The arguments are held
-- server-side so nothing between showing the user what will happen and running
-- it can change what runs.
CREATE TABLE IF NOT EXISTS chat_pending_tool_calls (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id UUID NOT NULL REFERENCES chat_conversations(id) ON DELETE CASCADE,
    tool_call_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    arguments JSONB NOT NULL,
    requested_by VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP NOT NULL,
    resolved_at TIMESTAMP,
    decision VARCHAR(20)
);

CREATE INDEX IF NOT EXISTS idx_chat_pending_tool_calls_open
    ON chat_pending_tool_calls(conversation_id) WHERE resolved_at IS NULL;
