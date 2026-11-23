-- The model a conversation was switched to with /model; NULL is the default.
ALTER TABLE chat_conversations ADD COLUMN IF NOT EXISTS model TEXT;
