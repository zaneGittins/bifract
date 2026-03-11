-- Add references column for alert context links (URLs, documentation, etc.)
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS "references" TEXT[] DEFAULT '{}';
