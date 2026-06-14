-- Collaboration features for saved queries: descriptions, personal/shared
-- visibility, usage stats, and per-user favorites.
ALTER TABLE saved_queries ADD COLUMN IF NOT EXISTS description  TEXT;
ALTER TABLE saved_queries ADD COLUMN IF NOT EXISTS visibility   VARCHAR(16) NOT NULL DEFAULT 'shared';
ALTER TABLE saved_queries ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMP;
ALTER TABLE saved_queries ADD COLUMN IF NOT EXISTS use_count    INTEGER NOT NULL DEFAULT 0;

-- A shared saved query cannot store one user's pin on the row itself, so
-- favorites live in their own per-user join table.
CREATE TABLE IF NOT EXISTS saved_query_favorites (
    username       VARCHAR(50) NOT NULL REFERENCES users(username) ON DELETE CASCADE,
    saved_query_id UUID NOT NULL REFERENCES saved_queries(id) ON DELETE CASCADE,
    created_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (username, saved_query_id)
);
CREATE INDEX IF NOT EXISTS idx_saved_query_favorites_user ON saved_query_favorites(username);
