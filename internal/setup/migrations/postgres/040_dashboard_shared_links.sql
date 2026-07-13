-- Shared Links: public, no-auth, read-only dashboard access (wallboards).
-- A link grants anonymous read of exactly ONE dashboard's cached widget results.
-- Only the SHA-256 hash of the token is stored; the plaintext token is shown once.
CREATE TABLE IF NOT EXISTS dashboard_shared_links (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dashboard_id UUID NOT NULL REFERENCES dashboards(id) ON DELETE CASCADE,
    token_hash CHAR(64) NOT NULL UNIQUE,
    token_prefix VARCHAR(16) NOT NULL,
    label VARCHAR(200),
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP,
    revoked_at TIMESTAMP,
    last_accessed_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_dashboard_shared_links_dashboard_id ON dashboard_shared_links(dashboard_id);

-- Shared Links default OFF: opt-in per install.
INSERT INTO settings (key, value) VALUES ('shared_links_enabled', 'false')
ON CONFLICT (key) DO NOTHING;
