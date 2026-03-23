-- Per-prism permissions for users and groups (mirrors fractal_permissions)
CREATE TABLE IF NOT EXISTS prism_permissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    prism_id UUID NOT NULL REFERENCES prisms(id) ON DELETE CASCADE,
    username VARCHAR(50) REFERENCES users(username) ON DELETE CASCADE,
    group_id UUID REFERENCES groups(id) ON DELETE CASCADE,
    role VARCHAR(20) NOT NULL CHECK (role IN ('viewer', 'analyst', 'admin')),
    granted_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT prism_exactly_one_grantee CHECK (
        (username IS NOT NULL AND group_id IS NULL) OR
        (username IS NULL AND group_id IS NOT NULL)
    ),
    UNIQUE(prism_id, username),
    UNIQUE(prism_id, group_id)
);

CREATE INDEX IF NOT EXISTS idx_prism_permissions_prism_id ON prism_permissions(prism_id);
CREATE INDEX IF NOT EXISTS idx_prism_permissions_username ON prism_permissions(username) WHERE username IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_prism_permissions_group_id ON prism_permissions(group_id) WHERE group_id IS NOT NULL;

DROP TRIGGER IF EXISTS update_prism_permissions_updated_at ON prism_permissions;
CREATE TRIGGER update_prism_permissions_updated_at BEFORE UPDATE ON prism_permissions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
