-- An instance-wide key is not a scoped key. It carries no fractal, no prism and
-- no scope role, so the two kinds of grant can never be issued together.
ALTER TABLE api_keys DROP CONSTRAINT IF EXISTS api_keys_scope_check;
UPDATE api_keys SET fractal_id = NULL, prism_id = NULL, role = '' WHERE tenant_admin;
ALTER TABLE api_keys ADD CONSTRAINT api_keys_scope_check CHECK (
    (tenant_admin AND fractal_id IS NULL AND prism_id IS NULL AND role = '') OR
    (NOT tenant_admin AND ((fractal_id IS NOT NULL) <> (prism_id IS NOT NULL)))
);

ALTER TABLE api_keys DROP CONSTRAINT IF EXISTS api_keys_tenant_admin_expires;
ALTER TABLE api_keys ADD CONSTRAINT api_keys_tenant_admin_expires
    CHECK (NOT tenant_admin OR expires_at IS NOT NULL);
