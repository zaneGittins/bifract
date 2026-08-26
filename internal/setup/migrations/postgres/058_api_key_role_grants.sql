-- API keys carry an RBAC role and an explicit tenant-admin grant instead of the
-- five boolean capabilities, so a key is authorized exactly like a person.
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS role VARCHAR(20) NOT NULL DEFAULT 'viewer';
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS tenant_admin BOOLEAN NOT NULL DEFAULT false;

-- Derive each existing key's role from the booleans it was issued with, matching
-- what resolveAPIKeyRole did at request time: any write capability meant analyst,
-- query alone meant viewer, and neither meant no access at all.
UPDATE api_keys SET role = CASE
    WHEN COALESCE((permissions->>'alert_manage')::boolean, false)
      OR COALESCE((permissions->>'comment')::boolean, false)
      OR COALESCE((permissions->>'notebook')::boolean, false)
      OR COALESCE((permissions->>'dashboard')::boolean, false) THEN 'analyst'
    WHEN COALESCE((permissions->>'query')::boolean, false) THEN 'viewer'
    ELSE ''
END;

-- No existing key can become tenant admin by migration: that grant is only ever
-- made explicitly, by an administrator, on a key with an expiry.
ALTER TABLE api_keys DROP CONSTRAINT IF EXISTS api_keys_tenant_admin_expires;
ALTER TABLE api_keys ADD CONSTRAINT api_keys_tenant_admin_expires
    CHECK (NOT tenant_admin OR expires_at IS NOT NULL);

CREATE INDEX IF NOT EXISTS idx_api_keys_tenant_admin ON api_keys(tenant_admin) WHERE tenant_admin;
