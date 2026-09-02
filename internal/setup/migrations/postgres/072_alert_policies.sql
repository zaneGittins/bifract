-- Policy checks: assertions about an alert's definition, enforced in the editor and
-- again on save. One row per assertion, scoped like the alerts it judges.
--
-- A rule is field/operator/value, not a script: it replaces the shell checks a CI gate
-- would run ("has a MITRE label", "description long enough") without introducing a
-- language to author or parse. `message` is what an analyst reads next to the offending
-- field, so it says what to do rather than what failed.
CREATE TABLE IF NOT EXISTS alert_policies (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fractal_id UUID REFERENCES fractals(id) ON DELETE CASCADE,
    prism_id   UUID REFERENCES prisms(id) ON DELETE CASCADE,
    field      TEXT NOT NULL,
    operator   TEXT NOT NULL,
    value      TEXT NOT NULL DEFAULT '',
    message    TEXT NOT NULL DEFAULT '',
    severity   VARCHAR(10) NOT NULL DEFAULT 'warn' CHECK (severity IN ('warn', 'block')),
    enabled    BOOLEAN NOT NULL DEFAULT true,
    position   INTEGER NOT NULL DEFAULT 0,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
ALTER TABLE alert_policies ADD COLUMN IF NOT EXISTS prism_id UUID REFERENCES prisms(id) ON DELETE CASCADE;
ALTER TABLE alert_policies ALTER COLUMN fractal_id DROP NOT NULL;

-- A rule set belongs to exactly one scope, matching the alerts it judges.
ALTER TABLE alert_policies DROP CONSTRAINT IF EXISTS alert_policies_scope_check;
ALTER TABLE alert_policies ADD CONSTRAINT alert_policies_scope_check CHECK (
    (fractal_id IS NOT NULL AND prism_id IS NULL) OR
    (fractal_id IS NULL AND prism_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_alert_policies_fractal ON alert_policies(fractal_id, position) WHERE fractal_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_alert_policies_prism ON alert_policies(prism_id, position) WHERE prism_id IS NOT NULL;
