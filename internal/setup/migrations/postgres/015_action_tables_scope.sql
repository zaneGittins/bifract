-- Add fractal_id / prism_id scoping to action tables (webhook, fractal, dictionary, email).
-- Prior to this migration these tables were globally scoped and visible from every
-- fractal and prism. Existing rows are backfilled into the default fractal.

ALTER TABLE webhook_actions    ADD COLUMN IF NOT EXISTS fractal_id UUID REFERENCES fractals(id) ON DELETE CASCADE;
ALTER TABLE webhook_actions    ADD COLUMN IF NOT EXISTS prism_id   UUID REFERENCES prisms(id)   ON DELETE CASCADE;
ALTER TABLE fractal_actions    ADD COLUMN IF NOT EXISTS fractal_id UUID REFERENCES fractals(id) ON DELETE CASCADE;
ALTER TABLE fractal_actions    ADD COLUMN IF NOT EXISTS prism_id   UUID REFERENCES prisms(id)   ON DELETE CASCADE;
ALTER TABLE dictionary_actions ADD COLUMN IF NOT EXISTS fractal_id UUID REFERENCES fractals(id) ON DELETE CASCADE;
ALTER TABLE dictionary_actions ADD COLUMN IF NOT EXISTS prism_id   UUID REFERENCES prisms(id)   ON DELETE CASCADE;
ALTER TABLE email_actions      ADD COLUMN IF NOT EXISTS fractal_id UUID REFERENCES fractals(id) ON DELETE CASCADE;
ALTER TABLE email_actions      ADD COLUMN IF NOT EXISTS prism_id   UUID REFERENCES prisms(id)   ON DELETE CASCADE;

-- Backfill existing unscoped rows into the default fractal so the scope check can apply.
UPDATE webhook_actions    SET fractal_id = (SELECT id FROM fractals WHERE is_default = true LIMIT 1) WHERE fractal_id IS NULL AND prism_id IS NULL;
UPDATE fractal_actions    SET fractal_id = (SELECT id FROM fractals WHERE is_default = true LIMIT 1) WHERE fractal_id IS NULL AND prism_id IS NULL;
UPDATE dictionary_actions SET fractal_id = (SELECT id FROM fractals WHERE is_default = true LIMIT 1) WHERE fractal_id IS NULL AND prism_id IS NULL;
UPDATE email_actions      SET fractal_id = (SELECT id FROM fractals WHERE is_default = true LIMIT 1) WHERE fractal_id IS NULL AND prism_id IS NULL;

ALTER TABLE webhook_actions    DROP CONSTRAINT IF EXISTS webhook_actions_scope_check;
ALTER TABLE webhook_actions    ADD  CONSTRAINT webhook_actions_scope_check CHECK (
    (fractal_id IS NOT NULL AND prism_id IS NULL) OR
    (fractal_id IS NULL AND prism_id IS NOT NULL)
);
ALTER TABLE fractal_actions    DROP CONSTRAINT IF EXISTS fractal_actions_scope_check;
ALTER TABLE fractal_actions    ADD  CONSTRAINT fractal_actions_scope_check CHECK (
    (fractal_id IS NOT NULL AND prism_id IS NULL) OR
    (fractal_id IS NULL AND prism_id IS NOT NULL)
);
ALTER TABLE dictionary_actions DROP CONSTRAINT IF EXISTS dictionary_actions_scope_check;
ALTER TABLE dictionary_actions ADD  CONSTRAINT dictionary_actions_scope_check CHECK (
    (fractal_id IS NOT NULL AND prism_id IS NULL) OR
    (fractal_id IS NULL AND prism_id IS NOT NULL)
);
ALTER TABLE email_actions      DROP CONSTRAINT IF EXISTS email_actions_scope_check;
ALTER TABLE email_actions      ADD  CONSTRAINT email_actions_scope_check CHECK (
    (fractal_id IS NOT NULL AND prism_id IS NULL) OR
    (fractal_id IS NULL AND prism_id IS NOT NULL)
);

-- Names were globally unique before; switch to per-scope uniqueness so different
-- fractals/prisms can independently reuse names.
ALTER TABLE webhook_actions    DROP CONSTRAINT IF EXISTS webhook_actions_name_key;
ALTER TABLE fractal_actions    DROP CONSTRAINT IF EXISTS fractal_actions_name_key;
ALTER TABLE dictionary_actions DROP CONSTRAINT IF EXISTS dictionary_actions_name_key;
ALTER TABLE email_actions      DROP CONSTRAINT IF EXISTS email_actions_name_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_webhook_actions_fractal_name    ON webhook_actions(fractal_id, name)    WHERE fractal_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_webhook_actions_prism_name      ON webhook_actions(prism_id, name)      WHERE prism_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_fractal_actions_fractal_name    ON fractal_actions(fractal_id, name)    WHERE fractal_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_fractal_actions_prism_name      ON fractal_actions(prism_id, name)      WHERE prism_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_dictionary_actions_fractal_name ON dictionary_actions(fractal_id, name) WHERE fractal_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_dictionary_actions_prism_name   ON dictionary_actions(prism_id, name)   WHERE prism_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_email_actions_fractal_name      ON email_actions(fractal_id, name)      WHERE fractal_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_email_actions_prism_name        ON email_actions(prism_id, name)        WHERE prism_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_webhook_actions_fractal_id    ON webhook_actions(fractal_id)    WHERE fractal_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_webhook_actions_prism_id      ON webhook_actions(prism_id)      WHERE prism_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fractal_actions_fractal_id    ON fractal_actions(fractal_id)    WHERE fractal_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fractal_actions_prism_id      ON fractal_actions(prism_id)      WHERE prism_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_dictionary_actions_fractal_id ON dictionary_actions(fractal_id) WHERE fractal_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_dictionary_actions_prism_id   ON dictionary_actions(prism_id)   WHERE prism_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_email_actions_fractal_id      ON email_actions(fractal_id)      WHERE fractal_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_email_actions_prism_id        ON email_actions(prism_id)        WHERE prism_id IS NOT NULL;
