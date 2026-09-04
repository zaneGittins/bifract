-- Webhook body shaping. 'envelope' keeps the built-in payload (existing rows);
-- 'template' renders body_template so an action can match a destination's wire
-- format. content_type empty means application/json.
ALTER TABLE webhook_actions ADD COLUMN IF NOT EXISTS body_mode VARCHAR(20) NOT NULL DEFAULT 'envelope';
ALTER TABLE webhook_actions ADD COLUMN IF NOT EXISTS body_template TEXT NOT NULL DEFAULT '';
ALTER TABLE webhook_actions ADD COLUMN IF NOT EXISTS content_type VARCHAR(100) NOT NULL DEFAULT '';

ALTER TABLE webhook_actions DROP CONSTRAINT IF EXISTS webhook_actions_body_mode_check;
ALTER TABLE webhook_actions ADD CONSTRAINT webhook_actions_body_mode_check
    CHECK (body_mode IN ('envelope', 'template'));
