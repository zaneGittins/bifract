-- Email actions for alerts (send email via SMTP when alert fires)
CREATE TABLE IF NOT EXISTS email_actions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL UNIQUE,
    recipients TEXT[] NOT NULL DEFAULT '{}',
    subject_template TEXT NOT NULL DEFAULT '',
    body_template TEXT NOT NULL DEFAULT '',
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS alert_email_actions (
    alert_id UUID NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
    email_action_id UUID NOT NULL REFERENCES email_actions(id) ON DELETE CASCADE,
    PRIMARY KEY (alert_id, email_action_id)
);

ALTER TABLE alert_executions ADD COLUMN IF NOT EXISTS email_results JSONB DEFAULT '[]';

CREATE INDEX IF NOT EXISTS idx_email_actions_enabled ON email_actions(enabled) WHERE enabled = true;
CREATE INDEX IF NOT EXISTS idx_email_actions_created_by ON email_actions(created_by);
CREATE INDEX IF NOT EXISTS idx_email_actions_name ON email_actions(name);
CREATE INDEX IF NOT EXISTS idx_alert_email_actions_alert ON alert_email_actions(alert_id);
CREATE INDEX IF NOT EXISTS idx_alert_email_actions_action ON alert_email_actions(email_action_id);

DROP TRIGGER IF EXISTS update_email_actions_updated_at ON email_actions;
CREATE TRIGGER update_email_actions_updated_at BEFORE UPDATE ON email_actions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
