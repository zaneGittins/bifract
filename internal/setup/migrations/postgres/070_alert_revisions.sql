-- Alert definition history: the last N revisions per alert, retention set live from
-- the admin settings page.
--
-- The head revision always equals the alert's current definition. Alerts that
-- predate this migration have no rows until their next edit, which seeds the
-- pre-edit state as their first revision.
--
-- enabled and disabled_reason are deliberately excluded from content. They are
-- operational state, and including them would let a bulk toggle or an engine
-- auto-disable evict every real edit from a capped history.
CREATE TABLE IF NOT EXISTS alert_revisions (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id     UUID NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
    revision     INTEGER NOT NULL,
    content      JSONB NOT NULL,
    content_hash CHAR(64) NOT NULL,
    summary      TEXT NOT NULL DEFAULT '',
    author       VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    author_label TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (alert_id, revision)
);
CREATE INDEX IF NOT EXISTS idx_alert_revisions_alert ON alert_revisions(alert_id, revision DESC);
