-- Sample events an alert is expected to match or not match, edited in the alert
-- editor's Tests tab and run whenever the editor runs its query.
--
-- events holds normalized event objects (the shape BQL runs against, which is what
-- both a picked search result and pasted JSON already are), so a test needs no
-- normalizer attached and stays stable when normalizers change.
--
-- Tests are not part of an alert's revision content: they are about the definition,
-- not the definition itself, and editing them must not consume capped history.
CREATE TABLE IF NOT EXISTS alert_tests (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id    UUID NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    expectation VARCHAR(10) NOT NULL CHECK (expectation IN ('match', 'no_match')),
    events      JSONB NOT NULL,
    position    INTEGER NOT NULL DEFAULT 0,
    created_by  VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_alert_tests_alert ON alert_tests(alert_id, position);
