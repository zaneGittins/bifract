-- The review gate: a proposed change to an alert, held apart from the alert itself so
-- the live definition keeps running while the proposal is reviewed.
--
-- Scoped like the alerts it governs (fractal xor prism), and off unless a row exists in
-- alert_gate_config for that scope.
CREATE TABLE IF NOT EXISTS alert_gate_config (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fractal_id   UUID REFERENCES fractals(id) ON DELETE CASCADE,
    prism_id     UUID REFERENCES prisms(id) ON DELETE CASCADE,
    enabled      BOOLEAN NOT NULL DEFAULT false,
    min_approvals INTEGER NOT NULL DEFAULT 1 CHECK (min_approvals >= 1),
    -- Self approval is an admin privilege, and configurable even for them.
    allow_self_approval BOOLEAN NOT NULL DEFAULT true,
    updated_by   VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    updated_at   TIMESTAMP NOT NULL DEFAULT NOW()
);
ALTER TABLE alert_gate_config DROP CONSTRAINT IF EXISTS alert_gate_config_scope_check;
ALTER TABLE alert_gate_config ADD CONSTRAINT alert_gate_config_scope_check CHECK (
    (fractal_id IS NOT NULL AND prism_id IS NULL) OR
    (fractal_id IS NULL AND prism_id IS NOT NULL)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_alert_gate_config_fractal ON alert_gate_config(fractal_id) WHERE fractal_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_alert_gate_config_prism ON alert_gate_config(prism_id) WHERE prism_id IS NOT NULL;

-- A proposal. content holds the proposed definition for create and update, and is null
-- for a delete. base_hash records the head revision it was written against, so a
-- proposal that went stale while it sat open is refused rather than silently clobbering
-- whatever landed in the meantime.
CREATE TABLE IF NOT EXISTS alert_change_requests (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fractal_id   UUID REFERENCES fractals(id) ON DELETE CASCADE,
    prism_id     UUID REFERENCES prisms(id) ON DELETE CASCADE,
    alert_id     UUID REFERENCES alerts(id) ON DELETE CASCADE,
    kind         VARCHAR(10) NOT NULL CHECK (kind IN ('create', 'update', 'delete')),
    status       VARCHAR(20) NOT NULL DEFAULT 'open'
                 CHECK (status IN ('open', 'changes_requested', 'merged', 'discarded')),
    title        TEXT NOT NULL DEFAULT '',
    summary      TEXT NOT NULL DEFAULT '',
    content      JSONB,
    tests        JSONB,
    content_hash CHAR(64) NOT NULL DEFAULT '',
    base_hash    CHAR(64) NOT NULL DEFAULT '',
    created_by   VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    author_label TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    merged_at    TIMESTAMP,
    merged_by    VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL
);
ALTER TABLE alert_change_requests DROP CONSTRAINT IF EXISTS alert_change_requests_scope_check;
ALTER TABLE alert_change_requests ADD CONSTRAINT alert_change_requests_scope_check CHECK (
    (fractal_id IS NOT NULL AND prism_id IS NULL) OR
    (fractal_id IS NULL AND prism_id IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS idx_alert_cr_fractal ON alert_change_requests(fractal_id, status, updated_at DESC) WHERE fractal_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_alert_cr_prism ON alert_change_requests(prism_id, status, updated_at DESC) WHERE prism_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_alert_cr_alert ON alert_change_requests(alert_id) WHERE alert_id IS NOT NULL;

-- Reviews, approvals and rejections alike. content_hash records what was actually
-- reviewed: an approval stops counting the moment the proposal is edited, which is the
-- property that keeps the gate from being decorative.
CREATE TABLE IF NOT EXISTS alert_change_reviews (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    change_request_id UUID NOT NULL REFERENCES alert_change_requests(id) ON DELETE CASCADE,
    reviewer          VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    reviewer_label    TEXT NOT NULL DEFAULT '',
    decision          VARCHAR(10) NOT NULL CHECK (decision IN ('approve', 'reject')),
    comment           TEXT NOT NULL DEFAULT '',
    content_hash      CHAR(64) NOT NULL DEFAULT '',
    created_at        TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_alert_change_reviews_cr ON alert_change_reviews(change_request_id, created_at);
