-- Per-user query history, synced server-side so recent queries follow the user
-- across browsers/devices instead of living only in localStorage. Scoped per
-- fractal or prism, deduped by query text with a run counter.
CREATE TABLE IF NOT EXISTS query_history (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username     VARCHAR(50) NOT NULL REFERENCES users(username) ON DELETE CASCADE,
    query_text   TEXT NOT NULL,
    time_range   VARCHAR(32),
    custom_start TIMESTAMP,
    custom_end   TIMESTAMP,
    result_count BIGINT,
    duration_ms  INTEGER,
    status       VARCHAR(16),
    run_count    INTEGER NOT NULL DEFAULT 1,
    first_run_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_run_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    fractal_id   UUID REFERENCES fractals(id) ON DELETE CASCADE,
    prism_id     UUID REFERENCES prisms(id) ON DELETE CASCADE,
    CONSTRAINT query_history_scope_check CHECK (
        (fractal_id IS NOT NULL AND prism_id IS NULL) OR
        (fractal_id IS NULL AND prism_id IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_query_history_dedup
    ON query_history (username, md5(query_text), COALESCE(fractal_id, prism_id));
CREATE INDEX IF NOT EXISTS idx_query_history_list
    ON query_history (username, COALESCE(fractal_id, prism_id), last_run_at DESC);
