-- Analytics models table.
-- Each row represents a behavioral model backed by a ClickHouse AggregatingMergeTree
-- table + Materialized View that auto-populates from the logs stream.
-- V1: per-fractal scope only. prism_id column reserved for future extension.
CREATE TABLE IF NOT EXISTS analytics_models (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    fractal_id      UUID         REFERENCES fractals(id) ON DELETE CASCADE,
    prism_id        UUID         REFERENCES prisms(id)   ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    description     TEXT         NOT NULL DEFAULT '',
    model_type      VARCHAR(50)  NOT NULL,
    definition      JSONB        NOT NULL DEFAULT '{}',
    ch_table_name   VARCHAR(255) NOT NULL DEFAULT '',
    ch_mv_name      VARCHAR(255) NOT NULL DEFAULT '',
    status          VARCHAR(50)  NOT NULL DEFAULT 'active',
    alert_mode      VARCHAR(50)  NOT NULL DEFAULT 'none',
    linked_alert_id UUID         REFERENCES alerts(id) ON DELETE SET NULL,
    error_message   TEXT         NOT NULL DEFAULT '',
    created_by      VARCHAR(255) NOT NULL DEFAULT '',
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_model_scope CHECK (
        (fractal_id IS NOT NULL AND prism_id IS NULL) OR
        (fractal_id IS NULL AND prism_id IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_analytics_models_fractal ON analytics_models(fractal_id);
CREATE INDEX IF NOT EXISTS idx_analytics_models_prism   ON analytics_models(prism_id);
