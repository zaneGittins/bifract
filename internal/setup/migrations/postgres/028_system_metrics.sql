-- Persisted health metrics time-series for the System -> Overview/Alerts tabs
-- (CPU% and alert evaluation latency). Kept in Postgres rather than ClickHouse
-- so health history survives ClickHouse degradation/restarts and is not subject
-- to ClickHouse's own metric-log TTL. Generic (metric, node, fractal_id) shape
-- so new series need no further migrations. Retention enforced by the app.
CREATE TABLE IF NOT EXISTS system_metrics (
    ts         TIMESTAMPTZ      NOT NULL,
    metric     TEXT             NOT NULL,
    node       TEXT             NOT NULL DEFAULT '',
    fractal_id TEXT             NOT NULL DEFAULT '',
    value      DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_system_metrics_metric_ts ON system_metrics (metric, ts);
