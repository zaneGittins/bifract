-- Persisted results of the background schema sweep. The schema tab used to
-- compute these on the request path from a ClickHouse sample whose cost scaled
-- with the retention window rather than the sample size; it now reads only these
-- tables, so the page renders in Postgres time no matter how large logs is.

-- Per-fractal, per-field distribution from the sampled sweep, plus the on-disk
-- footprint and column allocation read from ClickHouse part metadata.
CREATE TABLE IF NOT EXISTS schema_field_stats (
    fractal_id    VARCHAR(64)  NOT NULL,
    field_name    VARCHAR(255) NOT NULL,
    present       BIGINT       NOT NULL DEFAULT 0,
    cardinality   BIGINT       NOT NULL DEFAULT 0,
    -- [{"value":..,"count":..,"error":..}], approx_top_k output.
    top_values    JSONB        NOT NULL DEFAULT '[]',
    -- Compressed size of the field's own sub-column. Only type-hinted fields have
    -- one that ClickHouse accounts for separately; a dynamic path's bytes are not
    -- broken out in part metadata, so it stays 0 until the field is reserved.
    bytes_on_disk BIGINT       NOT NULL DEFAULT 0,
    PRIMARY KEY (fractal_id, field_name)
);

-- One row per fractal the sweep measured: the coverage denominator and the
-- column-budget figures that only part metadata can answer.
CREATE TABLE IF NOT EXISTS schema_fractal_stats (
    fractal_id    VARCHAR(64) PRIMARY KEY,
    sampled_rows  BIGINT      NOT NULL DEFAULT 0,
    -- Most dynamic paths held by any one part, from JSONDynamicPaths.
    -- max_dynamic_paths is a per-part budget, so this, not the union of names
    -- across parts, is the used capacity.
    max_paths     INT         NOT NULL DEFAULT 0,
    total_bytes   BIGINT      NOT NULL DEFAULT 0,
    sampled_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- How often each field is referenced by saved BQL, and by what. Derived by
-- parsing every alert, widget, saved query, and recent history entry, which is
-- far too much work to repeat per page load.
CREATE TABLE IF NOT EXISTS schema_field_usage (
    field_name  VARCHAR(255) PRIMARY KEY,
    weight      INT          NOT NULL DEFAULT 0,
    -- [{"kind":..,"title":..}], capped server-side.
    refs        JSONB        NOT NULL DEFAULT '[]',
    computed_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
