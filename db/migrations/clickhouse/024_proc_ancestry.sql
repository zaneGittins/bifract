-- Ancestry lookups (see proc_ancestry in db/init-clickhouse.sql). The lineage index covers new
-- parts; storage.ConvertLayouts rebuilds older ones with it.
ALTER TABLE proc_lineage ADD INDEX IF NOT EXISTS idx_ingest_ts ingest_timestamp TYPE minmax GRANULARITY 1;

CREATE TABLE IF NOT EXISTS proc_ancestry (
    fractal_id   LowCardinality(String),
    proc_key     String,
    depth        UInt8,
    process_guid String,
    image        LowCardinality(String),
    anc_key      String,
    anc_image    LowCardinality(String),
    open_key     String,
    -- The process's own lineage ingest, so its ancestry expires with its lineage.
    ingest_timestamp DateTime64(3, 'UTC'),
    version      DateTime64(3, 'UTC'),
    PROJECTION by_ancestor (SELECT _part_offset ORDER BY (fractal_id, anc_key))
) ENGINE = ReplacingMergeTree(version)
PARTITION BY fractal_id
ORDER BY (fractal_id, proc_key, depth)
TTL toDateTime(ingest_timestamp) + INTERVAL 730 DAY
SETTINGS index_granularity = 1024, deduplicate_merge_projection_mode = 'rebuild', non_replicated_deduplication_window = 10000;

-- The builder's progress: every lineage row ingested before at is reflected in proc_ancestry. The
-- latest write wins, so a lineage backfill can move it back to rows it added.
CREATE TABLE IF NOT EXISTS proc_ancestry_watermark (
    id     UInt8,
    at     DateTime64(3, 'UTC'),
    set_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(set_at)
ORDER BY id;
