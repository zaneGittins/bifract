-- pgr()/ptg() access paths: every lookup keyed by what the query already holds (a parent guid,
-- an image, an artifact, a target), so its cost follows the tree instead of the fractal.
--
-- Metadata-only. Existing parts compute the new columns on read and work without the
-- projections; the background backfill (storage.BackfillProvenancePaths) builds their
-- projections and the summary without a startup data pass.
-- Projections on Replacing/Aggregating engines need deduplicate_merge_projection_mode set.

-- Tables created by migration 007 and not yet rebuilt by the repartition conversion lack
-- ingest_timestamp, which the views below read. Old rows fall back to the event time.
ALTER TABLE proc_lineage ADD COLUMN IF NOT EXISTS ingest_timestamp DateTime64(3, 'UTC') DEFAULT timestamp AFTER timestamp;
ALTER TABLE proc_lineage MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
-- Rows written before the column read '\0', which pgr takes as unknown and looks up in logs.
ALTER TABLE proc_lineage ADD COLUMN IF NOT EXISTS proc_user String DEFAULT '\0' AFTER commandline;
-- Sibling-collapse key, the parser.abstractExpr path abstraction computed once per row.
ALTER TABLE proc_lineage ADD COLUMN IF NOT EXISTS child_image LowCardinality(String) MATERIALIZED lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(image, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*'));
ALTER TABLE proc_lineage ADD COLUMN IF NOT EXISTS child_cmd_key UInt64 MATERIALIZED cityHash64(lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(commandline, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')));
ALTER TABLE proc_lineage ADD COLUMN IF NOT EXISTS image_lc LowCardinality(String) MATERIALIZED lower(image);
-- Forward hop: children by parent, narrow so a hop never reads command lines.
ALTER TABLE proc_lineage ADD PROJECTION IF NOT EXISTS by_parent (SELECT fractal_id, parent_guid, process_guid, timestamp, child_image, child_cmd_key ORDER BY (fractal_id, parent_guid));
-- Reconnection: executors of a dropped file, by image. Index only.
ALTER TABLE proc_lineage ADD PROJECTION IF NOT EXISTS by_image (SELECT _part_offset ORDER BY (fractal_id, image_lc));

-- The process's user rides the lineage row, so pgr reads no logs for it. MODIFY QUERY swaps
-- the SELECT in place (see 016); keep it identical to db/init-clickhouse.sql.
ALTER TABLE proc_lineage_mv MODIFY QUERY
SELECT
    fractal_id,
    timestamp,
    ingest_timestamp,
    log_id,
    fields.process_guid::String        AS process_guid,
    fields.parent_process_guid::String AS parent_guid,
    fields.image::String               AS image,
    fields.parent_image::String        AS parent_image,
    fields.commandline::String         AS commandline,
    fields.user::String                AS proc_user,
    fields.computer_name::String       AS computer_name
FROM logs
WHERE fields.bifract_category = 'process_creation' AND fields.process_guid != '';

-- Reconnection peers: every process that touched an artifact. Index only.
ALTER TABLE process_edges MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE process_edges ADD PROJECTION IF NOT EXISTS by_target (SELECT _part_offset ORDER BY (fractal_id, event_type, fkey_tgt));

-- Baseline by target (target-side scoring, the reconnection rarity gate). Index only.
ALTER TABLE proc_freq MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE proc_freq ADD PROJECTION IF NOT EXISTS by_target (SELECT _part_offset ORDER BY (fractal_id, event_type, target_norm));

-- Fleet-wide facts pgr reads per call, one row per (fractal, kind, value): lineage hosts,
-- baseline hosts and source images, baseline days. Each row lives as long as the newest row
-- behind it; storage.ReconcileProvenanceSummaryTTL tracks the configured retentions.
CREATE TABLE IF NOT EXISTS provenance_summary (
    fractal_id LowCardinality(String),
    kind       LowCardinality(String),
    value      String,
    last_day   SimpleAggregateFunction(max, Date)
) ENGINE = AggregatingMergeTree()
PARTITION BY fractal_id
ORDER BY (fractal_id, kind, value)
TTL last_day + toIntervalDay(if(kind = 'lineage_host', 731, 730))
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS provenance_summary_lineage_mv TO provenance_summary
DEFINER = default SQL SECURITY DEFINER AS
SELECT fractal_id, 'lineage_host' AS kind, computer_name AS value, max(toDate(ingest_timestamp)) AS last_day
FROM proc_lineage
GROUP BY fractal_id, value;

CREATE MATERIALIZED VIEW IF NOT EXISTS provenance_summary_freq_mv TO provenance_summary
DEFINER = default SQL SECURITY DEFINER AS
SELECT fractal_id, kind, value, max(day) AS last_day
FROM (
    SELECT fractal_id, 'freq_image' AS kind, src_image AS value, day FROM proc_freq
    UNION ALL SELECT fractal_id, 'freq_day', toString(day), day FROM proc_freq
    UNION ALL SELECT fractal_id, 'freq_host', arrayJoin(finalizeAggregation(hosts)), day FROM proc_freq
)
GROUP BY fractal_id, kind, value;
