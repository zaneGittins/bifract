-- Injection and handle-access edges into the process_edges rollup (see db/init-clickhouse.sql).
-- The view is created before the mark, so a log ingested in between is in both places, which pgr's
-- edge aggregation absorbs, rather than in neither.
CREATE TABLE IF NOT EXISTS provenance_marks (
    name   LowCardinality(String),
    at     DateTime64(3, 'UTC'),
    set_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(set_at)
ORDER BY name;

CREATE MATERIALIZED VIEW IF NOT EXISTS process_edges_p2p_mv TO process_edges
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    fractal_id,
    fields.source_process_guid::String AS process_guid,
    fields.bifract_category::String AS event_type,
    fields.target_process_guid::String AS dst_node,
    anyLast(fields.target_image::String) AS label,
    anyLast(lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*'))) AS fkey_src,
    anyLast(lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.target_image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*'))) AS fkey_tgt,
    anyLast(log_id) AS log_id,
    max(timestamp) AS timestamp,
    max(ingest_timestamp) AS ingest_timestamp,
    anyLast(fields.computer_name::String) AS computer_name,
    count() AS cnt
FROM logs
WHERE fields.bifract_category::String IN ('remote_thread', 'process_access')
  AND fields.image::String != '' AND fields.target_image::String != ''
  AND fields.source_process_guid::String != '' AND fields.target_process_guid::String != ''
GROUP BY fractal_id, process_guid, event_type, dst_node;

-- An hour ahead, so a log stamped by a server clock running fast is still read from logs.
INSERT INTO provenance_marks (name, at) SELECT 'p2p_edges', now64(3) + INTERVAL 1 HOUR;
