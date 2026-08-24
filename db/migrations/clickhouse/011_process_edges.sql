-- DEAD: stamped but never executed since the ingest-partition reset. Kept for numbering (next: 016) and TestUpgradeFromV002.
-- process_edges: pgr() edge-fetch acceleration (see db/init-clickhouse.sql for the rationale).
-- One aggregated row per (fractal_id, src process_guid, event_type, abstracted target), so pgr
-- reads a tree's file/net/dns edges by a primary-key lookup on process_guid instead of scanning
-- raw logs over the query window. Abstraction expressions MUST stay byte-identical to
-- pkg/parser/abstractExpr() and the proc_freq MVs (guarded by TestAbstractExprMatchesMVs).
-- Gated by the advanced_endpoint_analysis toggle via ATTACH/DETACH (see endpointAnalysisMVNames).

CREATE TABLE IF NOT EXISTS process_edges (
    fractal_id    LowCardinality(String),
    process_guid  String,
    event_type    LowCardinality(String),
    dst_node      String,
    label         SimpleAggregateFunction(anyLast, String),
    fkey_src      SimpleAggregateFunction(anyLast, String),
    fkey_tgt      SimpleAggregateFunction(anyLast, String),
    log_id        SimpleAggregateFunction(anyLast, String),
    timestamp     SimpleAggregateFunction(max, DateTime64(3)),
    computer_name SimpleAggregateFunction(anyLast, String),
    cnt           SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
ORDER BY (fractal_id, process_guid, event_type, dst_node)
TTL toDateTime(timestamp) + INTERVAL 730 DAY
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS process_edges_mv TO process_edges
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    fractal_id,
    fields.process_guid::String AS process_guid,
    multiIf(fields.bifract_category = 'file_write', 'file_write',
            fields.bifract_category = 'network_connect', 'net_connect',
            'dns_query') AS event_type,
    multiIf(
        fields.bifract_category = 'file_write',
            concat('file:', lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.target_file::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*'))),
        fields.bifract_category = 'network_connect',
            concat('net:', multiIf(match(fields.dst_ip::String, '^(10\\.|172\\.(1[6-9]|2[0-9]|3[01])\\.|192\\.168\\.|127\\.|169\\.254\\.)'), concat(replaceRegexpOne(fields.dst_ip::String, '\\.[0-9]{1,3}$', ''), '.0/24'), match(fields.dst_ip::String, '^(::1$|fe80:|fc|fd)'), 'internal', fields.dst_ip::String)),
        concat('dns:', lower(replaceRegexpOne(fields.query::String, '\\.$', '')))
    ) AS dst_node,
    anyLast(multiIf(fields.bifract_category = 'file_write', fields.target_file::String,
                    fields.bifract_category = 'network_connect', fields.dst_ip::String,
                    fields.query::String)) AS label,
    anyLast(lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*'))) AS fkey_src,
    anyLast(multiIf(
        fields.bifract_category = 'file_write', lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.target_file::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')),
        fields.bifract_category = 'network_connect', multiIf(match(fields.dst_ip::String, '^(10\\.|172\\.(1[6-9]|2[0-9]|3[01])\\.|192\\.168\\.|127\\.|169\\.254\\.)'), concat(replaceRegexpOne(fields.dst_ip::String, '\\.[0-9]{1,3}$', ''), '.0/24'), match(fields.dst_ip::String, '^(::1$|fe80:|fc|fd)'), 'internal', fields.dst_ip::String),
        lower(replaceRegexpOne(fields.query::String, '\\.$', ''))
    )) AS fkey_tgt,
    anyLast(log_id) AS log_id,
    max(timestamp) AS timestamp,
    anyLast(fields.computer_name::String) AS computer_name,
    count() AS cnt
FROM logs
WHERE fields.bifract_category::String IN ('file_write', 'network_connect', 'dns_query')
  AND fields.image::String != '' AND fields.process_guid::String != ''
  AND multiIf(fields.bifract_category = 'file_write', fields.target_file::String != '',
              fields.bifract_category = 'network_connect', fields.dst_ip::String != '',
              fields.query::String != '' AND NOT match(fields.query::String, '^([0-9]{1,3}\\.){3}[0-9]{1,3}$'))
GROUP BY fractal_id, process_guid, event_type, dst_node;
