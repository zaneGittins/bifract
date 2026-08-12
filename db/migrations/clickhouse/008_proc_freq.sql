-- proc_freq: the NoDoze frequency baseline for pgr() edge scoring. One row per abstracted
-- behavioral pattern (src process -> relationship -> target) with its count and the set of
-- hosts that exhibited it. Aggregated + abstracted, so it stays tiny (megabytes) vs the raw
-- events. pgr() reads freq(src,rel,dst)/freq(src,rel,*) to score each edge by rarity.
--
-- IMPORTANT: the abstraction expressions in the MVs below MUST stay byte-identical to
-- pkg/parser/abstractExpr() (the pgr() read side) or the freq-join keys won't match. A
-- parity test (pkg/parser/provenance_test.go) locks them together -- do not edit one side
-- without the other.
--
-- New-logs-only: MVs populate from events ingested after this migration; an optional
-- chunked backfill (admin action) seeds the baseline from historical logs.
CREATE TABLE IF NOT EXISTS proc_freq (
    fractal_id  LowCardinality(String),
    src_image   LowCardinality(String),   -- abstracted source process image
    event_type  LowCardinality(String),   -- 'spawn' | 'file_write' | 'net_connect'
    target_norm String,                    -- abstracted DST (child image / file path / dst_ip /24)
    day         Date,
    event_count SimpleAggregateFunction(sum, UInt64),
    hosts       AggregateFunction(groupUniqArray(256), String)
) ENGINE = AggregatingMergeTree()
ORDER BY (fractal_id, src_image, event_type, target_norm, day)
TTL day + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;

-- spawn: parent_image spawns image (Pro_Start).
CREATE MATERIALIZED VIEW IF NOT EXISTS proc_freq_spawn_mv TO proc_freq
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.parent_image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'spawn' AS event_type,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS target_norm,
    toDate(timestamp) AS day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'process_creation' AND fields.parent_image::String != '' AND fields.image::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;

-- file_write: image writes/creates artifact (File_Write).
CREATE MATERIALIZED VIEW IF NOT EXISTS proc_freq_file_mv TO proc_freq
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'file_write' AS event_type,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.artifact::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS target_norm,
    toDate(timestamp) AS day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'file_write' AND fields.image::String != '' AND fields.artifact::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;

-- net_connect: image connects to dst_ip, abstracted to external IP or internal /24 (IP_Write).
CREATE MATERIALIZED VIEW IF NOT EXISTS proc_freq_net_mv TO proc_freq
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'net_connect' AS event_type,
    multiIf(match(fields.dst_ip::String, '^(10\\.|172\\.(1[6-9]|2[0-9]|3[01])\\.|192\\.168\\.|127\\.|169\\.254\\.)'), concat(replaceRegexpOne(fields.dst_ip::String, '\\.[0-9]{1,3}$', ''), '.0/24'), match(fields.dst_ip::String, '^(::1$|fe80:|fc|fd)'), 'internal', fields.dst_ip::String) AS target_norm,
    toDate(timestamp) AS day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'network_connect' AND fields.image::String != '' AND fields.dst_ip::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;
