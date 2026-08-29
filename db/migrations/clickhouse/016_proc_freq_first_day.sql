-- Add proc_freq.first_day: the earliest day a (src_image, event_type, target_norm) relationship
-- was ever observed.
--
-- This is the basis for NoDoze's IN/OUT node-stability terms (Eq.4/Eq.5), which the scoring model
-- previously omitted entirely. Only the transition term (Eq.1) was implemented, and on its own it
-- cannot separate never-seen from rarely-seen: as a source's total volume grows, 1 - 1/total
-- converges on 1 and every tail relationship collapses into the same score.
--
-- ALTER ADD COLUMN on an AggregatingMergeTree is metadata-only, and NO data pass is needed:
-- pre-existing rows carry the column default (1970-01-01), and the read side substitutes `day`
-- for that sentinel (see BuildProvenanceScoringSQL's rel CTE). Backfilling with a mutation would
-- be actively wrong -- min() over an epoch default dates every old relationship to 1970, making
-- every node read as permanently stable.
ALTER TABLE proc_freq ADD COLUMN IF NOT EXISTS first_day SimpleAggregateFunction(min, Date) AFTER day;

-- Repoint each proc_freq MV at the new column. MODIFY QUERY swaps the SELECT in place, so
-- ingestion is never interrupted (a DROP/CREATE would lose every row inserted in the gap).
-- Bodies are generated verbatim from db/init-clickhouse.sql; keep them identical.

ALTER TABLE proc_freq_spawn_mv MODIFY QUERY
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.parent_image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'spawn' AS event_type,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS target_norm,
    toDate(ingest_timestamp) AS day,
    toDate(ingest_timestamp) AS first_day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'process_creation' AND fields.parent_image::String != '' AND fields.image::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;

ALTER TABLE proc_freq_file_mv MODIFY QUERY
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'file_write' AS event_type,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.target_file::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS target_norm,
    toDate(ingest_timestamp) AS day,
    toDate(ingest_timestamp) AS first_day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'file_write' AND fields.image::String != '' AND fields.target_file::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;

ALTER TABLE proc_freq_net_mv MODIFY QUERY
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'net_connect' AS event_type,
    multiIf(match(fields.dst_ip::String, '^(10\\.|172\\.(1[6-9]|2[0-9]|3[01])\\.|192\\.168\\.|127\\.|169\\.254\\.)'), concat(replaceRegexpOne(fields.dst_ip::String, '\\.[0-9]{1,3}$', ''), '.0/24'), match(fields.dst_ip::String, '^(::1$|fe80:|fc|fd)'), 'internal', fields.dst_ip::String) AS target_norm,
    toDate(ingest_timestamp) AS day,
    toDate(ingest_timestamp) AS first_day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'network_connect' AND fields.image::String != '' AND fields.dst_ip::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;

ALTER TABLE proc_freq_dns_mv MODIFY QUERY
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'dns_query' AS event_type,
    lower(replaceRegexpOne(fields.query::String, '\\.$', '')) AS target_norm,
    toDate(ingest_timestamp) AS day,
    toDate(ingest_timestamp) AS first_day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'dns_query' AND fields.image::String != '' AND fields.query::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;

ALTER TABLE proc_freq_rthread_mv MODIFY QUERY
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'remote_thread' AS event_type,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.target_image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS target_norm,
    toDate(ingest_timestamp) AS day,
    toDate(ingest_timestamp) AS first_day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'remote_thread' AND fields.image::String != '' AND fields.target_image::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;

ALTER TABLE proc_freq_pacc_mv MODIFY QUERY
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'process_access' AS event_type,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.target_image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS target_norm,
    toDate(ingest_timestamp) AS day,
    toDate(ingest_timestamp) AS first_day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'process_access' AND fields.image::String != '' AND fields.target_image::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;
