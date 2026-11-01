-- Add proc_freq.first_event_day: the earliest EVENT date of a relationship, for pgr()'s
-- first_seen only. first_day stays on ingest days because scoring, TTL and leave-one-out use it;
-- the event date is clamped to the ingest day so a future-dated clock cannot push it forward.
--
-- Metadata-only. Old rows compute the DEFAULT on read (their ingest-day first seen), which a
-- min() merge with new rows can only lower.
ALTER TABLE proc_freq ADD COLUMN IF NOT EXISTS first_event_day SimpleAggregateFunction(min, Date) DEFAULT if(first_day = toDate(0), day, first_day) AFTER first_day;

-- MODIFY QUERY swaps the SELECT in place (see 016); keep the bodies identical to db/init-clickhouse.sql.

ALTER TABLE proc_freq_spawn_mv MODIFY QUERY
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.parent_image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'spawn' AS event_type,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS target_norm,
    toDate(ingest_timestamp) AS day,
    toDate(ingest_timestamp) AS first_day,
    least(min(toDate(timestamp)), toDate(ingest_timestamp)) AS first_event_day,
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
    least(min(toDate(timestamp)), toDate(ingest_timestamp)) AS first_event_day,
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
    least(min(toDate(timestamp)), toDate(ingest_timestamp)) AS first_event_day,
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
    least(min(toDate(timestamp)), toDate(ingest_timestamp)) AS first_event_day,
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
    least(min(toDate(timestamp)), toDate(ingest_timestamp)) AS first_event_day,
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
    least(min(toDate(timestamp)), toDate(ingest_timestamp)) AS first_event_day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'process_access' AND fields.image::String != '' AND fields.target_image::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;
