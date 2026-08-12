-- Extends the proc_freq baseline (008) with the remaining pgr() edge types and fixes a
-- released bug. All of these ship in init-clickhouse.sql for fresh installs; this migration
-- brings existing installs (already past migration 008) to parity.
--
-- Abstraction expressions MUST stay byte-identical to pkg/parser/abstractExpr() (the pgr()
-- read side), locked by the parity test in pkg/parser/provenance_test.go.
--
-- Note on the endpoint-analysis toggle: these MVs are created ATTACHED here; the startup
-- ReconcileEndpointAnalysisMVs pass runs after migrations and re-detaches them if the
-- "Advanced endpoint analysis" feature is off. DROP VIEW IF EXISTS also clears any
-- previously permanently-detached metadata, so the recreate below is safe in every state.

-- Fix: 008 keyed the file MV off fields.artifact, but the normalized field is target_file,
-- so file_write rows were effectively never written on installs that applied 008. Recreate it.
DROP VIEW IF EXISTS proc_freq_file_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS proc_freq_file_mv TO proc_freq
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'file_write' AS event_type,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.target_file::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS target_norm,
    toDate(timestamp) AS day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'file_write' AND fields.image::String != '' AND fields.target_file::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;

-- remote_thread: actor (fields.image) creates a remote thread in target_image (Sysmon EID 8, injection).
CREATE MATERIALIZED VIEW IF NOT EXISTS proc_freq_rthread_mv TO proc_freq
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'remote_thread' AS event_type,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.target_image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS target_norm,
    toDate(timestamp) AS day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'remote_thread' AND fields.image::String != '' AND fields.target_image::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;

-- process_access: actor (fields.image) opens a handle to target_image (Sysmon EID 10, e.g. LSASS access).
CREATE MATERIALIZED VIEW IF NOT EXISTS proc_freq_pacc_mv TO proc_freq
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'process_access' AS event_type,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.target_image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS target_norm,
    toDate(timestamp) AS day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'process_access' AND fields.image::String != '' AND fields.target_image::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;

-- dns_query: image resolves a domain (Sysmon EID 22). target is the lowercased, root-dot-stripped query.
CREATE MATERIALIZED VIEW IF NOT EXISTS proc_freq_dns_mv TO proc_freq
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'dns_query' AS event_type,
    lower(replaceRegexpOne(fields.query::String, '\\.$', '')) AS target_norm,
    toDate(timestamp) AS day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'dns_query' AND fields.image::String != '' AND fields.query::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;
