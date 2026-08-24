-- Create database if not exists
CREATE DATABASE IF NOT EXISTS logs;

-- Use the logs database
USE logs;

-- Create the main logs table with fractal isolation support
-- Every DateTime is pinned to 'UTC', not the server timezone: toDate()/toStartOfMinute()
-- over these columns feed the partition key and histogram rollup, and Go reads partition
-- names back as UTC dates (ParseLogPartition). A non-UTC server would shift both.
CREATE TABLE IF NOT EXISTS logs (
    -- Delta+ZSTD: sorted timestamps compress to ~1 byte/row vs ~5 under the LZ4 default,
    -- and this column is read by nearly every query. log_id is hex, which ZSTD halves.
    timestamp DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1)),
    log_id String CODEC(ZSTD(1)),
    fields JSON(
        max_dynamic_paths=1024,
        `computer_name`      String,
        `user`               String,
        `src_ip`             String,
        `dst_ip`             String,
        `src_port`           String,
        `dst_port`           String,
        `commandline`        String,
        `hash`               String,
        `event_id`           String,
        `image`              String,
        `parent_image`       String,
        `call_chain`         String,
        `operation`          String,
        `artifact`           String,
        `query`              String,
        `original_file_name` String,
        `proto`              String,
        `conn_state`         String,
        `duration`           String,
        `orig_bytes`         String,
        `resp_bytes`         String,
        `bifract_category`   String,
        `process_guid`        String,
        `parent_process_guid` String,
        -- Cross-tree reconnection fields read by pgr(). Hint only, no skip index
        -- (see ProjectDefaultFields in pkg/schemafields/defaults.go). Existing
        -- installs receive these via ReconcileSchemaFields, never a migration: a
        -- migration rewriting this list from a fixed set would strip user-added
        -- hints and break dependent skip indexes.
        `target_image`        String,
        `target_file`         String
    ),
    fractal_id LowCardinality(String) DEFAULT '',
    -- Near-constant within a part, so Delta leaves almost nothing to store.
    ingest_timestamp DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(Delta(8), ZSTD(1)),
    -- Flat, indefinitely-retained serialization of the normalized fields. A point-read of
    -- one ZSTD String file is subsecond, versus reconstructing the JSON column from ~1000+
    -- per-path subcolumn files. Populated by ClickHouse from the fields column at insert
    -- (DEFAULT toString(fields)), preserving the exact typed JSON the detail panel renders.
    -- This is the canonical text field for BQL and the log-detail lookup.
    norm_log String DEFAULT toString(fields) CODEC(ZSTD(3)),
    -- "name@version" of the normalizer applied to this row, for traceability.
    normalizer LowCardinality(String) DEFAULT '',
    -- Character n-gram full-text index on lower(norm_log). Indexes the lowercased
    -- expression (not the column) so the translator can route case-insensitive
    -- substring/regex search to match(lower(norm_log), ...) and prune granules.
    -- The n-gram tokenizer (unlike whole-word splitByNonAlpha) accelerates
    -- arbitrary substring and regex matches. Only the index stores the lowercased form.
    INDEX norm_log_ngram_lc lower(norm_log) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
    INDEX log_id_bloom log_id TYPE bloom_filter(0.001) GRANULARITY 1,
    -- Skip indexes on normalized fields. Defined inline so all new parts are indexed
    -- on insert without requiring MATERIALIZE INDEX. Direct sub-column references
    -- (no CAST) are required — ClickHouse's skip index optimizer does not match
    -- CAST/function expressions against bloom filter or set indexes.
    INDEX idx_src_ip             fields.src_ip             TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_dst_ip             fields.dst_ip             TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_computer_name      fields.computer_name      TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_user               fields.user               TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_hash               fields.hash               TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_image              fields.image              TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_parent_image       fields.parent_image       TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_original_file_name fields.original_file_name TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_query              fields.query              TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_event_id           fields.event_id           TYPE set(256)           GRANULARITY 1,
    INDEX idx_operation          fields.operation          TYPE set(256)            GRANULARITY 1,
    INDEX idx_artifact           fields.artifact           TYPE set(64)            GRANULARITY 1,
    INDEX idx_src_port           fields.src_port           TYPE set(4096)          GRANULARITY 1,
    INDEX idx_dst_port           fields.dst_port           TYPE set(4096)          GRANULARITY 1,
    INDEX idx_proto              fields.proto              TYPE set(16)            GRANULARITY 1,
    INDEX idx_conn_state         fields.conn_state         TYPE set(64)            GRANULARITY 1,
    -- process_guid bloom accelerates the process-tree leaf-fetch (pgr) that filters
    -- logs by GUID. parent_process_guid is type-hint only (no index): nothing filters
    -- logs by it; proc_lineage carries its own parent_guid bloom.
    INDEX idx_process_guid       fields.process_guid       TYPE bloom_filter(0.001) GRANULARITY 1
) ENGINE = MergeTree()
-- Ingest time, not event time: a batch spanning several event days would otherwise write a
-- part into each of those already-merged partitions and drag every one back through a merge.
-- timestamp stays the leading ORDER BY column, so event-time predicates still prune granules.
-- ingest_timestamp needs no minmax index: as a partition-key column it already has one.
PARTITION BY (fractal_id, toDate(ingest_timestamp))
ORDER BY (timestamp, log_id)
SETTINGS index_granularity = 8192;

-- Pre-normalization original log text, split out of the logs table into its own
-- daily-partitioned table so its 7-day retention is a metadata-only DROP PARTITION
-- (ttl_only_drop_parts) instead of a column TTL, which on the largest column forces
-- perpetual part rewrites during merges. Read only by the log-detail "Raw" tab and the
-- normalizer preview, both keyed by (timestamp, log_id).
-- Partition and TTL are both on ingest time, matching logs. They must share an axis or
-- ttl_only_drop_parts stops dropping whole parts, which is the point of the split.
CREATE TABLE IF NOT EXISTS logs_raw (
    timestamp        DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1)),
    fractal_id       LowCardinality(String) DEFAULT '',
    log_id           String CODEC(ZSTD(1)),
    raw_log          String CODEC(ZSTD(3)),
    ingest_timestamp DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(Delta(8), ZSTD(1)),
    INDEX log_id_bloom log_id TYPE bloom_filter(0.001) GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY (fractal_id, toDate(ingest_timestamp))
ORDER BY (timestamp, log_id)
TTL toDateTime(ingest_timestamp) + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

-- Pre-aggregated per-minute counts per fractal for fast landing-page histograms.
-- Querying this instead of raw logs reduces the recent-logs histogram from a
-- 200M-row scan to ~1440 rows for a 24-hour window.
--
-- logs_histogram_mv (below) aggregates only within each incoming insert block, so every
-- insert batch lands its own small part; SummingMergeTree reconciles duplicate
-- (fractal_id, minute) keys on merge, not on insert. Under bursty ingest, part creation can
-- outpace background merges and hit the parts_to_throw_insert default of 3000, rejecting
-- inserts until merges catch up. The table stays tiny (tens of MB), so tolerating far more
-- parts before throttling is low-risk.
--
-- ingest_day matches the logs partition key so DropLogPartition can prune this rollup
-- exactly. Readers group by minute and sum across ingest_day, so it is invisible to them.
CREATE TABLE IF NOT EXISTS logs_histogram (
    fractal_id LowCardinality(String),
    ingest_day Date,
    minute     DateTime('UTC'),
    cnt        UInt64
) ENGINE = SummingMergeTree(cnt)
ORDER BY (fractal_id, ingest_day, minute)
SETTINGS index_granularity = 256, parts_to_delay_insert = 3000, parts_to_throw_insert = 10000;

-- Feeds logs_histogram from every insert into the local logs table.
-- The MV writes to the local logs_histogram. The distributed table handles cross-shard reads.
CREATE MATERIALIZED VIEW IF NOT EXISTS logs_histogram_mv TO logs_histogram
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    fractal_id,
    toDate(ingest_timestamp) AS ingest_day,
    toStartOfMinute(timestamp) AS minute,
    count() AS cnt
FROM logs
GROUP BY fractal_id, ingest_day, minute;

-- Hot table for the alert engine: 2-hour rolling window ordered by ingest_timestamp.
-- Alert queries with UseIngestTimestamp=true route here when cursor is < 110 min old,
-- giving a primary-key range scan instead of a full day-partition scan on logs.
-- No fractal_id in PARTITION BY: keeps active partitions to ~24 for a 2h window.
-- fractal_id leads ORDER BY for efficient per-fractal ingest_timestamp range scans.
-- No skip indexes: ORDER BY covers the alert query pattern; indexes on 2h data waste writes.
-- TTL is a safety net only; the StartHotTableCleaner goroutine is the primary mechanism.
CREATE TABLE IF NOT EXISTS logs_hot (
    timestamp        DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1)),
    log_id           String CODEC(ZSTD(1)),
    fields           JSON(
        max_dynamic_paths=1024,
        `computer_name`      String,
        `user`               String,
        `src_ip`             String,
        `dst_ip`             String,
        `src_port`           String,
        `dst_port`           String,
        `commandline`        String,
        `hash`               String,
        `event_id`           String,
        `image`              String,
        `parent_image`       String,
        `call_chain`         String,
        `operation`          String,
        `artifact`           String,
        `query`              String,
        `original_file_name` String,
        `proto`              String,
        `conn_state`         String,
        `duration`           String,
        `orig_bytes`         String,
        `resp_bytes`         String,
        `bifract_category`   String,
        `process_guid`        String,
        `parent_process_guid` String,
        -- Cross-tree reconnection fields read by pgr(). Hint only, no skip index
        -- (see ProjectDefaultFields in pkg/schemafields/defaults.go). Existing
        -- installs receive these via ReconcileSchemaFields, never a migration: a
        -- migration rewriting this list from a fixed set would strip user-added
        -- hints and break dependent skip indexes.
        `target_image`        String,
        `target_file`         String
    ),
    fractal_id       LowCardinality(String) DEFAULT '',
    ingest_timestamp DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(Delta(8), ZSTD(1)),
    -- Mirrors logs.norm_log: the canonical BQL text column. Required, not optional --
    -- any alert query with a pipeline command projects norm_log, and those route here
    -- whenever the cursor is under 110 min old. No n-gram index (see note above on skip
    -- indexes); a 2h window is small enough to scan without one.
    norm_log         String DEFAULT toString(fields) CODEC(ZSTD(3)),
    normalizer       LowCardinality(String) DEFAULT ''
) ENGINE = MergeTree()
PARTITION BY toStartOfFiveMinutes(ingest_timestamp)
ORDER BY (fractal_id, ingest_timestamp, log_id)
TTL toDateTime(ingest_timestamp) + INTERVAL 4 HOUR DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

-- Feeds logs_hot from every insert into the local logs table.
-- The MV writes to local logs_hot on each shard — it fires per-shard when distributed
-- writes land on local logs, so the distributed layer is never in the write path.
CREATE MATERIALIZED VIEW IF NOT EXISTS logs_hot_mv TO logs_hot
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    timestamp,
    log_id,
    fields,
    fractal_id,
    ingest_timestamp,
    norm_log,
    normalizer
FROM logs;

-- Process-lineage skeleton: one row per process-create event, ordered by
-- (fractal_id, process_guid) so ptg() traversal hops are primary-key point lookups
-- instead of full-table scans over logs (which OOMs dfs/bfs at scale). This is a
-- self-sufficient, compact table (no raw_log/norm_log/fields bulk): the process tree
-- renders from these columns alone even after the source logs tier/expire, so it is
-- retained on a long, DFIR-driven TTL decoupled from logs. ReplacingMergeTree dedups
-- on re-ingestion / iceberg replay (process_guid is globally unique per process).
-- TTL is a default; the app applies BIFRACT_PROC_LINEAGE_TTL_DAYS via MODIFY TTL at startup.
CREATE TABLE IF NOT EXISTS proc_lineage (
    fractal_id       LowCardinality(String),
    timestamp        DateTime64(3, 'UTC'),
    ingest_timestamp DateTime64(3, 'UTC'),
    log_id           String,
    process_guid     String,
    parent_guid      String,
    image            LowCardinality(String),
    parent_image     LowCardinality(String),
    commandline      String,
    computer_name    LowCardinality(String),
    INDEX idx_parent_guid parent_guid TYPE bloom_filter(0.001) GRANULARITY 1
) ENGINE = ReplacingMergeTree(timestamp)
ORDER BY (fractal_id, process_guid)
-- Retention on the ingest axis, matching logs. On event time a source with a skewed
-- clock (an event dated years back) has its lineage row deleted at the first merge.
TTL toDateTime(ingest_timestamp) + INTERVAL 730 DAY
SETTINGS index_granularity = 8192;

-- Populates proc_lineage from process-create events, keyed on the normalized
-- category (Phase A). ::String is safe whether or not the type hint is applied yet.
CREATE MATERIALIZED VIEW IF NOT EXISTS proc_lineage_mv TO proc_lineage
DEFINER = default SQL SECURITY DEFINER AS
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
    fields.computer_name::String       AS computer_name
FROM logs
WHERE fields.bifract_category = 'process_creation' AND fields.process_guid != '';

-- proc_freq: NoDoze frequency baseline for pgr() edge scoring. Abstracted + aggregated
-- behavioral patterns (src process -> relationship -> abstracted target) with count + host
-- set. Abstraction expressions MUST match pkg/parser/abstractExpr() (locked by the parity
-- test in pkg/parser/provenance_test.go) so pgr() read-side join keys line up.
CREATE TABLE IF NOT EXISTS proc_freq (
    fractal_id  LowCardinality(String),
    src_image   LowCardinality(String),
    event_type  LowCardinality(String),
    target_norm String,
    -- Ingest day, not event day (see proc_lineage): it is both the TTL basis and an
    -- aggregation bucket, and nothing on the read side filters it.
    day         Date,
    event_count SimpleAggregateFunction(sum, UInt64),
    hosts       AggregateFunction(groupUniqArray(256), String)
) ENGINE = AggregatingMergeTree()
ORDER BY (fractal_id, src_image, event_type, target_norm, day)
TTL day + INTERVAL 730 DAY
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS proc_freq_spawn_mv TO proc_freq
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.parent_image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'spawn' AS event_type,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS target_norm,
    toDate(ingest_timestamp) AS day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'process_creation' AND fields.parent_image::String != '' AND fields.image::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;

CREATE MATERIALIZED VIEW IF NOT EXISTS proc_freq_file_mv TO proc_freq
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'file_write' AS event_type,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.target_file::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS target_norm,
    toDate(ingest_timestamp) AS day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'file_write' AND fields.image::String != '' AND fields.target_file::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;

CREATE MATERIALIZED VIEW IF NOT EXISTS proc_freq_net_mv TO proc_freq
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'net_connect' AS event_type,
    multiIf(match(fields.dst_ip::String, '^(10\\.|172\\.(1[6-9]|2[0-9]|3[01])\\.|192\\.168\\.|127\\.|169\\.254\\.)'), concat(replaceRegexpOne(fields.dst_ip::String, '\\.[0-9]{1,3}$', ''), '.0/24'), match(fields.dst_ip::String, '^(::1$|fe80:|fc|fd)'), 'internal', fields.dst_ip::String) AS target_norm,
    toDate(ingest_timestamp) AS day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'network_connect' AND fields.image::String != '' AND fields.dst_ip::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;

CREATE MATERIALIZED VIEW IF NOT EXISTS proc_freq_dns_mv TO proc_freq
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'dns_query' AS event_type,
    lower(replaceRegexpOne(fields.query::String, '\\.$', '')) AS target_norm,
    toDate(ingest_timestamp) AS day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'dns_query' AND fields.image::String != '' AND fields.query::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;

CREATE MATERIALIZED VIEW IF NOT EXISTS proc_freq_rthread_mv TO proc_freq
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'remote_thread' AS event_type,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.target_image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS target_norm,
    toDate(ingest_timestamp) AS day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'remote_thread' AND fields.image::String != '' AND fields.target_image::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;

CREATE MATERIALIZED VIEW IF NOT EXISTS proc_freq_pacc_mv TO proc_freq
DEFINER = default SQL SECURITY DEFINER AS
SELECT
    fractal_id,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS src_image,
    'process_access' AS event_type,
    lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(fields.target_image::String, '(?i)((users|home)[\\\\/])[^\\\\/]+', '\\1*'), '\\{?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\\}?', '*'), '[0-9]{6,}', '*')) AS target_norm,
    toDate(ingest_timestamp) AS day,
    toUInt64(count()) AS event_count,
    groupUniqArrayState(256)(fields.computer_name::String) AS hosts
FROM logs
WHERE fields.bifract_category = 'process_access' AND fields.image::String != '' AND fields.target_image::String != ''
GROUP BY fractal_id, src_image, event_type, target_norm, day;

-- process_edges: pgr() edge-fetch acceleration. One aggregated row per
-- (fractal_id, src process_guid, event_type, abstracted target), so pgr reads a tree's
-- file/net/dns edges by a PRIMARY-KEY lookup on process_guid (ORDER BY leads with it) instead of
-- scanning raw logs over the query window -- which is bloom-defeated for busy trees on high-rate
-- hosts (measured: 156s / 36B rows scanned -> 17ms / 1 granule via this table). AggregatingMergeTree
-- collapses a beaconing process's many events to one edge. The abstraction expressions MUST stay
-- byte-identical to pkg/parser/abstractExpr() and the proc_freq MVs (guarded by
-- TestAbstractExprMatchesMVs) so fkey_tgt matches proc_freq.target_norm for pgr's anomaly join.
-- Gated by the advanced_endpoint_analysis toggle via ATTACH/DETACH (see endpointAnalysisMVNames).
CREATE TABLE IF NOT EXISTS process_edges (
    fractal_id       LowCardinality(String),
    process_guid     String,
    event_type       LowCardinality(String),
    dst_node         String,
    label            SimpleAggregateFunction(anyLast, String),
    fkey_src         SimpleAggregateFunction(anyLast, String),
    fkey_tgt         SimpleAggregateFunction(anyLast, String),
    log_id           SimpleAggregateFunction(anyLast, String),
    timestamp        SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    ingest_timestamp SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    computer_name    SimpleAggregateFunction(anyLast, String),
    cnt              SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
ORDER BY (fractal_id, process_guid, event_type, dst_node)
-- Ingest axis (see proc_lineage). max() means an edge lives 730 days past its last ingest.
TTL toDateTime(ingest_timestamp) + INTERVAL 730 DAY
SETTINGS index_granularity = 8192;

-- Single MV feeding process_edges for the three default pgr leaf categories. One pass over each
-- insert block (WHERE category IN (...)), per-category projection via multiIf. Abstraction regexes
-- copied verbatim from the proc_freq MVs above -- keep them identical.
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
    max(ingest_timestamp) AS ingest_timestamp,
    anyLast(fields.computer_name::String) AS computer_name,
    count() AS cnt
FROM logs
WHERE fields.bifract_category::String IN ('file_write', 'network_connect', 'dns_query')
  AND fields.image::String != '' AND fields.process_guid::String != ''
  AND multiIf(fields.bifract_category = 'file_write', fields.target_file::String != '',
              fields.bifract_category = 'network_connect', fields.dst_ip::String != '',
              fields.query::String != '' AND NOT match(fields.query::String, '^([0-9]{1,3}\\.){3}[0-9]{1,3}$'))
GROUP BY fractal_id, process_guid, event_type, dst_node;
