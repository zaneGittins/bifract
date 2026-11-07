-- Sort proc_lineage and process_edges by proc_key (see parser.procKeyExpr), a Sysmon GUID
-- reordered so one time window is contiguous. Metadata-only: old parts compute the columns on
-- read, so the live tables match db/init-clickhouse.sql, and storage.ConvertLayouts rebuilds them
-- under the new sort key in the background.
ALTER TABLE proc_lineage ADD COLUMN IF NOT EXISTS proc_key String MATERIALIZED if(match(process_guid, '^\\{[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}\\}$'), concat(substring(process_guid, 1, 10), substring(process_guid, 16, 5), substring(process_guid, 11, 5), substring(process_guid, 21)), process_guid);
ALTER TABLE proc_lineage ADD COLUMN IF NOT EXISTS parent_key String MATERIALIZED if(match(parent_guid, '^\\{[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}\\}$'), concat(substring(parent_guid, 1, 10), substring(parent_guid, 16, 5), substring(parent_guid, 11, 5), substring(parent_guid, 21)), parent_guid);
-- Tables created by migration 011 lack ingest_timestamp, which pgr's edge reads and the conversion's
-- column check need. Old rows fall back to the edge's own time, as proc_lineage's do (see 019).
ALTER TABLE process_edges ADD COLUMN IF NOT EXISTS ingest_timestamp SimpleAggregateFunction(max, DateTime64(3, 'UTC')) DEFAULT timestamp AFTER timestamp;
ALTER TABLE process_edges ADD COLUMN IF NOT EXISTS proc_key String MATERIALIZED if(match(process_guid, '^\\{[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}\\}$'), concat(substring(process_guid, 1, 10), substring(process_guid, 16, 5), substring(process_guid, 11, 5), substring(process_guid, 21)), process_guid);
