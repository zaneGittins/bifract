-- Fix skip index expressions to use ::String cast so they match the query
-- translator output (fields.`name`::String). Indexes created by migration 002
-- used bare field paths (fields.src_ip) which don't match the cast expression
-- in WHERE clauses, causing marks_skipped = 0 on all queries.
-- DROP + re-ADD is idempotent; GRANULARITY 1 and types are unchanged.
ALTER TABLE logs DROP INDEX IF EXISTS idx_src_ip;
ALTER TABLE logs DROP INDEX IF EXISTS idx_dst_ip;
ALTER TABLE logs DROP INDEX IF EXISTS idx_computer_name;
ALTER TABLE logs DROP INDEX IF EXISTS idx_user;
ALTER TABLE logs DROP INDEX IF EXISTS idx_hash;
ALTER TABLE logs DROP INDEX IF EXISTS idx_image;
ALTER TABLE logs DROP INDEX IF EXISTS idx_parent_image;
ALTER TABLE logs DROP INDEX IF EXISTS idx_original_file_name;
ALTER TABLE logs DROP INDEX IF EXISTS idx_query;
ALTER TABLE logs DROP INDEX IF EXISTS idx_event_id;
ALTER TABLE logs DROP INDEX IF EXISTS idx_operation;
ALTER TABLE logs DROP INDEX IF EXISTS idx_artifact;
ALTER TABLE logs DROP INDEX IF EXISTS idx_src_port;
ALTER TABLE logs DROP INDEX IF EXISTS idx_dst_port;

ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_src_ip             fields.src_ip::String             TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_dst_ip             fields.dst_ip::String             TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_computer_name      fields.computer_name::String      TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_user               fields.user::String               TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_hash               fields.hash::String               TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_image              fields.image::String              TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_parent_image       fields.parent_image::String       TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_original_file_name fields.original_file_name::String TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_query              fields.query::String              TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_event_id           fields.event_id::String           TYPE set(256)           GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_operation          fields.operation::String          TYPE set(64)            GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_artifact           fields.artifact::String           TYPE set(64)            GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_src_port           fields.src_port::String           TYPE set(1024)          GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_dst_port           fields.dst_port::String           TYPE set(1024)          GRANULARITY 1;
