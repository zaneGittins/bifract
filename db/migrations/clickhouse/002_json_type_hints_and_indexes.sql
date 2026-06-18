-- Add JSON type hints for default normalized fields.
-- Type hints guarantee dedicated sub-column storage regardless of the
-- max_dynamic_paths limit, preventing overflow-bucket I/O on high-cardinality
-- deployments. MODIFY COLUMN schedules a background mutation on existing parts.
ALTER TABLE logs MODIFY COLUMN fields JSON(
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
    `original_file_name` String
);

-- Add skip indexes for normalized fields. IF NOT EXISTS makes these idempotent.
-- New parts are indexed automatically; on large production tables run
-- MATERIALIZE INDEX for each during off-peak hours to backfill existing parts.
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_src_ip             fields.src_ip             TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_dst_ip             fields.dst_ip             TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_computer_name      fields.computer_name      TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_user               fields.user               TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_hash               fields.hash               TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_image              fields.image              TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_parent_image       fields.parent_image       TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_original_file_name fields.original_file_name TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_query              fields.query              TYPE bloom_filter(0.001) GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_event_id           fields.event_id           TYPE set(256)           GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_operation          fields.operation          TYPE set(64)            GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_artifact           fields.artifact           TYPE set(64)            GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_src_port           fields.src_port           TYPE set(1024)          GRANULARITY 1;
ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_dst_port           fields.dst_port           TYPE set(1024)          GRANULARITY 1;
