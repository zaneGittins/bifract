-- Keep a dedup window on logs and every materialized view target, so a retried ingest batch
-- (same insert_deduplication_token) is dropped instead of written twice. Metadata-only;
-- replicated engines already keep a window and accept the setting unchanged.
ALTER TABLE logs MODIFY SETTING non_replicated_deduplication_window = 10000;
ALTER TABLE logs_histogram MODIFY SETTING non_replicated_deduplication_window = 10000;
ALTER TABLE logs_field_paths MODIFY SETTING non_replicated_deduplication_window = 10000;
ALTER TABLE logs_hot MODIFY SETTING non_replicated_deduplication_window = 10000;
ALTER TABLE proc_lineage MODIFY SETTING non_replicated_deduplication_window = 10000;
ALTER TABLE proc_freq MODIFY SETTING non_replicated_deduplication_window = 10000;
ALTER TABLE process_edges MODIFY SETTING non_replicated_deduplication_window = 10000;
ALTER TABLE provenance_summary MODIFY SETTING non_replicated_deduplication_window = 10000;
