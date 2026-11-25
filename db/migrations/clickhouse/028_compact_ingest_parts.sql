-- Keep ingest-sized parts compact. Above the 10 MiB default a batch is written wide, one
-- file per JSON path stream (~2,500 files), which slows inserts and merges ~4-9x on network
-- volumes. Parts past the threshold still merge into wide parts. Metadata-only.
ALTER TABLE logs MODIFY SETTING min_bytes_for_wide_part = 1073741824;
ALTER TABLE logs_hot MODIFY SETTING min_bytes_for_wide_part = 2147483648;
