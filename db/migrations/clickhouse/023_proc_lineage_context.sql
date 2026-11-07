-- Process-creation facts pgrContext() reads beside the command line and user. Metadata-only:
-- rows written before them read ''. MODIFY QUERY swaps the SELECT in place (see 016); keep it
-- identical to db/init-clickhouse.sql.
ALTER TABLE proc_lineage ADD COLUMN IF NOT EXISTS hash String DEFAULT '' AFTER proc_user;
ALTER TABLE proc_lineage ADD COLUMN IF NOT EXISTS original_file_name String DEFAULT '' AFTER hash;

ALTER TABLE proc_lineage_mv MODIFY QUERY
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
    fields.user::String                AS proc_user,
    fields.hash::String                AS hash,
    fields.original_file_name::String  AS original_file_name,
    fields.computer_name::String       AS computer_name
FROM logs
WHERE fields.bifract_category = 'process_creation' AND fields.process_guid != '';
