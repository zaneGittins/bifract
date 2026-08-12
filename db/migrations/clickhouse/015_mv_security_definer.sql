-- Make every logs-sourced materialized view run as DEFINER.
--
-- An MV created without a security clause executes with the INSERTING user's privileges.
-- The least-privilege ingest identity holds only INSERT on logs.*, so pushing an insert
-- through such an MV fails with code 497 ("Not enough privileges ... SELECT ... ON
-- logs.logs: while pushing to view ..."). The base-table row is already committed when
-- the MV push fails, but the INSERT still returns an error, so the ingest queue retries
-- the batch maxInsertRetries times and commits a duplicate copy on every attempt. The
-- observed result was avg 3 copies of every row plus consecutive-failure backpressure,
-- on a ClickHouse that was otherwise completely idle.
--
-- Only MVs reading logs.logs are affected, and only once the ingest tier writes rows the
-- MV's WHERE clause selects, which is why this stayed dormant until an EDR source started
-- populating bifract_category.
--
-- ReconcileMaterializedViewSecurity does this at app startup, but it cannot help an
-- install whose running binary predates it, so the conversion is pinned here too. The
-- ALTER is idempotent and cheap (metadata only, no data rewrite); MVs that already carry
-- the clause are unaffected. Fresh installs get it directly from init-clickhouse.sql.
ALTER TABLE logs_histogram_mv    MODIFY SQL SECURITY DEFINER DEFINER = default;
ALTER TABLE logs_hot_mv          MODIFY SQL SECURITY DEFINER DEFINER = default;
ALTER TABLE proc_lineage_mv      MODIFY SQL SECURITY DEFINER DEFINER = default;
ALTER TABLE proc_freq_spawn_mv   MODIFY SQL SECURITY DEFINER DEFINER = default;
ALTER TABLE proc_freq_file_mv    MODIFY SQL SECURITY DEFINER DEFINER = default;
ALTER TABLE proc_freq_net_mv     MODIFY SQL SECURITY DEFINER DEFINER = default;
ALTER TABLE proc_freq_dns_mv     MODIFY SQL SECURITY DEFINER DEFINER = default;
ALTER TABLE proc_freq_rthread_mv MODIFY SQL SECURITY DEFINER DEFINER = default;
ALTER TABLE proc_freq_pacc_mv    MODIFY SQL SECURITY DEFINER DEFINER = default;
ALTER TABLE process_edges_mv     MODIFY SQL SECURITY DEFINER DEFINER = default;
