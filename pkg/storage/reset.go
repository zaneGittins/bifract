package storage

import (
	"context"
	"fmt"
	"strings"
)

// resetMaterializedViews are the log-sourced views defined by init-clickhouse.sql.
// They must be dropped before their source table: a view's dependency on its source
// is registered by name, so recreating logs does not re-register a surviving view,
// and it would silently stop being fed.
var resetMaterializedViews = []string{
	"logs_histogram_mv",
	"logs_hot_mv",
	"proc_lineage_mv",
	"proc_freq_spawn_mv",
	"proc_freq_file_mv",
	"proc_freq_net_mv",
	"proc_freq_dns_mv",
	"proc_freq_rthread_mv",
	"proc_freq_pacc_mv",
	"process_edges_mv",
}

// resetShardedTables are the log tables and everything derived from them. Each has a
// _distributed twin in cluster mode (see distributedTableDDL, which is the same list).
// Dictionary tables are deliberately absent: they hold user-uploaded data that no log
// wipe invalidates.
var resetShardedTables = []string{
	"logs",
	"logs_raw",
	"logs_hot",
	"logs_histogram",
	"proc_lineage",
	"proc_freq",
	"process_edges",
}

// resetBookkeepingTables carry the migration ledger, which is per-shard and never
// fanned out. Dropping it puts the app back on the fresh-install path, where it
// provisions from the init SQL and stamps a baseline instead of replaying history.
var resetBookkeepingTables = []string{
	"_bifract_migrations",
	"_bifract_migration_steps",
}

// ResetKeeperTables returns the local table names whose ClickHouse Keeper metadata
// must be cleared after the drop, in cluster mode. RewriteEngine turns every local
// MergeTree into a ReplicatedMergeTree keyed on
// /clickhouse/tables/{shard}/{database}/{table}, and DROP TABLE only garbage-collects
// that path once the LAST registered replica is gone. Replica entries left behind by
// earlier incarnations of a pod keep the path alive, so re-provisioning the same
// table hits code 342 ("Metadata ... in ZooKeeper differs").
//
// Distributed tables are absent: they hold no data and no Keeper state. Dictionaries
// are absent because they are never dropped.
func ResetKeeperTables(extraTables []string) []string {
	return append(append([]string{}, extraTables...), resetShardedTables...)
}

// KeeperTablePath renders the Keeper path for one table, matching the macro layout
// RewriteEngine writes into the engine clause.
func KeeperTablePath(shard, database, table string) string {
	return fmt.Sprintf("/clickhouse/tables/%s/%s/%s", shard, database, table)
}

// KeeperReplicasQuery lists the replicas still registered for a table path. It
// errors when the path does not exist, which is the healthy case and callers should
// treat as "nothing to clean".
func KeeperReplicasQuery(path string) string {
	return fmt.Sprintf("SELECT name FROM system.zookeeper WHERE path = '%s/replicas'", escCHLiteral(path))
}

// DropKeeperReplicaStatement removes one replica's registration by path. Only valid
// for a replica that is not currently active, which is exactly the state a dropped
// table leaves behind.
func DropKeeperReplicaStatement(replica, path string) string {
	return fmt.Sprintf("SYSTEM DROP REPLICA '%s' FROM ZKPATH '%s'", escCHLiteral(replica), escCHLiteral(path))
}

// ShardMacroQuery reads a node's {shard} substitution, needed to build its Keeper
// paths. Read per node rather than assumed: the macro is what the engine clause
// interpolated, so anything else can target the wrong subtree.
const ShardMacroQuery = `SELECT substitution FROM system.macros WHERE macro = 'shard'`

// ResetLogDataStatements returns the DDL that drops every ClickHouse object derived
// from log data, in dependency order. Returned as statements rather than executed so
// the installer can run them over a driver connection to an external ClickHouse or
// pipe them into a bundled container's client, from one source of truth.
//
// extraViews and extraTables carry the runtime-generated analytics model objects,
// already expanded by the caller from models.CHObjectNames. They are named
// explicitly rather than matched by pattern against system.tables, which could
// catch a dictionary.
//
// DROP DATABASE is deliberately not used: dictionaries share the database and hold
// user-uploaded data that no log wipe invalidates.
func ResetLogDataStatements(extraViews, extraTables []string) []string {
	var stmts []string
	for _, mv := range append(append([]string{}, extraViews...), resetMaterializedViews...) {
		stmts = append(stmts, "DROP VIEW IF EXISTS "+quoteCHIdent(mv))
	}

	// Only the sharded log tables have a _distributed twin under that name; a
	// model's distributed tables use their own prefixes and arrive via extraTables,
	// and the bookkeeping tables have none.
	names := append([]string{}, extraTables...)
	for _, t := range resetShardedTables {
		names = append(names, t, t+"_distributed")
	}
	names = append(names, resetBookkeepingTables...)
	for _, name := range names {
		// max_table_size_to_drop (50GB by default) otherwise refuses the logs table
		// with code 359, and the usual escape hatch is a flag file on the server's
		// disk that a client connection cannot create. 0 lifts it for this statement.
		stmts = append(stmts, "DROP TABLE IF EXISTS "+quoteCHIdent(name)+
			" SYNC SETTINGS max_table_size_to_drop = 0")
	}
	return stmts
}

// ResetLogData runs ResetLogDataStatements against every shard. ON CLUSTER is
// avoided for the same reason as every other partition operation here: it
// serializes DDL through Keeper's global queue.
func (c *ClickHouseClient) ResetLogData(ctx context.Context, extraViews, extraTables []string) error {
	for _, stmt := range ResetLogDataStatements(extraViews, extraTables) {
		if err := c.execOnEveryShard(ctx, stmt, "reset"); err != nil {
			return fmt.Errorf("reset: %s: %w", stmt, err)
		}
	}
	return c.cleanKeeperPaths(ctx, extraTables)
}

// cleanKeeperPaths frees the Keeper registration of the tables just dropped. Only
// meaningful where RewriteEngine produced ReplicatedMergeTree; a non-replicated
// deployment registers nothing.
func (c *ClickHouseClient) cleanKeeperPaths(ctx context.Context, extraTables []string) error {
	if !c.topo.ReplicatedEngines {
		return nil
	}
	var shard string
	if err := c.conn.QueryRow(ctx, ShardMacroQuery).Scan(&shard); err != nil || shard == "" {
		return nil
	}
	for _, table := range ResetKeeperTables(extraTables) {
		path := KeeperTablePath(shard, c.Database, table)
		rows, err := c.Query(ctx, KeeperReplicasQuery(path))
		if err != nil {
			continue // absent path: nothing registered
		}
		for _, row := range rows {
			replica, _ := row["name"].(string)
			if replica == "" {
				continue
			}
			// Non-fatal: code 305 means the replica is active or a local table holds
			// the path, i.e. it is not an orphan.
			_ = c.execOnEveryShard(ctx, DropKeeperReplicaStatement(replica, path), "drop keeper replica")
		}
	}
	return nil
}

// quoteCHIdent backtick-quotes an identifier. Reset names come from the constants
// above and from Postgres, so this guards the interpolation rather than validating
// a user-supplied name.
func quoteCHIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// Analytics model objects are named from the model's UUID. Defined here, in the
// ClickHouse layer, because both pkg/models (which creates them) and the reset
// (which drops them) need the same answer and cannot import each other.
func modelObjectName(prefix, id string) string {
	return prefix + strings.ReplaceAll(id, "-", "_")
}

// ModelCHTableName is a model's local aggregate table.
func ModelCHTableName(id string) string { return modelObjectName("model_", id) }

// ModelCHMVName is the materialized view that feeds it.
func ModelCHMVName(id string) string { return modelObjectName("model_mv_", id) }

// ModelCHDistName is the cluster-mode distributed table over the aggregate.
func ModelCHDistName(id string) string { return modelObjectName("model_dist_", id) }

// ModelCHStateName is a scheduled (network) model's rolling-state table: the MV
// writes here at ingest and the scorer reads from here.
func ModelCHStateName(id string) string { return modelObjectName("model_state_", id) }

// ModelCHStateDistName is the distributed table over the state table, so a
// single-replica scoring pass aggregates state maintained per-shard.
func ModelCHStateDistName(id string) string { return modelObjectName("model_diststate_", id) }

// ModelCHObjectNames returns every ClickHouse object a model can own. The reset
// derives names here rather than reading the two recorded in Postgres, which omit
// a scheduled model's state table and both distributed tables.
//
// Views first, so a caller dropping in order never leaves an MV pointed at a
// table that is already gone.
func ModelCHObjectNames(id string) (views, tables []string) {
	return []string{ModelCHMVName(id)},
		[]string{
			ModelCHStateName(id),
			ModelCHStateDistName(id),
			ModelCHDistName(id),
			ModelCHTableName(id),
		}
}

// ModelIDsQuery lists the analytics model ids. The caller expands each into the
// ClickHouse objects it owns via ModelCHObjectNames: the two names recorded in
// Postgres omit a scheduled model's rolling-state table and both distributed
// tables. Emitted as a query because the installer reaches Postgres through psql
// rather than a driver.
const ModelIDsQuery = `SELECT id::text FROM analytics_models`

// ResetPostgresStateStatements clears the Postgres rows that describe ClickHouse
// log data a reset just destroyed. Everything else survives: users, fractals, saved
// searches, notebooks, dashboards, alert definitions, and the Iceberg archive's own
// bookkeeping, which still describes real data.
//
// The caller runs these in one transaction so a partial clear cannot leave a cursor
// pointing at data that is gone.
func ResetPostgresStateStatements() []string {
	return []string{
		// Alert cursors point into deleted data. The same five-minute rewind the
		// re-enable path uses, so the first tick after a reset is not a backfill.
		`UPDATE alerts SET last_evaluated_at = NOW() - INTERVAL '5 minutes'`,
		// Custom type hints and skip indexes live here, and the recreated schema
		// carries only the built-in defaults until they are reconciled again.
		`UPDATE clickhouse_schema_fields SET sync_status = 'pending', sync_error = '' WHERE sync_status = 'active'`,
		// Caches of ClickHouse part metadata.
		`TRUNCATE schema_field_stats`,
		`TRUNCATE schema_fractal_stats`,
		`TRUNCATE schema_field_usage`,
		// Archive gap detection compares a hot-store count against Iceberg's. A
		// stale high ch_count reads as "no gap" for every day already archived.
		`TRUNCATE archive_completeness`,
		`UPDATE fractals SET log_count = 0, size_bytes = 0, earliest_log = NULL, latest_log = NULL`,
		`UPDATE analytics_models SET last_scored_at = NULL`,
		// An in-flight restore would resume mid-window against an empty target.
		`UPDATE archive_restore_jobs SET status = 'canceled' WHERE status IN ('running', 'pending')`,
	}
}
