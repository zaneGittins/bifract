package storage

import (
	"context"
	"embed"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Shard-aware schema provisioning. Only deployments with Topology.PerNodeAdmin
// reach any of this: every decision is made per shard against a direct
// connection, because a load-balanced answer describes only whichever node the
// driver picked, which is how a silently half-provisioned cluster happens.

// shardSchemaState is one shard's provisioning status, read from that shard directly.
type shardSchemaState struct {
	addr         string
	reachable    bool
	hasLogs      bool
	migrationMax uint32
}

// initializeShards provisions every shard independently.
//
// A cluster is not a single thing to initialize: each shard has its own local tables and
// its own _bifract_migrations (the replica path is keyed by {shard}), so "is this a fresh
// install?" and "which migrations are applied?" have per-shard answers. Deciding once from
// the load-balanced connection answers for whichever node the driver picked and silently
// leaves the rest unprovisioned, which stays invisible until a query fans out.
//
// ON CLUSTER is avoided throughout: it queues DDL through Keeper and times out on a
// cluster that is still coming up, which is the very situation that produces a
// half-provisioned cluster. The same idempotent DDL run directly on each host reaches the
// same end state without the queue.
func (c *ClickHouseClient) initializeShards(ctx context.Context, sql string, migrations embed.FS, migrationsDir string) error {
	pool := adminNodePool
	states := c.probeShards(ctx, pool)

	needsSchema, anyReachable := false, false
	var refMax uint32
	for _, st := range states {
		if !st.reachable {
			continue
		}
		anyReachable = true
		if !st.hasLogs {
			needsSchema = true
		}
		if st.migrationMax > refMax {
			refMax = st.migrationMax
		}
	}
	// Never report the schema ready off an all-unreachable probe: that would let the
	// server start serving against a cluster whose state was never established.
	if !anyReachable {
		return fmt.Errorf("no ClickHouse shard reachable for schema initialization")
	}

	// A shard with no schema breaks reads and the ingest pool's health check as soon as the
	// write LB routes to it, so provision synchronously and fail loudly: the pod restarts
	// and retries, which is self-healing. When every shard already has a schema this is an
	// upgrade, so run it in the background where a slow migration cannot become a
	// CrashLoopBackOff.
	if needsSchema {
		if err := c.syncShardSchemas(ctx, states, refMax, sql, migrations, migrationsDir, pool); err != nil {
			return err
		}
		log.Printf("Cluster schema sync complete")
		c.markSchemaReady()
		return nil
	}

	go func() {
		if err := c.syncShardSchemas(ctx, states, refMax, sql, migrations, migrationsDir, pool); err != nil {
			log.Printf("Warning: cluster schema sync: %v", err)
		}
		log.Printf("Cluster schema sync complete")
		c.markSchemaReady()
	}()
	return nil
}

// probeShards reports each shard's schema state. An unreachable shard is marked and
// skipped rather than failing the probe, so one node being briefly down during a rolling
// restart does not stop the others from being repaired.
func (c *ClickHouseClient) probeShards(ctx context.Context, pool ClickHousePoolConfig) []shardSchemaState {
	states := make([]shardSchemaState, 0, len(c.addrs))
	for _, addr := range c.addrs {
		st := shardSchemaState{addr: addr}
		conn, err := openClickHouseConn(c.nodeConnOptions(addr, pool))
		if err != nil {
			log.Printf("Warning: schema probe of %s failed: %v", addr, err)
			states = append(states, st)
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		hasLogs, err := chTableExists(probeCtx, conn, "logs")
		if err != nil {
			log.Printf("Warning: schema probe of %s failed: %v", addr, err)
			cancel()
			conn.Close()
			states = append(states, st)
			continue
		}
		st.reachable = true
		st.hasLogs = hasLogs
		// A missing _bifract_migrations table and an empty one mean the same thing here:
		// nothing recorded on this shard.
		var maxNum uint32
		if err := conn.QueryRow(probeCtx, "SELECT max(number) FROM logs._bifract_migrations").Scan(&maxNum); err == nil {
			st.migrationMax = maxNum
		}
		cancel()
		conn.Close()
		states = append(states, st)
	}
	return states
}

// syncShardSchemas brings each shard to the current schema and creates the Distributed
// tables on it. Safe to re-run: every branch is idempotent.
func (c *ClickHouseClient) syncShardSchemas(ctx context.Context, states []shardSchemaState, refMax uint32, sql string, migrations embed.FS, migrationsDir string, pool ClickHousePoolConfig) error {
	// Migrations apply only when every shard is reachable, so one is never applied to a
	// subset and left divergent. Schema repair and Distributed table creation are
	// per-node and idempotent, so they still run.
	shardsOK := true
	if err := c.ensureAllShardsReachable(ctx, pool); err != nil {
		shardsOK = false
		log.Printf("Skipping cluster migration apply (self-heals on next restart): %v", err)
	}

	distStmts := c.distributedTableDDL()

	for _, st := range states {
		if !st.reachable {
			continue
		}
		conn, err := openClickHouseConn(c.nodeConnOptions(st.addr, pool))
		if err != nil {
			log.Printf("Warning: cluster schema sync to %s failed: %v", st.addr, err)
			continue
		}

		// Checked per shard, not once through the load balancer: a cluster-wide answer
		// describes only whichever node the driver picked. Fatal, so a half-reset
		// cluster cannot come up serving from the shards that were missed.
		if st.hasLogs {
			if err := checkPartitionKey(ctx, conn); err != nil {
				conn.Close()
				return fmt.Errorf("shard %s: %w", st.addr, err)
			}
		}

		switch {
		case !st.hasLogs:
			// No schema on this shard. Provision it from the init SQL, then stamp the
			// migrations that schema already embodies. Fatal on failure: a cluster serving
			// with a shard missing returns short results rather than an error.
			log.Printf("Provisioning schema on shard %s", st.addr)
			if err := runInitSQLOnConn(ctx, conn, sql, c.RewriteEngine); err != nil {
				conn.Close()
				return fmt.Errorf("provision schema on %s: %w", st.addr, err)
			}
			if err := setClickHouseMigrationsBaseline(ctx, conn, c.RewriteEngine, migrations, migrationsDir, 0); err != nil {
				conn.Close()
				return fmt.Errorf("baseline migrations on %s: %w", st.addr, err)
			}
		case st.migrationMax == 0 && refMax > 0:
			// Schema present but nothing recorded here, while a sibling shard has records:
			// this shard was provisioned by the init SQL and only the stamp was missed.
			// Replaying history against an already-current schema does not work (004 selects
			// raw_log, which 013 removed), so record the sibling's level instead.
			log.Printf("Stamping shard %s through migration %d (schema present, none recorded)", st.addr, refMax)
			if err := setClickHouseMigrationsBaseline(ctx, conn, c.RewriteEngine, migrations, migrationsDir, refMax); err != nil {
				log.Printf("Warning: stamp migrations on %s: %v", st.addr, err)
			}
		case shardsOK:
			n, err := runMigrationsOnConn(ctx, conn, c.RewriteEngine, true, migrations, migrationsDir)
			if err != nil {
				log.Printf("Warning: migration sync on %s: %v", st.addr, err)
			} else if n > 0 {
				log.Printf("Applied %d ClickHouse migration(s) to shard %s", n, st.addr)
			}
		}

		for _, stmt := range distStmts {
			stmtCtx, stmtCancel := context.WithTimeout(ctx, 30*time.Second)
			conn.Exec(stmtCtx, stmt)
			stmtCancel()
		}
		// Distributed tables are created "AS <local>" and do not inherit later
		// ALTER ADD COLUMN, so reconcile any columns a migration added to the local table
		// onto the Distributed variants.
		for _, pair := range [][2]string{
			{"logs_distributed", "logs"},
			{"logs_histogram_distributed", "logs_histogram"},
			{"logs_hot_distributed", "logs_hot"},
			{"proc_lineage_distributed", "proc_lineage"},
			{"proc_freq_distributed", "proc_freq"},
			{"process_edges_distributed", "process_edges"},
			{"logs_raw_distributed", "logs_raw"},
		} {
			syncCtx, syncCancel := context.WithTimeout(ctx, 30*time.Second)
			if err := syncDistributedColumns(syncCtx, conn, c.Database, pair[0], pair[1]); err != nil {
				log.Printf("Warning: column sync %s on %s: %v", pair[0], st.addr, err)
			}
			syncCancel()
		}
		// syncDistributedColumns only adds/modifies columns; it never drops. After migration
		// 013 removes raw_log from the local logs/logs_hot tables, the Distributed variants
		// still carry it, and an INSERT through a Distributed table forwards its columns to
		// the local table (which no longer has raw_log). Drop it explicitly so the two stay
		// in lockstep. Runs after sync so it is the final word.
		for _, dt := range []string{"logs_distributed", "logs_hot_distributed"} {
			dropCtx, dropCancel := context.WithTimeout(ctx, 30*time.Second)
			conn.Exec(dropCtx, "ALTER TABLE "+dt+" DROP COLUMN IF EXISTS raw_log")
			dropCancel()
		}
		conn.Close()
	}
	return nil
}

// distributedTableDDL returns the idempotent CREATE for every Distributed table, to be
// run directly on each host. These cannot live in the init SQL because they need the
// cluster name, which is only known to the running server.
func (c *ClickHouseClient) distributedTableDDL() []string {
	locals := []string{"logs", "logs_histogram", "logs_hot", "proc_lineage", "proc_freq", "process_edges", "logs_raw"}
	stmts := make([]string, 0, len(locals))
	for _, t := range locals {
		stmts = append(stmts, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s_distributed AS %s ENGINE = Distributed('%s', currentDatabase(), '%s', rand())",
			t, t, EscCHStr(c.topo.DDLCluster), t))
	}
	return stmts
}

// syncDistributedColumns brings a Distributed table's column definitions in line
// with its underlying local table. Distributed tables are created with
// "AS <local>", which snapshots the structure at creation time; a later
// ALTER TABLE <local> ADD COLUMN does not propagate. Two failures follow:
//
//  1. Missing column: inserts/reads that reference the new column fail with
//     "No such column ... in table <dist>" (code 16).
//  2. Missing DEFAULT: when an INSERT omits a column, the Distributed engine
//     materializes it from *its own* column default before forwarding to the
//     local shard, then sends that value explicitly -- so a Distributed column
//     added type-only writes an empty string that overrides the local table's
//     DEFAULT (e.g. norm_log's DEFAULT toString(fields) never fires, leaving
//     norm_log empty on every new row).
//
// Because a Distributed table stores no data, adding/altering columns is free
// and metadata-only. This reconciles both cases generically from the local
// table's system.columns, so any current or future schema change (column and
// its DEFAULT/MATERIALIZED/ALIAS expression) auto-propagates here without each
// migration having to remember the Distributed variants.
func syncDistributedColumns(ctx context.Context, conn driver.Conn, database, distTable, localTable string) error {
	type colDef struct {
		typ, kind, expr string
	}
	load := func(table string) (map[string]colDef, []string, error) {
		rows, qerr := conn.Query(ctx, `
			SELECT name, type, default_kind, default_expression
			FROM system.columns
			WHERE database = ? AND table = ?
			ORDER BY position`, database, table)
		if qerr != nil {
			return nil, nil, qerr
		}
		defer rows.Close()
		cols := make(map[string]colDef)
		var order []string
		for rows.Next() {
			var name, typ, kind, expr string
			if serr := rows.Scan(&name, &typ, &kind, &expr); serr != nil {
				return nil, nil, serr
			}
			cols[name] = colDef{typ: typ, kind: kind, expr: expr}
			order = append(order, name)
		}
		return cols, order, rows.Err()
	}

	local, localOrder, err := load(localTable)
	if err != nil {
		return err
	}
	dist, _, err := load(distTable)
	if err != nil {
		return err
	}

	// defaultClause renders the "DEFAULT <expr>" (or MATERIALIZED/ALIAS) suffix,
	// or "" when the column has no default. CODEC/TTL are deliberately omitted:
	// a Distributed table stores no data, so they are meaningless there.
	defaultClause := func(c colDef) string {
		if c.kind == "" || c.expr == "" {
			return ""
		}
		return " " + c.kind + " " + c.expr
	}

	var changed int
	for _, name := range localOrder {
		// Skip fields here: it needs the same type-hint list as logs, not just
		// column presence, so it's reconciled by ReconcileSchemaFields instead.
		if name == "fields" {
			continue
		}
		lc := local[name]
		dc, exists := dist[name]
		if !exists {
			stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS `%s` %s%s",
				distTable, name, lc.typ, defaultClause(lc))
			if err := conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("add column %s to %s: %w", name, distTable, err)
			}
			changed++
			continue
		}
		// Column present: repair a drifted or absent default so the Distributed
		// layer materializes omitted columns identically to the local table.
		if dc.kind != lc.kind || dc.expr != lc.expr {
			stmt := fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN `%s` %s%s",
				distTable, name, lc.typ, defaultClause(lc))
			if err := conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("modify column %s on %s: %w", name, distTable, err)
			}
			changed++
		}
	}
	if changed > 0 {
		log.Printf("Reconciled %d column(s) on %s", changed, distTable)
	}
	return nil
}

// ensureAllShardsReachable verifies every shard address accepts a trivial query.
// It gates cluster migrations: applying to only the reachable subset would leave
// shards on divergent schemas. Returns an error naming the first unreachable shard.
func (c *ClickHouseClient) ensureAllShardsReachable(ctx context.Context, pool ClickHousePoolConfig) error {
	for _, addr := range c.addrs {
		conn, err := openClickHouseConn(c.nodeConnOptions(addr, pool))
		if err != nil {
			return fmt.Errorf("shard %s unreachable: %w", addr, err)
		}
		pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		var one uint8
		err = conn.QueryRow(pingCtx, "SELECT 1").Scan(&one)
		cancel()
		conn.Close()
		if err != nil {
			return fmt.Errorf("shard %s not ready: %w", addr, err)
		}
	}
	return nil
}
