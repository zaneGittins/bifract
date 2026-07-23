package storage

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickHouseClient struct {
	conn     driver.Conn
	addrs    []string // host:port addresses
	User     string
	Password string
	Database string
	Cluster  string // ClickHouse cluster name; empty for single-node deployments

	// Shard-direct lookup (cluster mode only). shardHosts caches shard_num -> host:port
	// from system.clusters so detail queries can bypass the Distributed fan-out.
	shardHostsMu sync.RWMutex
	shardHosts   map[uint64]string

	shardConnsMu sync.Mutex
	shardConns   map[uint64]driver.Conn

	// typeHintFields caches the declared typed sub-paths of the fields JSON column
	// (schema type hints). Those materialize as "" even when a log did not contain
	// them, so they are stripped from log-detail output while genuinely-empty dynamic
	// fields are kept. Invalidated when ReconcileSchemaFields changes the hints.
	typeHintMu     sync.RWMutex
	typeHintFields map[string]bool

	// insertSettings are optional per-insert ClickHouse settings (group-by spill,
	// async_insert) applied to InsertLogs. Computed once at Initialize against the
	// server's supported setting names, then read-only. nil/empty means none.
	insertSettings clickhouse.Settings

	// schemaReady is closed when Initialize has finished applying migrations and
	// creating tables. In cluster mode migrations run in a background goroutine, so
	// callers that must observe the post-migration schema (e.g. the endpoint-analysis
	// MV reconcile) wait on this via WaitForSchemaReady before acting.
	schemaReady     chan struct{}
	schemaReadyOnce sync.Once
}

// markSchemaReady signals that Initialize's schema work is complete (idempotent).
func (c *ClickHouseClient) markSchemaReady() {
	c.schemaReadyOnce.Do(func() {
		if c.schemaReady != nil {
			close(c.schemaReady)
		}
	})
}

// WaitForSchemaReady blocks until Initialize's schema work is complete or ctx is done.
// Safe to call before Initialize; returns immediately if the signal was never wired.
func (c *ClickHouseClient) WaitForSchemaReady(ctx context.Context) {
	if c.schemaReady == nil {
		return
	}
	select {
	case <-c.schemaReady:
	case <-ctx.Done():
	}
}

// Addrs returns the host:port addresses this client connects to.
func (c *ClickHouseClient) Addrs() []string { return c.addrs }

// HTTPAddr returns the first host with port 8123 (ClickHouse HTTP interface).
// The native addrs use port 9000; the HTTP interface is always on 8123.
func (c *ClickHouseClient) HTTPAddr() string {
	if len(c.addrs) == 0 {
		return "localhost:8123"
	}
	host := c.addrs[0]
	if idx := strings.LastIndex(host, ":"); idx != -1 {
		host = host[:idx]
	}
	return host + ":8123"
}

// IsCluster returns true when the client is configured for a replicated cluster.
func (c *ClickHouseClient) IsCluster() bool {
	return c.Cluster != ""
}

// OnClusterSQL returns the ON CLUSTER clause for DDL statements, or an empty
// string for single-node deployments.
func (c *ClickHouseClient) OnClusterSQL() string {
	if c.Cluster == "" {
		return ""
	}
	return " ON CLUSTER '" + EscCHStr(c.Cluster) + "'"
}

// ReadTable returns the table name for read queries. In cluster mode this is
// "logs_distributed" for cross-shard reads; in single-node mode it is "logs".
func (c *ClickHouseClient) ReadTable() string {
	if c.Cluster != "" {
		return "logs_distributed"
	}
	return "logs"
}

// WriteTable returns the table name for insert queries. In cluster mode this is
// "logs_distributed" so the Distributed engine shards writes across all nodes;
// in single-node mode it is "logs".
func (c *ClickHouseClient) WriteTable() string {
	if c.Cluster != "" {
		return "logs_distributed"
	}
	return "logs"
}

// HistogramReadTable returns the table name for pre-aggregated histogram reads.
// In cluster mode this fans out to all shards via logs_histogram_distributed.
func (c *ClickHouseClient) HistogramReadTable() string {
	if c.Cluster != "" {
		return "logs_histogram_distributed"
	}
	return "logs_histogram"
}

// HotReadTable returns the table name for hot-path alert queries (recent cursor).
// In cluster mode this fans out to all shards via logs_hot_distributed.
func (c *ClickHouseClient) HotReadTable() string {
	if c.Cluster != "" {
		return "logs_hot_distributed"
	}
	return "logs_hot"
}

// ProcLineageReadTable returns the table name for ptg() process-tree traversal.
// In cluster mode this fans out to all shards via proc_lineage_distributed so a
// recursive hop gathers a process's children/parents from every shard (rows are
// rand-placed, following their source log row).
func (c *ClickHouseClient) ProcLineageReadTable() string {
	if c.Cluster != "" {
		return "proc_lineage_distributed"
	}
	return "proc_lineage"
}

// ProcEdgesReadTable returns the table for pgr()'s file/net/dns edge fetch. Cluster mode fans out
// to all shards via process_edges_distributed; a process's edges may be split across shards (each
// shard's MV writes locally), so the reader must GROUP BY to merge the AggregatingMergeTree
// partials across shards -- pgr's edgesAgg already does.
func (c *ClickHouseClient) ProcEdgesReadTable() string {
	if c.Cluster != "" {
		return "process_edges_distributed"
	}
	return "process_edges"
}

// ReconcileProcLineageTTL applies a configured retention (in days) to proc_lineage via a
// metadata-only ALTER ... MODIFY TTL. proc_lineage is decoupled from logs retention and
// kept long for DFIR (year-old process trees are common), so operators tune it with
// BIFRACT_PROC_LINEAGE_TTL_DAYS without editing DDL. Re-applying the same expression is a
// cheap no-op. days <= 0 leaves the DDL default (730 days) in place.
func (c *ClickHouseClient) ReconcileProcLineageTTL(ctx context.Context, days int) error {
	if days <= 0 {
		return nil
	}
	// materialize_ttl_after_modify = 0: metadata-only change, no part-rewriting mutation
	// (which would run on every startup the env var is set). New/merged parts adopt the TTL.
	stmt := fmt.Sprintf(
		"ALTER TABLE proc_lineage%s MODIFY TTL toDateTime(timestamp) + INTERVAL %d DAY SETTINGS materialize_ttl_after_modify = 0",
		c.OnClusterSQL(), days,
	)
	return c.conn.Exec(ctx, stmt)
}

// ProcFreqReadTable returns the read table for the pgr() frequency baseline. In cluster
// mode this fans out to all shards via proc_freq_distributed; the scoring join re-aggregates
// the AggregatingMergeTree state across shards (sum / groupUniqArrayMerge).
func (c *ClickHouseClient) ProcFreqReadTable() string {
	if c.Cluster != "" {
		return "proc_freq_distributed"
	}
	return "proc_freq"
}

// ReconcileProcFreqTTL applies a configured retention (in days) to proc_freq via a
// metadata-only ALTER ... MODIFY TTL. proc_freq is the aggregated behavioral baseline; a
// longer window gives more stable rarity for infrequent-but-normal behavior. Tuned via
// BIFRACT_PROC_FREQ_TTL_DAYS. days <= 0 leaves the DDL default (730 days) in place.
func (c *ClickHouseClient) ReconcileProcFreqTTL(ctx context.Context, days int) error {
	if days <= 0 {
		return nil
	}
	// metadata-only (see ReconcileProcLineageTTL): no part-rewriting mutation on startup.
	stmt := fmt.Sprintf(
		"ALTER TABLE proc_freq%s MODIFY TTL day + INTERVAL %d DAY SETTINGS materialize_ttl_after_modify = 0",
		c.OnClusterSQL(), days,
	)
	return c.conn.Exec(ctx, stmt)
}

// rewriteEngineRe matches ENGINE = MergeTree(), ReplacingMergeTree(), SummingMergeTree(), or AggregatingMergeTree(args).
var rewriteEngineRe = regexp.MustCompile(`(?i)ENGINE\s*=\s*(MergeTree|ReplacingMergeTree|SummingMergeTree|AggregatingMergeTree)\s*\(([^)]*)\)`)

// RewriteEngine replaces single-node table engines with their replicated
// equivalents when cluster mode is active. Returns the input unchanged for
// single-node deployments.
func (c *ClickHouseClient) RewriteEngine(sql string) string {
	if c.Cluster == "" {
		return sql
	}
	return rewriteEngineRe.ReplaceAllStringFunc(sql, func(match string) string {
		sub := rewriteEngineRe.FindStringSubmatch(match)
		if len(sub) < 3 {
			return match
		}
		engineName := strings.ToUpper(sub[1])
		innerArgs := strings.TrimSpace(sub[2])
		replicaPath := "'/clickhouse/tables/{shard}/{database}/{table}', '{replica}'"
		switch engineName {
		case "REPLACINGMERGETREE":
			if innerArgs == "" {
				return "ENGINE = ReplicatedReplacingMergeTree(" + replicaPath + ")"
			}
			return "ENGINE = ReplicatedReplacingMergeTree(" + replicaPath + ", " + innerArgs + ")"
		case "SUMMINGMERGETREE":
			if innerArgs == "" {
				return "ENGINE = ReplicatedSummingMergeTree(" + replicaPath + ")"
			}
			return "ENGINE = ReplicatedSummingMergeTree(" + replicaPath + ", " + innerArgs + ")"
		case "AGGREGATINGMERGETREE":
			if innerArgs == "" {
				return "ENGINE = ReplicatedAggregatingMergeTree(" + replicaPath + ")"
			}
			return "ENGINE = ReplicatedAggregatingMergeTree(" + replicaPath + ", " + innerArgs + ")"
		default:
			return "ENGINE = ReplicatedMergeTree(" + replicaPath + ")"
		}
	})
}

// injectOnClusterRe are precompiled patterns for DDL statement prefixes.
// Each captures the portion before where the ON CLUSTER clause belongs.
var injectOnClusterPatterns = []struct {
	prefix string
	re     *regexp.Regexp
}{
	{"CREATE MATERIALIZED VIEW", regexp.MustCompile(`(?i)(CREATE\s+MATERIALIZED\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?\S+)`)},
	{"CREATE TABLE", regexp.MustCompile(`(?i)(CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?\S+)`)},
	{"ALTER TABLE", regexp.MustCompile(`(?i)(ALTER\s+TABLE\s+\S+)`)},
	{"TRUNCATE", regexp.MustCompile(`(?i)(TRUNCATE\s+TABLE\s+(?:IF\s+EXISTS\s+)?\S+)`)},
	{"DROP TABLE", regexp.MustCompile(`(?i)(DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?\S+)`)},
	{"DROP VIEW", regexp.MustCompile(`(?i)(DROP\s+VIEW\s+(?:IF\s+EXISTS\s+)?\S+)`)},
	{"CREATE OR REPLACE DICTIONARY", regexp.MustCompile(`(?i)(CREATE\s+(?:OR\s+REPLACE\s+)?DICTIONARY\s+(?:IF\s+NOT\s+EXISTS\s+)?\S+)`)},
	{"CREATE DICTIONARY", regexp.MustCompile(`(?i)(CREATE\s+(?:OR\s+REPLACE\s+)?DICTIONARY\s+(?:IF\s+NOT\s+EXISTS\s+)?\S+)`)},
	{"DROP DICTIONARY", regexp.MustCompile(`(?i)(DROP\s+DICTIONARY\s+(?:IF\s+EXISTS\s+)?\S+)`)},
}

// InjectOnCluster inserts an ON CLUSTER clause into CREATE TABLE, ALTER TABLE,
// TRUNCATE TABLE, and DROP TABLE statements. No-op for single-node deployments.
func (c *ClickHouseClient) InjectOnCluster(sql string) string {
	if c.Cluster == "" {
		return sql
	}
	upper := strings.ToUpper(strings.TrimSpace(sql))
	for _, p := range injectOnClusterPatterns {
		if strings.HasPrefix(upper, p.prefix) {
			loc := p.re.FindStringIndex(sql)
			if loc != nil {
				// Insert ON CLUSTER clause directly after the matched prefix.
				// Avoids ReplaceAllString to prevent regex replacement char
				// interpretation (e.g. $ in cluster names).
				return sql[:loc[1]] + c.OnClusterSQL() + sql[loc[1]:]
			}
			break
		}
	}
	return sql
}

type LogEntry struct {
	Timestamp       time.Time
	IngestTimestamp time.Time
	RawLog          string
	LogID           string
	Fields          map[string]string
	FractalID       string // Fractal UUID for multi-tenant isolation
	Normalizer      string // "name@version" of the normalizer applied, empty if none
}

// Initialize ensures the ClickHouse schema is current.
//
// Fresh install (logs table absent): runs the full init SQL then marks all
// migrations as baseline so subsequent restarts skip them entirely.
//
// Upgrade (logs table present):
//   - Single-node: applies only unapplied numbered migrations; skips init SQL.
//   - Cluster: spawns a goroutine that connects to each shard directly and
//     applies only its pending migrations, avoiding ON CLUSTER timeouts during
//     rolling restarts. Distributed table creation runs idempotently each time.
func (c *ClickHouseClient) Initialize(ctx context.Context, sql string, migrations embed.FS, migrationsDir string) error {
	var count uint64
	if err := c.conn.QueryRow(ctx, `SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'logs'`).Scan(&count); err != nil {
		return fmt.Errorf("failed to check clickhouse initialization: %w", err)
	}
	tableExists := count > 0

	if !tableExists {
		// Fresh install: apply full init SQL, then create distributed tables and
		// mark all migrations as baseline so upgrades only run future deltas.
		for _, stmt := range splitClickHouseSQL(sql) {
			stmt = c.InjectOnCluster(stmt)
			stmt = c.RewriteEngine(stmt)
			if err := c.conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("failed to execute clickhouse init statement: %w\nstatement: %s", err, stmt)
			}
		}
		if c.IsCluster() {
			distSQL := fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS logs_distributed%s AS logs ENGINE = Distributed('%s', currentDatabase(), 'logs', rand())",
				c.OnClusterSQL(), EscCHStr(c.Cluster),
			)
			if err := c.conn.Exec(ctx, distSQL); err != nil {
				return fmt.Errorf("failed to create distributed table: %w\nstatement: %s", err, distSQL)
			}
			histDistSQL := fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS logs_histogram_distributed%s AS logs_histogram ENGINE = Distributed('%s', currentDatabase(), 'logs_histogram', rand())",
				c.OnClusterSQL(), EscCHStr(c.Cluster),
			)
			if err := c.conn.Exec(ctx, histDistSQL); err != nil {
				return fmt.Errorf("failed to create histogram distributed table: %w\nstatement: %s", err, histDistSQL)
			}
			hotDistSQL := fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS logs_hot_distributed%s AS logs_hot ENGINE = Distributed('%s', currentDatabase(), 'logs_hot', rand())",
				c.OnClusterSQL(), EscCHStr(c.Cluster),
			)
			if err := c.conn.Exec(ctx, hotDistSQL); err != nil {
				return fmt.Errorf("failed to create hot distributed table: %w\nstatement: %s", err, hotDistSQL)
			}
			procDistSQL := fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS proc_lineage_distributed%s AS proc_lineage ENGINE = Distributed('%s', currentDatabase(), 'proc_lineage', rand())",
				c.OnClusterSQL(), EscCHStr(c.Cluster),
			)
			if err := c.conn.Exec(ctx, procDistSQL); err != nil {
				return fmt.Errorf("failed to create proc_lineage distributed table: %w\nstatement: %s", err, procDistSQL)
			}
			freqDistSQL := fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS proc_freq_distributed%s AS proc_freq ENGINE = Distributed('%s', currentDatabase(), 'proc_freq', rand())",
				c.OnClusterSQL(), EscCHStr(c.Cluster),
			)
			if err := c.conn.Exec(ctx, freqDistSQL); err != nil {
				return fmt.Errorf("failed to create proc_freq distributed table: %w\nstatement: %s", err, freqDistSQL)
			}
		}
		setClickHouseMigrationsBaseline(ctx, c.conn, c.RewriteEngine, migrations, migrationsDir)
		c.markSchemaReady()
		return nil
	}

	if c.IsCluster() {
		// Cluster upgrade: apply only pending migrations to each shard individually.
		// ON CLUSTER can timeout when shards are restarting; per-shard direct
		// connections are reliable. Distributed table creation is idempotent.
		distSQL := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS logs_distributed AS logs ENGINE = Distributed('%s', currentDatabase(), 'logs', rand())",
			EscCHStr(c.Cluster),
		)
		histDistSQL := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS logs_histogram_distributed AS logs_histogram ENGINE = Distributed('%s', currentDatabase(), 'logs_histogram', rand())",
			EscCHStr(c.Cluster),
		)
		hotDistSQL := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS logs_hot_distributed AS logs_hot ENGINE = Distributed('%s', currentDatabase(), 'logs_hot', rand())",
			EscCHStr(c.Cluster),
		)
		procDistSQL := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS proc_lineage_distributed AS proc_lineage ENGINE = Distributed('%s', currentDatabase(), 'proc_lineage', rand())",
			EscCHStr(c.Cluster),
		)
		freqDistSQL := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS proc_freq_distributed AS proc_freq ENGINE = Distributed('%s', currentDatabase(), 'proc_freq', rand())",
			EscCHStr(c.Cluster),
		)
		edgeDistSQL := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS process_edges_distributed AS process_edges ENGINE = Distributed('%s', currentDatabase(), 'process_edges', rand())",
			EscCHStr(c.Cluster),
		)
		go func() {
			initPool := ClickHousePoolConfig{MaxOpenConns: 1, MaxIdleConns: 1, DialTimeout: 10 * time.Second}

			// Consistency gate: only apply migrations when every shard is reachable, so a
			// migration is never applied to a subset of shards (which would leave the
			// cluster on divergent schemas). If any shard is down we skip the apply this
			// cycle; it self-heals on the next pod restart once all shards are back.
			// Distributed table creation and column reconciliation below still run per
			// reachable node, since they are idempotent and metadata-only.
			shardsOK := true
			if err := ensureAllShardsReachable(ctx, c.addrs, c.Database, c.User, c.Password, initPool); err != nil {
				shardsOK = false
				log.Printf("Skipping cluster migration apply (self-heals on next restart): %v", err)
			}

			for _, addr := range c.addrs {
				hostConn, err := openClickHouseConn([]string{addr}, c.Database, c.User, c.Password, initPool)
				if err != nil {
					log.Printf("Warning: cluster migration sync to %s failed: %v", addr, err)
					continue
				}
				if shardsOK {
					n, err := runMigrationsOnConn(ctx, hostConn, c.RewriteEngine, true, migrations, migrationsDir)
					if err != nil {
						log.Printf("Warning: migration sync on %s: %v", addr, err)
					} else if n > 0 {
						log.Printf("Applied %d ClickHouse migration(s) to shard %s", n, addr)
					}
				}
				for _, stmt := range []string{distSQL, histDistSQL, hotDistSQL, procDistSQL, freqDistSQL, edgeDistSQL} {
					stmtCtx, stmtCancel := context.WithTimeout(ctx, 30*time.Second)
					hostConn.Exec(stmtCtx, stmt)
					stmtCancel()
				}
				// Distributed tables are created "AS <local>" and do not inherit
				// later ALTER ADD COLUMN, so reconcile any columns a migration
				// added to the local table onto the Distributed variants.
				for _, pair := range [][2]string{
					{"logs_distributed", "logs"},
					{"logs_histogram_distributed", "logs_histogram"},
					{"logs_hot_distributed", "logs_hot"},
					{"proc_lineage_distributed", "proc_lineage"},
					{"proc_freq_distributed", "proc_freq"},
					{"process_edges_distributed", "process_edges"},
				} {
					syncCtx, syncCancel := context.WithTimeout(ctx, 30*time.Second)
					if err := syncDistributedColumns(syncCtx, hostConn, c.Database, pair[0], pair[1]); err != nil {
						log.Printf("Warning: column sync %s on %s: %v", pair[0], addr, err)
					}
					syncCancel()
				}
				hostConn.Close()
			}
			log.Printf("Cluster schema sync complete")
			c.markSchemaReady()
		}()
		return nil
	}

	// Single-node upgrade: apply only pending migrations. Retry with backoff so a
	// transient failure (ClickHouse still warming up, a briefly slow statement) does
	// not turn into a pod CrashLoopBackOff. Per-statement progress means each retry
	// resumes where the last left off rather than repeating completed work.
	var n int
	var err error
	backoff := 5 * time.Second
	for attempt := 1; attempt <= migrationMaxAttempts; attempt++ {
		n, err = runMigrationsOnConn(ctx, c.conn, nil, false, migrations, migrationsDir)
		if err == nil {
			break
		}
		if attempt == migrationMaxAttempts {
			return fmt.Errorf("clickhouse migrations (after %d attempts): %w", attempt, err)
		}
		log.Printf("ClickHouse migration attempt %d/%d failed, retrying in %s: %v", attempt, migrationMaxAttempts, backoff, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 60*time.Second {
			backoff *= 2
		}
	}
	if n > 0 {
		log.Printf("Applied %d ClickHouse migration(s)", n)
	}
	c.markSchemaReady()
	return nil
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

type chMigrationEntry struct {
	number int
	name   string
	sql    string
}

func loadClickHouseMigrations(fsys embed.FS, dir string) ([]chMigrationEntry, error) {
	var migrations []chMigrationEntry
	err := fs.WalkDir(fsys, dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if !strings.HasSuffix(path, ".sql") {
			return nil
		}
		name := filepath.Base(path)
		parts := strings.SplitN(name, "_", 2)
		if len(parts) < 2 {
			return nil
		}
		num, convErr := strconv.Atoi(parts[0])
		if convErr != nil {
			return nil
		}
		content, readErr := fsys.ReadFile(path)
		if readErr != nil {
			return fmt.Errorf("read migration %s: %w", path, readErr)
		}
		migrations = append(migrations, chMigrationEntry{
			number: num,
			name:   strings.TrimSuffix(name, ".sql"),
			sql:    string(content),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].number < migrations[j].number
	})
	return migrations, nil
}

// migrationMaxAttempts bounds single-node migration retries before giving up.
const migrationMaxAttempts = 5

// migrationStmtTimeout is the per-statement deadline for ClickHouse migrations.
// Migrations can legitimately run for minutes (replicated ALTERs waiting on Keeper,
// column/index materializations, future backfills), so the default is intentionally
// large; it exists only to break a genuinely hung statement, not to bound normal
// work. The old 30s deadline forced any slow-but-healthy statement to time out, which
// (since a file is only recorded as applied after all its statements succeed) made the
// whole migration re-run from statement 1 on every restart. Override in seconds with
// BIFRACT_MIGRATION_STMT_TIMEOUT.
func migrationStmtTimeout() time.Duration {
	const def = 30 * time.Minute
	if v := os.Getenv("BIFRACT_MIGRATION_STMT_TIMEOUT"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return def
}

// loadCompletedSteps returns the set of statement indices already applied for a
// migration file, so an interrupted run resumes instead of re-executing earlier
// statements. This narrows, but does not close, the re-execution window: a statement
// that succeeds but whose recordCompletedStep does not commit (process crash between
// the two, or two replicas of a shard racing on the shared bookkeeping table) will run
// again on restart. That is safe for the idempotent CREATE/ALTER ... IF [NOT] EXISTS
// statements every current migration uses; a future non-idempotent backfill
// (INSERT...SELECT) MUST be written idempotently (guarded / dedup'd target) and cannot
// rely on this checkpoint for exactly-once.
func loadCompletedSteps(ctx context.Context, conn driver.Conn, number int) (map[int]bool, error) {
	rows, err := conn.Query(ctx, "SELECT statement_index FROM logs._bifract_migration_steps WHERE number = ?", uint32(number))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	done := make(map[int]bool)
	for rows.Next() {
		var idx uint32
		if err := rows.Scan(&idx); err != nil {
			return nil, err
		}
		done[int(idx)] = true
	}
	return done, rows.Err()
}

// recordCompletedStep marks one statement of a migration file as applied.
func recordCompletedStep(ctx context.Context, conn driver.Conn, number, statementIndex int, name string) error {
	record := fmt.Sprintf("INSERT INTO logs._bifract_migration_steps (number, statement_index, name) VALUES (%d, %d, '%s')",
		number, statementIndex, strings.ReplaceAll(name, "'", "''"))
	return conn.Exec(ctx, record)
}

// runMigrationsOnConn applies pending ClickHouse migrations via conn.
// transformStmt, if non-nil, is applied to each DDL statement before execution
// (used in cluster mode to rewrite engine names to their Replicated variants).
// clusterMode runs DDL with alter_sync=0/mutations_sync=0 so an ALTER does not
// block on a restarting replica (the Replicated engine still propagates via Keeper).
// Progress is recorded per statement, so a statement that fails or times out does not
// force the file's already-applied statements to re-run on the next restart.
// Returns the number of migrations applied.
func runMigrationsOnConn(ctx context.Context, conn driver.Conn, transformStmt func(string) string, clusterMode bool, migrations embed.FS, migrationsDir string) (int, error) {
	const createMigrationsTable = `CREATE TABLE IF NOT EXISTS logs._bifract_migrations (
		number UInt32,
		name String,
		applied_at DateTime DEFAULT now()
	) ENGINE = ReplacingMergeTree()
	ORDER BY number`

	// Per-statement bookkeeping: which statements of an in-progress file already
	// succeeded, so a later slow/failed statement does not re-run the earlier ones.
	const createStepsTable = `CREATE TABLE IF NOT EXISTS logs._bifract_migration_steps (
		number UInt32,
		statement_index UInt32,
		name String,
		applied_at DateTime DEFAULT now()
	) ENGINE = ReplacingMergeTree()
	ORDER BY (number, statement_index)`

	for _, ddl := range []string{createMigrationsTable, createStepsTable} {
		if transformStmt != nil {
			ddl = transformStmt(ddl)
		}
		if err := conn.Exec(ctx, ddl); err != nil {
			return 0, fmt.Errorf("create migration bookkeeping table: %w", err)
		}
	}

	var maxApplied uint32
	if err := conn.QueryRow(ctx, "SELECT max(number) FROM logs._bifract_migrations").Scan(&maxApplied); err != nil {
		return 0, fmt.Errorf("query migration state: %w", err)
	}

	allMigrations, err := loadClickHouseMigrations(migrations, migrationsDir)
	if err != nil {
		return 0, err
	}

	stmtTimeout := migrationStmtTimeout()

	applied := 0
	for _, m := range allMigrations {
		if uint32(m.number) <= maxApplied {
			continue
		}

		done, err := loadCompletedSteps(ctx, conn, m.number)
		if err != nil {
			return applied, fmt.Errorf("load migration steps %s: %w", m.name, err)
		}

		// Index by raw split position so it is stable across runs regardless of which
		// statements the filter below skips.
		for i, stmt := range splitClickHouseSQL(m.sql) {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			upper := strings.ToUpper(stmt)
			if !strings.HasPrefix(upper, "CREATE ") &&
				!strings.HasPrefix(upper, "ALTER ") &&
				!strings.HasPrefix(upper, "DROP ") &&
				!strings.HasPrefix(upper, "TRUNCATE ") &&
				!strings.HasPrefix(upper, "INSERT ") &&
				!strings.HasPrefix(upper, "RENAME ") {
				continue
			}
			if done[i] {
				continue
			}
			if transformStmt != nil {
				stmt = transformStmt(stmt)
			}
			execCtx := ctx
			if clusterMode {
				// Do not block migration DDL on replica acknowledgement: during a rolling
				// restart a replica may be temporarily down, and alter_sync=1 (the default)
				// would stall the ALTER until it returns.
				execCtx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
					"alter_sync":     0,
					"mutations_sync": 0,
				}))
			}
			stmtCtx, cancel := context.WithTimeout(execCtx, stmtTimeout)
			execErr := conn.Exec(stmtCtx, stmt)
			cancel()
			if execErr != nil {
				return applied, fmt.Errorf("migration %s (statement %d): %w", m.name, i, execErr)
			}
			if err := recordCompletedStep(ctx, conn, m.number, i, m.name); err != nil {
				return applied, fmt.Errorf("record migration step %s: %w", m.name, err)
			}
		}
		record := fmt.Sprintf("INSERT INTO logs._bifract_migrations (number, name) VALUES (%d, '%s')",
			m.number, strings.ReplaceAll(m.name, "'", "''"))
		if err := conn.Exec(ctx, record); err != nil {
			return applied, fmt.Errorf("record migration %s: %w", m.name, err)
		}
		applied++
	}
	return applied, nil
}

// ensureAllShardsReachable verifies every shard address accepts a trivial query.
// It gates cluster migrations: applying to only the reachable subset would leave
// shards on divergent schemas. Returns an error naming the first unreachable shard.
func ensureAllShardsReachable(ctx context.Context, addrs []string, database, user, password string, pool ClickHousePoolConfig) error {
	for _, addr := range addrs {
		conn, err := openClickHouseConn([]string{addr}, database, user, password, pool)
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

// setClickHouseMigrationsBaseline marks all known migrations as applied without
// running them. Called after a fresh install where init-clickhouse.sql already
// created the full schema, so subsequent restarts skip all current migrations.
func setClickHouseMigrationsBaseline(ctx context.Context, conn driver.Conn, transformStmt func(string) string, migrations embed.FS, migrationsDir string) {
	const createMigrationsTable = `CREATE TABLE IF NOT EXISTS logs._bifract_migrations (
		number UInt32,
		name String,
		applied_at DateTime DEFAULT now()
	) ENGINE = ReplacingMergeTree()
	ORDER BY number`

	tableSQL := createMigrationsTable
	if transformStmt != nil {
		tableSQL = transformStmt(tableSQL)
	}
	if err := conn.Exec(ctx, tableSQL); err != nil {
		log.Printf("Warning: could not create migrations table for baseline: %v", err)
		return
	}

	allMigrations, err := loadClickHouseMigrations(migrations, migrationsDir)
	if err != nil {
		log.Printf("Warning: could not load migrations for baseline: %v", err)
		return
	}
	for _, m := range allMigrations {
		record := fmt.Sprintf("INSERT INTO logs._bifract_migrations (number, name) VALUES (%d, '%s')",
			m.number, strings.ReplaceAll(m.name, "'", "''"))
		if err := conn.Exec(ctx, record); err != nil {
			log.Printf("Warning: could not record baseline migration %s: %v", m.name, err)
		}
	}
}

// splitSQLOnTopLevelSemicolons splits sql into segments on ';' characters that are not
// inside a line comment (-- to end of line), a block comment (/* ... */), or a single-
// quoted string literal. A naive strings.Split(sql, ";") truncates statements when a
// comment or literal contains a semicolon (e.g. "-- writes locally; reads cross-shard"
// inside a CREATE TABLE block), producing unmatched parentheses and a syntax error.
func splitSQLOnTopLevelSemicolons(sql string) []string {
	var stmts []string
	var b strings.Builder
	var inLineComment, inBlockComment, inString bool
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		switch {
		case inLineComment:
			b.WriteByte(c)
			if c == '\n' {
				inLineComment = false
			}
		case inBlockComment:
			b.WriteByte(c)
			if c == '*' && i+1 < len(sql) && sql[i+1] == '/' {
				b.WriteByte('/')
				i++
				inBlockComment = false
			}
		case inString:
			b.WriteByte(c)
			if c == '\\' && i+1 < len(sql) { // backslash-escaped char
				b.WriteByte(sql[i+1])
				i++
			} else if c == '\'' { // closing quote (doubled '' re-opens on next iter)
				inString = false
			}
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			inLineComment = true
			b.WriteByte(c)
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			inBlockComment = true
			b.WriteByte(c)
			b.WriteByte('*')
			i++
		case c == '\'':
			inString = true
			b.WriteByte(c)
		case c == ';':
			stmts = append(stmts, b.String())
			b.Reset()
		default:
			b.WriteByte(c)
		}
	}
	if strings.TrimSpace(b.String()) != "" {
		stmts = append(stmts, b.String())
	}
	return stmts
}

// splitClickHouseSQL splits a SQL string into individual statements, skipping
// USE and CREATE DATABASE statements since the DB is managed by the container env.
// Leading comment lines are stripped from each segment so that Initialize()'s
// prefix check (HasPrefix "ALTER"/"CREATE") works even when a comment block
// precedes a DDL keyword in the same semicolon-delimited segment.
func splitClickHouseSQL(sql string) []string {
	parts := splitSQLOnTopLevelSemicolons(sql)
	var stmts []string
	for _, part := range parts {
		stmt := strings.TrimSpace(part)
		if stmt == "" {
			continue
		}
		// Strip leading comment lines to expose the first SQL keyword.
		lines := strings.Split(stmt, "\n")
		firstSQL := -1
		for i, line := range lines {
			trimmed := strings.TrimSpace(line)
			if trimmed != "" && !strings.HasPrefix(trimmed, "--") {
				firstSQL = i
				break
			}
		}
		if firstSQL == -1 {
			continue // all comments
		}
		stmt = strings.TrimSpace(strings.Join(lines[firstSQL:], "\n"))
		upper := strings.ToUpper(stmt)
		if strings.HasPrefix(upper, "USE ") || strings.HasPrefix(upper, "CREATE DATABASE") {
			continue
		}
		stmts = append(stmts, stmt)
	}
	return stmts
}

// ClickHousePoolConfig controls connection pool sizing for a ClickHouse client.
type ClickHousePoolConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	DialTimeout     time.Duration
}

// DefaultQueryPoolConfig returns pool settings tuned for query/read workloads.
func DefaultQueryPoolConfig() ClickHousePoolConfig {
	return ClickHousePoolConfig{
		MaxOpenConns:    40,
		MaxIdleConns:    10,
		ConnMaxLifetime: 10 * time.Minute,
		DialTimeout:     10 * time.Second,
	}
}

// DefaultIngestPoolConfig returns pool settings tuned for write-heavy ingestion.
func DefaultIngestPoolConfig() ClickHousePoolConfig {
	return ClickHousePoolConfig{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 10 * time.Minute,
		DialTimeout:     30 * time.Second,
	}
}

// NewClickHouseClient creates a client with the default query pool config.
func NewClickHouseClient(host string, port int, database, user, password string) (*ClickHouseClient, error) {
	return NewClickHouseClientWithPool(host, port, database, user, password, DefaultQueryPoolConfig())
}

// NewClickHouseClientWithPool creates a client with explicit pool configuration.
func NewClickHouseClientWithPool(host string, port int, database, user, password string, pool ClickHousePoolConfig) (*ClickHouseClient, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := openClickHouseConn([]string{addr}, database, user, password, pool)
	if err != nil {
		return nil, err
	}
	c := &ClickHouseClient{conn: conn, addrs: []string{addr}, User: user, Password: password, Database: database, schemaReady: make(chan struct{})}
	c.configureInsertSettings()
	return c, nil
}

// validClusterName matches only safe ClickHouse cluster identifiers.
var validClusterName = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// NewClickHouseClusterClient creates a cluster-aware client that connects to
// multiple ClickHouse nodes. The driver handles failover across the provided
// addresses but does not load-balance writes; use logs_distributed for that.
//
// It bootstraps the target database (CREATE DATABASE IF NOT EXISTS) because on a
// fresh cluster the operator does not pre-create it. This is a schema-provisioning
// concern and requires a privileged (admin) identity -- only the app/control-plane
// tier should use this. The data-plane ingest tier, whose least-privilege user
// cannot (and must not) create databases, uses NewClickHouseClusterClientConnectOnly.
func NewClickHouseClusterClient(hosts []string, port int, database, user, password, cluster string, pool ClickHousePoolConfig) (*ClickHouseClient, error) {
	return newClickHouseClusterClient(hosts, port, database, user, password, cluster, pool, true)
}

// NewClickHouseClusterClientConnectOnly is like NewClickHouseClusterClient but does
// NOT create the database -- it connects to an already-provisioned one. Use it from
// the ingest tier: its least-privilege ClickHouse user (INSERT-only, no CREATE
// DATABASE) would otherwise fail the bootstrap with code 497, and by the time the
// ingest tier connects the app tier has already created the database.
func NewClickHouseClusterClientConnectOnly(hosts []string, port int, database, user, password, cluster string, pool ClickHousePoolConfig) (*ClickHouseClient, error) {
	return newClickHouseClusterClient(hosts, port, database, user, password, cluster, pool, false)
}

func newClickHouseClusterClient(hosts []string, port int, database, user, password, cluster string, pool ClickHousePoolConfig, createDB bool) (*ClickHouseClient, error) {
	if !validClusterName.MatchString(cluster) {
		return nil, fmt.Errorf("invalid cluster name %q: must be alphanumeric, hyphens, or underscores only", cluster)
	}
	addrs := make([]string, len(hosts))
	for i, h := range hosts {
		h = strings.TrimSpace(h)
		if strings.Contains(h, ":") {
			addrs[i] = h
		} else {
			addrs[i] = fmt.Sprintf("%s:%d", h, port)
		}
	}
	if createDB {
		// In cluster mode the target database may not exist yet (the operator
		// doesn't pre-create it). Connect to "default" first and ensure the
		// database is created locally. ON CLUSTER is omitted to avoid timeout on
		// slow/initializing clusters; table replication via ReplicatedMergeTree
		// and Keeper/ZooKeeper handles cluster-wide synchronization.
		bootstrap, err := openClickHouseConn(addrs, "default", user, password, pool)
		if err != nil {
			return nil, fmt.Errorf("bootstrap connection: %w", err)
		}
		createDBStmt := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s",
			EscCHStr(database))
		if execErr := bootstrap.Exec(context.Background(), createDBStmt); execErr != nil {
			bootstrap.Close()
			return nil, fmt.Errorf("create database %s: %w", database, execErr)
		}
		bootstrap.Close()
	}

	conn, err := openClickHouseConn(addrs, database, user, password, pool)
	if err != nil {
		return nil, err
	}
	c := &ClickHouseClient{conn: conn, addrs: addrs, User: user, Password: password, Database: database, Cluster: cluster, schemaReady: make(chan struct{})}
	c.configureInsertSettings()
	return c, nil
}

// openClickHouseConn opens a connection to ClickHouse with the given addresses.
func openClickHouseConn(addrs []string, database, user, password string, pool ClickHousePoolConfig) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: addrs,
		Auth: clickhouse.Auth{
			Database: database,
			Username: user,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"use_uncompressed_cache": 1,
			"output_format_native_use_flattened_dynamic_and_json_serialization": 1,
			// pgr()'s machine-generated scoring SQL repeats the (bounded) process guid IN-list across
			// its edge branches and inlines reconnection/command-line data as literals, so a midsize
			// graph legitimately exceeds ClickHouse's 256KB default and fails with code 62. This is a
			// parser buffer ceiling (not an allocation) and the SQL is bounded, so raising it fleet-
			// wide is safe and has no effect on the small SQL every other feature sends.
			"max_query_size": maxGeneratedQuerySize,
		},
		DialTimeout:     pool.DialTimeout,
		ReadTimeout:     0,
		MaxOpenConns:    pool.MaxOpenConns,
		MaxIdleConns:    pool.MaxIdleConns,
		ConnMaxLifetime: pool.ConnMaxLifetime,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	return conn, nil
}

// OpenClickHouseAddr opens a lightweight, single-connection ClickHouse conn
// to a specific host:port. Callers must Close() when done.
func OpenClickHouseAddr(addr, user, password string) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: user,
			Password: password,
		},
		DialTimeout:  3 * time.Second,
		MaxOpenConns: 1,
		MaxIdleConns: 0,
	})
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// QueryConn executes a query on a raw driver.Conn and returns results as
// []map[string]interface{}, mirroring ClickHouseClient.Query.
func QueryConn(ctx context.Context, conn driver.Conn, query string) ([]map[string]interface{}, error) {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}
	columnTypes := rows.ColumnTypes()
	for rows.Next() {
		row, err := scanRowMap(columnTypes, rows)
		if err != nil {
			return nil, err
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}
	return results, nil
}

// EscCHStr escapes a value for safe use inside single-quoted ClickHouse strings.
func EscCHStr(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'':
			out = append(out, '\\', '\'')
		case '\\':
			out = append(out, '\\', '\\')
		default:
			out = append(out, s[i])
		}
	}
	return string(out)
}

func (c *ClickHouseClient) HealthCheck(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

// ShardHealth returns the total number of cluster nodes and how many are
// currently healthy (estimated_recovery_time = 0 means ClickHouse's connection
// manager considers the node reachable). Returns 0, 0, nil for single-node.
func (c *ClickHouseClient) ShardHealth(ctx context.Context) (total, healthy int, err error) {
	if !c.IsCluster() {
		return 0, 0, nil
	}
	rows, err := c.Query(ctx, fmt.Sprintf(`
		SELECT count() AS total, countIf(estimated_recovery_time = 0) AS healthy
		FROM (
			SELECT shard_num, min(estimated_recovery_time) AS estimated_recovery_time
			FROM system.clusters
			WHERE cluster = '%s'
			GROUP BY shard_num
		)`, EscCHStr(c.Cluster)))
	if err != nil || len(rows) == 0 {
		return 0, 0, err
	}
	return int(distMonInt64(rows[0]["total"])), int(distMonInt64(rows[0]["healthy"])), nil
}

// ClusterServerStats holds the cluster-wide SERVER panel gauges. Load metrics are
// summed across every replica; memory reports the single worst node.
type ClusterServerStats struct {
	NodesTotal    int     `json:"nodes_total"`
	NodesHealthy  int     `json:"nodes_healthy"`
	ActiveQueries int64   `json:"active_queries"`
	ActiveMerges  int64   `json:"active_merges"`
	MemPeakPct    float64 `json:"mem_peak_pct"`
	MemPeakNode   string  `json:"mem_peak_node"`
	MemPeakBytes  int64   `json:"mem_peak_bytes"`
}

// ClusterServerStats aggregates node-local ClickHouse server gauges across every
// replica for the System tab's SERVER panel in cluster mode. Active queries and
// merges are summed cluster-wide (the meaningful quantity is total load); memory
// reports the single worst node by cgroup-aware utilization, since a cluster
// degrades when any one node saturates, not on average. Returns nil, nil for
// single-node deployments (the caller falls back to node-local gauges).
func (c *ClickHouseClient) ClusterServerStats(ctx context.Context) (*ClusterServerStats, error) {
	if !c.IsCluster() {
		return nil, nil
	}
	cl := EscCHStr(c.Cluster)
	stats := &ClusterServerStats{}

	total, healthy, err := c.ShardHealth(ctx)
	if err != nil {
		return nil, err
	}
	stats.NodesTotal, stats.NodesHealthy = total, healthy

	// Load gauges: summed across all replicas.
	loadRows, err := c.Query(ctx, fmt.Sprintf(`
		SELECT
			sumIf(value, metric = 'Query') AS active_queries,
			sumIf(value, metric = 'Merge') AS active_merges
		FROM clusterAllReplicas('%s', system.metrics)
		WHERE metric IN ('Query', 'Merge')
		SETTINGS skip_unavailable_shards = 1`, cl))
	if err != nil {
		return nil, err
	}
	if len(loadRows) > 0 {
		stats.ActiveQueries = distMonInt64(loadRows[0]["active_queries"])
		stats.ActiveMerges = distMonInt64(loadRows[0]["active_merges"])
	}

	// Memory saturation: worst node by cgroup-aware utilization, using the same
	// computation the backpressure monitor and metrics collector rely on (prefers
	// the pod's cgroup limit over node RAM, which matters in k8s), so the panel
	// agrees with them. hostName() is evaluated on each remote replica, labelling
	// rows by their origin node.
	memRows, err := c.Query(ctx, fmt.Sprintf(`
		SELECT hostName() AS node, metric, value
		FROM clusterAllReplicas('%s', system.asynchronous_metrics)
		WHERE metric IN ('CGroupMemoryUsed', 'CGroupMemoryTotal', 'MemoryResident', 'OSMemoryTotal')
		SETTINGS skip_unavailable_shards = 1`, cl))
	if err != nil {
		return nil, err
	}
	perNode := map[string][]map[string]interface{}{}
	for _, r := range memRows {
		node, _ := r["node"].(string)
		perNode[node] = append(perNode[node], r)
	}
	for node, rows := range perNode {
		m := MetricRowsToMap(rows)
		pct, ok := MemoryPercentFromMetrics(m)
		if !ok || pct <= stats.MemPeakPct {
			continue
		}
		stats.MemPeakPct = pct
		stats.MemPeakNode = node
		if used := m["CGroupMemoryUsed"]; used > 0 {
			stats.MemPeakBytes = int64(used)
		} else {
			stats.MemPeakBytes = int64(m["MemoryResident"])
		}
	}
	return stats, nil
}

func (c *ClickHouseClient) Close() error {
	return c.conn.Close()
}

// Conn returns the underlying ClickHouse driver connection for advanced
// operations such as PrepareBatch.
func (c *ClickHouseClient) Conn() driver.Conn {
	return c.conn
}

// configureInsertSettings computes the optional per-insert ClickHouse settings from
// the environment, applied to InsertLogs. ClickHouse is guaranteed >= 26.6, so every
// referenced setting exists (an unknown setting would otherwise fail the insert).
func (c *ClickHouseClient) configureInsertSettings() {
	s := clickhouse.Settings{}

	// GROUP BY spill: an insert into logs synchronously fires several aggregating
	// materialized views (rarity/beacon/long-connection/proc models). Bounding their
	// aggregation memory as a fraction of the server's per-query memory limit lets a
	// heavy block spill to disk instead of OOM-ing the insert (which backs up the
	// distribution queue and stalls merges). Memory-proportional, so it scales with
	// node size. Set BIFRACT_INSERT_GROUPBY_SPILL_RATIO to 0 to disable.
	ratio := getenvFloat("BIFRACT_INSERT_GROUPBY_SPILL_RATIO", 0.5)
	if ratio > 0 {
		s["max_bytes_ratio_before_external_group_by"] = ratio
	}

	// async_insert (opt-in via BIFRACT_ASYNC_INSERT=1): coalesce inserts server-side
	// into fewer, larger parts, easing merge pressure. wait_for_async_insert=1 keeps
	// the client blocked until the buffer is flushed, so a successful insert still
	// means durably written and ingest acks/backpressure stay meaningful.
	if os.Getenv("BIFRACT_ASYNC_INSERT") == "1" {
		s["async_insert"] = 1
		s["wait_for_async_insert"] = 1
	}

	if len(s) > 0 {
		c.insertSettings = s
		log.Printf("[ClickHouse] Per-insert settings: %v", s)
	}
}

// getenvFloat reads a float env var, returning def when unset or unparseable.
func getenvFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func (c *ClickHouseClient) InsertLogs(ctx context.Context, logs []LogEntry) error {
	if len(logs) == 0 {
		return nil
	}

	// Apply the per-insert relief settings (group-by spill, optional async_insert).
	if len(c.insertSettings) > 0 {
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(c.insertSettings))
	}

	// norm_log is intentionally omitted: it is a ClickHouse DEFAULT toString(fields)
	// column, auto-populated at insert. normalizer carries the "name@version" stamp.
	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO "+c.WriteTable()+" (timestamp, raw_log, log_id, fields, fractal_id, ingest_timestamp, normalizer)")
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, log := range logs {
		ingestTS := log.IngestTimestamp
		if ingestTS.IsZero() {
			ingestTS = time.Now()
		}
		err := batch.Append(
			log.Timestamp,
			log.RawLog,
			log.LogID,
			log.Fields,
			log.FractalID,
			ingestTS,
			log.Normalizer,
		)
		if err != nil {
			return fmt.Errorf("failed to append log to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

func (c *ClickHouseClient) Exec(ctx context.Context, query string) error {
	err := c.conn.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute statement: %w", err)
	}
	return nil
}

// ExecArgs executes a ClickHouse statement with parameterized arguments.
func (c *ClickHouseClient) ExecArgs(ctx context.Context, query string, args ...interface{}) error {
	err := c.conn.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to execute statement: %w", err)
	}
	return nil
}

// DeleteLogsByFractalID drops all partitions belonging to a fractal.
// With PARTITION BY (fractal_id, toDate(timestamp)), each partition holds one
// fractal's data for one day. DROP PARTITION is an instant metadata operation —
// no lightweight delete mutation or OPTIMIZE TABLE needed, no matter how much
// data the fractal holds. Replication happens via ZooKeeper automatically on
// ReplicatedMergeTree, so ON CLUSTER is not used.
func (c *ClickHouseClient) DeleteLogsByFractalID(ctx context.Context, fractalID string) error {
	rows, err := c.conn.Query(ctx,
		"SELECT DISTINCT partition FROM system.parts WHERE database = currentDatabase() AND table = 'logs' AND active = 1",
	)
	if err != nil {
		return fmt.Errorf("failed to list partitions for fractal %s: %w", fractalID, err)
	}

	// Partition strings look like ('my-fractal','2024-01-15'). Match the prefix,
	// escaping single quotes to match ClickHouse's canonical representation.
	escapedID := strings.ReplaceAll(fractalID, "'", "''")
	prefix := fmt.Sprintf("('%s','", escapedID)

	var partitions []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan partition: %w", err)
		}
		if strings.HasPrefix(p, prefix) {
			partitions = append(partitions, p)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("partition query error for fractal %s: %w", fractalID, err)
	}

	for _, partition := range partitions {
		if err := c.conn.Exec(ctx, "ALTER TABLE logs DROP PARTITION "+partition); err != nil {
			return fmt.Errorf("failed to drop partition %s for fractal %s: %w", partition, fractalID, err)
		}
	}

	log.Printf("Dropped %d partitions for fractal %s", len(partitions), fractalID)
	return nil
}

// RevertTieredStoragePolicy migrates the logs table off the legacy 'tiered'
// storage policy (native S3/Azure cold tiering, now replaced by the Iceberg
// archive) back onto the default policy. It runs at startup and is a no-op when
// the table is already on the default policy (the common case: cold tiering was
// never enabled).
//
// When the table IS tiered, it first moves any active parts sitting on a
// non-default disk (the cold volume) back to the 'default' volume, THEN switches
// the policy - the policy switch would otherwise be rejected while parts live on
// a disk the default policy does not include. This must run while the 'tiered'
// policy + 'cold' disk are still defined in ClickHouse config; only after it
// succeeds is it safe to remove that config. Failures are non-fatal so a partial
// move retries on the next startup rather than blocking boot; requires enough hot
// disk to hold the returning parts.
func (c *ClickHouseClient) RevertTieredStoragePolicy(ctx context.Context) error {
	var policy string
	if err := c.conn.QueryRow(ctx,
		"SELECT storage_policy FROM system.tables WHERE database = currentDatabase() AND name = 'logs'",
	).Scan(&policy); err != nil {
		return fmt.Errorf("failed to read logs storage policy: %w", err)
	}
	if policy != "tiered" {
		return nil
	}

	source := "system.parts"
	if c.Cluster != "" {
		source = fmt.Sprintf("clusterAllReplicas('%s', system.parts)", EscCHStr(c.Cluster))
	}
	rows, err := c.conn.Query(ctx,
		"SELECT DISTINCT partition FROM "+source+" WHERE database = currentDatabase() AND table = 'logs' AND active = 1 AND disk_name != 'default'",
	)
	if err != nil {
		return fmt.Errorf("failed to list cold partitions: %w", err)
	}
	var toMove []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan partition: %w", err)
		}
		toMove = append(toMove, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("cold partition query error: %w", err)
	}

	for _, partition := range toMove {
		stmt := c.InjectOnCluster("ALTER TABLE logs MOVE PARTITION " + partition + " TO VOLUME 'default'")
		if err := c.conn.Exec(ctx, stmt); err != nil {
			// Non-fatal: retry on next startup. Do NOT switch the policy while
			// parts remain on cold, or the switch will be rejected.
			return fmt.Errorf("move partition %s off cold volume: %w", partition, err)
		}
	}

	stmt := c.InjectOnCluster("ALTER TABLE logs MODIFY SETTING storage_policy = 'default'")
	if err := c.conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("failed to revert to default storage policy: %w", err)
	}
	log.Printf("Reverted logs table off 'tiered' storage policy (moved %d cold partitions back to hot)", len(toMove))
	return nil
}

// QueryWithID executes a query with a fixed query_id so the run can be
// correlated with system.query_log entries for profiling.
func (c *ClickHouseClient) QueryWithID(ctx context.Context, queryID, query string) ([]map[string]interface{}, error) {
	ctx = clickhouse.Context(ctx, clickhouse.WithQueryID(queryID))
	return c.Query(ctx, query)
}

// QueryStats is what a query actually cost server-side, accumulated from the
// ClickHouse progress packets rather than read back from system.query_log
// (which flushes asynchronously and so is not reliably readable the moment a
// query finishes).
type QueryStats struct {
	ReadRows  uint64
	ReadBytes uint64
}

// QueryWithStats runs a query under a fixed query_id and reports how much data
// ClickHouse read to answer it. Used by Recall, where the scan is over object
// storage and the cost is both large and invisible: without this a search that
// times out is indistinguishable from one that was simply too broad.
//
// Progress packets carry deltas, so they accumulate.
func (c *ClickHouseClient) QueryWithStats(ctx context.Context, queryID, query string) ([]map[string]interface{}, QueryStats, error) {
	var readRows, readBytes atomic.Uint64
	ctx = clickhouse.Context(ctx,
		clickhouse.WithQueryID(queryID),
		clickhouse.WithProgress(func(p *clickhouse.Progress) {
			readRows.Add(p.Rows)
			readBytes.Add(p.Bytes)
		}),
	)
	rows, err := c.Query(ctx, query)
	// Report the stats even on failure: a timed-out or killed query still read
	// whatever it read, and that is exactly the number the user needs to see.
	return rows, QueryStats{ReadRows: readRows.Load(), ReadBytes: readBytes.Load()}, err
}

// ExecWithID runs a statement with a fixed query_id so a long-running write (a
// restore's INSERT ... SELECT, for instance) can be interrupted from another
// process with KILL QUERY WHERE query_id = ..., and correlated with
// system.query_log afterwards.
func (c *ClickHouseClient) ExecWithID(ctx context.Context, queryID, query string) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithQueryID(queryID))
	return c.Exec(ctx, query)
}

// maxGeneratedQuerySize raises ClickHouse's max_query_size (default 256KB) for the query paths
// that carry machine-generated SQL. pgr()'s scoring/reconnection SQL repeats the (bounded) process
// guid IN-list across its edge branches and inlines reconnection edges as literals, so a midsize
// graph legitimately exceeds 256KB and would fail with code 62. The generated SQL is bounded
// (maxProvenanceGuids, maxReconnectPeers), so this headroom is safe; it is a parser buffer ceiling,
// not an allocation, and has no effect on the small queries every other feature sends.
const maxGeneratedQuerySize = 16 * 1024 * 1024 // 16 MB

// QueryLowPriority executes a query at ClickHouse priority 5, yielding CPU
// to user-facing queries (priority 0) when both are competing for threads.
// Use for background work (alert evaluation) that should never starve users.
func (c *ClickHouseClient) QueryLowPriority(ctx context.Context, query string) ([]map[string]interface{}, error) {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"priority":       5,
		"max_query_size": maxGeneratedQuerySize,
	}))
	return c.Query(ctx, query)
}

// QueryLowPriorityBounded is QueryLowPriority plus an explicit, isolated memory budget:
// max_memory_usage caps what this one query may ever allocate, and
// memory_overcommit_ratio_denominator=0 opts it out of the global overcommit killer (the
// mechanism that otherwise picks a "victim" query/task to kill under memory pressure). Together
// these mean the query can only ever fail itself cleanly on its own cap -- it can never be the
// thing that grows large enough to threaten, or get selected to be sacrificed for, unrelated
// background work like merges or mutations. Use for periodic introspection sweeps (schema
// overflow detection) that must be safe to run under any server memory condition.
func (c *ClickHouseClient) QueryLowPriorityBounded(ctx context.Context, query string, maxMemoryBytes int64) ([]map[string]interface{}, error) {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"priority":                            5,
		"max_query_size":                      maxGeneratedQuerySize,
		"max_memory_usage":                    maxMemoryBytes,
		"memory_overcommit_ratio_denominator": 0,
	}))
	return c.Query(ctx, query)
}

// Provenance scan budget: bounds each pgr ClickHouse scan server-side so a pathological (giant or
// firehose-host) process tree ABORTS at a hard limit instead of scanning the whole time window and
// pegging every shard. pgr's resolver passes run on the raw request context (before the
// request-level QueryTimeoutSeconds ctx is built), and even the downstream scoring scan's Go-ctx
// timeout does not reliably stop a running ClickHouse scan -- so these server-enforced ceilings
// (throw on overflow) are the real guard. ProvenanceQuerySettings mirrors them as a top-level
// SETTINGS suffix for the downstream query (SETTINGS are illegal inside the pgr subquery).
const (
	ProvenanceMaxRowsToRead   = 3_000_000_000 // ~3B rows/scan
	ProvenanceMaxExecutionSec = 30
)

// ProvenanceQuerySettings is the top-level SETTINGS clause (no leading space) the query layer
// appends to the downstream query when its source is pgr, applying the same budget as
// QueryProvenance to the scan the generic pipeline executes.
var ProvenanceQuerySettings = fmt.Sprintf("max_rows_to_read=%d, max_execution_time=%d, read_overflow_mode='throw', timeout_overflow_mode='throw'",
	ProvenanceMaxRowsToRead, ProvenanceMaxExecutionSec)

// QueryProvenance executes a pgr resolver scan at low priority with the provenance scan budget
// (see ProvenanceMaxRowsToRead). Used for pgr's tree hops, probe, reconnection, totals, and the
// diffusion scan so none of them can run away on a pathological tree.
func (c *ClickHouseClient) QueryProvenance(ctx context.Context, query string) ([]map[string]interface{}, error) {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"priority":              5,
		"max_query_size":        maxGeneratedQuerySize,
		"max_rows_to_read":      ProvenanceMaxRowsToRead,
		"max_execution_time":    ProvenanceMaxExecutionSec,
		"read_overflow_mode":    "throw",
		"timeout_overflow_mode": "throw",
	}))
	return c.Query(ctx, query)
}

func (c *ClickHouseClient) Query(ctx context.Context, query string) ([]map[string]interface{}, error) {
	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	columnTypes := rows.ColumnTypes()

	for rows.Next() {
		row, err := scanRowMap(columnTypes, rows)
		if err != nil {
			return nil, err
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
}

// unwrapSimpleAggregateFunction returns the underlying value type of a
// SimpleAggregateFunction(fn, T) column, or typeName unchanged. Aggregating over
// such a column (min/max on AggregatingMergeTree state) keeps the wrapper in the
// reported type, which would otherwise fall through to the default string scan.
func unwrapSimpleAggregateFunction(typeName string) string {
	const prefix = "SimpleAggregateFunction("
	if !strings.HasPrefix(typeName, prefix) || !strings.HasSuffix(typeName, ")") {
		return typeName
	}
	inner := typeName[len(prefix) : len(typeName)-1]
	// Split on the first top-level comma; the inner type itself may contain
	// commas, e.g. SimpleAggregateFunction(max, DateTime64(3, 'UTC')).
	depth := 0
	for i, r := range inner {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				return strings.TrimSpace(inner[i+1:])
			}
		}
	}
	return typeName
}

// scanRowMap scans the current row of rows into a map[string]interface{} using
// the supplied column types. It is shared by the buffered Query path and the
// streaming StreamQuery path so the two never diverge in type handling. The
// caller must have already positioned the cursor via rows.Next().
//
// Datetime columns land in the map as STRINGS, not time.Time: they are scanned
// into a time.Time and then formatted with chRowTimeLayout, because these maps
// are encoded straight to JSON and that shape is the API contract. So
// `row[col].(time.Time)` always fails, and with the usual `v, _ :=` idiom it
// silently yields the zero time. Read timestamps with RowTime instead.
// (Array-of-date columns are not in the type switch below and reach the map as
// real time.Time values via the interface path -- RowTime is only for scalars.)
func scanRowMap(columnTypes []driver.ColumnType, rows driver.Rows) (map[string]interface{}, error) {
	// Create typed destination variables based on column types
	values := make([]interface{}, len(columnTypes))
	for i, col := range columnTypes {
		typeName := unwrapSimpleAggregateFunction(col.DatabaseTypeName())
		switch {
		case typeName == "String" || typeName == "Nullable(String)":
			values[i] = new(string)
		case typeName == "UInt64" || typeName == "Nullable(UInt64)":
			values[i] = new(uint64)
		case typeName == "Int64" || typeName == "Nullable(Int64)":
			values[i] = new(int64)
		case typeName == "Float64" || typeName == "Nullable(Float64)":
			values[i] = new(float64)
		case typeName == "DateTime64(3)" || typeName == "DateTime" || typeName == "Nullable(DateTime64(3))":
			values[i] = new(time.Time)
		case typeName == "Date" || typeName == "Nullable(Date)":
			values[i] = new(time.Time)
		case strings.HasPrefix(typeName, "Array("):
			inner := typeName[6 : len(typeName)-1]
			switch inner {
			case "String":
				values[i] = new([]string)
			case "Float64":
				values[i] = new([]float64)
			case "UInt64":
				values[i] = new([]uint64)
			case "Int64":
				values[i] = new([]int64)
			default:
				// Complex array types (Array(Tuple(...)), etc.) -
				// let the driver scan into an interface slice.
				var v interface{}
				values[i] = &v
			}
		case strings.HasPrefix(typeName, "Tuple("):
			var v interface{}
			values[i] = &v
		default:
			// For unknown types (including JSON), try string
			values[i] = new(string)
		}
	}

	if err := rows.Scan(values...); err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	// Convert to map[string]interface{}
	row := make(map[string]interface{})
	for i, col := range columnTypes {
		colName := col.Name()
		switch v := values[i].(type) {
		case *string:
			val := *v
			// toString(fields) arrives as a JSON string; parse into a map
			// so the API response contains an object, not a string.
			if colName == "fields" || colName == "_all_fields" {
				var m map[string]interface{}
				if json.Unmarshal([]byte(val), &m) == nil {
					row[colName] = m
					continue
				}
			}
			row[colName] = val
		case *uint64:
			row[colName] = *v
		case *int64:
			row[colName] = *v
		case *float64:
			row[colName] = *v
		case *time.Time:
			row[colName] = v.Format("2006-01-02 15:04:05.000")
		case *[]string:
			row[colName] = *v
		case *[]float64:
			row[colName] = *v
		case *[]uint64:
			row[colName] = *v
		case *[]int64:
			row[colName] = *v
		case *interface{}:
			row[colName] = *v
		default:
			row[colName] = v
		}
	}

	return row, nil
}

func (c *ClickHouseClient) CountLogs(ctx context.Context, startTime, endTime time.Time) (uint64, error) {
	var count uint64
	err := c.conn.QueryRow(ctx,
		fmt.Sprintf("SELECT count() as count FROM %s WHERE toUnixTimestamp64Milli(timestamp) >= ? AND toUnixTimestamp64Milli(timestamp) <= ?", c.ReadTable()),
		startTime.UnixMilli(), endTime.UnixMilli(),
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count logs: %w", err)
	}

	return count, nil
}

// QueryRows executes a query and returns the raw driver.Rows for streaming iteration.
// The caller is responsible for closing the returned Rows.
func (c *ClickHouseClient) QueryRows(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	return c.conn.Query(ctx, query, args...)
}

// StreamQuery executes a query and invokes onRow for each row as ClickHouse
// produces it, without buffering the full result set. Rows are scanned with the
// same logic as the buffered Query path (scanRowMap), so type handling is
// identical. If onRow returns an error, iteration stops and that error is
// returned (callers use this to cap the number of rows read).
//
// queryID, when non-empty, tags the query for system.query_log correlation.
// onProgress, when non-nil, receives cumulative rows read and the server's
// estimated total rows for driving a progress indicator; TotalRows may be 0
// when unknown and may grow as the scan proceeds.
//
// Cancelling ctx (e.g. on client disconnect) aborts the underlying ClickHouse
// query: the driver propagates cancellation to the connection.
func (c *ClickHouseClient) StreamQuery(ctx context.Context, queryID, query string, onRow func(map[string]interface{}) error, onProgress func(read, total uint64)) error {
	// Raise max_query_size: a pgr() source subquery (scoring + inlined reconnection/diffused edges)
	// is machine-generated and legitimately large, so the default 256KB parser ceiling would fail
	// it with code 62. Bounded worst case, safe headroom, no effect on ordinary search SQL.
	opts := []clickhouse.QueryOption{clickhouse.WithSettings(clickhouse.Settings{"max_query_size": maxGeneratedQuerySize})}
	if onProgress != nil {
		var readSoFar uint64
		opts = append(opts, clickhouse.WithProgress(func(p *clickhouse.Progress) {
			// Progress packets report per-increment row counts; accumulate.
			readSoFar += p.Rows
			onProgress(readSoFar, p.TotalRows)
		}))
	}
	if queryID != "" {
		opts = append(opts, clickhouse.WithQueryID(queryID))
	}
	ctx = clickhouse.Context(ctx, opts...)

	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columnTypes := rows.ColumnTypes()
	for rows.Next() {
		row, scanErr := scanRowMap(columnTypes, rows)
		if scanErr != nil {
			return scanErr
		}
		if err := onRow(row); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}
	return nil
}

// QueryRow executes a query that is expected to return at most one row
func (c *ClickHouseClient) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
	return c.conn.QueryRow(ctx, query, args...)
}

// GetLogByTimestamp fetches a single log by log_id, optionally pinned by an
// exact timestamp and/or scoped to a fractal. The log table is
// ORDER BY (timestamp, log_id) and PARTITION BY (fractal_id, toDate(timestamp)),
// so a non-zero timestamp prunes to a single date partition and pins the primary
// index, and a non-empty fractalID prunes to that fractal's partitions - either
// predicate turns a whole-table scan into a near-pinpoint read. Callers that
// must read the log's own fractal_id for access control pass an empty fractalID
// and verify afterwards. A zero timestamp is omitted (used by comment creation,
// which resolves the timestamp from the matched row).
func (c *ClickHouseClient) GetLogByTimestamp(ctx context.Context, timestamp time.Time, logID string, fractalID string) (map[string]interface{}, error) {
	if logID == "" {
		return nil, fmt.Errorf("log_id is required")
	}

	// norm_log (retained) is the display source so comment/permalink views keep working
	// after raw_log ages out of its TTL window. The original raw_log is only fetched by
	// the manual detail-panel Raw tab (GetLogFieldsByID/Direct).
	query := fmt.Sprintf(
		"SELECT timestamp, log_id, norm_log AS fields, fractal_id, ingest_timestamp FROM %s WHERE log_id = ?",
		c.ReadTable())
	args := []interface{}{logID}

	if !timestamp.IsZero() {
		query += " AND timestamp = toDateTime64(?, 3, 'UTC')"
		args = append(args, timestamp.UTC().Format("2006-01-02 15:04:05.000"))
	}
	if fractalID != "" {
		query += " AND fractal_id = ?"
		args = append(args, fractalID)
	}
	query += " LIMIT 1"

	result, err := c.scanLogRow(ctx, query, args)
	if err != nil {
		return nil, err
	}
	if result == nil {
		log.Printf("[GetLogByTimestamp] No log found with log_id: %s", logID)
	}
	return result, nil
}

// scanLogRow executes a single-row log query and returns the result as a map.
func (c *ClickHouseClient) scanLogRow(ctx context.Context, query string, args []interface{}) (map[string]interface{}, error) {
	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query log: %w", err)
	}
	defer rows.Close()

	columnTypes := rows.ColumnTypes()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			log.Printf("[scanLogRow] rows iteration error: %v", err)
			return nil, fmt.Errorf("failed to iterate rows: %w", err)
		}
		return nil, nil
	}

	values := make([]interface{}, len(columnTypes))
	for i, col := range columnTypes {
		typeName := unwrapSimpleAggregateFunction(col.DatabaseTypeName())
		switch {
		case typeName == "String" || typeName == "Nullable(String)":
			values[i] = new(string)
		case typeName == "UInt64" || typeName == "Nullable(UInt64)":
			values[i] = new(uint64)
		case typeName == "Int64" || typeName == "Nullable(Int64)":
			values[i] = new(int64)
		case typeName == "Float64" || typeName == "Nullable(Float64)":
			values[i] = new(float64)
		case typeName == "DateTime64(3)" || typeName == "DateTime" || typeName == "Nullable(DateTime64(3))":
			values[i] = new(time.Time)
		case typeName == "Date" || typeName == "Nullable(Date)":
			values[i] = new(time.Time)
		default:
			values[i] = new(string)
		}
	}

	if err := rows.Scan(values...); err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	row := make(map[string]interface{})
	for i, col := range columnTypes {
		colName := col.Name()
		switch v := values[i].(type) {
		case *string:
			val := *v
			if colName == "fields" || colName == "_all_fields" {
				var m map[string]interface{}
				if json.Unmarshal([]byte(val), &m) == nil {
					row[colName] = m
					continue
				}
			}
			row[colName] = val
		case *uint64:
			row[colName] = *v
		case *int64:
			row[colName] = *v
		case *float64:
			row[colName] = *v
		case *time.Time:
			row[colName] = v.Format("2006-01-02 15:04:05.000")
		default:
			row[colName] = v
		}
	}

	return row, nil
}

// GetLogFieldsByID fetches the parsed fields for a single log by an exact
// (timestamp, log_id) key, optionally scoped to a fractal. The log table is
// ORDER BY (timestamp, log_id) and PARTITION BY (fractal_id, toDate(timestamp)),
// so the timestamp equality prunes to a single date partition and pins the
// primary index to one granule, and a non-empty fractalID prunes to one
// fractal - turning a whole-table bloom-filter scan into a near-pinpoint read.
// The frontend supplies the exact ClickHouse timestamp from the search result,
// so it bit-matches the DateTime64(3) value.
//
// The timestamp is required: this is the single, deterministic lookup path. A
// non-empty fractalID is normally left to the caller, which either passes a
// session-validated value as a partition-pruning filter or leaves it empty and
// verifies the row's own fractal_id against the accessible set afterwards.
// Returns nil (no error) when no matching row exists.
// typeHintFieldSet returns (lazily loading and caching) the set of declared typed
// sub-paths in the fields JSON column. On query failure it returns an empty set, which
// means no empties are stripped (safe: shows a little noise rather than hiding data).
func (c *ClickHouseClient) typeHintFieldSet(ctx context.Context) map[string]bool {
	c.typeHintMu.RLock()
	if c.typeHintFields != nil {
		s := c.typeHintFields
		c.typeHintMu.RUnlock()
		return s
	}
	c.typeHintMu.RUnlock()

	c.typeHintMu.Lock()
	defer c.typeHintMu.Unlock()
	if c.typeHintFields != nil {
		return c.typeHintFields
	}
	set := make(map[string]bool)
	var typ string
	// c.Database (not currentDatabase()) is the logs DB; the pool's session database
	// may be "default".
	db := c.Database
	if db == "" {
		db = "logs"
	}
	if err := c.conn.QueryRow(ctx,
		"SELECT type FROM system.columns WHERE database = ? AND table = 'logs' AND name = 'fields'",
		db,
	).Scan(&typ); err == nil {
		// type looks like JSON(artifact String, user String, max_dynamic_paths=1024, ...).
		// Each declared sub-path is "name Type"; parse the leading identifier of each
		// comma-separated entry, skipping settings like max_dynamic_paths=N.
		if i := strings.IndexByte(typ, '('); i >= 0 {
			inner := typ[i+1:]
			if j := strings.LastIndexByte(inner, ')'); j >= 0 {
				inner = inner[:j]
			}
			for _, part := range strings.Split(inner, ",") {
				part = strings.TrimSpace(part)
				if part == "" || strings.ContainsRune(part, '=') {
					continue
				}
				name := part
				if sp := strings.IndexAny(part, " \t"); sp >= 0 {
					name = part[:sp]
				}
				if name = strings.Trim(name, "`"); name != "" {
					set[name] = true
				}
			}
		}
	}
	c.typeHintFields = set
	return set
}

// invalidateTypeHintCache forces typeHintFieldSet to reload on next use, after the
// set of type hints changes.
func (c *ClickHouseClient) invalidateTypeHintCache() {
	c.typeHintMu.Lock()
	c.typeHintFields = nil
	c.typeHintMu.Unlock()
}

// parseLogFields unmarshals the serialized normalized fields and drops empty-string
// values that are type-hint sub-columns. The JSON column materializes every declared
// typed sub-path as "" even when the log did not contain it, so those are noise in the
// detail view; a genuinely-empty *dynamic* field (actually present in the log) is kept.
func (c *ClickHouseClient) parseLogFields(ctx context.Context, fieldsStr string) map[string]interface{} {
	var m map[string]interface{}
	if json.Unmarshal([]byte(fieldsStr), &m) != nil {
		return map[string]interface{}{}
	}
	hints := c.typeHintFieldSet(ctx)
	for k, v := range m {
		if s, ok := v.(string); ok && s == "" && hints[k] {
			delete(m, k)
		}
	}
	return m
}

func (c *ClickHouseClient) GetLogFieldsByID(ctx context.Context, logID string, ts time.Time, fractalID string) (map[string]interface{}, error) {
	if logID == "" {
		return nil, fmt.Errorf("log_id is required")
	}
	if ts.IsZero() {
		return nil, fmt.Errorf("timestamp is required")
	}

	// PREWHERE on (log_id, timestamp): timestamp is the leading primary-key column, so
	// this prunes granules before reading norm_log. Matches GetLogFieldsByIDDirect.
	query := fmt.Sprintf(
		"SELECT log_id, fractal_id, norm_log AS fields, raw_log, normalizer FROM %s PREWHERE log_id = ? AND timestamp = toDateTime64(?, 3, 'UTC')",
		c.ReadTable())
	args := []interface{}{logID, ts.UTC().Format("2006-01-02 15:04:05.000")}
	if fractalID != "" {
		query += " AND fractal_id = ?"
		args = append(args, fractalID)
	}
	query += " LIMIT 1"

	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query log fields: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating log fields row: %w", err)
		}
		return nil, nil
	}

	var resLogID, logFractalID, fieldsStr, rawLog, normalizer string
	if err := rows.Scan(&resLogID, &logFractalID, &fieldsStr, &rawLog, &normalizer); err != nil {
		return nil, fmt.Errorf("failed to scan log fields row: %w", err)
	}
	flds := c.parseLogFields(ctx, fieldsStr)
	// Surface the per-row normalizer stamp ("name@version") as a synthetic field so it
	// shows in the detail grid. Empty for system/audit logs (no normalizer applied).
	if normalizer != "" {
		flds["_normalizer"] = normalizer
	}
	entry := map[string]interface{}{"log_id": resLogID, "fractal_id": logFractalID, "raw_log": rawLog, "fields": flds}
	return entry, nil
}

// shardHostForNum returns the host:port for a given shard number by querying
// system.clusters. Results are cached for the lifetime of the client.
func (c *ClickHouseClient) shardHostForNum(ctx context.Context, shardNum uint64) (string, error) {
	c.shardHostsMu.RLock()
	if c.shardHosts != nil {
		host, ok := c.shardHosts[shardNum]
		c.shardHostsMu.RUnlock()
		if ok {
			return host, nil
		}
		return "", fmt.Errorf("shard %d not found in cluster topology", shardNum)
	}
	c.shardHostsMu.RUnlock()

	c.shardHostsMu.Lock()
	defer c.shardHostsMu.Unlock()
	// Double-check after acquiring write lock.
	if c.shardHosts != nil {
		if host, ok := c.shardHosts[shardNum]; ok {
			return host, nil
		}
		return "", fmt.Errorf("shard %d not found in cluster topology", shardNum)
	}

	rows, err := c.conn.Query(ctx, fmt.Sprintf(
		"SELECT shard_num, host_name, port FROM system.clusters WHERE cluster = '%s' AND replica_num = 1 ORDER BY shard_num",
		EscCHStr(c.Cluster),
	))
	if err != nil {
		return "", fmt.Errorf("query system.clusters: %w", err)
	}
	defer rows.Close()

	hosts := make(map[uint64]string)
	for rows.Next() {
		var sn uint32 // system.clusters.shard_num is UInt32
		var hostName string
		var port uint16 // system.clusters.port is UInt16
		if err := rows.Scan(&sn, &hostName, &port); err != nil {
			continue
		}
		hosts[uint64(sn)] = fmt.Sprintf("%s:%d", hostName, port)
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("iterate system.clusters: %w", err)
	}
	c.shardHosts = hosts

	if host, ok := hosts[shardNum]; ok {
		return host, nil
	}
	return "", fmt.Errorf("shard %d not found in cluster topology", shardNum)
}

// shardConnForNum returns (or lazily opens) a direct connection to the shard
// identified by shardNum. Connections are cached for the client's lifetime.
func (c *ClickHouseClient) shardConnForNum(ctx context.Context, shardNum uint64) (driver.Conn, error) {
	c.shardConnsMu.Lock()
	defer c.shardConnsMu.Unlock()

	if c.shardConns != nil {
		if conn, ok := c.shardConns[shardNum]; ok {
			return conn, nil
		}
	}

	hostPort, err := c.shardHostForNum(ctx, shardNum)
	if err != nil {
		return nil, err
	}

	pool := ClickHousePoolConfig{
		MaxOpenConns:    4,
		MaxIdleConns:    2,
		ConnMaxLifetime: 10 * time.Minute,
		DialTimeout:     5 * time.Second,
	}
	conn, err := openClickHouseConn([]string{hostPort}, c.Database, c.User, c.Password, pool)
	if err != nil {
		return nil, fmt.Errorf("open shard %d connection to %s: %w", shardNum, hostPort, err)
	}

	if c.shardConns == nil {
		c.shardConns = make(map[uint64]driver.Conn)
	}
	c.shardConns[shardNum] = conn
	return conn, nil
}

// GetLogFieldsByIDDirect fetches log fields by routing directly to the shard
// that owns the row, bypassing the Distributed engine fan-out. shardNum must
// be the _shard_num value from the search result. Falls back to the distributed
// path when not in cluster mode, when shardNum is 0, or when the direct shard
// connection fails.
func (c *ClickHouseClient) GetLogFieldsByIDDirect(ctx context.Context, logID string, ts time.Time, fractalID string, shardNum uint64) (map[string]interface{}, error) {
	if !c.IsCluster() || shardNum == 0 {
		return c.GetLogFieldsByID(ctx, logID, ts, fractalID)
	}

	conn, err := c.shardConnForNum(ctx, shardNum)
	if err != nil {
		log.Printf("[GetLogFieldsByIDDirect] shard %d unavailable (%v), falling back to distributed", shardNum, err)
		return c.GetLogFieldsByID(ctx, logID, ts, fractalID)
	}

	query := "SELECT log_id, fractal_id, norm_log AS fields, raw_log, normalizer FROM logs PREWHERE log_id = ? AND timestamp = toDateTime64(?, 3, 'UTC')"
	args := []interface{}{logID, ts.UTC().Format("2006-01-02 15:04:05.000")}
	if fractalID != "" {
		query += " AND fractal_id = ?"
		args = append(args, fractalID)
	}
	query += " LIMIT 1"

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		log.Printf("[GetLogFieldsByIDDirect] direct query on shard %d failed (%v), falling back to distributed", shardNum, err)
		return c.GetLogFieldsByID(ctx, logID, ts, fractalID)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating log fields row: %w", err)
		}
		return nil, nil
	}

	var resLogID, logFractalID, fieldsStr, rawLog, normalizer string
	if err := rows.Scan(&resLogID, &logFractalID, &fieldsStr, &rawLog, &normalizer); err != nil {
		return nil, fmt.Errorf("failed to scan log fields row: %w", err)
	}
	flds := c.parseLogFields(ctx, fieldsStr)
	// Surface the per-row normalizer stamp ("name@version") as a synthetic field so it
	// shows in the detail grid. Empty for system/audit logs (no normalizer applied).
	if normalizer != "" {
		flds["_normalizer"] = normalizer
	}
	entry := map[string]interface{}{"log_id": resLogID, "fractal_id": logFractalID, "raw_log": rawLog, "fields": flds}
	return entry, nil
}

// GetLogFieldsByIDs batch-fetches parsed field data for multiple log_ids.
func (c *ClickHouseClient) GetLogFieldsByIDs(ctx context.Context, logIDs []string, fractalID string) ([]map[string]interface{}, error) {
	if len(logIDs) == 0 {
		return nil, nil
	}
	if len(logIDs) > 500 {
		return nil, fmt.Errorf("too many log IDs (max 500, got %d)", len(logIDs))
	}

	var rows driver.Rows
	var err error
	if fractalID != "" {
		rows, err = c.conn.Query(ctx,
			fmt.Sprintf("SELECT log_id, fractal_id, norm_log AS fields FROM %s WHERE log_id IN (?) AND fractal_id = ?", c.ReadTable()),
			logIDs, fractalID)
	} else {
		rows, err = c.conn.Query(ctx,
			fmt.Sprintf("SELECT log_id, fractal_id, norm_log AS fields FROM %s WHERE log_id IN (?)", c.ReadTable()),
			logIDs)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query logs by IDs: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var logID, logFractalID, fieldsStr string
		if err := rows.Scan(&logID, &logFractalID, &fieldsStr); err != nil {
			return nil, fmt.Errorf("failed to scan log fields row: %w", err)
		}
		entry := map[string]interface{}{"log_id": logID, "fractal_id": logFractalID}
		entry["fields"] = c.parseLogFields(ctx, fieldsStr)
		results = append(results, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating log fields rows: %w", err)
	}
	return results, nil
}

// StartHotTableCleaner starts a background goroutine that drops expired
// logs_hot partitions every 5 minutes. DROP PARTITION is a near-instant
// metadata operation and never blocks concurrent reads or writes.
//
// The TTL defined on logs_hot is a safety net; this cleaner is the primary
// cleanup mechanism, giving deterministic bounded retention.
//
// On a cluster, each shard is cleaned by opening a direct connection to that
// shard and running DROP PARTITION locally. ON CLUSTER is deliberately avoided:
// it routes DDL through Keeper's global distributed task queue, which is
// sequential. If any slow DDL (e.g. MODIFY COLUMN from ReconcileSchemaFields
// on startup) clogs that queue, every subsequent ON CLUSTER DROP PARTITION
// blocks behind it, times out, and gets re-queued — compounding into a large
// backlog that saturates ClickHouse CPU.
//
// Note: DROP PARTITION on ReplicatedMergeTree replicates only within a shard
// (to replicas of the same shard), not across shards. The per-shard loop
// ensures all shards are cleaned regardless of replication topology.
//
// Multiple pods running the cleaner simultaneously are safe — dropping an
// already-dropped partition is a no-op in ClickHouse.
//
// The caller must cancel ctx on shutdown to stop the goroutine.
func (c *ClickHouseClient) StartHotTableCleaner(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.dropExpiredHotPartitions(ctx)
			}
		}
	}()
}

// dropExpiredHotPartitions cleans expired logs_hot partitions on every shard.
// In cluster mode it opens a short-lived direct connection to each shard address
// and runs DROP PARTITION locally, bypassing the distributed DDL task queue.
func (c *ClickHouseClient) dropExpiredHotPartitions(ctx context.Context) {
	if c.Cluster == "" {
		dropHotPartitionsOnConn(ctx, c.conn, "local")
		return
	}
	pool := ClickHousePoolConfig{MaxOpenConns: 1, MaxIdleConns: 1, DialTimeout: 10 * time.Second}
	for _, addr := range c.addrs {
		conn, err := openClickHouseConn([]string{addr}, c.Database, c.User, c.Password, pool)
		if err != nil {
			log.Printf("[HotTableCleaner] connect to %s: %v", addr, err)
			continue
		}
		dropHotPartitionsOnConn(ctx, conn, addr)
		conn.Close()
	}
}

// dropHotPartitionsOnConn queries system.parts on conn and drops any logs_hot
// partitions older than 2 hours. label is used only for log messages.
func dropHotPartitionsOnConn(ctx context.Context, conn driver.Conn, label string) {
	rows, err := conn.Query(ctx,
		"SELECT DISTINCT partition FROM system.parts"+
			" WHERE database = currentDatabase() AND table = 'logs_hot'"+
			" AND active = 1 AND max_time < now() - INTERVAL 2 HOUR",
	)
	if err != nil {
		log.Printf("[HotTableCleaner] query partitions on %s: %v", label, err)
		return
	}
	defer rows.Close()
	var partitions []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			log.Printf("[HotTableCleaner] scan partition on %s: %v", label, err)
			return
		}
		partitions = append(partitions, p)
	}
	if err := rows.Err(); err != nil {
		log.Printf("[HotTableCleaner] rows error on %s: %v", label, err)
		return
	}
	for _, partition := range partitions {
		if err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE logs_hot DROP PARTITION '%s'", partition)); err != nil {
			log.Printf("[HotTableCleaner] drop partition %s on %s: %v", partition, label, err)
		}
	}
}

// FractalPartition describes one active logs partition (one fractal, one day)
// and its on-disk size, summed across all shards in a cluster.
type FractalPartition struct {
	// Partition is ClickHouse's canonical partition expression, e.g.
	// ('my-fractal','2026-07-01'). It is used verbatim in DROP PARTITION.
	Partition string
	FractalID string
	Bytes     int64
	MinTime   time.Time
}

// parseFractalFromPartition extracts the fractal_id from a logs partition string.
// The PARTITION BY (fractal_id, toDate(timestamp)) key renders as ('<id>','<date>')
// with any single quote in the id doubled (ClickHouse's canonical form).
func parseFractalFromPartition(p string) string {
	if !strings.HasPrefix(p, "('") {
		return ""
	}
	rest := p[2:]
	idx := strings.Index(rest, "','")
	if idx < 0 {
		return ""
	}
	return strings.ReplaceAll(rest[:idx], "''", "'")
}

// FractalPartitionUsage returns the on-disk size of every active logs partition,
// grouped by (fractal, day) and summed across all shards. It reads only
// system.parts (metadata), so it is cheap and safe to call frequently. Used by
// quota rollover to find the oldest partitions to drop. Requires an admin-level
// client (all shard addresses); the restricted ingest user is INSERT-only.
func (c *ClickHouseClient) FractalPartitionUsage(ctx context.Context) ([]FractalPartition, error) {
	agg := make(map[string]*FractalPartition)
	collect := func(conn driver.Conn, label string) {
		rows, err := conn.Query(ctx,
			"SELECT partition, sum(bytes_on_disk) AS bytes, min(min_time) AS mn"+
				" FROM system.parts"+
				" WHERE database = currentDatabase() AND table = 'logs' AND active = 1"+
				" GROUP BY partition",
		)
		if err != nil {
			log.Printf("[QuotaRollover] usage query on %s: %v", label, err)
			return
		}
		defer rows.Close()
		for rows.Next() {
			var partition string
			var bytes uint64
			var mn time.Time
			if err := rows.Scan(&partition, &bytes, &mn); err != nil {
				log.Printf("[QuotaRollover] scan usage on %s: %v", label, err)
				return
			}
			p := agg[partition]
			if p == nil {
				fid := parseFractalFromPartition(partition)
				if fid == "" {
					continue
				}
				p = &FractalPartition{Partition: partition, FractalID: fid, MinTime: mn}
				agg[partition] = p
			}
			p.Bytes += int64(bytes)
			if mn.Before(p.MinTime) {
				p.MinTime = mn
			}
		}
		if err := rows.Err(); err != nil {
			log.Printf("[QuotaRollover] rows error on %s: %v", label, err)
		}
	}

	if c.Cluster == "" {
		collect(c.conn, "local")
	} else {
		pool := ClickHousePoolConfig{MaxOpenConns: 1, MaxIdleConns: 1, DialTimeout: 10 * time.Second}
		for _, addr := range c.addrs {
			conn, err := openClickHouseConn([]string{addr}, c.Database, c.User, c.Password, pool)
			if err != nil {
				log.Printf("[QuotaRollover] connect to %s: %v", addr, err)
				continue
			}
			collect(conn, addr)
			conn.Close()
		}
	}

	out := make([]FractalPartition, 0, len(agg))
	for _, p := range agg {
		out = append(out, *p)
	}
	return out, nil
}

// DropLogPartition drops a single logs partition (one fractal, one day) on every
// shard. DROP PARTITION is near-instant metadata and idempotent (dropping an
// already-dropped partition is a no-op), so this is safe to retry and to run from
// multiple pods. ON CLUSTER is deliberately avoided for the same reason as the hot
// table cleaner (see StartHotTableCleaner): it serializes DDL through Keeper's
// global queue, which a slow schema mutation can clog. partition must be a value
// returned by FractalPartitionUsage (ClickHouse's canonical form).
func (c *ClickHouseClient) DropLogPartition(ctx context.Context, partition string) error {
	stmt := "ALTER TABLE logs DROP PARTITION " + partition
	if c.Cluster == "" {
		return c.conn.Exec(ctx, stmt)
	}
	pool := ClickHousePoolConfig{MaxOpenConns: 1, MaxIdleConns: 1, DialTimeout: 10 * time.Second}
	var firstErr error
	for _, addr := range c.addrs {
		conn, err := openClickHouseConn([]string{addr}, c.Database, c.User, c.Password, pool)
		if err != nil {
			log.Printf("[QuotaRollover] connect to %s: %v", addr, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := conn.Exec(ctx, stmt); err != nil {
			log.Printf("[QuotaRollover] drop partition %s on %s: %v", partition, addr, err)
			if firstErr == nil {
				firstErr = err
			}
		}
		conn.Close()
	}
	return firstErr
}

// NormLogIndexName is the lower(norm_log) n-gram text index (migration 006) used to
// accelerate case-insensitive substring/regex search on the canonical text field.
const NormLogIndexName = "norm_log_ngram_lc"

// rawLogIndexBackfillLockID is a Postgres advisory-lock id that ensures only one
// replica submits the one-time MATERIALIZE INDEX backfill. Distinct from the
// schema-init lock ("bifract\0").
const rawLogIndexBackfillLockID int64 = 0x6269667261637401 // "bifract\x01"

// indexBackfillDoneKey returns the settings-table key that durably records that the
// one-time MATERIALIZE INDEX backfill for idx has been submitted.
func indexBackfillDoneKey(idx string) string { return idx + "_backfilled" }

// StartNormLogIndexBackfill materializes the lower(norm_log) n-gram index on parts
// written before the index existed, so historical data benefits from granule
// pruning. It never blocks startup:
//
//   - Schema init adds the index as metadata only (instant). Older parts carry no
//     index data until MATERIALIZE INDEX rebuilds them, which can take hours on
//     large tables, so the rebuild runs here in a background goroutine.
//   - The ALTER is submitted with alter_sync=0, so it returns as soon as the
//     mutation is queued and ClickHouse rebuilds parts asynchronously.
//   - A Postgres advisory lock ensures only one replica submits it.
//
// Re-submitting MATERIALIZE INDEX is NOT cheap: ClickHouse rebuilds the index on
// every active part, saturating CPU even when the data is already indexed. The
// system.mutations existence check alone cannot prevent this, because ClickHouse
// prunes finished mutation records once a table exceeds finished_mutations_to_keep
// (100 by default) -- and schema reconciliation issues many ALTER mutations, so the
// original backfill record is eventually evicted. We therefore persist a durable
// marker in Postgres the moment the backfill is queued; once set, the backfill is
// never resubmitted, regardless of mutation-history eviction or restarts. The
// mutation is durable server-side once queued, so it completes even if this process
// exits immediately after submitting.
//
// pg may be nil, in which case the (fragile) system.mutations existence check is the
// only guard, acceptable for single-replica deployments without Postgres.
func (c *ClickHouseClient) StartNormLogIndexBackfill(ctx context.Context, pg *PostgresClient) {
	go func() {
		doneKey := indexBackfillDoneKey(NormLogIndexName)

		if pg != nil {
			unlock, ok := pg.TryAdvisoryLock(ctx, rawLogIndexBackfillLockID)
			if !ok {
				return // another replica owns the backfill
			}
			defer unlock()

			// Durable guard: once submitted, never submit again. Survives
			// ClickHouse pruning the mutation record on busy clusters.
			if v, err := pg.GetSetting(ctx, doneKey); err == nil && v == "true" {
				return
			}
		}

		exists, err := c.indexMutationExists(ctx, NormLogIndexName)
		if err != nil {
			log.Printf("[IndexBackfill] check existing mutation: %v", err)
			return
		}
		if !exists {
			if err := c.submitMaterializeIndex(ctx, NormLogIndexName); err != nil {
				log.Printf("[IndexBackfill] submit MATERIALIZE INDEX %s: %v", NormLogIndexName, err)
				return
			}
			log.Printf("[IndexBackfill] submitted MATERIALIZE INDEX %s; backfilling existing parts in the background", NormLogIndexName)
		}

		// The mutation is now queued durably (or was already present). Record the
		// marker so future restarts skip the expensive re-materialization even
		// after ClickHouse prunes the mutation record.
		if pg != nil {
			if err := pg.SetSetting(ctx, doneKey, "true"); err != nil {
				log.Printf("[IndexBackfill] persist backfill marker: %v", err)
			}
		}

		c.awaitIndexMutation(ctx, NormLogIndexName)
	}()
}

// indexMutationExists reports whether a MATERIALIZE INDEX mutation for idx already
// exists for the logs table (running or finished).
func (c *ClickHouseClient) indexMutationExists(ctx context.Context, idx string) (bool, error) {
	var n uint64
	q := fmt.Sprintf(
		"SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'logs' AND command LIKE '%%MATERIALIZE INDEX %s%%'",
		idx,
	)
	if err := c.conn.QueryRow(ctx, q).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

// submitMaterializeIndex queues an asynchronous MATERIALIZE INDEX (alter_sync=0)
// so the call returns immediately while ClickHouse rebuilds parts in the background.
func (c *ClickHouseClient) submitMaterializeIndex(ctx context.Context, idx string) error {
	sql := "ALTER TABLE logs"
	if c.IsCluster() {
		sql += c.OnClusterSQL()
	}
	sql += fmt.Sprintf(" MATERIALIZE INDEX %s", idx)
	actx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"alter_sync":     0,
		"mutations_sync": 0,
	}))
	return c.conn.Exec(actx, sql)
}

// awaitIndexMutation polls mutation progress for logging only; the mutation
// proceeds server-side regardless of this goroutine's lifetime.
func (c *ClickHouseClient) awaitIndexMutation(ctx context.Context, idx string) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	q := fmt.Sprintf(
		"SELECT countIf(is_done = 0), toInt64(sum(parts_to_do)) FROM system.mutations"+
			" WHERE database = currentDatabase() AND table = 'logs' AND command LIKE '%%MATERIALIZE INDEX %s%%'",
		idx,
	)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var pending uint64
			var remaining int64
			if err := c.conn.QueryRow(ctx, q).Scan(&pending, &remaining); err != nil {
				log.Printf("[IndexBackfill] poll progress: %v", err)
				return
			}
			if pending == 0 {
				log.Printf("[IndexBackfill] MATERIALIZE INDEX %s complete", idx)
				return
			}
			log.Printf("[IndexBackfill] MATERIALIZE INDEX %s in progress (%d parts remaining)", idx, remaining)
		}
	}
}

// jsonTypeHintRe matches one declared path inside a JSON column type.
//
// It must accept BOTH spellings. Type hints are written with backticks, but
// ClickHouse normalises them away when reporting the column type back through
// system.columns, so what goes in as "`src_ip` String" comes back as
// "src_ip String". A backtick-only pattern therefore matched nothing in
// practice, which silently broke the additive contract below.
var jsonTypeHintRe = regexp.MustCompile("(?:`([^`]+)`|([A-Za-z_][A-Za-z0-9_.]*))\\s+([A-Za-z][A-Za-z0-9_]*)")

// parseJSONTypeHints extracts the declared path names from a ClickHouse JSON
// column type, in either the backticked form used when writing DDL or the bare
// form system.columns reports:
//
//	JSON(max_dynamic_paths=1024, `src_ip` String, ...)
//	JSON(src_ip String, user String, ...)
//
// Getting this wrong is not a cosmetic failure. ReconcileSchemaFields merges
// what it parses here with the requested set, so returning nothing makes the
// reconcile authoritative instead of additive: it drops every hint outside the
// current defaults-plus-custom list, and it re-issues MODIFY COLUMN on every
// startup because every field looks new.
func parseJSONTypeHints(typeStr string) []string {
	// Trim the JSON(...) wrapper so the settings prefix cannot be misread as a
	// path, then consider each comma-separated declaration on its own.
	if open := strings.IndexByte(typeStr, '('); open >= 0 {
		if close := strings.LastIndexByte(typeStr, ')'); close > open {
			typeStr = typeStr[open+1 : close]
		}
	}

	var fields []string
	seen := make(map[string]struct{})
	for _, part := range strings.Split(typeStr, ",") {
		part = strings.TrimSpace(part)
		// Settings ("max_dynamic_paths=1024") and SKIP clauses declare no path.
		if part == "" || strings.ContainsRune(part, '=') || strings.HasPrefix(part, "SKIP") {
			continue
		}
		m := jsonTypeHintRe.FindStringSubmatch(part)
		if m == nil {
			continue
		}
		name := m[1] // backticked
		if name == "" {
			name = m[2] // bare
		}
		if name == "" {
			continue
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		fields = append(fields, name)
	}
	return fields
}

// SchemaFieldSpec describes a single type-hinted field and its skip index type.
// Used by ReconcileSchemaFields and TruncateAndReschema to avoid coupling the
// storage package to the schemafields package.
type SchemaFieldSpec struct {
	FieldName string
	IndexType string // "none" (type hint only), "bloom_filter", or "set"
}

// jsonLazyTypeHintSettings must be appended to EVERY "MODIFY COLUMN fields JSON(...)"
// statement. It is the difference between a metadata-only change and a mutation
// that rewrites every part in the table.
//
// Without allow_experimental_json_lazy_type_hints, adding a type hint queues a
// background mutation across all existing parts: on a cluster holding billions of
// rows that saturates CPU for hours. With it, the ALTER completes instantly and
// touches no existing part. Old parts stay correct because the hinted path is cast
// from Dynamic at read time; new parts materialize the hint on INSERT, and old
// parts converge for free as normal background merges rewrite them anyway.
//
// The setting is written into the SQL text rather than sent as a connection-level
// setting for two reasons. First, the Go driver pools connections, so a session
// SET would not reliably apply to the statement that needs it. Second, and more
// importantly, ON CLUSTER replicates the query text verbatim through the DDL queue
// to every replica: inlining it guarantees remote replicas run the lazy form too,
// which a client-side setting may not.
//
// This makes the safety structural rather than procedural. On a ClickHouse that
// does not support the setting, the statement fails with UNKNOWN_SETTING (code 115)
// and does nothing, instead of silently falling back to the mutating form. There is
// therefore no reachable code path in which reconciliation rewrites history, even
// if the calling logic is wrong.
const jsonLazyTypeHintSettings = " SETTINGS allow_experimental_json_lazy_type_hints=1, alter_sync=0, mutations_sync=0"

// buildFieldsTypeHintSQL renders the MODIFY COLUMN statement that declares the
// full merged type-hint set on a table's `fields` JSON column. Kept pure so the
// two properties that make reconciliation safe are testable without a live
// ClickHouse: the hint list is sorted (byte-stable across restarts, where Go map
// order would churn the DDL on every boot) and the statement always carries
// jsonLazyTypeHintSettings (metadata-only instead of a full-table mutation).
func buildFieldsTypeHintSQL(table string, fields []string) string {
	sorted := append([]string(nil), fields...)
	sort.Strings(sorted)

	var sb strings.Builder
	sb.WriteString("ALTER TABLE ")
	sb.WriteString(table)
	sb.WriteString(" MODIFY COLUMN fields JSON(\n    max_dynamic_paths=1024")
	for _, f := range sorted {
		escaped := strings.ReplaceAll(f, "`", "``")
		sb.WriteString(",\n    `")
		sb.WriteString(escaped)
		sb.WriteString("` String")
	}
	sb.WriteString("\n)")
	sb.WriteString(jsonLazyTypeHintSettings)
	return sb.String()
}

// ReconcileResult reports the per-field outcome of a schema reconcile so callers
// can record an accurate sync status. A field listed in IndexErrors did get its
// type hint (the MODIFY COLUMN is all-or-nothing and succeeded); only its skip
// index is missing, so queries on it are correct but unpruned.
type ReconcileResult struct {
	IndexErrors map[string]string
}

// mirrorFieldsTypeHint applies the fields JSON type-hint declaration to a
// secondary table (logs_hot, logs_distributed) that must stay in sync with logs.
// Failure is only logged: the authoritative MODIFY COLUMN on logs already succeeded.
func (c *ClickHouseClient) mirrorFieldsTypeHint(ctx context.Context, table string, fields []string) {
	stmt := c.InjectOnCluster(buildFieldsTypeHintSQL(table, fields))
	if err := c.conn.Exec(ctx, stmt); err != nil {
		log.Printf("Warning: mirror type hints to %s: %v", table, err)
	}
}

// ReconcileSchemaFields ensures ClickHouse has type hints and skip indexes for
// all requested fields. It is additive: existing type hints and indexes are
// never removed. New fields are added via MODIFY COLUMN and ADD INDEX IF NOT EXISTS.
// All DDL is wrapped with InjectOnCluster for multi-node deployments.
//
// Every statement here is metadata-only and completes without rewriting a single
// existing part, which is what makes it safe to run unattended at startup against
// a cluster holding billions of rows. See jsonLazyTypeHintSettings for why the
// MODIFY COLUMN does not mutate, and note that ADD INDEX is inherently
// forward-only (it is computed for parts written or merged after the ADD; there
// is deliberately no MATERIALIZE INDEX here, as that is the one operation in this
// area that would rewrite history).
func (c *ClickHouseClient) ReconcileSchemaFields(ctx context.Context, fields []SchemaFieldSpec) (ReconcileResult, error) {
	result := ReconcileResult{IndexErrors: map[string]string{}}

	// Read current type hints from ClickHouse.
	rows, err := c.conn.Query(ctx,
		"SELECT type FROM system.columns WHERE database = 'logs' AND table = 'logs' AND name = 'fields'")
	if err != nil {
		return result, fmt.Errorf("read fields column type: %w", err)
	}
	var currentType string
	if rows.Next() {
		_ = rows.Scan(&currentType)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return result, fmt.Errorf("read fields column type: %w", err)
	}
	rows.Close()

	existing := parseJSONTypeHints(currentType)
	existingSet := make(map[string]struct{}, len(existing))
	for _, f := range existing {
		existingSet[f] = struct{}{}
	}

	// Compute merged set for MODIFY COLUMN.
	merged := make(map[string]struct{}, len(existingSet)+len(fields))
	for k := range existingSet {
		merged[k] = struct{}{}
	}
	var newFields []string
	for _, f := range fields {
		if _, ok := merged[f.FieldName]; !ok {
			newFields = append(newFields, f.FieldName)
		}
		merged[f.FieldName] = struct{}{}
	}

	mergedList := make([]string, 0, len(merged))
	for f := range merged {
		mergedList = append(mergedList, f)
	}

	// Run MODIFY COLUMN on logs only when there are new fields to add.
	if len(newFields) > 0 {
		colDef := buildFieldsTypeHintSQL("logs", mergedList)
		if err := c.conn.Exec(ctx, c.InjectOnCluster(colDef)); err != nil {
			return result, fmt.Errorf("modify fields column: %w", err)
		}
	}
	// Mirror unconditionally (not gated on newFields above): logs_hot/logs_distributed
	// can still be behind logs even when logs itself needed no change this call, e.g.
	// right after this mirroring was added, or if a prior mirror attempt failed on one
	// table. Metadata-only, so reissuing with an unchanged list costs nothing.
	// No skip indexes needed on logs_hot — its ORDER BY covers alert query patterns.
	c.mirrorFieldsTypeHint(ctx, "logs_hot", mergedList)
	// logs_distributed and logs_hot_distributed are each created "AS <local>" once
	// at bootstrap and never inherit later MODIFY COLUMN on logs/logs_hot; mirror
	// here so neither drifts. logs_hot_distributed is what HotReadTable() actually
	// queries in cluster mode, so this is the table the alert engine reads.
	if c.IsCluster() {
		c.mirrorFieldsTypeHint(ctx, "logs_distributed", mergedList)
		c.mirrorFieldsTypeHint(ctx, "logs_hot_distributed", mergedList)
	}

	c.applySchemaFieldIndexes(ctx, fields, result.IndexErrors)
	// Type hints may have changed (MODIFY COLUMN above); drop the cached set so the
	// log-detail empty-field filter picks up new/removed hints.
	c.invalidateTypeHintCache()
	return result, nil
}

// applySchemaFieldIndexes creates the skip index each field asks for. Shared by
// the additive reconcile and the authoritative reset so the two cannot drift.
// Failures are recorded per field rather than aborting: the type hint has
// already applied, so the field is queryable and correct, it just will not prune.
func (c *ClickHouseClient) applySchemaFieldIndexes(ctx context.Context, fields []SchemaFieldSpec, errs map[string]string) {
	for _, f := range fields {
		var idxExpr string
		switch f.IndexType {
		case "none":
			// Type hint only; no skip index. Writes and merges would otherwise pay
			// for something no query uses.
			continue
		case "set":
			idxExpr = "TYPE set(256)"
		default:
			idxExpr = "TYPE bloom_filter(0.001)"
		}
		escaped := strings.ReplaceAll(f.FieldName, "`", "``")
		idxName := schemaFieldIndexName(f.FieldName)
		idxSQL := fmt.Sprintf(
			"ALTER TABLE logs ADD INDEX IF NOT EXISTS %s fields.`%s` %s GRANULARITY 1",
			idxName, escaped, idxExpr,
		)
		if err := c.conn.Exec(ctx, c.InjectOnCluster(idxSQL)); err != nil {
			log.Printf("Warning: add index %s: %v", idxName, err)
			if errs != nil {
				errs[f.FieldName] = fmt.Sprintf("skip index not created: %v", err)
			}
		}
	}
}

// TruncateAndReschema deletes all log data and rebuilds the field schema from
// the supplied set, discarding any type hint not in it.
//
// Unlike ReconcileSchemaFields this is AUTHORITATIVE, not additive, and that is
// the whole point of the operation. Reconciliation deliberately never removes a
// hint, so hints accumulate for the life of an install and are the one thing a
// user cannot reclaim by any other means. Routing the reset through the additive
// path (as this once did) left every accumulated hint in place, so the operation
// destroyed all log data and reclaimed nothing.
//
// Issuing a narrowing MODIFY COLUMN from a fixed list is normally forbidden
// because it strips user-added hints and rewrites every part. Here both are
// intended and neither is costly: dropping unwanted hints IS the request, and
// the TRUNCATE above leaves zero parts, so the rewrite has nothing to rewrite.
func (c *ClickHouseClient) TruncateAndReschema(ctx context.Context, fields []SchemaFieldSpec) error {
	// Read the hints that exist now, before truncating: their skip indexes must
	// be dropped too, and they are not necessarily in the desired set.
	existing, err := c.currentFieldHints(ctx)
	if err != nil {
		return fmt.Errorf("read current type hints: %w", err)
	}

	for _, tbl := range []string{"logs.logs_histogram", "logs.logs", "logs.logs_hot"} {
		sql := fmt.Sprintf("TRUNCATE TABLE %s", tbl)
		if err := c.conn.Exec(ctx, c.InjectOnCluster(sql)); err != nil {
			return fmt.Errorf("truncate %s: %w", tbl, err)
		}
	}

	// Drop indexes for the union of current and desired fields. Dropping only the
	// desired ones would orphan the indexes of hints being discarded.
	toDrop := make(map[string]struct{}, len(existing)+len(fields))
	for _, f := range existing {
		toDrop[f] = struct{}{}
	}
	for _, f := range fields {
		toDrop[f.FieldName] = struct{}{}
	}
	dropNames := make([]string, 0, len(toDrop))
	for f := range toDrop {
		dropNames = append(dropNames, f)
	}
	sort.Strings(dropNames) // deterministic order for reproducible logs
	for _, f := range dropNames {
		idxName := schemaFieldIndexName(f)
		dropSQL := fmt.Sprintf("ALTER TABLE logs DROP INDEX IF EXISTS %s", idxName)
		if err := c.conn.Exec(ctx, c.InjectOnCluster(dropSQL)); err != nil {
			log.Printf("Warning: drop index %s: %v", idxName, err)
		}
	}

	// Authoritative: exactly the requested fields, nothing carried over.
	desired := make([]string, 0, len(fields))
	for _, f := range fields {
		desired = append(desired, f.FieldName)
	}
	if err := c.conn.Exec(ctx, c.InjectOnCluster(buildFieldsTypeHintSQL("logs", desired))); err != nil {
		return fmt.Errorf("reset fields column: %w", err)
	}
	c.mirrorFieldsTypeHint(ctx, "logs_hot", desired)
	if c.IsCluster() {
		c.mirrorFieldsTypeHint(ctx, "logs_distributed", desired)
		c.mirrorFieldsTypeHint(ctx, "logs_hot_distributed", desired)
	}

	c.applySchemaFieldIndexes(ctx, fields, nil)
	c.invalidateTypeHintCache()
	return nil
}

// currentFieldHints returns the type-hinted field names declared on logs.fields.
func (c *ClickHouseClient) currentFieldHints(ctx context.Context) ([]string, error) {
	rows, err := c.conn.Query(ctx,
		"SELECT type FROM system.columns WHERE database = 'logs' AND table = 'logs' AND name = 'fields'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var currentType string
	if rows.Next() {
		_ = rows.Scan(&currentType)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return parseJSONTypeHints(currentType), nil
}

// schemaFieldIndexSanitizeRe replaces any character that is not alphanumeric or
// underscore with an underscore, producing a valid bare ClickHouse identifier.
var schemaFieldIndexSanitizeRe = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

// schemaFieldIndexName returns the skip-index name for a custom field. Add and drop
// paths must use this single source of truth so the names can never drift apart.
func schemaFieldIndexName(field string) string {
	return "idx_" + schemaFieldIndexSanitizeRe.ReplaceAllString(field, "_")
}

// DropSchemaFieldIndex removes the skip index for a single custom field, used when a
// field is deleted so a later recreate with a different index type applies cleanly
// (ReconcileSchemaFields is additive and ADD INDEX IF NOT EXISTS would otherwise keep
// the stale index). It deliberately leaves the type hint (dedicated sub-column): that is
// harmless, is reused if the field is recreated, and removing it would need a heavy
// MODIFY COLUMN mutation.
//
// Safe on clusters and for the distributed insert path: a skip index is a local,
// query-time pruning structure and is NOT part of the column/insert schema, so dropping
// it never changes what the Distributed table forwards. Shards may converge independently
// without any insert mismatch or distributed-queue backlog. IF EXISTS makes it idempotent;
// InjectOnCluster propagates it to every shard on multi-node deployments.
func (c *ClickHouseClient) DropSchemaFieldIndex(ctx context.Context, fieldName string) error {
	dropSQL := fmt.Sprintf("ALTER TABLE logs DROP INDEX IF EXISTS %s", schemaFieldIndexName(fieldName))
	return c.conn.Exec(ctx, c.InjectOnCluster(dropSQL))
}
