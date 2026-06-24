package storage

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
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
		}
		setClickHouseMigrationsBaseline(ctx, c.conn, c.RewriteEngine, migrations, migrationsDir)
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
		go func() {
			initPool := ClickHousePoolConfig{MaxOpenConns: 1, MaxIdleConns: 1, DialTimeout: 10 * time.Second}
			for _, addr := range c.addrs {
				hostConn, err := openClickHouseConn([]string{addr}, c.Database, c.User, c.Password, initPool)
				if err != nil {
					log.Printf("Warning: cluster migration sync to %s failed: %v", addr, err)
					continue
				}
				n, err := runMigrationsOnConn(ctx, hostConn, c.RewriteEngine, migrations, migrationsDir)
				if err != nil {
					log.Printf("Warning: migration sync on %s: %v", addr, err)
				} else if n > 0 {
					log.Printf("Applied %d ClickHouse migration(s) to shard %s", n, addr)
				}
				for _, stmt := range []string{distSQL, histDistSQL, hotDistSQL} {
					stmtCtx, stmtCancel := context.WithTimeout(ctx, 30*time.Second)
					hostConn.Exec(stmtCtx, stmt)
					stmtCancel()
				}
				hostConn.Close()
			}
			log.Printf("Cluster schema sync complete")
		}()
		return nil
	}

	// Single-node upgrade: apply only pending migrations.
	n, err := runMigrationsOnConn(ctx, c.conn, nil, migrations, migrationsDir)
	if err != nil {
		return fmt.Errorf("clickhouse migrations: %w", err)
	}
	if n > 0 {
		log.Printf("Applied %d ClickHouse migration(s)", n)
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

// runMigrationsOnConn applies pending ClickHouse migrations via conn.
// transformStmt, if non-nil, is applied to each DDL statement before execution
// (used in cluster mode to rewrite engine names to their Replicated variants).
// Returns the number of migrations applied.
func runMigrationsOnConn(ctx context.Context, conn driver.Conn, transformStmt func(string) string, migrations embed.FS, migrationsDir string) (int, error) {
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
		return 0, fmt.Errorf("create migrations table: %w", err)
	}

	var maxApplied uint32
	if err := conn.QueryRow(ctx, "SELECT max(number) FROM logs._bifract_migrations").Scan(&maxApplied); err != nil {
		return 0, fmt.Errorf("query migration state: %w", err)
	}

	allMigrations, err := loadClickHouseMigrations(migrations, migrationsDir)
	if err != nil {
		return 0, err
	}

	applied := 0
	for _, m := range allMigrations {
		if uint32(m.number) <= maxApplied {
			continue
		}
		for _, stmt := range splitClickHouseSQL(m.sql) {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			upper := strings.ToUpper(stmt)
			if !strings.HasPrefix(upper, "CREATE ") && !strings.HasPrefix(upper, "ALTER ") {
				continue
			}
			if transformStmt != nil {
				stmt = transformStmt(stmt)
			}
			stmtCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			execErr := conn.Exec(stmtCtx, stmt)
			cancel()
			if execErr != nil {
				return applied, fmt.Errorf("migration %s: %w", m.name, execErr)
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
	return &ClickHouseClient{conn: conn, addrs: []string{addr}, User: user, Password: password, Database: database}, nil
}

// validClusterName matches only safe ClickHouse cluster identifiers.
var validClusterName = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// NewClickHouseClusterClient creates a cluster-aware client that connects to
// multiple ClickHouse nodes. The driver handles failover across the provided
// addresses but does not load-balance writes; use logs_distributed for that.
func NewClickHouseClusterClient(hosts []string, port int, database, user, password, cluster string, pool ClickHousePoolConfig) (*ClickHouseClient, error) {
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
	// In cluster mode the target database may not exist yet (the operator
	// doesn't pre-create it). Connect to "default" first and ensure the
	// database is created locally. ON CLUSTER is omitted to avoid timeout on
	// slow/initializing clusters; table replication via ReplicatedMergeTree
	// and Keeper/ZooKeeper handles cluster-wide synchronization.
	bootstrap, err := openClickHouseConn(addrs, "default", user, password, pool)
	if err != nil {
		return nil, fmt.Errorf("bootstrap connection: %w", err)
	}
	createDB := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s",
		EscCHStr(database))
	if execErr := bootstrap.Exec(context.Background(), createDB); execErr != nil {
		bootstrap.Close()
		return nil, fmt.Errorf("create database %s: %w", database, execErr)
	}
	bootstrap.Close()

	conn, err := openClickHouseConn(addrs, database, user, password, pool)
	if err != nil {
		return nil, err
	}
	return &ClickHouseClient{conn: conn, addrs: addrs, User: user, Password: password, Database: database, Cluster: cluster}, nil
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
		values := make([]interface{}, len(columnTypes))
		for i, col := range columnTypes {
			typeName := col.DatabaseTypeName()
			switch {
			case typeName == "Float64" || typeName == "Nullable(Float64)":
				values[i] = new(float64)
			case typeName == "String" || typeName == "Nullable(String)":
				values[i] = new(string)
			case typeName == "UInt64" || typeName == "Nullable(UInt64)":
				values[i] = new(uint64)
			default:
				values[i] = new(string)
			}
		}
		if err := rows.Scan(values...); err != nil {
			continue
		}
		row := make(map[string]interface{})
		for i, col := range columnTypes {
			switch v := values[i].(type) {
			case *float64:
				row[col.Name()] = *v
			case *string:
				row[col.Name()] = *v
			case *uint64:
				row[col.Name()] = *v
			}
		}
		results = append(results, row)
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

func (c *ClickHouseClient) Close() error {
	return c.conn.Close()
}

// Conn returns the underlying ClickHouse driver connection for advanced
// operations such as PrepareBatch.
func (c *ClickHouseClient) Conn() driver.Conn {
	return c.conn
}

func (c *ClickHouseClient) InsertLogs(ctx context.Context, logs []LogEntry) error {
	if len(logs) == 0 {
		return nil
	}

	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO "+c.WriteTable()+" (timestamp, raw_log, log_id, fields, fractal_id, ingest_timestamp)")
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

// EnsureColdStoragePolicy switches the logs table onto the 'tiered' storage
// policy when cold storage is enabled. The policy and its disks (hot 'default'
// volume + cold object-storage volume) must be defined in server config
// (config.d/storage.xml); this only points the table at it. Idempotent: a no-op
// when logs is already on the tiered policy.
func (c *ClickHouseClient) EnsureColdStoragePolicy(ctx context.Context) error {
	var policy string
	if err := c.conn.QueryRow(ctx,
		"SELECT storage_policy FROM system.tables WHERE database = currentDatabase() AND name = 'logs'",
	).Scan(&policy); err != nil {
		return fmt.Errorf("failed to read logs storage policy: %w", err)
	}
	if policy == "tiered" {
		return nil
	}
	stmt := c.InjectOnCluster("ALTER TABLE logs MODIFY SETTING storage_policy = 'tiered'")
	if err := c.conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("failed to apply tiered storage policy: %w", err)
	}
	log.Printf("Applied 'tiered' storage policy to logs table (was '%s')", policy)
	return nil
}

// parsePartitionDate extracts the date element from a ClickHouse partition tuple
// string of the form ('fractal-id','2024-01-15'). The date is always the final
// single-quoted token, so we read between the last two quotes.
func parsePartitionDate(p string) (time.Time, bool) {
	last := strings.LastIndex(p, "'")
	if last < 0 {
		return time.Time{}, false
	}
	prev := strings.LastIndex(p[:last], "'")
	if prev < 0 {
		return time.Time{}, false
	}
	t, err := time.Parse("2006-01-02", p[prev+1:last])
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// MoveOldPartitionsToCold relocates active log partitions older than coldDays for
// the given fractal from the hot volume to the cold object-storage volume. The
// data stays in the same logs table and remains queryable in place; this is a
// per-partition metadata-light move, mirroring the partition-enumeration approach
// of DeleteLogsByFractalID. Idempotent: parts already on the cold volume report a
// non-default disk and are skipped, and concurrent-move races are tolerated.
//
// PARTITION BY (fractal_id, toDate(timestamp)) makes each partition a single
// fractal+day, so per-fractal age-based tiering is exact. In cluster mode the
// move is issued ON CLUSTER (MOVE PARTITION is not auto-replicated) and the
// partition list is gathered across all replicas.
func (c *ClickHouseClient) MoveOldPartitionsToCold(ctx context.Context, fractalID string, coldDays int, isDefault bool) (int, error) {
	source := "system.parts"
	if c.Cluster != "" {
		source = fmt.Sprintf("clusterAllReplicas('%s', system.parts)", EscCHStr(c.Cluster))
	}

	// Only consider parts still on the hot tier. The hot volume is built on the
	// 'default' disk; parts already moved to cold report their cache disk name.
	rows, err := c.conn.Query(ctx,
		"SELECT DISTINCT partition FROM "+source+" WHERE database = currentDatabase() AND table = 'logs' AND active = 1 AND disk_name = 'default'",
	)
	if err != nil {
		return 0, fmt.Errorf("failed to list hot partitions for fractal %s: %w", fractalID, err)
	}

	prefixes := []string{fmt.Sprintf("('%s','", strings.ReplaceAll(fractalID, "'", "''"))}
	if isDefault {
		// Default fractal also owns rows with an empty fractal_id.
		prefixes = append(prefixes, "('','")
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -coldDays)

	var toMove []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			rows.Close()
			return 0, fmt.Errorf("failed to scan partition: %w", err)
		}
		matched := false
		for _, prefix := range prefixes {
			if strings.HasPrefix(p, prefix) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		if d, ok := parsePartitionDate(p); ok && d.Before(cutoff) {
			toMove = append(toMove, p)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("partition query error for fractal %s: %w", fractalID, err)
	}

	moved := 0
	for _, partition := range toMove {
		stmt := c.InjectOnCluster("ALTER TABLE logs MOVE PARTITION " + partition + " TO VOLUME 'cold'")
		if err := c.conn.Exec(ctx, stmt); err != nil {
			// Tolerate races: a concurrent merge or background move may already be
			// relocating the partition. Log and continue rather than failing the run.
			log.Printf("cold move skipped for partition %s (fractal %s): %v", partition, fractalID, err)
			continue
		}
		moved++
	}
	if moved > 0 {
		log.Printf("Moved %d partitions to cold storage for fractal %s", moved, fractalID)
	}
	return moved, nil
}

// QueryWithID executes a query with a fixed query_id so the run can be
// correlated with system.query_log entries for profiling.
func (c *ClickHouseClient) QueryWithID(ctx context.Context, queryID, query string) ([]map[string]interface{}, error) {
	ctx = clickhouse.Context(ctx, clickhouse.WithQueryID(queryID))
	return c.Query(ctx, query)
}

// QueryLowPriority executes a query at ClickHouse priority 5, yielding CPU
// to user-facing queries (priority 0) when both are competing for threads.
// Use for background work (alert evaluation) that should never starve users.
func (c *ClickHouseClient) QueryLowPriority(ctx context.Context, query string) ([]map[string]interface{}, error) {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"priority": 5}))
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

// scanRowMap scans the current row of rows into a map[string]interface{} using
// the supplied column types. It is shared by the buffered Query path and the
// streaming StreamQuery path so the two never diverge in type handling. The
// caller must have already positioned the cursor via rows.Next().
func scanRowMap(columnTypes []driver.ColumnType, rows driver.Rows) (map[string]interface{}, error) {
	// Create typed destination variables based on column types
	values := make([]interface{}, len(columnTypes))
	for i, col := range columnTypes {
		typeName := col.DatabaseTypeName()
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
	var opts []clickhouse.QueryOption
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
	if len(opts) > 0 {
		ctx = clickhouse.Context(ctx, opts...)
	}

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

	query := fmt.Sprintf(
		"SELECT timestamp, raw_log, log_id, toString(fields) AS fields, fractal_id, ingest_timestamp FROM %s WHERE log_id = ?",
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
		typeName := col.DatabaseTypeName()
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
func (c *ClickHouseClient) GetLogFieldsByID(ctx context.Context, logID string, ts time.Time, fractalID string) (map[string]interface{}, error) {
	if logID == "" {
		return nil, fmt.Errorf("log_id is required")
	}
	if ts.IsZero() {
		return nil, fmt.Errorf("timestamp is required")
	}

	query := fmt.Sprintf(
		"SELECT log_id, fractal_id, toString(fields) AS fields FROM %s WHERE log_id = ? AND timestamp = toDateTime64(?, 3, 'UTC')",
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

	var resLogID, logFractalID, fieldsStr string
	if err := rows.Scan(&resLogID, &logFractalID, &fieldsStr); err != nil {
		return nil, fmt.Errorf("failed to scan log fields row: %w", err)
	}
	entry := map[string]interface{}{"log_id": resLogID, "fractal_id": logFractalID}
	var m map[string]interface{}
	if json.Unmarshal([]byte(fieldsStr), &m) == nil {
		entry["fields"] = m
	} else {
		entry["fields"] = map[string]interface{}{}
	}
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

	query := "SELECT log_id, fractal_id, toString(fields) AS fields FROM logs WHERE log_id = ? AND timestamp = toDateTime64(?, 3, 'UTC')"
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

	var resLogID, logFractalID, fieldsStr string
	if err := rows.Scan(&resLogID, &logFractalID, &fieldsStr); err != nil {
		return nil, fmt.Errorf("failed to scan log fields row: %w", err)
	}
	entry := map[string]interface{}{"log_id": resLogID, "fractal_id": logFractalID}
	var m map[string]interface{}
	if json.Unmarshal([]byte(fieldsStr), &m) == nil {
		entry["fields"] = m
	} else {
		entry["fields"] = map[string]interface{}{}
	}
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
			fmt.Sprintf("SELECT log_id, fractal_id, toString(fields) AS fields FROM %s WHERE log_id IN (?) AND fractal_id = ?", c.ReadTable()),
			logIDs, fractalID)
	} else {
		rows, err = c.conn.Query(ctx,
			fmt.Sprintf("SELECT log_id, fractal_id, toString(fields) AS fields FROM %s WHERE log_id IN (?)", c.ReadTable()),
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
		var m map[string]interface{}
		if json.Unmarshal([]byte(fieldsStr), &m) == nil {
			entry["fields"] = m
		} else {
			entry["fields"] = map[string]interface{}{}
		}
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
// On ReplicatedMergeTree (cluster mode), DROP PARTITION replicates
// automatically via ZooKeeper. InjectOnCluster is used for DDL consistency.
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

// dropExpiredHotPartitions queries system.parts for logs_hot partitions whose
// max_time is older than 2 hours and drops them. Safe to call concurrently.
func (c *ClickHouseClient) dropExpiredHotPartitions(ctx context.Context) {
	rows, err := c.conn.Query(ctx,
		"SELECT DISTINCT partition FROM system.parts"+
			" WHERE database = currentDatabase() AND table = 'logs_hot'"+
			" AND active = 1 AND max_time < now() - INTERVAL 2 HOUR",
	)
	if err != nil {
		log.Printf("[HotTableCleaner] query partitions: %v", err)
		return
	}
	defer rows.Close()
	var partitions []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			log.Printf("[HotTableCleaner] scan partition: %v", err)
			return
		}
		partitions = append(partitions, p)
	}
	if err := rows.Err(); err != nil {
		log.Printf("[HotTableCleaner] rows error: %v", err)
		return
	}
	for _, partition := range partitions {
		dropSQL := c.InjectOnCluster(
			fmt.Sprintf("ALTER TABLE logs_hot DROP PARTITION '%s'", partition),
		)
		if err := c.conn.Exec(ctx, dropSQL); err != nil {
			log.Printf("[HotTableCleaner] drop partition %s: %v", partition, err)
		}
	}
}

// RawLogIndexName is the lower(raw_log) n-gram text index (migration 005) used to
// accelerate case-insensitive substring/regex search on raw_log.
const RawLogIndexName = "raw_log_ngram_lc"

// rawLogIndexBackfillLockID is a Postgres advisory-lock id that ensures only one
// replica submits the one-time MATERIALIZE INDEX backfill. Distinct from the
// schema-init lock ("bifract\0").
const rawLogIndexBackfillLockID int64 = 0x6269667261637401 // "bifract\x01"

// StartRawLogIndexBackfill materializes the lower(raw_log) n-gram index on parts
// written before the index existed, so historical data benefits from granule
// pruning. It never blocks startup:
//
//   - Schema init adds the index as metadata only (instant). Older parts carry no
//     index data until MATERIALIZE INDEX rebuilds them, which can take hours on
//     large tables, so the rebuild runs here in a background goroutine.
//   - The ALTER is submitted with alter_sync=0, so it returns as soon as the
//     mutation is queued and ClickHouse rebuilds parts asynchronously.
//   - A Postgres advisory lock ensures only one replica submits it. Submitting is
//     skipped when a matching mutation already exists; re-running would be cheap
//     anyway because ClickHouse skips parts that already carry the index.
//
// pg may be nil, in which case the system.mutations existence check is the only
// guard (sufficient for single-replica deployments).
func (c *ClickHouseClient) StartRawLogIndexBackfill(ctx context.Context, pg *PostgresClient) {
	go func() {
		if pg != nil {
			unlock, ok := pg.TryAdvisoryLock(ctx, rawLogIndexBackfillLockID)
			if !ok {
				return // another replica owns the backfill
			}
			defer unlock()
		}

		exists, err := c.indexMutationExists(ctx, RawLogIndexName)
		if err != nil {
			log.Printf("[IndexBackfill] check existing mutation: %v", err)
			return
		}
		if exists {
			return // already submitted (running or finished)
		}
		if err := c.submitMaterializeIndex(ctx, RawLogIndexName); err != nil {
			log.Printf("[IndexBackfill] submit MATERIALIZE INDEX %s: %v", RawLogIndexName, err)
			return
		}
		log.Printf("[IndexBackfill] submitted MATERIALIZE INDEX %s; backfilling existing parts in the background", RawLogIndexName)
		c.awaitIndexMutation(ctx, RawLogIndexName)
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

// SyncJSONTypeHints merges extraFields into the type hints declared on the
// fields JSON column and adds bloom_filter skip indexes for any newly added
// fields. It is a no-op when all requested fields are already hinted.
//
// The operation schedules a background mutation on existing parts; new parts
// receive the updated schema immediately. On large tables, callers should run
// MATERIALIZE INDEX for each new index during off-peak hours.
func (c *ClickHouseClient) SyncJSONTypeHints(ctx context.Context, extraFields []string) error {
	// Read the current column type string from system.columns.
	rows, err := c.conn.Query(ctx,
		"SELECT type FROM system.columns WHERE database = 'logs' AND table = 'logs' AND name = 'fields'")
	if err != nil {
		return fmt.Errorf("read fields column type: %w", err)
	}
	var currentType string
	if rows.Next() {
		_ = rows.Scan(&currentType)
	}
	rows.Close()

	// Parse existing type-hinted field names. The type string looks like:
	// JSON(max_dynamic_paths=1024, `src_ip` String, `user` String, ...)
	existing := parseJSONTypeHints(currentType)

	// Compute the union of existing and requested fields.
	merged := make(map[string]struct{}, len(existing))
	for _, f := range existing {
		merged[f] = struct{}{}
	}
	var newFields []string
	for _, f := range extraFields {
		if f == "" {
			continue
		}
		if _, ok := merged[f]; !ok {
			merged[f] = struct{}{}
			newFields = append(newFields, f)
		}
	}
	if len(newFields) == 0 {
		return nil
	}

	// Build MODIFY COLUMN with the full merged set.
	var sb strings.Builder
	sb.WriteString("ALTER TABLE logs MODIFY COLUMN fields JSON(\n    max_dynamic_paths=1024")
	for f := range merged {
		escaped := strings.ReplaceAll(f, "`", "``")
		sb.WriteString(",\n    `")
		sb.WriteString(escaped)
		sb.WriteString("` String")
	}
	sb.WriteString("\n)")

	modifySQL := c.InjectOnCluster(sb.String())
	if err := c.conn.Exec(ctx, modifySQL); err != nil {
		return fmt.Errorf("modify fields column: %w", err)
	}

	// Add a bloom_filter index for each newly added field.
	for _, f := range newFields {
		escaped := strings.ReplaceAll(f, "`", "``")
		idxName := "idx_" + strings.ReplaceAll(f, " ", "_")
		idxSQL := fmt.Sprintf(
			"ALTER TABLE logs ADD INDEX IF NOT EXISTS %s fields.`%s` TYPE bloom_filter(0.001) GRANULARITY 1",
			idxName, escaped,
		)
		idxSQL = c.InjectOnCluster(idxSQL)
		if err := c.conn.Exec(ctx, idxSQL); err != nil {
			log.Printf("Warning: add index %s: %v", idxName, err)
		}
	}
	return nil
}

// parseJSONTypeHints extracts field names from a ClickHouse JSON column type
// string of the form: JSON(max_dynamic_paths=N, `field` String, ...).
func parseJSONTypeHints(typeStr string) []string {
	var fields []string
	// Find each backtick-quoted identifier followed by a type keyword.
	re := regexp.MustCompile("`([^`]+)`\\s+\\w+")
	for _, match := range re.FindAllStringSubmatch(typeStr, -1) {
		if len(match) >= 2 {
			fields = append(fields, match[1])
		}
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

// ReconcileSchemaFields ensures ClickHouse has type hints and skip indexes for
// all requested fields. It is additive: existing type hints and indexes are
// never removed. New fields are added via MODIFY COLUMN and ADD INDEX IF NOT EXISTS.
// All DDL is wrapped with InjectOnCluster for multi-node deployments.
func (c *ClickHouseClient) ReconcileSchemaFields(ctx context.Context, fields []SchemaFieldSpec) error {
	// Read current type hints from ClickHouse.
	rows, err := c.conn.Query(ctx,
		"SELECT type FROM system.columns WHERE database = 'logs' AND table = 'logs' AND name = 'fields'")
	if err != nil {
		return fmt.Errorf("read fields column type: %w", err)
	}
	var currentType string
	if rows.Next() {
		_ = rows.Scan(&currentType)
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

	// Run MODIFY COLUMN only when there are new fields to add.
	if len(newFields) > 0 {
		var sb strings.Builder
		sb.WriteString("ALTER TABLE logs MODIFY COLUMN fields JSON(\n    max_dynamic_paths=1024")
		for f := range merged {
			escaped := strings.ReplaceAll(f, "`", "``")
			sb.WriteString(",\n    `")
			sb.WriteString(escaped)
			sb.WriteString("` String")
		}
		sb.WriteString("\n)")
		colDef := sb.String()
		if err := c.conn.Exec(ctx, c.InjectOnCluster(colDef)); err != nil {
			return fmt.Errorf("modify fields column: %w", err)
		}
		// Mirror to logs_hot so the parser's type-hint registry stays in sync with
		// both tables. Without this, newly-registered fields generate bare subcolumn
		// references (no ::String cast) that return NULL on logs_hot because the
		// typed subcolumn only exists in logs.
		// No skip indexes needed on logs_hot — its ORDER BY covers alert query patterns.
		hotColDef := strings.Replace(colDef, "ALTER TABLE logs ", "ALTER TABLE logs_hot ", 1)
		if err := c.conn.Exec(ctx, c.InjectOnCluster(hotColDef)); err != nil {
			log.Printf("Warning: mirror type hints to logs_hot: %v", err)
		}
	}

	// Ensure each desired field has the correct skip index.
	// ADD INDEX IF NOT EXISTS is a no-op when the index already exists.
	for _, f := range fields {
		var idxExpr string
		switch f.IndexType {
		case "none":
			// Type hint only (dedicated sub-column already applied via MODIFY COLUMN
			// above); no skip index. Skip writes/merges pay for nothing otherwise.
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
		}
	}
	return nil
}

// TruncateAndReschema deletes all log data by truncating the logs tables, drops
// all managed skip indexes, then calls ReconcileSchemaFields to apply the desired
// schema fresh. TRUNCATE does not require the force_drop_table filesystem flag.
func (c *ClickHouseClient) TruncateAndReschema(ctx context.Context, fields []SchemaFieldSpec) error {
	for _, tbl := range []string{"logs.logs_histogram", "logs.logs"} {
		sql := fmt.Sprintf("TRUNCATE TABLE %s", tbl)
		if err := c.conn.Exec(ctx, c.InjectOnCluster(sql)); err != nil {
			return fmt.Errorf("truncate %s: %w", tbl, err)
		}
	}

	// Drop all managed skip indexes so ReconcileSchemaFields can recreate them
	// with the correct expressions from scratch.
	for _, f := range fields {
		idxName := schemaFieldIndexName(f.FieldName)
		dropSQL := fmt.Sprintf("ALTER TABLE logs DROP INDEX IF EXISTS %s", idxName)
		if err := c.conn.Exec(ctx, c.InjectOnCluster(dropSQL)); err != nil {
			log.Printf("Warning: drop index %s: %v", idxName, err)
		}
	}

	return c.ReconcileSchemaFields(ctx, fields)
}

// schemaFieldIndexName returns the skip-index name for a custom field. Add and drop
// paths must use this single source of truth so the names can never drift apart.
func schemaFieldIndexName(field string) string {
	return "idx_" + strings.ReplaceAll(field, " ", "_")
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
