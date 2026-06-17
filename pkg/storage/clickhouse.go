package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"
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

func (c *ClickHouseClient) Initialize(ctx context.Context, sql string) error {
	var count uint64
	err := c.conn.QueryRow(ctx, `SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'logs'`).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check clickhouse initialization: %w", err)
	}
	tableExists := count > 0

	if c.IsCluster() && tableExists {
		// Upgrade path: new tables (e.g. logs_histogram) may not exist on all shards yet.
		// ON CLUSTER times out when replica pods are absent, so push all CREATE/ALTER
		// statements — including distributed tables — to each shard host individually.
		// IF NOT EXISTS makes every statement idempotent.
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
		initPool := ClickHousePoolConfig{MaxOpenConns: 1, MaxIdleConns: 1, DialTimeout: 10 * time.Second}
		for _, addr := range c.addrs {
			hostConn, err := openClickHouseConn([]string{addr}, c.Database, c.User, c.Password, initPool)
			if err != nil {
				log.Printf("Warning: cluster schema sync to %s failed: %v", addr, err)
				continue
			}
			for _, stmt := range splitClickHouseSQL(sql) {
				upper := strings.ToUpper(strings.TrimSpace(stmt))
				if !strings.HasPrefix(upper, "CREATE ") && !strings.HasPrefix(upper, "ALTER ") {
					continue
				}
				stmt = c.RewriteEngine(stmt)
				if execErr := hostConn.Exec(ctx, stmt); execErr != nil {
					log.Printf("Warning: schema sync on %s: %v", addr, execErr)
				}
			}
			for _, stmt := range []string{distSQL, histDistSQL, hotDistSQL} {
				if execErr := hostConn.Exec(ctx, stmt); execErr != nil {
					log.Printf("Warning: distributed table sync on %s: %v", addr, execErr)
				}
			}
			hostConn.Close()
		}
	} else {
		for _, stmt := range splitClickHouseSQL(sql) {
			upper := strings.ToUpper(strings.TrimSpace(stmt))
			if tableExists && !strings.HasPrefix(upper, "ALTER ") && !strings.HasPrefix(upper, "CREATE ") {
				continue
			}
			stmt = c.InjectOnCluster(stmt)
			stmt = c.RewriteEngine(stmt)
			if err := c.conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("failed to execute clickhouse init statement: %w\nstatement: %s", err, stmt)
			}
		}

		// Fresh install: create Distributed tables ON CLUSTER so all shards get them.
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
	}

	return nil
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
			"max_execution_time":     60,
			"use_uncompressed_cache": 1,
			"output_format_native_use_flattened_dynamic_and_json_serialization": 1,
		},
		DialTimeout:     pool.DialTimeout,
		ReadTimeout:     0, // no TCP read deadline; server-side max_execution_time enforces query limits
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

// QueryWithID executes a query with a fixed query_id so the run can be
// correlated with system.query_log entries for profiling.
func (c *ClickHouseClient) QueryWithID(ctx context.Context, queryID, query string) ([]map[string]interface{}, error) {
	ctx = clickhouse.Context(ctx, clickhouse.WithQueryID(queryID))
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

// GetLogByTimestamp fetches a single log by log_id with an optional fractal_id
// scope. When fractalID is non-empty the query is restricted to that fractal
// (used by comment creation to prevent cross-fractal log_id probing). When
// empty, the lookup is unscoped and the caller must enforce access control.
func (c *ClickHouseClient) GetLogByTimestamp(ctx context.Context, timestamp time.Time, logID string, fractalID string) (map[string]interface{}, error) {
	if logID == "" {
		return nil, fmt.Errorf("log_id is required")
	}

	query := fmt.Sprintf(
		"SELECT timestamp, raw_log, log_id, toString(fields) AS fields, fractal_id, ingest_timestamp FROM %s WHERE log_id = ?",
		c.ReadTable())
	args := []interface{}{logID}

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

// GetLogFieldsByID fetches the parsed fields for a single log. When ts is
// non-zero it is added as an exact-match predicate: because the log table is
// ORDER BY (timestamp, log_id) and PARTITION BY (fractal_id, toDate(timestamp)),
// an equality on timestamp prunes to a single date partition and pins the
// primary index to one granule, turning a full-table bloom-filter scan into a
// near-pinpoint read. The frontend supplies the exact ClickHouse timestamp from
// the search result (not round-tripped through Postgres), so it bit-matches the
// DateTime64(3) value.
//
// If the timestamped lookup returns nothing (e.g. a caller passed a stale or
// reformatted timestamp), it transparently falls back to the log_id-only scan
// so correctness never regresses.
//
// fractalID is normally left empty by callers that must read the log's own
// fractal_id and verify it against the session's accessible set afterwards
// (this is what keeps the lookup correct for both fractal and prism sessions).
func (c *ClickHouseClient) GetLogFieldsByID(ctx context.Context, logID string, ts time.Time, fractalID string) (map[string]interface{}, error) {
	if logID == "" {
		return nil, fmt.Errorf("log_id is required")
	}

	if !ts.IsZero() {
		entry, err := c.queryLogFields(ctx, logID, ts, fractalID)
		if err != nil {
			return nil, err
		}
		if entry != nil {
			return entry, nil
		}
		// Fall through to the untimed scan: the timestamp didn't match a row
		// (stale/reformatted value), so don't fail-closed on it.
		log.Printf("[GetLogFieldsByID] timestamp exact-match missed for log_id=%s ts=%s, falling back to full scan", logID, ts.UTC().Format("2006-01-02 15:04:05.000"))
	}

	return c.queryLogFields(ctx, logID, time.Time{}, fractalID)
}

// queryLogFields runs a single-row field lookup. A non-zero ts adds an exact
// timestamp predicate; a non-empty fractalID scopes the lookup. Returns nil
// (no error) when no matching row exists.
func (c *ClickHouseClient) queryLogFields(ctx context.Context, logID string, ts time.Time, fractalID string) (map[string]interface{}, error) {
	query := fmt.Sprintf(
		"SELECT log_id, fractal_id, toString(fields) AS fields FROM %s WHERE log_id = ?",
		c.ReadTable())
	args := []interface{}{logID}
	if !ts.IsZero() {
		query += " AND timestamp = ?"
		args = append(args, ts)
	}
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
