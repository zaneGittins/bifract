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

// rewriteEngineRe matches ENGINE = MergeTree(...) or ReplacingMergeTree(...) etc.
var rewriteEngineRe = regexp.MustCompile(`(?i)ENGINE\s*=\s*(MergeTree|ReplacingMergeTree)\s*\(\s*\)`)

// RewriteEngine replaces single-node table engines with their replicated
// equivalents when cluster mode is active. Returns the input unchanged for
// single-node deployments.
func (c *ClickHouseClient) RewriteEngine(sql string) string {
	if c.Cluster == "" {
		return sql
	}
	return rewriteEngineRe.ReplaceAllStringFunc(sql, func(match string) string {
		upper := strings.ToUpper(match)
		if strings.Contains(upper, "REPLACINGMERGETREE") {
			return "ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')"
		}
		return "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')"
	})
}

// injectOnClusterRe are precompiled patterns for DDL statement prefixes.
// Each captures the portion before where the ON CLUSTER clause belongs.
var injectOnClusterPatterns = []struct {
	prefix string
	re     *regexp.Regexp
}{
	{"CREATE TABLE", regexp.MustCompile(`(?i)(CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?\S+)`)},
	{"ALTER TABLE", regexp.MustCompile(`(?i)(ALTER\s+TABLE\s+\S+)`)},
	{"TRUNCATE", regexp.MustCompile(`(?i)(TRUNCATE\s+TABLE\s+(?:IF\s+EXISTS\s+)?\S+)`)},
	{"DROP TABLE", regexp.MustCompile(`(?i)(DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?\S+)`)},
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
	Timestamp      time.Time
	IngestTimestamp time.Time
	RawLog         string
	LogID          string
	Fields         map[string]string
	FractalID      string // Fractal UUID for multi-tenant isolation
}

func (c *ClickHouseClient) Initialize(ctx context.Context, sql string) error {
	var count uint64
	err := c.conn.QueryRow(ctx, `SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'logs'`).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check clickhouse initialization: %w", err)
	}
	tableExists := count > 0

	for _, stmt := range splitClickHouseSQL(sql) {
		upper := strings.ToUpper(strings.TrimSpace(stmt))
		// If the table already exists, only run ALTER statements (idempotent schema fixes).
		if tableExists && !strings.HasPrefix(upper, "ALTER ") {
			continue
		}
		// In cluster mode, inject ON CLUSTER and rewrite engines to replicated variants.
		stmt = c.InjectOnCluster(stmt)
		stmt = c.RewriteEngine(stmt)
		if err := c.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("failed to execute clickhouse init statement: %w\nstatement: %s", err, stmt)
		}
	}

	// In cluster mode, create the Distributed table for cross-shard reads.
	if c.IsCluster() {
		distSQL := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS logs_distributed%s AS logs ENGINE = Distributed('%s', currentDatabase(), 'logs', rand())",
			c.OnClusterSQL(), EscCHStr(c.Cluster),
		)
		if err := c.conn.Exec(ctx, distSQL); err != nil {
			return fmt.Errorf("failed to create distributed table: %w\nstatement: %s", err, distSQL)
		}
	}

	return nil
}

// splitClickHouseSQL splits a SQL string into individual statements, skipping
// USE and CREATE DATABASE statements since the DB is managed by the container env.
func splitClickHouseSQL(sql string) []string {
	parts := strings.Split(sql, ";")
	var stmts []string
	for _, part := range parts {
		stmt := strings.TrimSpace(part)
		if stmt == "" {
			continue
		}
		upper := strings.ToUpper(stmt)
		if strings.HasPrefix(upper, "USE ") || strings.HasPrefix(upper, "CREATE DATABASE") {
			continue
		}
		// Skip blocks that are only comments
		allComment := true
		for _, line := range strings.Split(stmt, "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed != "" && !strings.HasPrefix(trimmed, "--") {
				allComment = false
				break
			}
		}
		if !allComment {
			stmts = append(stmts, stmt)
		}
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
// multiple ClickHouse nodes. The driver handles failover and load-balancing
// across the provided addresses.
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
	// database is created ON CLUSTER before opening the real connection.
	bootstrap, err := openClickHouseConn(addrs, "default", user, password, pool)
	if err != nil {
		return nil, fmt.Errorf("bootstrap connection: %w", err)
	}
	createDB := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s ON CLUSTER '%s'",
		EscCHStr(database), EscCHStr(cluster))
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

	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO logs (timestamp, raw_log, log_id, fields, fractal_id, ingest_timestamp)")
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

// DeleteLogsByFractalID deletes all logs for a specific fractal using a
// lightweight delete. The session max_execution_time is overridden to unlimited
// for this query so large fractals don't hit the default 60s cap.
func (c *ClickHouseClient) DeleteLogsByFractalID(ctx context.Context, fractalID string) error {
	return c.DeleteLogsByFractalIDOpt(ctx, fractalID, true)
}

// DeleteLogsByFractalIDOpt is like DeleteLogsByFractalID but allows skipping
// the OPTIMIZE TABLE FINAL that forces immediate merge of deleted rows.
// The lightweight delete takes effect immediately for queries; the optimize
// only affects reported disk size and can be deferred.
func (c *ClickHouseClient) DeleteLogsByFractalIDOpt(ctx context.Context, fractalID string, optimize bool) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"max_execution_time": 0,
	}))
	// Lightweight deletes replicate automatically via ZooKeeper on
	// ReplicatedMergeTree tables, so ON CLUSTER is not needed and would
	// cause the query to block waiting for all replicas synchronously.
	err := c.conn.Exec(ctx, "DELETE FROM logs WHERE fractal_id = ?", fractalID)
	if err != nil {
		return fmt.Errorf("failed to delete logs for fractal %s: %w", fractalID, err)
	}

	if optimize {
		// Force ClickHouse to merge away lightweight-deleted rows so that
		// system.parts reflects the actual on-disk size. Without this, the
		// deleted rows stay in parts indefinitely and inflate the reported
		// storage size for other fractals sharing the table.
		optimizeSQL := c.InjectOnCluster("OPTIMIZE TABLE logs FINAL")
		if err := c.conn.Exec(ctx, optimizeSQL); err != nil {
			log.Printf("Warning: OPTIMIZE after delete for fractal %s failed: %v", fractalID, err)
		}
	}

	log.Printf("Successfully deleted logs for fractal %s", fractalID)
	return nil
}

func (c *ClickHouseClient) Query(ctx context.Context, query string) ([]map[string]interface{}, error) {
	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	columnTypes := rows.ColumnTypes()

	// Debug: removed debug logging

	for rows.Next() {
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

		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
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

// QueryRow executes a query that is expected to return at most one row
func (c *ClickHouseClient) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
	return c.conn.QueryRow(ctx, query, args...)
}

// GetLogByTimestamp fetches a log by log_id ONLY - no timestamp fallback
func (c *ClickHouseClient) GetLogByTimestamp(ctx context.Context, timestamp time.Time, logID string, fractalID string) (map[string]interface{}, error) {
	if logID == "" {
		return nil, fmt.Errorf("log_id is required")
	}

	log.Printf("[GetLogByTimestamp] Searching for log_id: %s", logID)

	// Build query using ordering key columns (fractal_id, timestamp, log_id) for efficient index usage.
	query := fmt.Sprintf("SELECT timestamp, raw_log, log_id, toString(fields) AS fields, fractal_id, ingest_timestamp FROM %s WHERE log_id = ?", c.ReadTable())
	args := []interface{}{logID}

	if fractalID != "" {
		query += " AND fractal_id = ?"
		args = append(args, fractalID)
	}
	if !timestamp.IsZero() {
		// Use a window around the timestamp to account for precision differences
		// between PostgreSQL and ClickHouse DateTime64(3). The log_id filter is
		// the real unique key; this just helps narrow partition/index scanning.
		query += " AND timestamp >= ? AND timestamp <= ?"
		args = append(args, timestamp.Add(-5*time.Second), timestamp.Add(5*time.Second))
	}
	query += " LIMIT 1"

	// Use explicit column list to avoid scan issues with the JSON column type (SELECT * would fail)
	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query log: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	columnTypes := rows.ColumnTypes()

	for rows.Next() {
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
			default:
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

		results = append(results, row)
	}

	if len(results) == 0 {
		log.Printf("[GetLogByTimestamp] No log found with log_id: %s", logID)
		return nil, nil
	}

	log.Printf("[GetLogByTimestamp] Found log with %d fields", len(results[0]))
	return results[0], nil
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
			fmt.Sprintf("SELECT log_id, toString(fields) AS fields FROM %s WHERE log_id IN (?) AND fractal_id = ?", c.ReadTable()),
			logIDs, fractalID)
	} else {
		rows, err = c.conn.Query(ctx,
			fmt.Sprintf("SELECT log_id, toString(fields) AS fields FROM %s WHERE log_id IN (?)", c.ReadTable()),
			logIDs)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query logs by IDs: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var logID, fieldsStr string
		if err := rows.Scan(&logID, &fieldsStr); err != nil {
			return nil, fmt.Errorf("failed to scan log fields row: %w", err)
		}
		entry := map[string]interface{}{"log_id": logID}
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
