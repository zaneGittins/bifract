package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickHouseClient struct {
	conn     driver.Conn
	User     string
	Password string
	Database string
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
		if err := c.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("failed to execute clickhouse init statement: %w\nstatement: %s", err, stmt)
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
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, port)},
		Auth: clickhouse.Auth{
			Database: database,
			Username: user,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time":     60,
			"use_uncompressed_cache": 1,
			"output_format_native_use_flattened_dynamic_and_json_serialization": 1,
			"json_type_escape_dots_in_keys":                                     1,
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

	return &ClickHouseClient{conn: conn, User: user, Password: password, Database: database}, nil
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

// DeleteLogsByFractalID deletes all logs for a specific fractal using a
// lightweight delete. The session max_execution_time is overridden to unlimited
// for this query so large fractals don't hit the default 60s cap.
func (c *ClickHouseClient) DeleteLogsByFractalID(ctx context.Context, fractalID string) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"max_execution_time": 0,
	}))
	err := c.conn.Exec(ctx, "DELETE FROM logs WHERE fractal_id = ?", fractalID)
	if err != nil {
		return fmt.Errorf("failed to delete logs for fractal %s: %w", fractalID, err)
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
						row[colName] = decodeJSONFieldKeys(m)
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

// decodeJSONFieldKeys decodes %2E back to '.' in JSON field keys so the
// frontend sees original field names (ClickHouse escapes dots when
// json_type_escape_dots_in_keys=1). Recurses into nested maps.
func decodeJSONFieldKeys(m map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		decoded := strings.ReplaceAll(k, "%2E", ".")
		if nested, ok := v.(map[string]interface{}); ok {
			out[decoded] = decodeJSONFieldKeys(nested)
		} else {
			out[decoded] = v
		}
	}
	return out
}

func (c *ClickHouseClient) CountLogs(ctx context.Context, startTime, endTime time.Time) (uint64, error) {
	var count uint64
	err := c.conn.QueryRow(ctx,
		"SELECT count() as count FROM logs WHERE timestamp >= ? AND timestamp <= ?",
		startTime, endTime,
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
func (c *ClickHouseClient) GetLogByTimestamp(ctx context.Context, timestamp time.Time, logID string) (map[string]interface{}, error) {
	if logID == "" {
		return nil, fmt.Errorf("log_id is required")
	}

	log.Printf("[GetLogByTimestamp] Searching for log_id: %s", logID)

	// Use explicit column list to avoid scan issues with the JSON column type (SELECT * would fail)
	rows, err := c.conn.Query(ctx,
		"SELECT timestamp, raw_log, log_id, toString(fields) AS fields, fractal_id, ingest_timestamp FROM logs WHERE log_id = ? LIMIT 1",
		logID)
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
						row[colName] = decodeJSONFieldKeys(m)
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
		return nil, fmt.Errorf("no log found with log_id: %s", logID)
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

	rows, err := c.conn.Query(ctx,
		"SELECT log_id, toString(fields) AS fields FROM logs WHERE log_id IN (?) AND fractal_id = ?",
		logIDs, fractalID)
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
			entry["fields"] = decodeJSONFieldKeys(m)
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
