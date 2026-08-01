package db

import "embed"

//go:embed init-postgres.sql
var PostgresSQL string

//go:embed init-clickhouse.sql
var ClickHouseSQL string

//go:embed migrations/clickhouse/*.sql
var ClickHouseMigrations embed.FS

const ClickHouseMigrationsDir = "migrations/clickhouse"

// RawLogSplitMigration is 013_logs_raw_split, which removed logs.raw_log. A logs table
// without that column is at or past this level no matter what _bifract_migrations says:
// migrations 004 and 012 project raw_log and cannot be replayed against it. Used to stamp
// a schema that was provisioned from init-clickhouse.sql rather than by the migration runner.
const RawLogSplitMigration = 13
