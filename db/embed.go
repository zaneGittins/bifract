package db

import "embed"

//go:embed init-postgres.sql
var PostgresSQL string

//go:embed init-clickhouse.sql
var ClickHouseSQL string

//go:embed migrations/clickhouse/*.sql
var ClickHouseMigrations embed.FS

const ClickHouseMigrationsDir = "migrations/clickhouse"
