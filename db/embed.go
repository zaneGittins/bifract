package db

import _ "embed"

//go:embed init-postgres.sql
var PostgresSQL string

//go:embed init-clickhouse.sql
var ClickHouseSQL string
