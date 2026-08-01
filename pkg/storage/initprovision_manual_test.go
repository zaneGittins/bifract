//go:build chupgrade

// Regression: a schema provisioned from init-clickhouse.sql outside the migration runner
// (older installers mounted it into the ClickHouse container's docker-entrypoint-initdb.d)
// carries no migration stamp. Replaying history against it dies on 004, which projects the
// raw_log column that 013 removed.
//
//	docker run -d --name bfupgrade -e CLICKHOUSE_PASSWORD=bifract -p 19010:9000 \
//	  clickhouse/clickhouse-server:26.6.2.81-alpine
//	go test -tags chupgrade ./pkg/storage/ -run TestInitProvisioned -v
package storage

import (
	"context"
	"testing"

	dbsql "bifract/db"
)

func TestInitProvisionedSchemaIsStampedNotReplayed(t *testing.T) {
	ctx := context.Background()

	root, err := openClickHouseConn([]string{upgradeAddr}, "default", "default", "bifract",
		ClickHousePoolConfig{MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := root.Exec(ctx, "DROP DATABASE IF EXISTS logs SYNC"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if err := root.Exec(ctx, "CREATE DATABASE logs"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	root.Close()

	logsConn, err := openClickHouseConn([]string{upgradeAddr}, "logs", "default", "bifract",
		ClickHousePoolConfig{MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		t.Fatalf("connect to logs: %v", err)
	}
	defer logsConn.Close()

	// What the ClickHouse container's initdb did: current schema, no migration records.
	if err := runInitSQLOnConn(ctx, logsConn, dbsql.ClickHouseSQL, nil); err != nil {
		t.Fatalf("provision from init SQL: %v", err)
	}

	if err := stampProvisionedSchema(ctx, logsConn, dbsql.ClickHouseMigrations, dbsql.ClickHouseMigrationsDir); err != nil {
		t.Fatalf("stamp provisioned schema: %v", err)
	}
	if _, err := runMigrationsOnConn(ctx, logsConn, nil, false, dbsql.ClickHouseMigrations, dbsql.ClickHouseMigrationsDir); err != nil {
		t.Fatalf("migrations against init-provisioned schema FAILED: %v", err)
	}

	var maxNum uint32
	if err := logsConn.QueryRow(ctx, "SELECT max(number) FROM logs._bifract_migrations").Scan(&maxNum); err != nil {
		t.Fatalf("read state: %v", err)
	}
	if maxNum < dbsql.RawLogSplitMigration {
		t.Errorf("recorded migration level %d, want at least %d", maxNum, dbsql.RawLogSplitMigration)
	}

	// A pre-013 schema must still replay normally: the marker is the missing column.
	if err := logsConn.Exec(ctx, "ALTER TABLE logs ADD COLUMN IF NOT EXISTS raw_log String"); err != nil {
		t.Fatalf("re-add raw_log: %v", err)
	}
	if err := logsConn.Exec(ctx, "TRUNCATE TABLE logs._bifract_migrations"); err != nil {
		t.Fatalf("clear stamps: %v", err)
	}
	if err := stampProvisionedSchema(ctx, logsConn, dbsql.ClickHouseMigrations, dbsql.ClickHouseMigrationsDir); err != nil {
		t.Fatalf("stamp with raw_log present: %v", err)
	}
	if err := logsConn.QueryRow(ctx, "SELECT max(number) FROM logs._bifract_migrations").Scan(&maxNum); err != nil {
		t.Fatalf("read state: %v", err)
	}
	if maxNum != 0 {
		t.Errorf("stamped level %d on a schema that still has raw_log, want 0", maxNum)
	}
}
