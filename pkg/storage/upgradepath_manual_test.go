//go:build chupgrade

// Verifies the oldest supported upgrade path still works: a v0.0.2 install (whose init SQL
// created only the logs table, with _bifract_migrations stamped at 1) taken all the way to
// the current schema by the numbered migrations alone.
//
//	docker run -d --name bfupgrade -e CLICKHOUSE_PASSWORD=bifract -p 19010:9000 \
//	  clickhouse/clickhouse-server:26.6.2.81-alpine
//	go test -tags chupgrade ./pkg/storage/ -run TestUpgradeFrom -v
package storage

import (
	"context"
	"os"
	"testing"

	dbsql "bifract/db"
)

const upgradeAddr = "localhost:19010"

func upgradeConn(t *testing.T, database string) interface {
	Exec(context.Context, string, ...interface{}) error
} {
	t.Helper()
	conn, err := openClickHouseConn([]string{upgradeAddr}, database, "default", "bifract",
		ClickHousePoolConfig{MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		t.Fatalf("connect (%s): %v", database, err)
	}
	return conn
}

// TestUpgradeFromV002 replays the real v0.0.2 -> current path.
func TestUpgradeFromV002(t *testing.T) {
	// Pinned copy of db/init-clickhouse.sql as of tag v0.0.2, so the test does not depend
	// on git history being present.
	v002SQL, err := os.ReadFile("testdata/v002-init-clickhouse.sql")
	if err != nil {
		t.Fatalf("read v0.0.2 init SQL: %v", err)
	}

	root, err := openClickHouseConn([]string{upgradeAddr}, "default", "default", "bifract",
		ClickHousePoolConfig{MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	ctx := context.Background()
	if err := root.Exec(ctx, "DROP DATABASE IF EXISTS logs SYNC"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if err := root.Exec(ctx, "CREATE DATABASE logs"); err != nil {
		t.Fatalf("create db: %v", err)
	}
	root.Close()

	conn, err := openClickHouseConn([]string{upgradeAddr}, "logs", "default", "bifract",
		ClickHousePoolConfig{MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		t.Fatalf("connect to logs: %v", err)
	}
	defer conn.Close()

	// The v0.0.2 schema.
	for _, stmt := range splitClickHouseSQL(string(v002SQL)) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("v0.0.2 init statement failed: %v\n%s", err, stmt)
		}
	}

	// bifract-setup stamped v0.0.2 installs at migration 1 (internal/setup/migrate.go).
	if err := createMigrationBookkeeping(ctx, conn, nil); err != nil {
		t.Fatalf("bookkeeping: %v", err)
	}
	if err := conn.Exec(ctx,
		"INSERT INTO logs._bifract_migrations (number, name) VALUES (1, '001_initial')"); err != nil {
		t.Fatalf("stamp: %v", err)
	}

	n, err := runMigrationsOnConn(ctx, conn, nil, false, dbsql.ClickHouseMigrations, dbsql.ClickHouseMigrationsDir)
	if err != nil {
		t.Fatalf("upgrade from v0.0.2 FAILED after %d migrations: %v", n, err)
	}
	t.Logf("applied %d migrations", n)

	var maxNum uint32
	if err := conn.QueryRow(ctx, "SELECT max(number) FROM logs._bifract_migrations").Scan(&maxNum); err != nil {
		t.Fatalf("read state: %v", err)
	}
	if maxNum != 13 {
		t.Errorf("ended at migration %d, want 13", maxNum)
	}

	// raw_log must be gone from logs (013) and logs_raw must exist.
	for _, tbl := range []string{"logs_raw", "logs_hot", "proc_lineage", "proc_freq", "process_edges"} {
		ok, err := chTableExists(ctx, conn, tbl)
		if err != nil || !ok {
			t.Errorf("%s missing after upgrade (err=%v)", tbl, err)
		}
	}
	var rawCols uint64
	conn.QueryRow(ctx,
		"SELECT count() FROM system.columns WHERE database='logs' AND table='logs' AND name='raw_log'").Scan(&rawCols)
	if rawCols != 0 {
		t.Errorf("logs.raw_log still present after migration 013")
	}
}
