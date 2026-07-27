//go:build chcluster

// Manual harness for the cluster schema-init path. Needs the two-shard ClickHouse in
// test/chcluster (see the README there):
//
//	docker compose -p chtest -f test/chcluster/docker-compose.yml up -d
//	go test -tags chcluster ./pkg/storage/ -run TestClusterInit -v
package storage

import (
	"context"
	"fmt"
	"testing"

	dbsql "bifract/db"
)

var chTestHosts = []string{"localhost:19001", "localhost:19002"}

const chTestCluster = "bftest"

func chTestPool() ClickHousePoolConfig {
	return ClickHousePoolConfig{MaxOpenConns: 2, MaxIdleConns: 1}
}

func newTestClusterClient(t *testing.T) *ClickHouseClient {
	t.Helper()
	c, err := NewClickHouseClusterClient(chTestHosts, 9000, "logs", "default", "bifract", chTestCluster, chTestPool())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return c
}

// shardCounts reports (tables, max migration) per shard, read from each shard directly.
func shardCounts(t *testing.T) []struct {
	Tables    uint64
	Migration uint32
} {
	t.Helper()
	out := make([]struct {
		Tables    uint64
		Migration uint32
	}, 0, len(chTestHosts))
	for _, addr := range chTestHosts {
		conn, err := openClickHouseConn([]string{addr}, "default", "default", "bifract", chTestPool())
		if err != nil {
			t.Fatalf("open %s: %v", addr, err)
		}
		var row struct {
			Tables    uint64
			Migration uint32
		}
		if err := conn.QueryRow(context.Background(),
			"SELECT count() FROM system.tables WHERE database='logs'").Scan(&row.Tables); err != nil {
			t.Fatalf("count tables on %s: %v", addr, err)
		}
		conn.QueryRow(context.Background(),
			"SELECT max(number) FROM logs._bifract_migrations").Scan(&row.Migration)
		conn.Close()
		out = append(out, row)
	}
	return out
}

func dropEverything(t *testing.T) {
	t.Helper()
	for _, addr := range chTestHosts {
		conn, err := openClickHouseConn([]string{addr}, "default", "default", "bifract", chTestPool())
		if err != nil {
			t.Fatalf("open %s: %v", addr, err)
		}
		if err := conn.Exec(context.Background(), "DROP DATABASE IF EXISTS logs SYNC"); err != nil {
			t.Fatalf("drop on %s: %v", addr, err)
		}
		conn.Close()
	}
}

func execOnShard(t *testing.T, addr, query string) {
	t.Helper()
	conn, err := openClickHouseConn([]string{addr}, "default", "default", "bifract", chTestPool())
	if err != nil {
		t.Fatalf("open %s: %v", addr, err)
	}
	defer conn.Close()
	if err := conn.Exec(context.Background(), query); err != nil {
		t.Fatalf("exec on %s: %v", addr, err)
	}
}

func initialize(t *testing.T) {
	t.Helper()
	c := newTestClusterClient(t)
	defer c.Close()
	if err := c.Initialize(context.Background(), dbsql.ClickHouseSQL, dbsql.ClickHouseMigrations, dbsql.ClickHouseMigrationsDir); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	c.WaitForSchemaReady(context.Background())
}

func requireHealthy(t *testing.T, stage string) {
	t.Helper()
	counts := shardCounts(t)
	for i, c := range counts {
		t.Logf("%s: shard %d -> %d tables, migration %d", stage, i, c.Tables, c.Migration)
	}
	for i, c := range counts {
		if c.Tables < 20 {
			t.Errorf("%s: shard %d has only %d tables", stage, i, c.Tables)
		}
		if c.Migration < 13 {
			t.Errorf("%s: shard %d stuck at migration %d", stage, i, c.Migration)
		}
	}
	if counts[0].Tables != counts[1].Tables {
		t.Errorf("%s: shards disagree on table count: %d vs %d", stage, counts[0].Tables, counts[1].Tables)
	}
}

// Fresh install must fully provision every shard, not just the one the driver picks.
func TestClusterInitFresh(t *testing.T) {
	dropEverything(t)
	initialize(t)
	requireHealthy(t, "fresh")

	// process_edges_distributed was missing from the fresh path entirely.
	for _, addr := range chTestHosts {
		conn, _ := openClickHouseConn([]string{addr}, "logs", "default", "bifract", chTestPool())
		ok, err := chTableExists(context.Background(), conn, "process_edges_distributed")
		conn.Close()
		if err != nil || !ok {
			t.Errorf("%s: process_edges_distributed missing (err=%v)", addr, err)
		}
	}
}

// Re-running must be a no-op, not a re-provision or a migration replay.
func TestClusterInitIdempotent(t *testing.T) {
	dropEverything(t)
	initialize(t)
	before := shardCounts(t)
	initialize(t)
	initialize(t)
	after := shardCounts(t)
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Errorf("not idempotent: %v -> %v", before, after)
	}
	requireHealthy(t, "idempotent")
}

// The reported incident: one shard has the database, the other has nothing.
func TestClusterInitRepairsMissingShard(t *testing.T) {
	dropEverything(t)
	initialize(t)
	execOnShard(t, chTestHosts[1], "DROP DATABASE IF EXISTS logs SYNC")

	counts := shardCounts(t)
	t.Logf("damaged: shard 1 -> %d tables", counts[1].Tables)

	initialize(t)
	requireHealthy(t, "repaired")
}

// The one-node baseline stamp: schema present everywhere, migrations recorded on one
// shard only. Replaying history here fails at 004 (raw_log), so the shard must be
// stamped instead.
func TestClusterInitStampsUnrecordedShard(t *testing.T) {
	dropEverything(t)
	initialize(t)
	execOnShard(t, chTestHosts[1], "TRUNCATE TABLE IF EXISTS logs._bifract_migrations")
	execOnShard(t, chTestHosts[1], "TRUNCATE TABLE IF EXISTS logs._bifract_migration_steps")

	counts := shardCounts(t)
	if counts[1].Migration != 0 {
		t.Fatalf("setup failed: shard 1 still at migration %d", counts[1].Migration)
	}

	initialize(t)
	requireHealthy(t, "stamped")
}
