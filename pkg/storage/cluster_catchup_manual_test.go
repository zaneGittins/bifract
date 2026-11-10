//go:build chcluster

package storage

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	dbsql "bifract/db"
)

// A shard that drops out while the server boots, as during a k8s rolling upgrade, must
// still catch up on migrations once it returns, without another restart.
func TestClusterInitCatchesUpShardDownAtBoot(t *testing.T) {
	dropEverything(t)
	initialize(t)
	all, err := loadClickHouseMigrations(dbsql.ClickHouseMigrations, dbsql.ClickHouseMigrationsDir)
	if err != nil {
		t.Fatal(err)
	}
	latest := uint32(all[len(all)-1].number)
	// Rewind shard 2 by one migration: its records and the table it creates.
	behind := chTestHosts[1]
	for _, stmt := range []string{
		"DROP TABLE IF EXISTS logs.process_edges_p2p_mv SYNC",
		"DROP TABLE IF EXISTS logs.provenance_marks_distributed SYNC",
		"DROP TABLE IF EXISTS logs.provenance_marks SYNC",
	} {
		execOnShard(t, behind, stmt)
	}
	// TRUNCATE and refill, since a lightweight DELETE also hides the row written back later.
	for _, table := range []string{"_bifract_migrations", "_bifract_migration_steps"} {
		for _, stmt := range []string{
			"CREATE TABLE logs.keep_" + table + " ENGINE = Memory AS SELECT * FROM logs." + table + " WHERE number < " + itoa(latest),
			"TRUNCATE TABLE logs." + table,
			"INSERT INTO logs." + table + " SELECT * FROM logs.keep_" + table,
			"DROP TABLE logs.keep_" + table,
		} {
			execOnShard(t, behind, stmt)
		}
	}

	proxy, open := blockableProxy(t, behind)
	old := shardCatchUpDelay
	shardCatchUpDelay = 200 * time.Millisecond
	t.Cleanup(func() { shardCatchUpDelay = old })

	open.Store(true)
	c := newTestClusterClientAt(t, []string{chTestHosts[0], proxy})
	open.Store(false)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Initialize(ctx, dbsql.ClickHouseSQL, dbsql.ClickHouseMigrations, dbsql.ClickHouseMigrationsDir); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	time.Sleep(time.Second)
	if n := shardUint(t, behind, "SELECT toUInt64(max(number)) FROM logs._bifract_migrations"); n == uint64(latest) {
		t.Fatal("setup: the shard was migrated while unreachable")
	}

	open.Store(true)
	deadline := time.Now().Add(60 * time.Second)
	for shardUint(t, behind, "SELECT toUInt64(max(number)) FROM logs._bifract_migrations") != uint64(latest) {
		if time.Now().After(deadline) {
			t.Fatalf("shard %s never caught up to migration %d", behind, latest)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if n := shardUint(t, behind, "SELECT count() FROM system.tables WHERE database = 'logs' AND name IN ('provenance_marks', 'provenance_marks_distributed')"); n != 2 {
		t.Errorf("caught-up shard has %d of the 2 provenance_marks tables", n)
	}
}

func itoa(n uint32) string { return strconv.FormatUint(uint64(n), 10) }

func newTestClusterClientAt(t *testing.T, addrs []string) *ClickHouseClient {
	t.Helper()
	topo, err := DeriveTopology(TopologySpec{Kind: DeploymentSelfManagedCluster, Cluster: chTestCluster})
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewClickHouseClient(ClientOptions{
		Conn: ConnOptions{Addrs: addrs, Database: "logs", User: "default", Password: "bifract", Pool: chTestPool()},
		Topo: topo, Role: RoleControlPlane,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// blockableProxy forwards to target once open is set, and drops connections before.
func blockableProxy(t *testing.T, target string) (string, *atomic.Bool) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	open := &atomic.Bool{}
	go func() {
		for {
			in, err := l.Accept()
			if err != nil {
				return
			}
			if !open.Load() {
				in.Close()
				continue
			}
			out, err := net.Dial("tcp", target)
			if err != nil {
				in.Close()
				continue
			}
			go func() { io.Copy(out, in); out.Close() }()
			go func() { io.Copy(in, out); in.Close() }()
		}
	}()
	return l.Addr().String(), open
}
