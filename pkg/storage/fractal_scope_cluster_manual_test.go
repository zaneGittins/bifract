//go:build chcluster

package storage

import (
	"context"
	"fmt"
	"testing"

	dbsql "bifract/db"
)

// The alert identity on a sharded cluster: a read through a Distributed table is
// confined on every shard, and a client pinned to one node reaches that node as the
// identity, since rule tests read a scratch table only that node holds.
func TestAlertIdentityAcrossCluster(t *testing.T) {
	c := newTestClusterClient(t)
	defer c.Close()
	ctx := context.Background()
	if err := c.Initialize(ctx, dbsql.ClickHouseSQL, dbsql.ClickHouseMigrations, dbsql.ClickHouseMigrationsDir); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	c.reconcileQueryIdentities(ctx, nil)

	for i, addr := range chTestHosts {
		execOnShard(t, addr, fmt.Sprintf(
			"INSERT INTO logs.logs (fractal_id, log_id) VALUES ('fa', 'a%d'), ('fb', 'b%d')", i, i))
	}

	count := func(ctx context.Context) uint64 {
		t.Helper()
		var n uint64
		if err := c.QueryRow(ctx, "SELECT count() FROM logs.logs_distributed").Scan(&n); err != nil {
			t.Fatalf("distributed read: %v", err)
		}
		return n
	}
	scoped := UncappedReadContext(ScopeContext(ctx, FractalScope{FractalIDs: []string{"fa"}}))
	if n := count(scoped); n != uint64(len(chTestHosts)) {
		t.Errorf("scoped read saw %d rows, want one per shard", n)
	}
	if n := count(UncappedReadContext(ctx)); n != 2*uint64(len(chTestHosts)) {
		t.Errorf("unscoped read saw %d rows, want every row", n)
	}

	for _, addr := range chTestHosts {
		pinned, err := c.PinnedNodeClient(ctx, addr)
		if err != nil {
			t.Fatalf("pin %s: %v", addr, err)
		}
		defer pinned.Close()
		if err := pinned.Exec(ctx, "CREATE TABLE IF NOT EXISTS logs.bf_scratch (msg String) ENGINE = MergeTree ORDER BY tuple()"); err != nil {
			t.Fatalf("%s: %v", addr, err)
		}
		defer execOnShard(t, addr, "DROP TABLE IF EXISTS logs.bf_scratch SYNC")
		if err := pinned.Exec(ctx, "INSERT INTO logs.bf_scratch VALUES ('"+addr+"')"); err != nil {
			t.Fatalf("%s: %v", addr, err)
		}
		rows, err := pinned.Query(scoped, "SELECT msg, currentUser() AS who FROM logs.bf_scratch")
		if err != nil {
			t.Fatalf("%s: %v", addr, err)
		}
		if len(rows) != 1 || rows[0]["msg"] != addr || rows[0]["who"] != AlertCHUser {
			t.Errorf("%s: pinned read = %v", addr, rows)
		}
	}
}
