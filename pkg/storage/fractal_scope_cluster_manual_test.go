//go:build chcluster

package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"

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
	// An unmarked read runs as the reader on every shard it fans out to.
	if n := count(ScopeContext(ctx, FractalScope{FractalIDs: []string{"fb"}})); n != uint64(len(chTestHosts)) {
		t.Errorf("scoped unmarked read saw %d rows, want one per shard", n)
	}
	var who string
	if err := c.QueryRow(ctx, "SELECT currentUser()").Scan(&who); err != nil || who != ReaderCHUser {
		t.Errorf("unmarked read ran as %q: %v", who, err)
	}

	// Keeper holds the cluster's DDL verbatim, identity passwords included: only an
	// admin read may see it, never the reader or the ingest user.
	const keeper = "SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/task_queue/ddl'"
	var entries uint64
	if err := c.QueryRow(AdminReadContext(ctx), keeper).Scan(&entries); err != nil || entries == 0 {
		t.Errorf("admin Keeper read: %d %v", entries, err)
	}
	var denied *proto.Exception
	if err := c.QueryRow(ctx, keeper).Scan(&entries); !errors.As(err, &denied) || denied.Code != 497 {
		t.Errorf("reader read Keeper: %v", err)
	}
	if err := c.EnsureIngestUser(ctx, "IngestPw1!x"); err != nil {
		t.Fatalf("ingest user: %v", err)
	}
	ingest, err := openClickHouseConn(ConnOptions{Addrs: []string{chTestHosts[0]}, Database: "logs", User: IngestCHUser, Password: "IngestPw1!x", Pool: chTestPool()})
	if err != nil {
		t.Fatal(err)
	}
	defer ingest.Close()
	if err := ingest.QueryRow(ctx, keeper).Scan(&entries); !errors.As(err, &denied) || denied.Code != 497 {
		t.Errorf("ingest user read Keeper: %v", err)
	}
	if err := ingest.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = 'logs'").Scan(&entries); err != nil {
		t.Errorf("ingest user lost its system read: %v", err)
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
		if rows, err := pinned.Query(ctx, "SELECT currentUser() AS who"); err != nil || rows[0]["who"] != ReaderCHUser {
			t.Errorf("%s: pinned unmarked read = %v %v", addr, rows, err)
		}
	}
}
