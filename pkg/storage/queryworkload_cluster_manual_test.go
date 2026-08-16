//go:build chcluster

package storage

import (
	"context"
	"strings"
	"testing"
)

// Query resource limits must actually reach every shard of a cluster whose workload
// entities are Keeper-replicated (workload_zookeeper_path, which the bundled
// ClickHouseInstallation always sets). This previously failed on 100% of clusters:
// the DDL carried ON CLUSTER, ClickHouse answered code 80 INCORRECT_QUERY, the error
// was only logged, and system.workloads stayed empty everywhere -- so heavy searches
// ran with no CPU or memory ceiling and nothing said so.
func TestReconcileQueryWorkloadsAcrossCluster(t *testing.T) {
	c := newTestClusterClient(t)
	defer c.Close()
	ctx := context.Background()

	if err := c.ReconcileQueryWorkloads(ctx, WorkloadLimits{
		SearchCPUPercent: 50, SearchMemoryPercent: 50,
		RecallCPUPercent: 25, RecallMemoryPercent: 25,
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// Assert per shard, directly. A cluster-wide query would be satisfied by one node.
	for _, addr := range chTestHosts {
		conn, err := openClickHouseConn(ConnOptions{Addrs: []string{addr}, Database: "default", User: "default", Password: "bifract", Pool: chTestPool()})
		if err != nil {
			t.Fatalf("connect %s: %v", addr, err)
		}
		for _, w := range []string{QueryRootWorkload, QuerySearchWorkload, QueryRecallWorkload} {
			var n uint64
			if err := conn.QueryRow(ctx,
				"SELECT count() FROM system.workloads WHERE name = ?", w).Scan(&n); err != nil {
				conn.Close()
				t.Fatalf("%s: query workloads: %v", addr, err)
			}
			if n != 1 {
				t.Errorf("%s: workload %q missing (count=%d)", addr, w, n)
			}
		}
		var res uint64
		if err := conn.QueryRow(ctx,
			"SELECT count() FROM system.resources WHERE name IN (?, ?)",
			QueryCPUResource, QueryMemoryResource).Scan(&res); err != nil {
			conn.Close()
			t.Fatalf("%s: query resources: %v", addr, err)
		}
		if res != 2 {
			t.Errorf("%s: expected cpu+memory resources, got %d", addr, res)
		}

		// The search workload must carry a real ceiling, not just exist.
		var create string
		if err := conn.QueryRow(ctx,
			"SELECT create_query FROM system.workloads WHERE name = ?", QuerySearchWorkload).Scan(&create); err == nil {
			if !strings.Contains(create, "max_concurrent_threads_ratio_to_cores") || !strings.Contains(create, "max_memory") {
				t.Errorf("%s: search workload has no CPU/memory ceiling: %q", addr, create)
			}
		}
		conn.Close()
	}

	// Reconcile is re-run on every settings change, so it must be repeatable.
	if err := c.ReconcileQueryWorkloads(ctx, WorkloadLimits{
		SearchCPUPercent: 40, SearchMemoryPercent: 40,
		RecallCPUPercent: 20, RecallMemoryPercent: 20,
	}); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	// Disabling every share tears the tree down without erroring.
	if err := c.ReconcileQueryWorkloads(ctx, WorkloadLimits{}); err != nil {
		t.Fatalf("disable: %v", err)
	}
	for _, addr := range chTestHosts {
		conn, err := openClickHouseConn(ConnOptions{Addrs: []string{addr}, Database: "default", User: "default", Password: "bifract", Pool: chTestPool()})
		if err != nil {
			t.Fatalf("connect %s: %v", addr, err)
		}
		var n uint64
		conn.QueryRow(ctx, "SELECT count() FROM system.workloads WHERE name = ?", QuerySearchWorkload).Scan(&n)
		conn.Close()
		if n != 0 {
			t.Errorf("%s: search workload should be dropped when disabled, count=%d", addr, n)
		}
	}
}
