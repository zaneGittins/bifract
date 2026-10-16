package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// clusterHosts scripts system.clusters with the driver's real column types:
// shard_num UInt32 and port UInt16. Scanning those into wider Go types fails on
// a real server.
func clusterHosts(rows ...[]any) fakedb.CHResult {
	return fakedb.CHResult{Cols: []fakedb.Col{{Column: "shard_num", Type: "UInt32"}, {Column: "host_name", Type: "String"}, {Column: "port", Type: "UInt16"}}, Rows: rows}
}

func newShardedClient() (*ClickHouseClient, *fakedb.CH) {
	c, f := newFakeClient()
	c.topo = Topology{Kind: DeploymentSelfManagedCluster, DistributedTables: true, ShardRouting: true, DDLCluster: "c1", FanoutCluster: "c1"}
	f.On("FROM system.clusters WHERE cluster = 'c1' AND replica_num = 1", clusterHosts(
		[]any{uint32(2), "ch-1", uint16(9000)},
		[]any{uint32(1), "ch-0", uint16(9000)},
	))
	return c, f
}

func TestShardNumsSortedAndCached(t *testing.T) {
	c, f := newShardedClient()
	nums, err := c.ShardNums(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(nums, []uint64{1, 2}) {
		t.Errorf("ShardNums = %v", nums)
	}
	host, err := c.shardHostForNum(context.Background(), 2)
	if err != nil || host != "ch-1:9000" {
		t.Errorf("shard 2 = %q, %v", host, err)
	}
	if _, err := c.shardHostForNum(context.Background(), 9); err == nil {
		t.Error("an unknown shard must be an error")
	}
	if n := len(f.Queried()); n != 1 {
		t.Errorf("system.clusters read %d times, want once", n)
	}

	single, _ := newFakeClient()
	if nums, err := single.ShardNums(context.Background()); nums != nil || err != nil {
		t.Errorf("single node ShardNums = %v, %v", nums, err)
	}
}

// One bad shard is reported as unreachable rather than failing the call, so the
// healthy shards' numbers stay visible exactly when a shard is degraded.
func TestDistributionQueueByShard(t *testing.T) {
	c, _ := newShardedClient()
	healthy := fakedb.NewCH()
	healthy.On("FROM system.distribution_queue", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "data_files", Type: "Int64"}, {Column: "broken_data_files", Type: "Int64"}, {Column: "error_count", Type: "Int64"}, {Column: "last_exception", Type: "String"}},
		Rows: [][]any{{int64(5), int64(1), int64(2), "boom"}},
	})
	broken := fakedb.NewCH()
	broken.On("FROM system.distribution_queue", fakedb.CHResult{Err: errors.New("connection refused")})
	c.shardConns = map[uint64]driver.Conn{1: healthy, 2: broken}

	stats, err := c.DistributionQueueByShard(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []ShardDistQueueStat{
		{ShardNum: 1, Host: "ch-0:9000", DataFiles: 5, BrokenDataFiles: 1, ErrorCount: 2, LastException: "boom"},
		{ShardNum: 2, Host: "ch-1:9000", Unreachable: true, Error: "connection refused"},
	}
	if !reflect.DeepEqual(stats, want) {
		t.Errorf("stats = %+v\nwant %+v", stats, want)
	}
	if q := healthy.Queried()[0]; !strings.Contains(q, "WHERE table = 'logs_distributed'") {
		t.Errorf("query = %q", q)
	}

	single, _ := newFakeClient()
	if stats, err := single.DistributionQueueByShard(context.Background()); stats != nil || err != nil {
		t.Errorf("single node = %v, %v", stats, err)
	}
}

// The reset is local DDL on one shard: DROP then CREATE, never ON CLUSTER. A
// transient CREATE failure is retried, since giving up leaves the shard with no
// logs_distributed at all.
func TestResetDistributedQueue(t *testing.T) {
	c, _ := newShardedClient()
	shard := fakedb.NewCH()
	failures := 1
	shard.FailExecFunc(func(stmt string) error {
		if strings.HasPrefix(stmt, "CREATE") && failures > 0 {
			failures--
			return errors.New("blip")
		}
		return nil
	})
	c.shardConns = map[uint64]driver.Conn{2: shard}

	if err := c.ResetDistributedQueue(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	execs := shard.Executed()
	want := "CREATE TABLE IF NOT EXISTS logs_distributed AS logs ENGINE = Distributed('c1', currentDatabase(), 'logs', rand())"
	if len(execs) != 3 || execs[0] != "DROP TABLE IF EXISTS logs_distributed" || execs[1] != want || execs[2] != want {
		t.Errorf("executed %q", execs)
	}
	for _, s := range execs {
		if strings.Contains(s, "ON CLUSTER") || strings.Contains(s, "SYNC") {
			t.Errorf("%q: must be local and must not wait on in-flight queries", s)
		}
	}

	single, _ := newFakeClient()
	if err := single.ResetDistributedQueue(context.Background(), 1); err == nil {
		t.Error("single node has no distribution queue to reset")
	}
	if err := c.ResetDistributedQueue(context.Background(), 9); err == nil {
		t.Error("an unknown shard must be an error")
	}
}

func TestResetDistributedQueueStopsOnDropFailure(t *testing.T) {
	c, _ := newShardedClient()
	shard := fakedb.NewCH().FailExec("DROP", errors.New("denied"))
	c.shardConns = map[uint64]driver.Conn{1: shard}
	if err := c.ResetDistributedQueue(context.Background(), 1); err == nil {
		t.Fatal("expected the drop failure")
	}
	if len(shard.ExecutedMatching("CREATE")) != 0 {
		t.Error("must not recreate after a failed drop")
	}
}

// The direct path reads the shard's local logs table; on a direct failure it falls
// back to the distributed read rather than failing the detail view.
func TestGetLogFieldsByIDDirect(t *testing.T) {
	cols := []fakedb.Col{{Column: "log_id", Type: "String"}, {Column: "fractal_id", Type: "String"}, {Column: "fields", Type: "String"}, {Column: "normalizer", Type: "String"}, {Column: "ingest_timestamp", Type: "DateTime64(3, 'UTC')"}}
	row := []any{"L1", "F1", `{"k":"v"}`, "", time.Unix(0, 0)}

	c, pooled := newShardedClient()
	shard := fakedb.NewCH()
	shard.On("FROM logs PREWHERE", fakedb.CHResult{Cols: cols, Rows: [][]any{row}})
	c.shardConns = map[uint64]driver.Conn{2: shard}
	got, err := c.GetLogFieldsByIDDirect(context.Background(), "L1", time.Unix(1, 0), "F1", 2)
	if err != nil || got["log_id"] != "L1" {
		t.Fatalf("direct = %v, %v", got, err)
	}
	if q := shard.Queried()[0]; !strings.Contains(q, "FROM logs PREWHERE") || !strings.Contains(q, "fractal_id = ?") {
		t.Errorf("direct query = %q", q)
	}
	// raw_log is never on the shard: it comes from the fan-out table.
	if !strings.Contains(strings.Join(pooled.Queried(), "\n"), "FROM logs_raw_distributed") {
		t.Error("raw_log must be read through logs_raw_distributed")
	}

	c, pooled = newShardedClient()
	pooled.On("FROM logs_distributed PREWHERE", fakedb.CHResult{Cols: cols, Rows: [][]any{row}})
	c.shardConns = map[uint64]driver.Conn{2: fakedb.NewCH().On("PREWHERE", fakedb.CHResult{Err: errors.New("down")})}
	got, err = c.GetLogFieldsByIDDirect(context.Background(), "L1", time.Unix(1, 0), "", 2)
	if err != nil || got["log_id"] != "L1" {
		t.Fatalf("fallback = %v, %v", got, err)
	}
}

func TestShardHealthAndClusterServerStats(t *testing.T) {
	single, _ := newFakeClient()
	if total, healthy, err := single.ShardHealth(context.Background()); total != 0 || healthy != 0 || err != nil {
		t.Errorf("single node ShardHealth = %d, %d, %v", total, healthy, err)
	}
	if stats, err := single.ClusterServerStats(context.Background()); stats != nil || err != nil {
		t.Errorf("single node stats = %v, %v", stats, err)
	}

	c, f := newShardedClient()
	f.On("countIf(estimated_recovery_time = 0)", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "total", Type: "UInt64"}, {Column: "healthy", Type: "UInt64"}},
		Rows: [][]any{{uint64(3), uint64(2)}},
	})
	f.On("sumIf(value, metric = 'Query')", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "active_queries", Type: "Float64"}, {Column: "active_merges", Type: "Float64"}},
		Rows: [][]any{{float64(7), float64(4)}},
	})
	f.On("hostName() AS node", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "node", Type: "String"}, {Column: "metric", Type: "String"}, {Column: "value", Type: "Float64"}},
		Rows: [][]any{
			// cgroup-limited node at 50%
			{"a", "CGroupMemoryUsed", 50.0}, {"a", "CGroupMemoryTotal", 100.0},
			// bare-metal node at 80% by resident set: the worst node wins
			{"b", "MemoryResident", 80.0}, {"b", "OSMemoryTotal", 100.0},
			// a node reporting nothing usable is ignored
			{"c", "OSMemoryTotal", 0.0},
		},
	})
	stats, err := c.ClusterServerStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := ClusterServerStats{NodesTotal: 3, NodesHealthy: 2, ActiveQueries: 7, ActiveMerges: 4, MemPeakPct: 80, MemPeakNode: "b", MemPeakBytes: 80}
	if *stats != want {
		t.Errorf("stats = %+v, want %+v", *stats, want)
	}
	for _, q := range f.Queried() {
		if strings.Contains(q, "clusterAllReplicas") && !strings.Contains(q, "skip_unavailable_shards = 1") {
			t.Errorf("a fan-out must tolerate an unavailable shard: %q", q)
		}
	}

	c, f = newShardedClient()
	f.On("countIf(estimated_recovery_time = 0)", fakedb.CHResult{Err: errors.New("down")})
	if _, err := c.ClusterServerStats(context.Background()); err == nil {
		t.Error("expected the health error")
	}
}
