//go:build chscope

// Fractal scope backstop against a real server. Uses a throwaway database and the
// real class identities, so point it at a disposable ClickHouse:
//
//	docker run -d --name bfscope -e CLICKHOUSE_PASSWORD=bifract -p 19020:9000 \
//	  clickhouse/clickhouse-server:26.8.34.13-alpine
//	go test -tags chscope ./pkg/storage/ -run TestFractalScopeLive -v
//
// BFSCOPE_ADDR and BFSCOPE_PASSWORD override the address and password.
package storage

import (
	"context"
	"os"
	"sort"
	"strings"
	"testing"
)

func scopeEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func TestFractalScopeLive(t *testing.T) {
	ctx := context.Background()
	addr := scopeEnv("BFSCOPE_ADDR", "localhost:19020")
	pw := scopeEnv("BFSCOPE_PASSWORD", "bifract")
	const db = "bfscope_test"

	root, err := openClickHouseConn(ConnOptions{Addrs: []string{addr}, Database: "default", User: "default", Password: pw, Pool: ClickHousePoolConfig{MaxOpenConns: 2, MaxIdleConns: 1}})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	for _, stmt := range []string{
		"DROP DATABASE IF EXISTS " + db + " SYNC",
		"CREATE DATABASE " + db,
	} {
		if err := root.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	for _, table := range fractalScopeTables {
		for _, stmt := range []string{
			"CREATE TABLE " + db + "." + table + " (fractal_id LowCardinality(String), msg String) ENGINE = MergeTree ORDER BY fractal_id",
			"INSERT INTO " + db + "." + table + " VALUES ('a', 'a1'), ('b', 'b1'), ('', 'u1')",
		} {
			if err := root.Exec(ctx, stmt); err != nil {
				t.Fatalf("%s: %v", stmt, err)
			}
		}
	}
	t.Cleanup(func() {
		root.Exec(ctx, "DROP DATABASE IF EXISTS "+db+" SYNC")
		for _, p := range []string{fractalScopePolicy, scopeRequiredPolicy} {
			root.Exec(ctx, "DROP ROW POLICY IF EXISTS "+p+" ON "+db+".logs, "+db+".logs_hot, "+db+".logs_raw")
		}
		root.Close()
	})

	c := NewClickHouseClientFromConn(root, db, Topology{Kind: DeploymentSingleNode})
	c.addrs = []string{addr}
	c.Password = pw
	c.reconcileQueryIdentities(ctx, map[string]int64{QuerySearchWorkload: 1 << 30})
	defer c.closeQueryIdentities()
	if m := c.queryConns.Load(); m == nil || (*m)[QuerySearchWorkload] == nil {
		t.Fatal("search identity was not provisioned")
	}

	msgs := func(ctx context.Context, sql string) ([]string, error) {
		rows, err := c.QueryUserSearchWithID(ctx, "bfscope-live", sql)
		if err != nil {
			return nil, err
		}
		var out []string
		for _, r := range rows {
			out = append(out, r["msg"].(string))
		}
		sort.Strings(out)
		return out, nil
	}
	expect := func(name string, ctx context.Context, sql string, want ...string) {
		t.Helper()
		got, err := msgs(ctx, sql)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			return
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s: got %v, want %v", name, got, want)
		}
	}

	a := UserSearchContext(ctx, FractalScope{FractalIDs: []string{"a"}})
	for _, table := range fractalScopeTables {
		expect("no predicate on "+table, a, "SELECT msg FROM "+table, "a1")
	}
	// The translator bug class: a predicate that tests an alias instead of the column.
	expect("alias shadowing", a, "SELECT msg FROM (SELECT 'a' AS fractal_id, msg FROM logs) WHERE fractal_id = 'a'", "a1")
	expect("default fractal", UserSearchContext(ctx, FractalScope{FractalIDs: []string{"a"}, Unassigned: true}), "SELECT msg FROM logs", "a1", "u1")
	expect("prism", UserSearchContext(ctx, FractalScope{FractalIDs: []string{"a", "b"}}), "SELECT msg FROM logs", "a1", "b1")
	expect("empty scope", UserSearchContext(ctx, FractalScope{}), "SELECT msg FROM logs")

	// The query cache ignores query_id: identical SQL under another scope must miss.
	cached := "SELECT msg FROM logs ORDER BY msg SETTINGS use_query_cache = 1, query_cache_min_query_duration = 0"
	expect("cache fill", a, cached, "a1")
	expect("cache under another scope", UserSearchContext(ctx, FractalScope{FractalIDs: []string{"a", "b"}}), cached, "a1", "b1")

	// The privileged connection: unscoped work is untouched, scoped work (alerts) is bound.
	expect("unscoped default connection", ctx, "SELECT msg FROM "+db+".logs", "a1", "b1", "u1")
	expect("scoped default connection", ScopeContext(ctx, FractalScope{FractalIDs: []string{"b"}}), "SELECT msg FROM "+db+".logs", "b1")

	// Nothing in the SQL can widen it or reach other queries' text.
	for name, sql := range map[string]string{
		"serialize_query_plan": "SELECT msg FROM logs SETTINGS serialize_query_plan = 1",
		"memory ceiling":       "SELECT msg FROM logs SETTINGS max_memory_usage_for_user = 0",
		"query_log":            "SELECT query AS msg FROM system.query_log LIMIT 1",
		"processes":            "SELECT query AS msg FROM system.processes LIMIT 1",
		"remote()":             "SELECT msg FROM remote('" + addr + "', " + db + ".logs)",
	} {
		if got, err := msgs(a, sql); err == nil {
			t.Errorf("%s: allowed on the search identity, got %v", name, got)
		}
	}

	// Idempotent: a second reconcile finds the policies and issues no DDL for them.
	c.ensureFractalScopePolicies(ctx, true)
	if present := c.presentScopePolicies(ctx); !present[fractalScopePolicy] || !present[scopeRequiredPolicy] {
		t.Errorf("policies present = %v", present)
	}
}
