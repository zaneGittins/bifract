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
	"errors"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
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
		for _, p := range []string{fractalScopePolicy, retiredScopePolicy, scopeRequiredPolicy} {
			root.Exec(ctx, "DROP ROW POLICY IF EXISTS "+p+" ON "+db+".logs, "+db+".logs_hot, "+db+".logs_raw")
		}
		root.Close()
	})

	// An install from before v2 carries the retired policy; reconcile replaces it.
	if err := root.Exec(ctx, "CREATE ROW POLICY "+retiredScopePolicy+" ON "+db+".logs, "+db+".logs_hot, "+db+".logs_raw AS RESTRICTIVE FOR SELECT USING 1 TO ALL"); err != nil {
		t.Fatalf("seed retired policy: %v", err)
	}

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

	// The privileged connection: unscoped work is untouched, scoped work is bound.
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

	// The alert identity: a rule's scope binds it, an unscoped rule still reads, and a
	// translator bug has none of the admin connection's reach.
	if err := root.Exec(ctx, "CREATE TABLE IF NOT EXISTS default.bfscope_other (msg String) ENGINE = Memory"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		root.Exec(ctx, "DROP TABLE IF EXISTS default.bfscope_other")
		root.Exec(ctx, "DROP USER IF EXISTS evil")
	})
	alert := UncappedReadContext(ScopeContext(ctx, FractalScope{FractalIDs: []string{"a"}}))
	expect("scoped alert", alert, "SELECT msg FROM logs", "a1")
	expect("alias shadowing on alert", alert, "SELECT msg FROM (SELECT 'a' AS fractal_id, msg FROM logs) WHERE fractal_id = 'a'", "a1")
	expect("unscoped alert", UncappedReadContext(ctx), "SELECT msg FROM logs", "a1", "b1", "u1")
	expect("alert user", alert, "SELECT currentUser() AS msg", AlertCHUser)
	// The code matters: file(), url() and s3() also fail with a grant held, on the
	// target rather than on the privilege.
	const accessDenied, settingConstraint = 497, 452
	for name, tc := range map[string]struct {
		sql  string
		code int32
	}{
		"other database": {"SELECT msg FROM default.bfscope_other", accessDenied},
		"write":          {"INSERT INTO logs (fractal_id, msg) SELECT 'a', 'x'", accessDenied},
		"ddl":            {"CREATE TABLE evil (msg String) ENGINE = Memory", accessDenied},
		"drop":           {"DROP TABLE logs", accessDenied},
		"file()":         {"SELECT * FROM file('bfscope.txt', 'LineAsString')", accessDenied},
		"url()":          {"SELECT * FROM url('http://127.0.0.1:1/', 'LineAsString')", accessDenied},
		"s3()":           {"SELECT * FROM s3('http://127.0.0.1:1/bucket/key', 'LineAsString')", accessDenied},
		"remote()":       {"SELECT msg FROM remote('" + addr + "', " + db + ".logs)", accessDenied},
		"query_log":      {"SELECT query AS msg FROM system.query_log LIMIT 1", accessDenied},
		"processes":      {"SELECT query AS msg FROM system.processes LIMIT 1", accessDenied},
		"text_log":       {"SELECT message AS msg FROM system.text_log LIMIT 1", accessDenied},
		"query_cache":    {"SELECT query AS msg FROM system.query_cache LIMIT 1", accessDenied},
		"users":          {"CREATE USER evil", accessDenied},
		"plan shipping":  {"SELECT msg FROM logs SETTINGS serialize_query_plan = 1", settingConstraint},
	} {
		_, err := msgs(alert, tc.sql)
		var ex *proto.Exception
		if !errors.As(err, &ex) || ex.Code != tc.code {
			t.Errorf("%s: err = %v, want code %d", name, err, tc.code)
		}
	}
	if got, err := c.Query(ctx, "SELECT count() AS n FROM system.settings_profile_elements WHERE user_name = '"+AlertCHUser+"' AND setting_name = 'max_memory_usage_for_user'"); err != nil || got[0]["n"].(uint64) != 0 {
		t.Errorf("alert identity carries a memory ceiling: %v %v", got, err)
	}

	// An identity lost at boot comes back through the repair, without a restart.
	c.closeQueryIdentities()
	c.queryConns.Store(&map[string]*queryIdentity{})
	expect("lost identity falls back", UncappedReadContext(ctx), "SELECT currentUser() AS msg", "default")
	c.repairQueryIdentities(ctx)
	expect("repaired identity", UncappedReadContext(ctx), "SELECT currentUser() AS msg", AlertCHUser)

	// Idempotent: a second reconcile finds the policies and issues no DDL for them.
	c.ensureFractalScopePolicies(ctx, true)
	all := uint64(len(fractalScopeTables))
	if n := c.scopePolicyCounts(ctx); n[fractalScopePolicy] != all || n[scopeRequiredPolicy] != all || n[retiredScopePolicy] != 0 {
		t.Errorf("policy table counts = %v", n)
	}
}
