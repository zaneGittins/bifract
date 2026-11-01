package storage

import (
	"context"
	"strings"
	"testing"

	"bifract/internal/fakedb"
)

func TestFractalScopeEncode(t *testing.T) {
	cases := []struct {
		name  string
		scope FractalScope
		want  string
	}{
		// Empty must not render as a lone empty element, or it would admit unassigned rows.
		{"empty reads nothing", FractalScope{}, ""},
		{"one fractal", FractalScope{FractalIDs: []string{"a"}}, "a,"},
		{"prism", FractalScope{FractalIDs: []string{"a", "b"}}, "a,b,"},
		{"default fractal", FractalScope{FractalIDs: []string{"a"}, Unassigned: true}, "a,,"},
	}
	for _, tc := range cases {
		got, err := tc.scope.encode()
		if err != nil || got != tc.want {
			t.Errorf("%s: encode = %q, %v; want %q", tc.name, got, err, tc.want)
		}
	}
	// A separator inside an id would let it name more fractals than it is.
	for _, bad := range []string{"", "a,b", "a~bfscope~b"} {
		if _, err := (FractalScope{FractalIDs: []string{bad}}).encode(); err == nil {
			t.Errorf("id %q accepted", bad)
		}
	}
}

func TestScopedQueryID(t *testing.T) {
	bg := context.Background()
	if id, err := scopedQueryID(withQueryID(bg, "q1")); id != "q1" || err != nil {
		t.Errorf("unscoped read changed its id: %q, %v", id, err)
	}
	if id, err := scopedQueryID(bg); id != "" || err != nil {
		t.Errorf("unscoped read without id = %q, %v; want the server to assign one", id, err)
	}

	scoped := ScopeContext(withQueryID(bg, "bif-search-1-main"), FractalScope{FractalIDs: []string{"f1", "f2"}, Unassigned: true})
	if id, err := scopedQueryID(scoped); id != "bif-search-1-main~bfscope~f1,f2,," || err != nil {
		t.Errorf("scoped id = %q, %v", id, err)
	}
	// Kill-by-prefix and profiling key off the caller's id, so it must stay the prefix.
	id, _ := scopedQueryID(ScopeContext(bg, FractalScope{FractalIDs: []string{"f1"}}))
	if !strings.HasSuffix(id, "~bfscope~f1,") || len(id) <= len("~bfscope~f1,") {
		t.Errorf("scoped id without caller id = %q, want a generated id carrying the scope", id)
	}

	if _, err := scopedQueryID(WorkloadContext(bg, QuerySearchWorkload)); err == nil {
		t.Error("a search without a scope must be refused")
	}
	if _, err := scopedQueryID(WorkloadContext(bg, QueryRecallWorkload)); err != nil {
		t.Errorf("recall carries no scope: %v", err)
	}
}

// An unscoped search must fail before it reaches ClickHouse, on either connection.
func TestUnscopedSearchNeverRuns(t *testing.T) {
	c, def := newFakeClient()
	search := fakedb.NewCH()
	c.queryConns.Store(&map[string]*queryIdentity{QuerySearchWorkload: {conn: search}})
	ctx := WorkloadContext(context.Background(), QuerySearchWorkload)

	if _, err := c.QueryUserSearchWithID(ctx, "q", "SELECT 1"); err == nil {
		t.Error("query ran without a scope")
	}
	if err := c.StreamQuery(ctx, "q", "SELECT 1", func(map[string]interface{}) error { return nil }, nil); err == nil {
		t.Error("stream ran without a scope")
	}
	if _, err := c.QueryStream(ctx, "q", "SELECT 1", nil, nil); err == nil {
		t.Error("query stream ran without a scope")
	}
	if len(def.Queried())+len(search.Queried()) != 0 {
		t.Error("an unscoped search reached ClickHouse")
	}
}

// The query cache ignores query_id, so scoped reads must key it by scope.
func TestScopedReadsTagTheQueryCache(t *testing.T) {
	c, _ := newFakeClient()
	scoped := ScopeContext(context.Background(), FractalScope{FractalIDs: []string{"f1"}})
	if got := c.applyQuerySettings(scoped, nil)["query_cache_tag"]; got != "~bfscope~f1," {
		t.Errorf("applyQuerySettings tag = %v", got)
	}
	if got := c.taggedSettings(scoped, nil)["query_cache_tag"]; got != "~bfscope~f1," {
		t.Errorf("taggedSettings tag = %v", got)
	}
	if got := c.applyQuerySettings(context.Background(), nil); got != nil {
		t.Errorf("unscoped read got settings %v", got)
	}
}

func TestQueryIDMatchSQL(t *testing.T) {
	got := QueryIDMatchSQL("query_id", "a'b")
	want := `(query_id = 'a\'b' OR startsWith(query_id, 'a\'b~bfscope~'))`
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func policyRows(rows ...[]any) fakedb.CHResult {
	return fakedb.CHResult{Cols: []fakedb.Col{{Column: "short_name", Type: "String"}, {Column: "n", Type: "UInt64"}}, Rows: rows}
}

func TestEnsureFractalScopePolicies(t *testing.T) {
	all := uint64(len(fractalScopeTables))

	c, f := newFakeClient()
	c.topo.DDLCluster = "c1"
	c.ensureFractalScopePolicies(context.Background(), true)
	got := f.Executed()
	if len(got) != 2 {
		t.Fatalf("executed %q, want both policies", got)
	}
	tables := "`logs`.`logs`, `logs`.`logs_hot`, `logs`.`logs_raw`"
	if want := "CREATE ROW POLICY IF NOT EXISTS bifract_fractal_scope_v1 ON CLUSTER 'c1' ON " + tables +
		" AS RESTRICTIVE FOR SELECT USING NOT position(initialQueryID(), '~bfscope~') > 0 OR has(arrayPopBack(splitByChar(',', splitByString('~bfscope~', initialQueryID())[2])), fractal_id) TO ALL"; got[0] != want {
		t.Errorf("scope policy:\n%s\nwant:\n%s", got[0], want)
	}
	if want := "CREATE ROW POLICY IF NOT EXISTS bifract_scope_required_v1 ON CLUSTER 'c1' ON " + tables +
		" AS RESTRICTIVE FOR SELECT USING position(initialQueryID(), '~bfscope~') > 0 TO bifract_search"; got[1] != want {
		t.Errorf("required policy:\n%s\nwant:\n%s", got[1], want)
	}

	// Present on every table: no DDL. Missing on one: recreated (IF NOT EXISTS fills the gap).
	c, f = newFakeClient()
	f.On("system.row_policies", policyRows([]any{fractalScopePolicy, all}, []any{scopeRequiredPolicy, all - 1}))
	c.ensureFractalScopePolicies(context.Background(), true)
	if got := f.Executed(); len(got) != 1 || !strings.Contains(got[0], scopeRequiredPolicy) {
		t.Errorf("executed %q, want only the incomplete policy", got)
	}

	// Without a search identity the required policy cannot name its user.
	c, f = newFakeClient()
	c.ensureFractalScopePolicies(context.Background(), false)
	if got := f.Executed(); len(got) != 1 || !strings.Contains(got[0], fractalScopePolicy) {
		t.Errorf("executed %q, want only the scope policy", got)
	}

	// A server that refuses row policies is logged, not fatal.
	c, f = newFakeClient()
	f.FailExec("ROW POLICY", fakedb.Exception(497, "denied"))
	c.ensureFractalScopePolicies(context.Background(), true)
}
