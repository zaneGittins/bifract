package query

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"bifract/pkg/parser"
	"bifract/pkg/storage"
)

func TestFractalScopeMirrorsTranslator(t *testing.T) {
	cases := []struct {
		name   string
		opts   parser.QueryOptions
		want   storage.FractalScope
		scoped bool
	}{
		{"fractal", parser.QueryOptions{FractalID: "f1"}, storage.FractalScope{FractalIDs: []string{"f1"}}, true},
		{"default fractal", parser.QueryOptions{FractalID: "f1", IncludeEmptyFractalID: true}, storage.FractalScope{FractalIDs: []string{"f1"}, Unassigned: true}, true},
		// The translator lets a prism's member list win over FractalID; so must the backstop.
		{"prism", parser.QueryOptions{FractalID: "f9", FractalIDs: []string{"f1", "f2"}}, storage.FractalScope{FractalIDs: []string{"f1", "f2"}}, true},
		{"unscoped", parser.QueryOptions{}, storage.FractalScope{}, false},
	}
	for _, tc := range cases {
		got, ok := FractalScope(tc.opts)
		if ok != tc.scoped || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: got %+v %v, want %+v %v", tc.name, got, ok, tc.want, tc.scoped)
		}
	}
}

// recordCtx records what read extracts from the context of every ClickHouse read.
func recordCtx(e *testEnv, read func(context.Context) string) func() []string {
	var mu sync.Mutex
	var got []string
	e.ch.CtxArgs = func(ctx context.Context) []any {
		mu.Lock()
		got = append(got, read(ctx))
		mu.Unlock()
		return nil
	}
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), got...)
	}
}

// sentIDs records the query id of every ClickHouse read the env serves.
func sentIDs(e *testEnv) func() []string { return recordCtx(e, storage.SentQueryIDOf) }

func assertScoped(t *testing.T, name string, ids []string, scope string) {
	t.Helper()
	if len(ids) == 0 {
		t.Fatalf("%s: no ClickHouse reads", name)
	}
	for _, id := range ids {
		if !strings.HasSuffix(id, "~bfscope~"+scope) {
			t.Errorf("%s: read sent as %q, want scope %q", name, id, scope)
		}
	}
}

// Every read a search issues reaches ClickHouse carrying the scope it was
// translated with, so the row policy holds even if the SQL loses its predicate.
func TestSearchReadsCarryFractalScope(t *testing.T) {
	cases := []struct {
		name  string
		req   QueryRequest
		vals  ctxVals
		scope string
	}{
		{"fractal", QueryRequest{Query: "level=error", FractalID: "f1"}, adminCtx(), "f1,"},
		{"default fractal", QueryRequest{Query: "level=error", FractalID: "fdef"}, adminCtx(), "fdef,,"},
		{"prism", QueryRequest{Query: "level=error"}, ctxVals{"user": testAdmin, "selected_prism": "p1"}, "f1,f2,"},
	}
	for _, tc := range cases {
		e := newTestEnv(t, 1000)
		e.members["p1"] = []string{"f1", "f2"}
		ids := sentIDs(e)
		e.respond(func(string) ([]map[string]interface{}, error) { return logRows(1, testEndTime, "f1"), nil })

		rec, resp := runQuery(t, e, tc.req, tc.vals)
		if rec.Code != http.StatusOK || !resp.Success {
			t.Fatalf("%s: got %d %+v", tc.name, rec.Code, resp)
		}
		assertScoped(t, tc.name, ids(), tc.scope)
	}
}

func TestFieldStatsCarriesFractalScope(t *testing.T) {
	e := newTestEnv(t, 1000)
	ids := sentIDs(e)
	e.respond(func(string) ([]map[string]interface{}, error) { return logRows(1, testEndTime, "f1"), nil })
	rec := httptest.NewRecorder()
	e.h.HandleFieldStats(rec, testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: "level=error", FractalID: "f1"}), adminCtx()))
	assertScoped(t, "field stats", ids(), "f1,")
}

func TestExecuteBQLCarriesFractalScope(t *testing.T) {
	e := newTestEnv(t, 0)
	ids := sentIDs(e)
	e.respond(func(string) ([]map[string]interface{}, error) { return logRows(1, testEndTime, "f1"), nil })
	s := execWindow
	s.FractalID = "f1"
	if _, err := e.h.ExecuteBQL(context.Background(), "level=error", s); err != nil {
		t.Fatal(err)
	}
	assertScoped(t, "widget", ids(), "f1,")
}

// Field stats and export run user queries outside the search budget, on the
// least-privilege uncapped identity rather than the privileged connection. That
// includes the reads prepareQuery makes, such as the ptg() walk, even for a query
// export then refuses.
func TestFieldStatsAndExportRunOnTheUncappedIdentity(t *testing.T) {
	export := func(q string) func(t *testing.T, e *testEnv) {
		return func(t *testing.T, e *testEnv) {
			e.h.HandleExport(httptest.NewRecorder(), testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: q, FractalID: "f1"}), adminCtx()))
		}
	}
	stats := func(q string) func(t *testing.T, e *testEnv) {
		return func(t *testing.T, e *testEnv) { runFieldStats(t, e, q) }
	}
	cases := []struct {
		name string
		run  func(t *testing.T, e *testEnv)
	}{
		{"field stats", stats("level=error")},
		{"field stats ptg", stats(`ptg(start="seed")`)},
		{"export", export("level=error")},
		{"export ptg", export(`ptg(start="seed")`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEnv(t, 1000)
			classes := recordCtx(e, storage.QueryClassOf)
			tc.run(t, e)
			got := classes()
			if len(got) == 0 {
				t.Fatal("no ClickHouse reads")
			}
			for _, class := range got {
				if class != storage.QueryAlertClass {
					t.Errorf("read ran as %q", class)
				}
			}
		})
	}
}

// The reads prepareQuery makes to resolve a command belong to the query that asked
// for them: the same class and the same scope, before any translated SQL exists.
func TestResolverReadsRunInTheQuerysClassAndScope(t *testing.T) {
	search := func(q string, vals ctxVals, fractal string) func(t *testing.T, e *testEnv) {
		return func(t *testing.T, e *testEnv) {
			runQuery(t, e, QueryRequest{Query: q, FractalID: fractal}, vals)
		}
	}
	export := func(t *testing.T, e *testEnv) {
		e.h.HandleExport(httptest.NewRecorder(), testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: `ptg(start="seed")`, FractalID: "f1"}), adminCtx()))
	}
	stats := func(t *testing.T, e *testEnv) { runFieldStats(t, e, `ptg(start="seed")`) }
	prism := ctxVals{"user": testAdmin, "selected_prism": "p1"}
	cases := []struct {
		name  string
		run   func(t *testing.T, e *testEnv)
		class string
		scope string
	}{
		{"search ptg", search(`ptg(start="seed")`, adminCtx(), "f1"), storage.QuerySearchWorkload, "f1,"},
		{"search pgr", search(`pgr(start="seed")`, adminCtx(), "f1"), storage.QuerySearchWorkload, "f1,"},
		{"search ptg default fractal", search(`ptg(start="seed")`, adminCtx(), "fdef"), storage.QuerySearchWorkload, "fdef,,"},
		{"search ptg prism", search(`ptg(start="seed")`, prism, ""), storage.QuerySearchWorkload, "f1,f2,"},
		{"field stats ptg", stats, storage.QueryAlertClass, "f1,"},
		{"export ptg", export, storage.QueryAlertClass, "f1,"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEnv(t, 1000)
			e.members["p1"] = []string{"f1", "f2"}
			reads := recordCtx(e, func(ctx context.Context) string {
				return storage.QueryClassOf(ctx) + " " + storage.SentQueryIDOf(ctx)
			})
			tc.run(t, e)
			got := reads()
			if len(got) == 0 {
				t.Fatal("no ClickHouse reads")
			}
			for _, read := range got {
				if !strings.HasPrefix(read, tc.class+" ") || !strings.HasSuffix(read, "~bfscope~"+tc.scope) {
					t.Errorf("read sent as %q, want class %q scope %q", read, tc.class, tc.scope)
				}
			}
		})
	}
}
