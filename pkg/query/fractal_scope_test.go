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

// sentIDs records the query id of every ClickHouse read the env serves.
func sentIDs(e *testEnv) func() []string {
	var mu sync.Mutex
	var ids []string
	e.ch.CtxArgs = func(ctx context.Context) []any {
		mu.Lock()
		ids = append(ids, storage.SentQueryIDOf(ctx))
		mu.Unlock()
		return nil
	}
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), ids...)
	}
}

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
