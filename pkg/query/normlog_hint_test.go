package query

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Every path that queries the logs table hints its field filters: streaming and
// buffered search with all their histogram, single-scan and fetch queries, export,
// and dashboard widgets.
func TestLogsQueriesCarryNormLogIndexHints(t *testing.T) {
	const query = `computer_name=~"MAvi01"`
	paths := map[string]func(e *testEnv){
		"stream":   func(e *testEnv) { runStream(t, e, QueryRequest{Query: query, FractalID: "f1"}) },
		"buffered": func(e *testEnv) { runQuery(t, e, QueryRequest{Query: query, FractalID: "f1"}, adminCtx()) },
		"export": func(e *testEnv) {
			e.h.HandleExport(httptest.NewRecorder(), testRequest(http.MethodPost, searchBody(t, QueryRequest{Query: query, FractalID: "f1"}), adminCtx()))
		},
		"dashboard": func(e *testEnv) {
			s := execWindow
			s.FractalID = "f1"
			if _, err := e.h.ExecuteBQL(context.Background(), query, s); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, run := range paths {
		t.Run(name, func(t *testing.T) {
			e := newTestEnv(t, 1000)
			e.respond(func(sql string) ([]map[string]interface{}, error) {
				if isNewestSQL(sql) {
					return newestScan(16, nil), nil
				}
				return []map[string]interface{}{}, nil
			})
			run(e)
			queries := e.ch.QueriedMatching("computer_name")
			if len(queries) == 0 {
				t.Fatal("no logs queries issued")
			}
			for _, q := range queries {
				if !strings.Contains(q, "indexHint(match(lower(norm_log), 'mavi01'))") {
					t.Errorf("unhinted: %s", q)
				}
			}
		})
	}
}
