package query

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"

	"bifract/internal/fakedb"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// adminMark carries a read's marker from its context to the responder, which sees the
// SQL, so each flag is paired with its own query even when reads run concurrently.
type adminMark bool

// recordAdminReads records, per read, whether it was marked for the privileged
// connection, keyed by the SQL so a failure names the read. It defers every answer to
// the fake's rules.
func recordAdminReads(ch *fakedb.CH) func() map[string]bool {
	var mu sync.Mutex
	got := map[string]bool{}
	ch.CtxArgs = func(ctx context.Context) []any { return []any{adminMark(storage.IsAdminRead(ctx))} }
	ch.Respond(func(query string, args []any) (fakedb.CHResult, bool) {
		for _, a := range args {
			if m, ok := a.(adminMark); ok {
				mu.Lock()
				got[strings.Join(strings.Fields(query), " ")] = bool(m)
				mu.Unlock()
			}
		}
		return fakedb.CHResult{}, false
	})
	return func() map[string]bool {
		mu.Lock()
		defer mu.Unlock()
		out := make(map[string]bool, len(got))
		for k, v := range got {
			out[k] = v
		}
		return out
	}
}

// Reads of other queries' text and cluster fan-outs are explicitly admin; the rest of
// the System tab runs read-only, so the marker cannot spread to reads that need none.
func TestSystemTabReadsChooseTheirConnection(t *testing.T) {
	type tc struct {
		name    string
		cluster bool
		handle  func(h *PerformanceHandler) http.HandlerFunc
		target  string
		admin   func(sql string) bool
	}
	all := func(string) bool { return true }
	none := func(string) bool { return false }
	fanout := func(sql string) bool { return strings.Contains(sql, "clusterAllReplicas(") }
	cases := []tc{
		{"activity stream", false, func(h *PerformanceHandler) http.HandlerFunc { return h.HandleActivityStream }, "/?range=8h", all},
		{"activity summary", false, func(h *PerformanceHandler) http.HandlerFunc { return h.HandleActivitySummary }, "/?patterns=1", all},
		{"background ops", true, func(h *PerformanceHandler) http.HandlerFunc { return h.HandleBackgroundOps }, "/", all},
		{"activity detail", false, func(h *PerformanceHandler) http.HandlerFunc { return h.HandleActivityDetail }, "/?query_id=bif-search-1", all},
		{"processes", false, func(h *PerformanceHandler) http.HandlerFunc { return h.HandleProcesses }, "/", all},
		{"metrics", false, func(h *PerformanceHandler) http.HandlerFunc { return h.HandleMetrics }, "/", none},
		{"metrics cluster", true, func(h *PerformanceHandler) http.HandlerFunc { return h.HandleMetrics }, "/", fanout},
		{"ingest daily", false, func(h *PerformanceHandler) http.HandlerFunc { return h.HandleIngestDaily }, "/", none},
		{"hot table", false, func(h *PerformanceHandler) http.HandlerFunc { return h.HandleHotTableStats }, "/", none},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, ch := storagetest.ClickHouse()
			if c.cluster {
				db, ch = clusterClient(t)
			}
			reads := recordAdminReads(ch)
			serve(c.handle(&PerformanceHandler{db: db}), c.target)
			got := reads()
			if len(got) == 0 {
				t.Fatal("no ClickHouse reads")
			}
			for sql, admin := range got {
				if want := c.admin(sql); admin != want {
					t.Errorf("admin = %v, want %v: %.120s", admin, want, sql)
				}
			}
		})
	}
}

// The profile poll reads query_log, which holds other queries' text. The env answers
// through its own responder, so only the markers are recorded.
func TestProfileReadsAreAdmin(t *testing.T) {
	e := newTestEnv(t, 1000)
	var mu sync.Mutex
	var marks []bool
	e.ch.CtxArgs = func(ctx context.Context) []any {
		mu.Lock()
		marks = append(marks, storage.IsAdminRead(ctx))
		mu.Unlock()
		return nil
	}
	e.h.fetchProfileData("bif-search-1")
	mu.Lock()
	defer mu.Unlock()
	if len(marks) == 0 {
		t.Fatal("no ClickHouse reads")
	}
	for i, admin := range marks {
		if !admin {
			t.Errorf("profile read %d not admin: %q", i, e.ch.Queried())
		}
	}
}
