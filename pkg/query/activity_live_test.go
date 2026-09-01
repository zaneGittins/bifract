//go:build chlive

// Live checks for the activity endpoints. They need a reachable ClickHouse, so
// they sit behind the chlive tag:
//
//	BIFRACT_CH_ADDR=localhost:9000 go test -tags chlive ./pkg/query -run Live -v
package query

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"testing"

	"bifract/pkg/storage"
)

func liveClient(t *testing.T) *storage.ClickHouseClient {
	t.Helper()
	addr := os.Getenv("BIFRACT_CH_ADDR")
	if addr == "" {
		t.Skip("BIFRACT_CH_ADDR not set")
	}
	pw := os.Getenv("BIFRACT_CH_PASSWORD")
	if pw == "" {
		pw = "bifract"
	}
	db, err := storage.NewClickHouseClient(storage.ClientOptions{
		Conn: storage.ConnOptions{
			Addrs: []string{addr}, Database: "logs", User: "default", Password: pw,
		},
		Role: storage.RoleIngest,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func decodeLive(t *testing.T, rec *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var out map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v (%s)", err, rec.Body.String())
	}
	if out["success"] != true {
		t.Fatalf("handler reported failure: %s", rec.Body.String())
	}
	return out
}

func TestLiveActivityStream(t *testing.T) {
	h := &PerformanceHandler{db: liveClient(t)}
	for _, q := range []string{
		"", "?state=running", "?state=error", "?state=slow",
		"?class=search", "?class=ingest", "?class=system", "?class=alert",
		"?q=select", "?q=o%27brien", "?range=24h&limit=10", "?node=nosuchnode",
	} {
		rec := httptest.NewRecorder()
		h.HandleActivityStream(rec, httptest.NewRequest("GET", "/api/v1/admin/activity"+q, nil))
		out := decodeLive(t, rec)
		rows, _ := out["rows"].([]interface{})
		t.Logf("%-28s -> %d rows", q, len(rows))
	}
}

func TestLiveActivitySummary(t *testing.T) {
	h := &PerformanceHandler{db: liveClient(t)}
	for _, r := range []string{"1h", "8h", "24h", "7d"} {
		rec := httptest.NewRecorder()
		h.HandleActivitySummary(rec, httptest.NewRequest("GET", "/api/v1/admin/activity/summary?range="+r, nil))
		out := decodeLive(t, rec)
		if err, bad := out["buckets_error"]; bad {
			t.Fatalf("range %s: bucket query failed: %v", r, err)
		}
		for _, key := range []string{"buckets", "running", "patterns"} {
			if _, ok := out[key]; !ok {
				t.Errorf("range %s: summary is missing %q", r, key)
			}
		}
		buckets, _ := out["buckets"].([]interface{})
		patterns, _ := out["patterns"].([]interface{})
		t.Logf("range %-4s -> %d buckets, %d patterns", r, len(buckets), len(patterns))
	}
}

// Background operations live on their own endpoint, behind the Overview tab.
func TestLiveBackgroundOps(t *testing.T) {
	h := &PerformanceHandler{db: liveClient(t)}
	rec := httptest.NewRecorder()
	h.HandleBackgroundOps(rec, httptest.NewRequest("GET", "/api/v1/admin/background", nil))
	out := decodeLive(t, rec)
	for _, key := range []string{"merges", "mutations"} {
		if _, ok := out[key]; !ok {
			t.Errorf("background response is missing %q", key)
		}
	}
	merges, _ := out["merges"].([]interface{})
	mutations, _ := out["mutations"].([]interface{})
	t.Logf("%d merges, %d mutations", len(merges), len(mutations))
}

// A class chip has to actually narrow the result, not just re-issue the query.
// The bug this guards is a filter that silently drops out of the WHERE clause.
func TestLiveClassFilterNarrows(t *testing.T) {
	h := &PerformanceHandler{db: liveClient(t)}
	seen := map[string]map[string]bool{}
	for _, class := range []string{"search", "alert", "ingest", "system"} {
		rec := httptest.NewRecorder()
		h.HandleActivityStream(rec, httptest.NewRequest("GET", "/api/v1/admin/activity?limit=40&class="+class, nil))
		rows, _ := decodeLive(t, rec)["rows"].([]interface{})
		ids := map[string]bool{}
		for _, row := range rows {
			m, _ := row.(map[string]interface{})
			if got, _ := m["class"].(string); got != class {
				t.Fatalf("class=%s returned a row classified %q", class, got)
			}
			id, _ := m["query_id"].(string)
			ids[id] = true
		}
		seen[class] = ids
		t.Logf("class=%-7s -> %d rows", class, len(rows))
	}
	// Different classes must not be handing back the same rows.
	for _, a := range []string{"search", "alert"} {
		for _, b := range []string{"ingest", "system"} {
			for id := range seen[a] {
				if seen[b][id] {
					t.Fatalf("query %s appears under both %s and %s", id, a, b)
				}
			}
		}
	}
}

// Every panel on the page reads the same query log, so they have to agree. The
// bug this guards: the Errors chip built a single-branch query, which lost the
// UNION ALL that had been quietly widening Int32 exception_code to Int64, and
// the whole request failed the moment a real failure existed to scan. The page
// showed the failure count and an empty stream, which reads as a dead chip.
func TestLiveErrorFilterAgreesWithFailurePanel(t *testing.T) {
	h := &PerformanceHandler{db: liveClient(t)}

	rec := httptest.NewRecorder()
	h.HandleActivitySummary(rec, httptest.NewRequest("GET", "/api/v1/admin/activity/summary?range=1h", nil))
	failures, _ := decodeLive(t, rec)["failures"].([]interface{})
	counted := 0
	for _, f := range failures {
		m, _ := f.(map[string]interface{})
		n, _ := m["n"].(float64)
		counted += int(n)
	}

	rec = httptest.NewRecorder()
	h.HandleActivityStream(rec, httptest.NewRequest("GET", "/api/v1/admin/activity?state=error&range=1h&limit=60", nil))
	rows, _ := decodeLive(t, rec)["rows"].([]interface{})
	t.Logf("failures panel counts %d, Errors filter returns %d rows", counted, len(rows))

	if counted > 0 && len(rows) == 0 {
		t.Fatal("the failures panel counts failures the Errors filter cannot find")
	}
	for _, row := range rows {
		m, _ := row.(map[string]interface{})
		if state, _ := m["state"].(string); state != "error" && state != "killed" {
			t.Errorf("state=error returned a row in state %q", state)
		}
	}
}

// A state filter builds one branch instead of a union, so each branch has to
// declare column types the row scanner can handle on its own.
func TestLiveSingleBranchStatesScan(t *testing.T) {
	h := &PerformanceHandler{db: liveClient(t)}
	for _, state := range []string{"running", "error", "slow"} {
		rec := httptest.NewRecorder()
		h.HandleActivityStream(rec, httptest.NewRequest("GET", "/api/v1/admin/activity?range=24h&limit=20&state="+state, nil))
		// decodeLive fails the test on a non-200 or success:false, which is the
		// point: a scan error surfaced as an empty list before.
		rows, _ := decodeLive(t, rec)["rows"].([]interface{})
		t.Logf("state=%-8s -> %d rows", state, len(rows))
	}
}

// The view must not report its own reads as cluster activity.
func TestLiveActivityExcludesItself(t *testing.T) {
	h := &PerformanceHandler{db: liveClient(t)}
	rec := httptest.NewRecorder()
	h.HandleActivityStream(rec, httptest.NewRequest("GET", "/api/v1/admin/activity?limit=200", nil))
	rows, _ := decodeLive(t, rec)["rows"].([]interface{})
	for _, row := range rows {
		m, _ := row.(map[string]interface{})
		if tag, _ := m["tag"].(string); tag == activityTag {
			t.Fatalf("the activity view listed one of its own queries: %v", m["query"])
		}
	}
}

// A tagged query has to come back out of the query log carrying its tag.
func TestLiveTagReachesQueryLog(t *testing.T) {
	db := liveClient(t)
	tag := storage.QueryTag{
		Source:  storage.SourceSearch,
		User:    "livetest",
		Fractal: "f-live",
		Label:   "tag round trip",
		// The BQL rides along so the drawer can show it beside the SQL.
		BQL: "event_type=process_creation AND host=web-01 | stats count() by image",
	}
	ctx := storage.TagContext(context.Background(), tag)
	if _, err := db.Query(ctx, "SELECT toUInt64(1) AS activity_tag_probe"); err != nil {
		t.Fatalf("probe query: %v", err)
	}
	if err := db.Exec(context.Background(), "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("flush logs: %v", err)
	}
	rows, err := db.Query(context.Background(),
		"SELECT log_comment FROM system.query_log WHERE positionCaseInsensitive(query, 'activity_tag_probe') > 0 AND type = 'QueryFinish' ORDER BY event_time DESC LIMIT 1")
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("probe query never reached the query log")
	}
	got, _ := rows[0]["log_comment"].(string)
	if got != tag.String() {
		t.Fatalf("log_comment is %q, want %q", got, tag.String())
	}
	var back storage.QueryTag
	if err := json.Unmarshal([]byte(got), &back); err != nil {
		t.Fatalf("log_comment is not the JSON the reader parses: %v", err)
	}
	if back.BQL != tag.BQL {
		t.Errorf("BQL did not survive the round trip: %q", back.BQL)
	}
}
