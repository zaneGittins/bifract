//go:build pglive

// Live checks for the alert engine endpoints. They need a reachable Postgres:
//
//	BIFRACT_PG_HOST=localhost BIFRACT_PG_PASSWORD=... \
//	  go test -tags pglive ./pkg/query -run Live -v
package query

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"bifract/pkg/storage"
)

func livePG(t *testing.T) *storage.PostgresClient {
	t.Helper()
	host := os.Getenv("BIFRACT_PG_HOST")
	if host == "" {
		t.Skip("BIFRACT_PG_HOST not set")
	}
	port := 5432
	if v := os.Getenv("BIFRACT_PG_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			port = n
		}
	}
	pg, err := storage.NewPostgresClient(host, port,
		envOr("BIFRACT_PG_DATABASE", "bifract"),
		envOr("BIFRACT_PG_USER", "bifract"),
		os.Getenv("BIFRACT_PG_PASSWORD"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { pg.Close() })
	return pg
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func decodePG(t *testing.T, rec *httptest.ResponseRecorder) map[string]interface{} {
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

func TestLiveAlertStats(t *testing.T) {
	h := &PerformanceHandler{pg: livePG(t)}
	for _, r := range []string{"", "1h", "8h", "24h", "7d"} {
		rec := httptest.NewRecorder()
		h.HandleAlertStats(rec, httptest.NewRequest("GET", "/api/v1/admin/alert-stats?range="+r, nil))
		out := decodePG(t, rec)
		for _, key := range []string{"summary", "exec_history", "fires_history", "disabled_alerts"} {
			if _, ok := out[key]; !ok {
				t.Errorf("range %q: response is missing %q", r, key)
			}
		}
		sum, _ := out["summary"].(map[string]interface{})
		t.Logf("range %-4s evaluating=%v disabled=%v lag_p95=%v fires=%v action_failures=%v",
			r, sum["evaluating"], sum["disabled"], sum["lag_p95_sec"], sum["fires"], sum["action_failures"])
	}
}

// The count the tile shows has to be the same set the strip lists. The bug this
// guards: the old query counted disabled_reason inside a WHERE enabled = true,
// so it could only ever find alerts that had been re-enabled without their
// reason cleared, and reported zero on a fleet with real auto-disabled alerts.
func TestLiveDisabledCountMatchesList(t *testing.T) {
	h := &PerformanceHandler{pg: livePG(t)}
	rec := httptest.NewRecorder()
	h.HandleAlertStats(rec, httptest.NewRequest("GET", "/api/v1/admin/alert-stats", nil))
	out := decodePG(t, rec)

	sum, _ := out["summary"].(map[string]interface{})
	counted, _ := sum["disabled"].(float64)
	listed, _ := out["disabled_alerts"].([]interface{})
	t.Logf("tile says %d auto-disabled, strip lists %d", int(counted), len(listed))

	if counted == 0 && len(listed) > 0 {
		t.Fatal("the tile reports no auto-disabled alerts while the strip lists some")
	}
	if counted > 0 && len(listed) == 0 {
		t.Fatal("the tile reports auto-disabled alerts the strip cannot name")
	}
	for _, item := range listed {
		m, _ := item.(map[string]interface{})
		if name, _ := m["name"].(string); name == "" {
			t.Error("a disabled entry has no alert name")
		}
		if reason, _ := m["reason"].(string); reason == "" {
			t.Error("a disabled entry has no reason, which is the point of the strip")
		}
	}
}

// Both charts on the row have to describe the same span, and that span has to
// divide into enough buckets to be a chart. The bug this guards: the window came
// from one function and the bucket width from another, so a 30-day selection
// drew a capped 24-hour window in 4-hour buckets -- six points.
func TestAlertRangeWindowAndBucketAgree(t *testing.T) {
	for _, r := range []string{"", "1h", "8h", "24h", "7d", "30d", "bogus"} {
		window, bucket := alertRange(r)
		if window <= 0 || window > alertMaxWindowMinutes {
			t.Errorf("range %q gives an unusable window of %d minutes", r, window)
		}
		if bucket <= 0 {
			t.Fatalf("range %q gives a zero bucket", r)
		}
		if points := window * 60 / bucket; points < 12 || points > 200 {
			t.Errorf("range %q divides into %d buckets, which does not draw a chart", r, points)
		}
	}
}

func TestAlertShortReasonIsRuneSafe(t *testing.T) {
	long := "Auto-disabled: " + strings.Repeat("日本語", 200)
	got := alertShortReason(long)
	if !utf8.ValidString(got) {
		t.Fatalf("truncation produced invalid UTF-8: %q", got)
	}
	if strings.HasPrefix(got, "Auto-disabled") {
		t.Error("the prefix implied by the column was not trimmed")
	}
	if len([]rune(got)) > 180 {
		t.Errorf("reason is %d runes, over the cap", len([]rune(got)))
	}
}

func TestLiveAlertRows(t *testing.T) {
	h := &PerformanceHandler{pg: livePG(t)}
	cases := []string{
		"", "?mode=alerts", "?mode=fires",
		"?mode=alerts&range=24h", "?mode=fires&range=7d",
		"?mode=alerts&q=rare", "?mode=alerts&q=o%27brien",
		"?mode=fires&q=beacon", "?mode=alerts&fractal=00000000-0000-0000-0000-000000000000",
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		h.HandleAlertRows(rec, httptest.NewRequest("GET", "/api/v1/admin/alert-stats/rows"+c, nil))
		out := decodePG(t, rec)
		rows, _ := out["rows"].([]interface{})
		t.Logf("%-52s -> %d rows", c, len(rows))
		if len(rows) > alertRowLimit {
			t.Errorf("%s returned %d rows, over the %d cap", c, len(rows), alertRowLimit)
		}
	}
}

// Rows that need attention have to sort to the top; the alphabet must not decide.
func TestLiveAlertRowsSortProblemsFirst(t *testing.T) {
	h := &PerformanceHandler{pg: livePG(t)}
	rec := httptest.NewRecorder()
	h.HandleAlertRows(rec, httptest.NewRequest("GET", "/api/v1/admin/alert-stats/rows?mode=alerts", nil))
	rows, _ := decodePG(t, rec)["rows"].([]interface{})
	if len(rows) < 2 {
		t.Skip("not enough alerts to check ordering")
	}
	seenEnabled := false
	for _, row := range rows {
		m, _ := row.(map[string]interface{})
		enabled, _ := m["enabled"].(bool)
		if enabled {
			seenEnabled = true
		} else if seenEnabled {
			t.Fatalf("a disabled alert (%v) sorted below an enabled one", m["name"])
		}
	}
}
