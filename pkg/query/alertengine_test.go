package query

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"bifract/internal/fakedb"
	"bifract/pkg/storage/storagetest"
)

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

// alertRowFilters numbers its placeholders after the window ($1). A gap or a
// reuse makes Postgres reject the statement ("could not determine data type of
// parameter"), which empties the table.
func TestAlertRowFiltersPlaceholders(t *testing.T) {
	cases := []struct {
		q, fractal string
		wantWhere  string
		wantArgs   []interface{}
	}{
		{"", "", "", []interface{}{60}},
		{" disk ", "", " AND a.name ILIKE $2", []interface{}{60, "%disk%"}},
		{"", "f1", " AND fr = $2", []interface{}{60, "f1"}},
		{"disk", "f1", " AND a.name ILIKE $2 AND fr = $3", []interface{}{60, "%disk%", "f1"}},
	}
	for _, tc := range cases {
		args := []interface{}{60}
		where := alertRowFilters(tc.q, tc.fractal, "a.name", "fr", &args)
		if where != tc.wantWhere || fmt.Sprint(args) != fmt.Sprint(tc.wantArgs) {
			t.Errorf("(%q, %q): where=%q args=%v, want %q %v", tc.q, tc.fractal, where, args, tc.wantWhere, tc.wantArgs)
		}
	}
}

// The name search is cut to a bounded length on a rune boundary: a byte cut
// through a multi-byte character hands Postgres invalid UTF-8, and it rejects
// the whole query rather than searching a shorter term.
func TestAlertRowFiltersTruncatesOnRuneBoundary(t *testing.T) {
	args := []interface{}{60}
	alertRowFilters("a"+strings.Repeat("日", 200), "", "a.name", "fr", &args)
	term := args[1].(string)
	if !utf8.ValidString(term) {
		t.Fatalf("truncated term is invalid UTF-8: %q", term)
	}
	if n := utf8.RuneCountInString(strings.Trim(term, "%")); n != 120 {
		t.Errorf("term is %d runes, want 120", n)
	}
}

// Each action column names its action under a different key; all three must read
// the same way, and a failure must say why.
func TestDecodeAlertActions(t *testing.T) {
	got := decodeAlertActions("webhook", `[
		{"webhook_name":"pager","success":true,"error":"stale"},
		{"webhook_name":"slack","success":false,"error":"connection refused"},
		{"success":false,"status_code":503},
		{"webhook_name":"x","success":false}
	]`, "webhook_name")
	want := []alertAction{
		{Kind: "webhook", Name: "pager", OK: true},
		{Kind: "webhook", Name: "slack", Detail: "connection refused"},
		{Kind: "webhook", Name: "webhook", Detail: "HTTP 503"},
		{Kind: "webhook", Name: "x", Detail: "failed"},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got  %+v\nwant %+v", got, want)
	}
	if got := decodeAlertActions("email", "not json", "email_action_name"); got != nil {
		t.Errorf("malformed column decoded to %+v", got)
	}
	if got := decodeAlertActions("fractal", "[]", "fractal_action_name"); len(got) != 0 {
		t.Errorf("empty column decoded to %+v", got)
	}
}

func TestAlertHandlersWithoutPostgres(t *testing.T) {
	h := &PerformanceHandler{}
	for name, handle := range map[string]http.HandlerFunc{"stats": h.HandleAlertStats, "rows": h.HandleAlertRows} {
		rec := httptest.NewRecorder()
		handle(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("%s: status %d, want 503", name, rec.Code)
		}
	}
}

// The page shows how far the ingest watermark trails and which writer holds it,
// and says so when it cannot be read, since then no alert evaluates.
func TestAlertStatsReportsTheIngestWatermark(t *testing.T) {
	pgc, fake := storagetest.Postgres()
	defer pgc.Close()
	h := &PerformanceHandler{pg: pgc}
	stats := func() map[string]interface{} {
		rec := httptest.NewRecorder()
		h.HandleAlertStats(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		var out map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out["summary"].(map[string]interface{})
	}

	fake.On("FROM ingest_watermarks", fakedb.RowsOf(
		[]driver.Value{"ingest@a", time.Now().Add(-40 * time.Second), 1.0},
		[]driver.Value{"ingest@b", time.Now(), 30.0},
	))
	sum := stats()
	if lag := sum["ingest_wm_lag_sec"].(float64); lag < 39 || lag > 42 {
		t.Errorf("lag = %v, want about 40s", lag)
	}
	held := sum["ingest_wm_held"].([]interface{})
	if len(held) != 1 || !strings.Contains(held[0].(string), "ingest@b") {
		t.Errorf("held = %v, want the node with a stale heartbeat", held)
	}

	fake.ClearRules().On("FROM ingest_watermarks", fakedb.PGResult{Err: errors.New("down")})
	if sum := stats(); sum["ingest_wm_error"] != true {
		t.Errorf("an unreadable watermark must be reported, got %v", sum)
	}
}
