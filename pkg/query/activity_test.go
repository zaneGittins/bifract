package query

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"bifract/pkg/storage"
)

// The activity view classifies query_log rows with startsWith over the rendered
// tag rather than parsing JSON per row. That makes the tag's exact rendering part
// of the contract: reorder QueryTag's fields or rename a source and the classifier
// silently stops matching, which shows up as every query reading "System".
func TestActivityClassSQLMatchesTagRendering(t *testing.T) {
	for _, source := range []string{
		storage.SourceSearch, storage.SourceDashboard, storage.SourceNotebook,
		storage.SourceAlert, storage.SourceRecall, storage.SourceModel,
		storage.SourceChat, storage.SourceSystem,
	} {
		rendered := storage.QueryTag{Source: source, User: "zane", Fractal: "f1", Label: "x"}.String()
		want := `{"src":"` + source + `"`
		if !strings.HasPrefix(rendered, want) {
			t.Fatalf("tag for %q renders as %q, which does not start with %q", source, rendered, want)
		}
	}

	// The two classifiers must recognise the same tags, or the same query lands in
	// one class while running and another once it finishes.
	for _, source := range []string{storage.SourceAlert, storage.SourceSystem} {
		prefix := `'{"src":"` + source + `"'`
		if !strings.Contains(activityClassSQL, prefix) {
			t.Errorf("activityClassSQL does not test for %s", prefix)
		}
		if !strings.Contains(activityProcClassSQL, prefix) {
			t.Errorf("activityProcClassSQL does not test for %s", prefix)
		}
	}
}

// The view excludes its own reads by exact tag equality, so the string it filters
// on has to be the string it writes.
func TestActivityTagRoundTrips(t *testing.T) {
	if activityTag == "" {
		t.Fatal("activity tag rendered empty")
	}
	if activityTag != activityQueryTag.String() {
		t.Fatalf("filter tag %q != written tag %q", activityTag, activityQueryTag.String())
	}
	if !strings.Contains(activityTag, storage.LabelActivity) {
		t.Fatalf("activity tag %q does not carry its label", activityTag)
	}
}

func TestActivityFiltersEscapeInput(t *testing.T) {
	got := activityTextFilter("o'brien")
	if strings.Contains(got, "'o'brien'") {
		t.Fatalf("text filter did not escape the quote: %s", got)
	}
	if activityTextFilter("   ") != "" {
		t.Error("blank text should not add a predicate")
	}
	if activityClassFilter("nonsense", activityClassSQL) != "" {
		t.Error("an unknown class should select everything, not nothing")
	}
	if !strings.Contains(activityClassFilter("ingest", activityClassSQL), "= 'ingest'") {
		t.Error("class filter did not build a predicate")
	}
	long := strings.Repeat("z", 500)
	if strings.Count(activityNodeFilter(long), "z") > 128 {
		t.Error("node filter did not bound its input")
	}
	if strings.Count(activityTextFilter(long), "z") > 400 {
		t.Error("text filter did not bound its input")
	}
}

func TestActivityRangeIsBounded(t *testing.T) {
	// A long range must not turn every poll into a month-wide query_log scan.
	for _, r := range []string{"1h", "8h", "24h", "7d", "30d", "bogus"} {
		window, bucket := activityRange(r)
		if window <= 0 || window > 1440 {
			t.Errorf("range %q gives an unusable window of %d minutes", r, window)
		}
		if bucket <= 0 || window*60/bucket < 8 {
			t.Errorf("range %q gives %d buckets, too few to chart", r, window*60/bucket)
		}
	}
}

// The stream's state chip decides which side of the union runs: an error cannot
// be running yet and a running query has not finished, so each of those drops a
// branch; "slow" bounds both sides by duration.
func TestActivityStreamSQLStates(t *testing.T) {
	single := storage.Topology{}
	const running, finished = "FROM system.processes", "FROM system.query_log"
	cases := []struct {
		state                 string
		wantRunning, wantDone bool
		wantPreds             []string
	}{
		{"", true, true, nil},
		{"running", true, false, nil},
		{"error", false, true, []string{"type = 'ExceptionWhileProcessing'"}},
		{"slow", true, true, []string{"elapsed > 5", "query_duration_ms > 5000"}},
		{"bogus", true, true, nil},
	}
	for _, tc := range cases {
		sql := activityStreamSQL(single, tc.state, "", "", "", 60, 60)
		if strings.Contains(sql, running) != tc.wantRunning || strings.Contains(sql, finished) != tc.wantDone {
			t.Errorf("state %q: running=%v finished=%v, want %v %v", tc.state,
				strings.Contains(sql, running), strings.Contains(sql, finished), tc.wantRunning, tc.wantDone)
		}
		if (tc.wantRunning && tc.wantDone) != strings.Contains(sql, "UNION ALL") {
			t.Errorf("state %q: union mismatch", tc.state)
		}
		for _, p := range tc.wantPreds {
			if !strings.Contains(sql, p) {
				t.Errorf("state %q: missing %q", tc.state, p)
			}
		}
	}
}

// Every read is bounded and excludes the view's own queries, and on a cluster
// it reads every replica rather than whichever node the driver picked.
func TestActivityStreamSQLBoundsAndFanout(t *testing.T) {
	sql := activityStreamSQL(storage.Topology{FanoutCluster: "c1"}, "", "search", "o'brien", "node-2", 75, 480)
	for _, want := range []string{
		"clusterAllReplicas('c1', system.processes)",
		"clusterAllReplicas('c1', system.query_log)",
		"INTERVAL 480 MINUTE",
		"LIMIT 75\n)",                                  // inner finished-side limit
		fmt.Sprintf("LIMIT %d", activityRunningCap),    // running side
		fmt.Sprintf("LIMIT %d", 75+activityRunningCap), // outer
		"Settings['log_comment'] != '" + storage.EscCHStr(activityTag) + "'",
		"log_comment != '" + storage.EscCHStr(activityTag) + "'",
		`o\'brien`,
		"hostName() = 'node-2'",
		"= 'search'",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing %q", want)
		}
	}
}

func TestActivityLimit(t *testing.T) {
	cases := map[string]int{"": activityDefaultRows, "x": activityDefaultRows, "-4": activityDefaultRows, "0": activityDefaultRows, "25": 25, "100000": activityMaxRows}
	for in, want := range cases {
		r := httptest.NewRequest(http.MethodGet, "/?limit="+in, nil)
		if got := activityLimit(r); got != want {
			t.Errorf("limit=%q -> %d, want %d", in, got, want)
		}
	}
}

func TestActivityProcTextFilter(t *testing.T) {
	if activityProcTextFilter("  ") != "" {
		t.Error("blank text should not add a predicate")
	}
	got := activityProcTextFilter("o'brien")
	if strings.Contains(got, "'o'brien'") || !strings.Contains(got, "positionCaseInsensitive(user,") {
		t.Errorf("proc filter: %s", got)
	}
	if strings.Count(activityProcTextFilter(strings.Repeat("z", 500)), "z") > 3*200 {
		t.Error("proc text filter did not bound its input")
	}
}

// Query ids reach SQL literals and KILL statements, so only the characters a
// real id carries are accepted.
func TestValidActivityQueryID(t *testing.T) {
	for id, want := range map[string]bool{
		"bif-search-0b6e1d2a-1c2d-4e5f-8a9b-0c1d2e3f4a5b-h3": true,
		"a_b-C9":                 true,
		"x' OR 1=1 --":           false,
		"id;DROP":                false,
		"tab\tbed":               false,
		strings.Repeat("a", 129): false,
		strings.Repeat("a", 128): true,
	} {
		if got := validActivityQueryID(id); got != want {
			t.Errorf("validActivityQueryID(%q) = %v, want %v", id, got, want)
		}
	}

	rec := httptest.NewRecorder()
	(&PerformanceHandler{}).HandleActivityDetail(rec, httptest.NewRequest(http.MethodGet, "/?query_id=x%27", nil))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("invalid id: status %d, want 400", rec.Code)
	}
	rec = httptest.NewRecorder()
	(&PerformanceHandler{}).HandleActivityDetail(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("missing id: status %d, want 400", rec.Code)
	}
}
