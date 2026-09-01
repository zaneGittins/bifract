package query

import (
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
