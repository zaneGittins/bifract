package alerts

import (
	"strings"
	"testing"
)

// Listing, counting and bulk toggling must see exactly the same rows, so they share
// one predicate builder; its placeholders have to line up with its arguments.
func TestFeedAlertWherePlaceholdersMatchArgs(t *testing.T) {
	q := FeedAlertQuery{
		FractalID: "f1", Search: "powershell", Status: "enabled",
		FeedID: "feed-1", Severity: "HIGH", Label: "attack.t1059",
	}
	b := &argBuilder{}
	where := q.where(b)

	want := []string{
		"a.feed_id IS NOT NULL",
		"a.fractal_id = $1",
		"a.name ILIKE $2 ESCAPE",
		"a.query_string ILIKE $2 ESCAPE",
		"a.enabled = true",
		"a.feed_id = $3",
		"$4 = ANY(a.labels)",
		"$5 = ANY(a.labels)",
	}
	for _, w := range want {
		if !strings.Contains(where, w) {
			t.Errorf("predicate %q is missing %q", where, w)
		}
	}
	if len(b.args) != 5 {
		t.Fatalf("args = %v", b.args)
	}
	if b.args[0] != "f1" || b.args[1] != "%powershell%" || b.args[2] != "feed-1" || b.args[3] != "sigma:high" || b.args[4] != "attack.t1059" {
		t.Errorf("args = %v", b.args)
	}
}

func TestFeedAlertWhereScopeAndStatus(t *testing.T) {
	b := &argBuilder{}
	where := FeedAlertQuery{FractalID: "f1", PrismID: "p1", Status: "disabled"}.where(b)
	if !strings.Contains(where, "a.prism_id = $1") || strings.Contains(where, "fractal_id") || b.args[0] != "p1" {
		t.Errorf("a prism scope replaces the fractal one: %q %v", where, b.args)
	}
	if !strings.Contains(where, "a.enabled = false") {
		t.Errorf("status filter missing: %q", where)
	}

	b = &argBuilder{}
	where = FeedAlertQuery{FractalID: "f1", Status: "all", Search: "   "}.where(b)
	if strings.Contains(where, "enabled") || strings.Contains(where, "ILIKE") || len(b.args) != 1 {
		t.Errorf("an unknown status and a blank search filter nothing: %q %v", where, b.args)
	}
}

// A user's % or _ searches for that character, not for anything.
func TestFeedAlertSearchEscapesWildcards(t *testing.T) {
	b := &argBuilder{}
	FeedAlertQuery{FractalID: "f1", Search: `50%_off\x`}.where(b)
	if got := b.args[1]; got != `%50\%\_off\\x%` {
		t.Errorf("search arg = %q", got)
	}
}

// Placeholders continue from whatever the caller already bound, as the bulk toggle
// binds its own values first.
func TestFeedAlertWhereContinuesNumbering(t *testing.T) {
	b := &argBuilder{}
	if b.next(true) != "$1" || b.next("u") != "$2" {
		t.Fatal("argBuilder numbers from one")
	}
	where := FeedAlertQuery{FractalID: "f1", Label: "x"}.where(b)
	if !strings.Contains(where, "a.fractal_id = $3") || !strings.Contains(where, "$4 = ANY(a.labels)") {
		t.Errorf("got %q", where)
	}
}

// Sort input picks from a fixed set of expressions: anything else falls back to the
// default rather than reaching the SQL.
func TestFeedAlertOrderBy(t *testing.T) {
	cases := []struct {
		sort, dir, want string
	}{
		{"", "", "COALESCE(f.name,''), a.name, a.id"},
		{"name; DROP TABLE alerts", "desc", "COALESCE(f.name,''), a.name, a.id"},
		{"name", "", "lower(a.name) ASC, a.name, a.id"},
		{"name", "DESC", "lower(a.name) DESC, a.name, a.id"},
		{"exec_time", "sideways", "COALESCE(a.last_execution_time_ms, -1) ASC, a.name, a.id"},
		{"last_triggered", "desc", "COALESCE(a.last_triggered, to_timestamp(0)) DESC, a.name, a.id"},
	}
	for _, tc := range cases {
		if got := (FeedAlertQuery{Sort: tc.sort, Dir: tc.dir}).orderBy(); got != tc.want {
			t.Errorf("sort %q dir %q: got %q, want %q", tc.sort, tc.dir, got, tc.want)
		}
	}
	if got := (FeedAlertQuery{Sort: "severity", Dir: "desc"}).orderBy(); !strings.HasPrefix(got, "CASE") || !strings.HasSuffix(got, "END DESC, a.name, a.id") {
		t.Errorf("severity sort: %q", got)
	}
}
