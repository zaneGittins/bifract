package alerts

import (
	"testing"
	"time"

	"bifract/pkg/parser"
)

func mustParse(t *testing.T, q string) *parser.PipelineNode {
	t.Helper()
	pl, err := parser.ParseQuery(q)
	if err != nil {
		t.Fatalf("parse %q: %v", q, err)
	}
	return pl
}

// The contract is declared by the query, not inferred from a command name here.
func TestQueryWindowContract(t *testing.T) {
	cases := []struct {
		name         string
		query        string
		wantLookback int
		wantColumn   string
	}{
		{"plain filter declares nothing", `status=500`, 0, ""},
		{"aggregation declares nothing", `* | groupby(user) | count()`, 0, ""},
		{"correlation declares its bound", `* | chain(user, within=5m) { a="x"; b="y" }`, 300, "_chain_done"},
		{"correlation without a bound", `* | chain(user) { a="x"; b="y" }`, 0, "_chain_done"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lb, col := parser.QueryWindowContract(mustParse(t, tc.query))
			if lb != tc.wantLookback || col != tc.wantColumn {
				t.Errorf("got (%d, %q), want (%d, %q)", lb, col, tc.wantLookback, tc.wantColumn)
			}
		})
	}
}

// Existing alerts must take an unchanged path: no completion column means no filtering.
func TestDropAlreadyReportedIsInertWithoutContract(t *testing.T) {
	rows := []map[string]interface{}{{"user": "a"}, {"user": "b"}}
	got := dropAlreadyReported(rows, "", time.Now())
	if len(got) != 2 {
		t.Fatalf("rows without a contract must pass through, got %d", len(got))
	}
}

// A correlation that completed before the window start was already reported by the
// previous window; keeping it would double-fire once the read is widened.
func TestDropAlreadyReportedRemovesOnlyPriorCompletions(t *testing.T) {
	start := time.UnixMilli(1_000_000)
	rows := []map[string]interface{}{
		{"user": "old", "_chain_done": uint64(950_000)},          // completed before start
		{"user": "straddling", "_chain_done": uint64(1_050_000)}, // began before, completed after
		{"user": "new", "_chain_done": uint64(1_200_000)},        // fully inside
	}
	got := dropAlreadyReported(rows, "_chain_done", start)
	if len(got) != 2 {
		t.Fatalf("expected 2 rows kept, got %d", len(got))
	}
	if got[0]["user"] != "straddling" || got[1]["user"] != "new" {
		t.Errorf("wrong rows kept: %v, %v", got[0]["user"], got[1]["user"])
	}
}

func TestCompletionMillisAcceptsDriverTypes(t *testing.T) {
	for name, v := range map[string]interface{}{
		"uint64": uint64(9), "int64": int64(9), "uint32": uint32(9), "float64": float64(9),
	} {
		if got := completionMillis(v); got != 9 {
			t.Errorf("%s: got %d, want 9", name, got)
		}
	}
	// A missing marker must keep the row: dropping on absent data would lose alerts.
	if got := completionMillis(nil); got != 0 {
		t.Errorf("nil must yield 0, got %d", got)
	}
}

// An unbounded correlation cannot be evaluated correctly over a moving window.
func TestValidateWindowContract(t *testing.T) {
	if err := validateWindowContract(mustParse(t, `* | chain(user) { a="x"; b="y" }`)); err == nil {
		t.Error("expected an unbounded correlation to be rejected")
	}
	if err := validateWindowContract(mustParse(t, `* | chain(user, within=5m) { a="x"; b="y" }`)); err != nil {
		t.Errorf("bounded correlation must be accepted: %v", err)
	}
	if err := validateWindowContract(mustParse(t, `status=500 | groupby(user) | count()`)); err != nil {
		t.Errorf("ordinary alert must be unaffected: %v", err)
	}
}

// The contract must survive queries that cannot translate without full options,
// which is why it is read from handlers rather than from a translation.
func TestContractSurvivesUntranslatableQueries(t *testing.T) {
	for _, q := range []string{
		`comment() | chain(user, within=5m) { a="x"; b="y" }`,
		`* | chain(user, within=5m) { a="x"; b="y" } | model_lookup(m, on=user)`,
	} {
		pl, err := parser.ParseQuery(q)
		if err != nil {
			t.Fatalf("parse %s: %v", q, err)
		}
		if _, terr := parser.TranslateToSQLWithOrder(pl, parser.QueryOptions{}); terr == nil {
			t.Fatalf("precondition: %s should not translate bare", q)
		}
		lb, col := parser.QueryWindowContract(pl)
		if lb != 300 || col != "_chain_done" {
			t.Errorf("%s: got (%d,%q), want (300,\"_chain_done\")", q, lb, col)
		}
	}
}
