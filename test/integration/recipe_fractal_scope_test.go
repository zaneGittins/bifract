//go:build integration

// A search reads only the fractal it names. The fractal and time guards are
// column predicates in the same SELECT as the query's own outputs, so an output
// named after one of those columns must never stand in for it: a query that tries
// is refused, and nothing from another fractal comes back.
//
//	go test -tags integration ./test/integration/ -run TestFractalScopeHoldsAgainstAliases -v

package integration

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestFractalScopeHoldsAgainstAliases(t *testing.T) {
	t.Parallel()
	c := New(t)

	mine, mineScoped := newFractal(t, c, "scope-mine")
	other, otherScoped := newFractal(t, c, "scope-other")

	marker := fmt.Sprintf("api-suite-scope-%d", time.Now().UnixNano())
	newShipper(t, mineScoped, mine).DoRaw(t, "POST", "/ingest", []map[string]any{
		{"suite_marker": marker, "side": "mine"},
	}, nil)
	newShipper(t, otherScoped, other).DoRaw(t, "POST", "/ingest", []map[string]any{
		{"suite_marker": marker, "side": "other-" + marker},
	}, nil)

	type result struct {
		Success bool             `json:"success"`
		Error   string           `json:"error"`
		Results []map[string]any `json:"results"`
	}
	start := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
	// A refused query answers 400 with the same body, so read it whatever the status.
	query := func(fractalID, q string) result {
		var res result
		_, raw := c.call(t, "POST", "/query", map[string]any{
			"query": q, "fractal_id": fractalID,
			"start": start, "end": time.Now().Add(time.Minute).UTC().Format(time.RFC3339),
		})
		if err := json.Unmarshal(raw, &res); err != nil {
			t.Fatalf("%s: decoding the answer: %v\n%s", q, err, truncate(raw))
		}
		return res
	}
	all := fmt.Sprintf("suite_marker=%q", marker)
	for _, id := range []string{mine, other} {
		Eventually(t, "both fractals' logs to become queryable", 90*time.Second, func() bool {
			return len(query(id, all).Results) == 1
		})
	}

	for _, q := range []string{
		all + fmt.Sprintf(` | eval(fractal_id=%q)`, mine),
		all + fmt.Sprintf(` | fractal_id := %q`, mine),
		all + ` | groupby([fractal_id, side])`,
		all + ` | regex("(?<fractal_id>.*)", field=suite_marker)`,
		all + ` | strftime("%Y", as=timestamp)`,
		all + ` | count(as=fractal_id)`,
	} {
		res := query(mine, q)
		// A row is the event's own text, so look for the other side anywhere in it.
		for _, row := range res.Results {
			if b, _ := json.Marshal(row); strings.Contains(string(b), "other-"+marker) {
				t.Errorf("%s: read a row from another fractal: %s", q, b)
			}
		}
		if res.Success {
			t.Errorf("%s: ran, want it refused for naming an output after a scope column", q)
		}
	}
}
