//go:build integration

package integration

import (
	"fmt"
	"testing"
	"time"
)

// TestHTMLCharactersSearchable: '<', '>' and '&' inside arrays and nested objects
// are stored as sent, so free text and field searches for them match.
func TestHTMLCharactersSearchable(t *testing.T) {
	t.Parallel()
	c := New(t)
	fractalID, scoped := newFractal(t, c, "html-chars")
	shipper := newShipper(t, scoped, fractalID)

	marker := fmt.Sprintf("api-suite-%d", time.Now().UnixNano())
	shipper.DoRaw(t, "POST", "/ingest", []map[string]any{
		{"suite_marker": marker, "args": []string{"cat a>b && c<d"}},
	}, nil)

	for _, q := range []string{
		`"cat a>b && c<d"`,
		`args=/a>b && c<d/`,
	} {
		query := fmt.Sprintf("suite_marker=%q %s", marker, q)
		Eventually(t, "a search for "+q+" to match", 90*time.Second, func() bool {
			var res struct {
				Success bool `json:"success"`
				Count   int  `json:"count"`
			}
			c.DoRaw(t, "POST", "/query", map[string]any{
				"query":      query,
				"fractal_id": fractalID,
				"start":      time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339),
				"end":        time.Now().Add(1 * time.Minute).UTC().Format(time.RFC3339),
			}, &res)
			return res.Success && res.Count == 1
		})
	}
}
