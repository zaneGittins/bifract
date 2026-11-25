//go:build integration

// Ingest servers cache each token's fractal and normalizer. Editing the
// normalizer, deleting it, or deleting the fractal must take effect on the next
// request, not when the cache entry expires.
//
//	go test -tags integration ./test/integration/ -run TestIngestTokenFollowsChanges -v

package integration

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestIngestTokenFollowsChanges(t *testing.T) {
	t.Parallel()
	c := New(t)

	fractalID, scoped := newFractal(t, c, "token-cache")

	var normalizer struct {
		ID string `json:"id"`
	}
	normName := fmt.Sprintf("api-suite-token-cache-%d", time.Now().UnixNano())
	mapping := func(target string) map[string]any {
		return map[string]any{
			"name":           normName,
			"field_mappings": []map[string]any{{"sources": []string{"src"}, "target": target}},
		}
	}
	c.Do(t, "POST", "/normalizers", mapping("first_name"), &normalizer)
	t.Cleanup(func() { c.Status(t, "DELETE", "/normalizers/"+normalizer.ID, nil) })

	var token struct {
		Token string `json:"token"`
	}
	scoped.Do(t, "POST", "/fractals/"+fractalID+"/ingest-tokens", map[string]any{
		"name": "api-suite", "parser_type": "json", "normalizer_id": normalizer.ID,
	}, &token)
	shipper := c.WithKey(token.Token)

	// Each send is queryable once ingested, under the field the normalizer gave it.
	sendAndFind := func(field string) {
		t.Helper()
		marker := fmt.Sprintf("m%d", time.Now().UnixNano())
		shipper.DoRaw(t, "POST", "/ingest", []map[string]any{{"src": marker}}, nil)
		Eventually(t, field+" to hold "+marker, 90*time.Second, func() bool {
			var res struct {
				Success bool `json:"success"`
				Count   int  `json:"count"`
			}
			c.DoRaw(t, "POST", "/query", map[string]any{
				"query":      fmt.Sprintf("%s=%q", field, marker),
				"fractal_id": fractalID,
				"start":      time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339),
				"end":        time.Now().Add(1 * time.Minute).UTC().Format(time.RFC3339),
			}, &res)
			return res.Success && res.Count == 1
		})
	}

	sendAndFind("first_name")

	c.Do(t, "PUT", "/normalizers/"+normalizer.ID, mapping("second_name"), nil)
	sendAndFind("second_name")

	c.Do(t, "DELETE", "/normalizers/"+normalizer.ID, nil, nil)
	sendAndFind("src")

	c.Do(t, "DELETE", "/fractals/"+fractalID, nil, nil)
	if code := shipper.Status(t, "POST", "/ingest", []map[string]any{{"src": "late"}}); code != http.StatusUnauthorized {
		t.Fatalf("ingest with a deleted fractal's token: got %d, want %d", code, http.StatusUnauthorized)
	}
}
