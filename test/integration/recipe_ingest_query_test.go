//go:build integration

// The first recipe: stand up a place for logs, get a credential that can write
// to it, send some, and read them back. Everything else in the API assumes this
// works, so it is the one to run first against a new instance.
//
//	go test -tags integration ./test/integration/ -run TestIngestAndQuery -v

package integration

import (
	"fmt"
	"testing"
	"time"
)

func TestIngestAndQuery(t *testing.T) {
	c := New(t)

	// 1. A fractal is the container logs land in. Creating one needs tenant
	//    admin; everything after this works with a key scoped to just it.
	var fractal struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	name := fmt.Sprintf("api-suite-%d", time.Now().UnixNano())
	c.Do(t, "POST", "/fractals", map[string]any{
		"name":        name,
		"description": "Created by the API test suite",
	}, &fractal)

	if fractal.ID == "" {
		t.Fatal("the created fractal came back without an id")
	}
	t.Cleanup(func() {
		if code := c.Status(t, "DELETE", "/fractals/"+fractal.ID, nil); code >= 300 {
			t.Logf("could not clean up fractal %s: %d", fractal.ID, code)
		}
	})

	scoped := c.InScope(fractal.ID)

	// 2. Log shippers authenticate with an ingest token, not an API key. The
	//    token is scoped to this one fractal and cannot read anything back.
	var created struct {
		Token string `json:"token"`
	}
	scoped.Do(t, "POST", "/fractals/"+fractal.ID+"/ingest-tokens", map[string]any{
		"name":        "api-suite",
		"description": "Created by the API test suite",
		"parser_type": "json",
	}, &created)

	if created.Token == "" {
		t.Fatal("the created ingest token came back without a secret")
	}

	// 3. Send logs as that token. A marker in the payload is what the query
	//    below looks for, so the recipe never depends on other data.
	marker := fmt.Sprintf("api-suite-%d", time.Now().UnixNano())
	shipper := c.WithKey(created.Token)
	shipper.DoRaw(t, "POST", "/ingest", []map[string]any{
		{"message": "first line", "level": "info", "suite_marker": marker},
		{"message": "second line", "level": "error", "suite_marker": marker},
	}, nil)

	// 4. Read them back. A scoped key is bound to the fractal it was issued for,
	//    so a key acting across fractals names the one it means in the request
	//    body.
	//
	//    Ingestion is asynchronous and batched, so this is eventual: a query
	//    issued straight after the write legitimately returns nothing.
	var found int
	Eventually(t, "the ingested logs to become queryable", 90*time.Second, func() bool {
		var res struct {
			Success bool `json:"success"`
			Count   int  `json:"count"`
		}
		c.DoRaw(t, "POST", "/query", map[string]any{
			"query":      fmt.Sprintf("suite_marker=%q", marker),
			"fractal_id": fractal.ID,
			"start":      time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339),
			"end":        time.Now().Add(1 * time.Minute).UTC().Format(time.RFC3339),
		}, &res)
		found = res.Count
		return res.Success && res.Count >= 2
	})

	if found < 2 {
		t.Fatalf("sent 2 logs, read back %d", found)
	}
}
