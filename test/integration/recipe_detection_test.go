//go:build integration

// The detection lifecycle: write a rule, confirm the server accepts the query
// before it is ever scheduled, enable it, and read back what it did. This is
// the path a detection-as-code pipeline follows.
//
//	go test -tags integration ./test/integration/ -run TestDetectionLifecycle -v

package integration

import (
	"fmt"
	"testing"
	"time"
)

func TestDetectionLifecycle(t *testing.T) {
	c := New(t)
	fractalID := requireFractal(t, c)
	scoped := c.InScope(fractalID)

	// 1. Check the query parses before committing to a rule. This costs nothing
	//    and is what a CI gate should run on every proposed detection.
	var check struct {
		Valid bool   `json:"valid"`
		Error string `json:"error"`
	}
	scoped.DoRaw(t, "POST", "/query/validate", map[string]any{
		"query": `level="error"`,
	}, &check)
	if !check.Valid {
		t.Fatalf("a query the suite believes is valid was rejected: %s", check.Error)
	}

	// A rule that cannot translate must be refused here rather than at midnight.
	scoped.DoRaw(t, "POST", "/query/validate", map[string]any{
		"query": `| bogus_command(`,
	}, &check)
	if check.Valid {
		t.Fatal("a malformed query passed validation")
	}

	// 2. Create the rule. An "event" alert matches individual logs as they are
	//    ingested, so its query filters rather than aggregates; a rule that
	//    counts or groups is a "compound" alert and is refused here. It starts
	//    disabled so a mistake cannot page anyone.
	name := fmt.Sprintf("api-suite-%d", time.Now().UnixNano())
	rule := map[string]any{
		"name":                  name,
		"description":           "Created by the API test suite",
		"query_string":          `level="error"`,
		"alert_type":            "event",
		"severity":              "low",
		"enabled":               false,
		"labels":                []string{"suite:api"},
		"references":            []string{},
		"webhook_action_ids":    []string{},
		"fractal_action_ids":    []string{},
		"dictionary_action_ids": []string{},
		"email_action_ids":      []string{},
		"throttle_time_seconds": 0,
		"throttle_field":        "",
	}

	var alert struct {
		ID      string `json:"id"`
		Enabled bool   `json:"enabled"`
	}
	scoped.Do(t, "POST", "/alerts", rule, &alert)

	if alert.ID == "" {
		t.Fatal("the created alert came back without an id")
	}
	t.Cleanup(func() {
		if code := scoped.Status(t, "DELETE", "/alerts/"+alert.ID, nil); code >= 300 {
			t.Logf("could not clean up alert %s: %d", alert.ID, code)
		}
	})
	if alert.Enabled {
		t.Error("the alert was created enabled despite the request asking for disabled")
	}

	// A type no engine evaluates must be refused, not stored as a rule that can
	// never fire.
	bogus := map[string]any{}
	for k, v := range rule {
		bogus[k] = v
	}
	bogus["name"] = name + "-bogus"
	bogus["alert_type"] = "threshold"
	if status, _ := scoped.Failure(t, "POST", "/alerts", bogus); status != 400 {
		t.Errorf("an unknown alert_type was accepted with %d", status)
	}

	// 3. Enable it once it looks right. PUT replaces the rule rather than
	//    patching it, so send the whole thing back with the one field changed.
	rule["enabled"] = true
	var updated struct {
		Enabled bool `json:"enabled"`
	}
	scoped.Do(t, "PUT", "/alerts/"+alert.ID, rule, &updated)
	if !updated.Enabled {
		t.Fatal("enabling the alert did not take")
	}

	// 4. Its execution history is readable immediately, empty until the engine
	//    has run it. A pipeline polls this to see whether a rule is firing.
	var executions []map[string]any
	scoped.Do(t, "GET", "/alerts/"+alert.ID+"/executions", nil, &executions)
}

// requireFractal returns the fractal to work in, preferring the configured one
// and otherwise the first the credential can reach.
func requireFractal(t *testing.T, c *Client) string {
	t.Helper()

	if id := fractalFromEnv(); id != "" {
		return id
	}
	var listed struct {
		Fractals []struct {
			ID string `json:"id"`
		} `json:"fractals"`
	}
	c.Do(t, "GET", "/fractals", nil, &listed)
	if len(listed.Fractals) == 0 {
		t.Skip("the credential can reach no fractal to work in")
	}
	return listed.Fractals[0].ID
}
