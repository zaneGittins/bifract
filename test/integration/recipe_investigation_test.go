//go:build integration

// Recording an investigation. Findings live next to the logs that prompted
// them, tagged so a case can be pulled back together later, which is what makes
// the work reviewable by someone who was not there.
//
//	go test -tags integration ./test/integration/ -run TestInvestigation -v

package integration

import (
	"fmt"
	"testing"
	"time"
)

func TestInvestigation(t *testing.T) {
	c := New(t)
	fractalID := requireFractal(t, c)
	scoped := c.InScope(fractalID)

	// 1. Find something worth recording. Any query will do; an investigation
	//    usually starts from an alert or a hunch.
	var results struct {
		Success bool             `json:"success"`
		Count   int              `json:"count"`
		Results []map[string]any `json:"results"`
	}
	c.DoRaw(t, "POST", "/query", map[string]any{
		"query":      "* | head(1)",
		"fractal_id": fractalID,
		"start":      time.Now().Add(-24 * time.Hour).UTC().Format(time.RFC3339),
		"end":        time.Now().Add(time.Minute).UTC().Format(time.RFC3339),
	}, &results)

	if results.Count == 0 {
		t.Skip("the fractal holds no logs in the last day to investigate")
	}

	logID, _ := results.Results[0]["log_id"].(string)
	if logID == "" {
		t.Skip("the query returned no log_id to attach a finding to")
	}
	// A query answers timestamps in ClickHouse's format, while comments want
	//    RFC3339, so a caller bridges the two.
	timestamp := rfc3339(t, results.Results[0]["timestamp"])

	// 2. Record the finding against that log. The tag is what ties scattered
	//    notes into one case, so it is worth agreeing a convention: IR-<name>.
	caseTag := fmt.Sprintf("IR-api-suite-%d", time.Now().UnixNano())
	var comment struct {
		ID string `json:"id"`
	}
	scoped.Do(t, "POST", "/comments", map[string]any{
		"log_id":        logID,
		"log_timestamp": timestamp,
		"fractal_id":    fractalID,
		"text":          "First observation, recorded by the API test suite.",
		"tags":          []string{caseTag},
	}, &comment)

	if comment.ID == "" {
		t.Fatal("the created comment came back without an id")
	}
	t.Cleanup(func() {
		scoped.Status(t, "DELETE", "/comments/"+comment.ID, nil)
	})

	// 3. Tags can also be applied in bulk, which is how an analyst folds earlier
	//    notes into a case once they realise the two are related.
	scoped.Do(t, "POST", "/comments/bulk-add-tag", map[string]any{
		"comment_ids": []string{comment.ID},
		"tag":         caseTag + "-confirmed",
	}, nil)

	// 4. The case is now findable by tag.
	var tags []string
	scoped.Do(t, "GET", "/comments/tags", nil, &tags)

	var found bool
	for _, tag := range tags {
		if tag == caseTag {
			found = true
		}
	}
	if !found {
		t.Errorf("the case tag %q is not listed among the scope's tags", caseTag)
	}

	// 5. And the log it was recorded against is listed as commented, which is
	//    what the review view reads.
	var commented struct {
		Data []map[string]any `json:"data"`
	}
	scoped.DoRaw(t, "GET", "/logs/commented?limit=50", nil, &commented)
	if len(commented.Data) == 0 {
		t.Error("a comment was created but no log is reported as commented")
	}
}

// rfc3339 converts a timestamp as a query returns it into the form the rest of
// the API expects. Query results carry ClickHouse's "2006-01-02 15:04:05.000",
// which is not RFC3339 and is rejected where a timestamp is an input.
func rfc3339(t *testing.T, value any) string {
	t.Helper()

	raw, _ := value.(string)
	if raw == "" {
		return ""
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC().Format(time.RFC3339Nano)
		}
	}
	t.Fatalf("a query returned a timestamp in an unrecognised format: %q", raw)
	return ""
}
