//go:build integration

// Rarity scoring end to end: ingest a known pattern of ports per host over 28
// days, seed a rarity model from it, and check the exact scores. A host that
// uses the same three ports every day and then one new one must flag the new
// port with high confidence; a host that touches a new port most days must not.
//
//	go test -tags integration ./test/integration/ -run TestRarityScoring -v

package integration

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestRarityScoring(t *testing.T) {
	c := New(t)

	var fractal struct {
		ID string `json:"id"`
	}
	c.Do(t, "POST", "/fractals", map[string]any{
		"name":        fmt.Sprintf("api-suite-rarity-%d", time.Now().UnixNano()),
		"description": "Created by the API test suite",
	}, &fractal)
	t.Cleanup(func() {
		if code := c.Status(t, "DELETE", "/fractals/"+fractal.ID, nil); code >= 300 {
			t.Logf("could not clean up fractal %s: %d", fractal.ID, code)
		}
	})
	scoped := c.InScope(fractal.ID)

	var token struct {
		Token string `json:"token"`
	}
	scoped.Do(t, "POST", "/fractals/"+fractal.ID+"/ingest-tokens", map[string]any{
		"name": "api-suite", "parser_type": "json",
	}, &token)

	// WS02: ports 80, 443 and 8080 every day for 28 days, then port 22 once.
	// DEV1: port 443 every day, plus a different new port on each of 20 days.
	// Each port shows up many times on its day: rarity counts days, not events.
	const days = 28
	marker := fmt.Sprintf("rarity-%d", time.Now().UnixNano())
	noon := time.Now().UTC().Truncate(24 * time.Hour).Add(12 * time.Hour)
	var logs []map[string]any
	add := func(host, port string, daysAgo, repeats int) {
		for i := 0; i < repeats; i++ {
			logs = append(logs, map[string]any{
				"timestamp":     noon.AddDate(0, 0, -daysAgo).Add(time.Duration(i) * time.Minute).Format(time.RFC3339),
				"computer_name": host, "dst_port": port, "suite_marker": marker,
			})
		}
	}
	for d := 1; d <= days; d++ {
		for _, p := range []string{"80", "443", "8080"} {
			add("WS02", p, d, 3)
		}
		add("DEV1", "443", d, 2)
		if d <= 20 {
			add("DEV1", fmt.Sprintf("4%04d", d), d, 1)
		}
	}
	add("WS02", "22", 1, 5)
	c.WithKey(token.Token).DoRaw(t, "POST", "/ingest", logs, nil)

	Eventually(t, "the ingested logs to become queryable", 120*time.Second, func() bool {
		var rows struct {
			Results []map[string]any `json:"results"`
		}
		c.DoRaw(t, "POST", "/query", map[string]any{
			"query":      fmt.Sprintf("suite_marker=%q | count()", marker),
			"fractal_id": fractal.ID,
			"start":      noon.AddDate(0, 0, -days-1).Format(time.RFC3339),
			"end":        time.Now().UTC().Add(time.Minute).Format(time.RFC3339),
		}, &rows)
		return len(rows.Results) == 1 && fmt.Sprint(rows.Results[0]["_count"]) == fmt.Sprint(len(logs))
	})

	// Backfill only reads logs ingested before the model existed, so create it
	// after the logs are queryable.
	var model struct {
		ID string `json:"id"`
	}
	modelName := "suite_rarity_" + fmt.Sprint(time.Now().UnixNano())
	scoped.Do(t, "POST", "/models", map[string]any{
		"name":       modelName,
		"model_type": "rarity",
		"alert_mode": "none",
		"definition": map[string]any{
			"source_bql":    fmt.Sprintf("suite_marker=%q", marker),
			"partition_key": "computer_name",
			"value_key":     "dst_port",
			"min_sample":    1,
		},
	}, &model)
	t.Cleanup(func() { scoped.Status(t, "DELETE", "/models/"+model.ID, nil) })

	Eventually(t, "the model to become active", 60*time.Second, func() bool {
		var m struct {
			Status string `json:"status"`
		}
		scoped.Do(t, "GET", "/models/"+model.ID, nil, &m)
		return m.Status == "active"
	})
	scoped.Do(t, "POST", "/models/"+model.ID+"/backfill", map[string]any{"window": "30d"}, nil)
	Eventually(t, "the backfill to complete", 180*time.Second, func() bool {
		var m struct {
			BackfillStatus string `json:"backfill_status"`
		}
		scoped.Do(t, "GET", "/models/"+model.ID, nil, &m)
		if m.BackfillStatus == "failed" {
			t.Fatal("backfill failed")
		}
		return m.BackfillStatus == "completed"
	})

	var rows []map[string]any
	scoped.Do(t, "GET", "/models/"+model.ID+"/data?limit=100", nil, &rows)
	byKey := map[string]map[string]any{}
	for _, r := range rows {
		byKey[fmt.Sprint(r["partition_val"], "/", r["value_val"])] = r
	}

	// WS02 has 85 value-days (3 x 28 + 1), one of them a one-day value: coverage
	// 1 - 1/85. Port 22 was seen on 1 of the host's 28 days.
	expectRarity(t, byKey, "WS02/22", 1, days, 100.0/days, 1-1.0/85)
	expectRarity(t, byKey, "WS02/443", days, days, 100, 1-1.0/85)
	// DEV1 has 48 value-days (28 + 20), 20 of them one-day values.
	expectRarity(t, byKey, "DEV1/40005", 1, days, 100.0/days, 1-20.0/48)

	// modelLookup() reads the same scores the data view shows.
	var res struct {
		Results []map[string]any `json:"results"`
	}
	c.DoRaw(t, "POST", "/query", map[string]any{
		"query": fmt.Sprintf("suite_marker=%q dst_port=\"22\" | modelLookup(model=%q, key=[computer_name, dst_port]) | head(1) | table(percent, confidence, model_count, model_total)",
			marker, modelName),
		"fractal_id": fractal.ID,
		"start":      noon.AddDate(0, 0, -days-1).Format(time.RFC3339),
		"end":        time.Now().UTC().Add(time.Minute).Format(time.RFC3339),
	}, &res)
	if len(res.Results) != 1 {
		t.Fatalf("modelLookup returned %d rows for port 22, want 1", len(res.Results))
	}
	expectRarity(t, map[string]map[string]any{"lookup": res.Results[0]}, "lookup", 1, days, 100.0/days, 1-1.0/85)
}

func expectRarity(t *testing.T, rows map[string]map[string]any, key string, seen, total int, percent, confidence float64) {
	t.Helper()
	r, ok := rows[key]
	if !ok {
		t.Fatalf("no row for %s", key)
	}
	num := func(col string) float64 {
		var f float64
		if _, err := fmt.Sscan(fmt.Sprint(r[col]), &f); err != nil {
			t.Fatalf("%s %s = %v, not a number", key, col, r[col])
		}
		return f
	}
	if got := num("model_count"); got != float64(seen) {
		t.Errorf("%s model_count = %v, want %d days seen", key, got, seen)
	}
	if got := num("model_total"); got != float64(total) {
		t.Errorf("%s model_total = %v, want %d days observed", key, got, total)
	}
	if got := num("percent"); math.Abs(got-percent) > 0.001 {
		t.Errorf("%s percent = %v, want %.4f", key, got, percent)
	}
	if got := num("confidence"); math.Abs(got-confidence) > 0.0001 {
		t.Errorf("%s confidence = %v, want %.4f", key, got, confidence)
	}
}
