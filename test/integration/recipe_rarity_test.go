//go:build integration

// Rarity scoring end to end: ingest a known pattern of ports per host over 28
// days, seed a rarity model from it, and check the exact scores. A host that
// uses the same three ports every day and then one new one must flag the new
// port with high coverage; a host that touches a new port most days must not.
//
//	go test -tags integration ./test/integration/ -run TestRarityScoring -v

package integration

import (
	"fmt"
	"math"
	"strings"
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
	// WS03: the same three ports every day, plus port 3389 on two days.
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
			add("WS03", p, d, 2)
		}
		add("DEV1", "443", d, 2)
		if d <= 20 {
			add("DEV1", fmt.Sprintf("4%04d", d), d, 1)
		}
	}
	add("WS02", "22", 1, 5)
	add("WS03", "3389", 2, 1)
	add("WS03", "3389", 9, 1)
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
			// Thresholds are scored at read time; the Findings view applies them.
			"alert": map[string]any{"coverage_threshold": 0.9, "percent_threshold": 10},
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
		"query": fmt.Sprintf("suite_marker=%q dst_port=\"22\" | modelLookup(model=%q, key=[computer_name, dst_port]) | head(1) | table(percent, coverage, model_count, model_total)",
			marker, modelName),
		"fractal_id": fractal.ID,
		"start":      noon.AddDate(0, 0, -days-1).Format(time.RFC3339),
		"end":        time.Now().UTC().Add(time.Minute).Format(time.RFC3339),
	}, &res)
	if len(res.Results) != 1 {
		t.Fatalf("modelLookup returned %d rows for port 22, want 1", len(res.Results))
	}
	expectRarity(t, map[string]map[string]any{"lookup": res.Results[0]}, "lookup", 1, days, 100.0/days, 1-1.0/85)

	// Findings are exactly what the alert (coverage > 0.9, share < 10%) would
	// raise, rarest first: WS02/22 on 1 of 28 days, then WS03/3389 on 2 of 28
	// (WS03 has no one-day value, so its coverage is 1). DEV1's new ports are
	// as rare but routine for DEV1 (coverage 0.58), so none of them alert.
	expectRarity(t, byKey, "WS03/3389", 2, days, 200.0/days, 1)
	findings, total := modelFindings(t, scoped, model.ID, "")
	if got := rarityKeys(findings); total != 2 || got != "WS02/22,WS03/3389" {
		t.Errorf("findings = %s (total %d), want WS02/22,WS03/3389", got, total)
	}
	// Paging walks the same ordered set.
	if page, total := modelFindings(t, scoped, model.ID, "&limit=1&offset=1"); total != 2 || rarityKeys(page) != "WS03/3389" {
		t.Errorf("second findings page = %s (total %d), want WS03/3389", rarityKeys(page), total)
	}

	// The summary counts the same rule, and each pair's first day from its stored
	// day set: WS02/22 and DEV1's ports from the last 6 days are new this week.
	var stats struct {
		Findings     int    `json:"findings"`
		FindingsRule string `json:"findings_rule"`
		NewWeek      int    `json:"new_week"`
		Series       []struct {
			Day   string `json:"day"`
			Count int    `json:"count"`
		} `json:"series"`
	}
	scoped.Do(t, "GET", "/models/"+model.ID+"/stats", nil, &stats)
	if stats.Findings != 2 || stats.FindingsRule == "" {
		t.Errorf("stats findings = %d (rule %q), want 2 under a rule", stats.Findings, stats.FindingsRule)
	}
	if len(stats.Series) != 30 {
		t.Errorf("series has %d days, want 30", len(stats.Series))
	}
	if time.Now().UTC().Truncate(24*time.Hour) == noon.Truncate(24*time.Hour) && stats.NewWeek != 7 {
		t.Errorf("new_week = %d, want 7 (WS02/22 and DEV1's ports from days 1-6)", stats.NewWeek)
	}

	if code := scoped.Status(t, "GET", "/models/"+model.ID+"/data?view=bogus", nil); code != 400 {
		t.Errorf("an unknown view answered %d, want 400", code)
	}

	// Min history is per group: WS02 and WS03 each have 28 days, so a 29-day
	// requirement holds both back and a 28-day one lets both alert. It is
	// applied at read time, so the edit keeps the model's data.
	setMinHistory := func(days int) {
		scoped.Do(t, "PUT", "/models/"+model.ID, map[string]any{
			"name": modelName, "alert_mode": "none",
			"definition": map[string]any{
				"source_bql":    fmt.Sprintf("suite_marker=%q", marker),
				"partition_key": "computer_name",
				"value_key":     "dst_port",
				"min_sample":    days,
				"alert":         map[string]any{"coverage_threshold": 0.9, "percent_threshold": 10},
			},
		}, nil)
	}
	setMinHistory(days + 1)
	if findings, total := modelFindings(t, scoped, model.ID, ""); total != 0 {
		t.Errorf("min history %d: findings = %s, want none while every group is learning", days+1, rarityKeys(findings))
	}
	setMinHistory(days)
	if findings, total := modelFindings(t, scoped, model.ID, ""); total != 2 || rarityKeys(findings) != "WS02/22,WS03/3389" {
		t.Errorf("min history %d: findings = %s (total %d), want WS02/22,WS03/3389", days, rarityKeys(findings), total)
	}
}

// modelFindings reads a model's Findings view: the rows its alert would raise,
// in the order the view ranks them, and the view's total.
func modelFindings(t *testing.T, c *Client, modelID, query string) ([]map[string]any, int) {
	t.Helper()
	var page struct {
		Data []map[string]any `json:"data"`
		Page struct {
			Total int `json:"total"`
		} `json:"page"`
	}
	c.DoRaw(t, "GET", "/models/"+modelID+"/data?view=findings"+query, nil, &page)
	return page.Data, page.Page.Total
}

func rarityKeys(rows []map[string]any) string {
	keys := make([]string, len(rows))
	for i, r := range rows {
		keys[i] = fmt.Sprint(r["partition_val"], "/", r["value_val"])
	}
	return strings.Join(keys, ",")
}

func expectRarity(t *testing.T, rows map[string]map[string]any, key string, seen, total int, percent, coverage float64) {
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
	if got := num("coverage"); math.Abs(got-coverage) > 0.0001 {
		t.Errorf("%s coverage = %v, want %.4f", key, got, coverage)
	}
}
