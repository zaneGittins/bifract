//go:build integration

// Volume baseline scoring end to end: ingest known per-day counts for a few
// entities over 28 days, seed a daily volume_baseline model from them, and check
// the exact scores of yesterday (the latest complete day). A perfectly steady
// entity that spikes must flag rather than score 0 on MAD = 0, and an entity
// active only every few days has its empty days counted as zeros.
//
//	go test -tags integration ./test/integration/ -run TestVolumeBaselineScoring -v

package integration

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

func TestVolumeBaselineScoring(t *testing.T) {
	c := New(t)

	var fractal struct {
		ID string `json:"id"`
	}
	c.Do(t, "POST", "/fractals", map[string]any{
		"name":        fmt.Sprintf("api-suite-volume-%d", time.Now().UnixNano()),
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

	// Day 1 is yesterday (UTC), the latest complete bucket the model scores;
	// nothing is ingested for today, the incomplete bucket.
	//   STEADY: 4 a day on days 2..28, 40 yesterday.
	//   NEAR:   as STEADY, but 6 on day 10 (MAD is 0, mean deviation is not).
	//   SPARSE: 6 on every 4th day (4, 8, ..., 28), nothing since.
	//   BURSTY: as SPARSE, plus 6 yesterday.
	const days = 28
	marker := fmt.Sprintf("volume-%d", time.Now().UnixNano())
	today := time.Now().UTC().Truncate(24 * time.Hour)
	noon := today.Add(12 * time.Hour)
	yesterday := today.AddDate(0, 0, -1).Format("2006-01-02")
	var logs []map[string]any
	add := func(user string, daysAgo, n int) {
		for i := 0; i < n; i++ {
			logs = append(logs, map[string]any{
				"timestamp":    noon.AddDate(0, 0, -daysAgo).Add(time.Duration(i) * time.Minute).Format(time.RFC3339),
				"user":         user,
				"suite_marker": marker,
			})
		}
	}
	for d := 1; d <= days; d++ {
		steady := 4
		if d == 1 {
			steady = 40
		}
		add("STEADY", d, steady)
		near := steady
		if d == 10 {
			near = 6
		}
		add("NEAR", d, near)
		if d%4 == 0 {
			add("SPARSE", d, 6)
			add("BURSTY", d, 6)
		}
	}
	add("BURSTY", 1, 6)
	c.WithKey(token.Token).DoRaw(t, "POST", "/ingest", logs, nil)

	window := map[string]any{
		"fractal_id": fractal.ID,
		"start":      noon.AddDate(0, 0, -days-1).Format(time.RFC3339),
		"end":        time.Now().UTC().Add(time.Minute).Format(time.RFC3339),
	}
	query := func(q string) []map[string]any {
		var res struct {
			Results []map[string]any `json:"results"`
		}
		body := map[string]any{"query": q}
		for k, v := range window {
			body[k] = v
		}
		c.DoRaw(t, "POST", "/query", body, &res)
		return res.Results
	}

	Eventually(t, "the ingested logs to become queryable", 120*time.Second, func() bool {
		rows := query(fmt.Sprintf("suite_marker=%q | count()", marker))
		return len(rows) == 1 && fmt.Sprint(rows[0]["_count"]) == fmt.Sprint(len(logs))
	})

	// Backfill only reads logs ingested before the model existed, so create it
	// after the logs are queryable.
	var model struct {
		ID string `json:"id"`
	}
	modelName := "suite_volume_" + fmt.Sprint(time.Now().UnixNano())
	scoped.Do(t, "POST", "/models", map[string]any{
		"name":       modelName,
		"model_type": "volume_baseline",
		"alert_mode": "none",
		"definition": map[string]any{
			"source_bql":  fmt.Sprintf("suite_marker=%q", marker),
			"key_fields":  []string{"user"},
			"time_bucket": "day",
			"min_sample":  7,
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

	// The scored bucket follows the server clock, so the expectations only hold
	// while it is still the day the data was laid out against.
	if time.Now().UTC().Truncate(24*time.Hour) != today {
		t.Skip("the UTC day rolled over during the test; yesterday is no longer the scored bucket")
	}

	var rows []map[string]any
	scoped.Do(t, "GET", "/models/"+model.ID+"/data?limit=100", nil, &rows)
	byEntity := map[string]map[string]any{}
	for _, r := range rows {
		byEntity[fmt.Sprint(r["entity_val"])] = r
	}

	// History is days 28..2: 27 buckets, the scored day excluded.
	want := map[string]volumeWant{
		// Flat history, any change: the finite sentinel, not 0.
		"STEADY": {latest: 40, median: 4, n: 27, z: 1000000},
		// MAD 0, mean absolute deviation 2/27: 36 / (1.253314 * 2/27).
		"NEAR": {latest: 40, median: 4, n: 27, z: 387.7719},
		// 20 of 27 history days are empty, so the median is 0, and yesterday was empty too.
		"SPARSE": {latest: 0, median: 0, n: 27, z: 0},
		// Median 0, mean absolute deviation 42/27: 6 / (1.253314 * 42/27).
		"BURSTY": {latest: 6, median: 0, n: 27, z: 3.0776},
	}
	for entity, w := range want {
		expectVolume(t, "data "+entity, byEntity[entity], yesterday, w)
	}

	// modelLookup(), which the alert runs, reads the same scores. SPARSE's newest
	// log is 4 days old, yet it carries yesterday's empty bucket, not that day.
	for entity, w := range want {
		res := query(fmt.Sprintf("suite_marker=%q user=%q | modelLookup(model=%q, key=[user]) | head(1) | table(z_score, latest_count, baseline_median, mad, n_buckets, latest_bucket)",
			marker, entity, modelName))
		if len(res) != 1 {
			t.Fatalf("modelLookup returned %d rows for %s, want 1", len(res), entity)
		}
		expectVolume(t, "lookup "+entity, res[0], yesterday, w)
	}

	// Findings are what the alert (z_score > 3.5, the default) would raise, the
	// largest z first: STEADY's flat-history sentinel, then NEAR. BURSTY's 3.08
	// stays under the threshold and SPARSE scores 0.
	findings, total := modelFindings(t, scoped, model.ID, "")
	got := make([]string, len(findings))
	for i, r := range findings {
		got[i] = fmt.Sprint(r["entity_val"])
	}
	if total != 2 || strings.Join(got, ",") != "STEADY,NEAR" {
		t.Errorf("findings = %v (total %d), want STEADY,NEAR", got, total)
	}
	var stats struct {
		Findings int `json:"findings"`
	}
	scoped.Do(t, "GET", "/models/"+model.ID+"/stats", nil, &stats)
	if stats.Findings != 2 {
		t.Errorf("stats findings = %d, want 2", stats.Findings)
	}
}

type volumeWant struct {
	latest, median float64
	n              int
	z              float64
}

func expectVolume(t *testing.T, label string, r map[string]any, yesterday string, w volumeWant) {
	t.Helper()
	if r == nil {
		t.Fatalf("%s: no row", label)
	}
	num := func(col string) float64 {
		var f float64
		if _, err := fmt.Sscan(fmt.Sprint(r[col]), &f); err != nil {
			t.Fatalf("%s %s = %v, not a number", label, col, r[col])
		}
		return f
	}
	if got := fmt.Sprint(r["latest_bucket"]); !strings.HasPrefix(got, yesterday) {
		t.Errorf("%s latest_bucket = %s, want the latest complete day %s", label, got, yesterday)
	}
	if got := num("latest_count"); got != w.latest {
		t.Errorf("%s latest_count = %v, want %v", label, got, w.latest)
	}
	if got := num("baseline_median"); got != w.median {
		t.Errorf("%s baseline_median = %v, want %v", label, got, w.median)
	}
	if got := num("mad"); got != 0 {
		t.Errorf("%s mad = %v, want 0", label, got)
	}
	if got := num("n_buckets"); got != float64(w.n) {
		t.Errorf("%s n_buckets = %v, want %d (empty days included)", label, got, w.n)
	}
	if got := num("z_score"); math.Abs(got-w.z) > 0.0001 {
		t.Errorf("%s z_score = %v, want %v", label, got, w.z)
	}
}
