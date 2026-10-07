//go:build integration

// First-seen end to end: "new" means new to the model, not a recent event time.
// History seeded by a backfill is never new, and an entity the live model records
// for the first time is new even when its log carries an event time days old, as
// a late or replayed log does. The state holds one row per entity, not per log.
//
//	go test -tags integration ./test/integration/ -run TestFirstSeenNewToModel -v

package integration

import (
	"fmt"
	"net/url"
	"testing"
	"time"
)

func TestFirstSeenNewToModel(t *testing.T) {
	c := New(t)

	var fractal struct {
		ID string `json:"id"`
	}
	c.Do(t, "POST", "/fractals", map[string]any{
		"name":        fmt.Sprintf("api-suite-first-seen-%d", time.Now().UnixNano()),
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
	ingest := c.WithKey(token.Token)

	marker := fmt.Sprintf("first-seen-%d", time.Now().UnixNano())
	userA, userB := marker+"-a", marker+"-b"
	noon := time.Now().UTC().Truncate(24 * time.Hour).Add(12 * time.Hour)
	// A: four logs across two days, all at least two days old.
	aTimes := []time.Time{
		noon.AddDate(0, 0, -3),
		noon.AddDate(0, 0, -3).Add(time.Minute),
		noon.AddDate(0, 0, -3).Add(2 * time.Minute),
		noon.AddDate(0, 0, -2),
	}
	var logs []map[string]any
	for _, ts := range aTimes {
		logs = append(logs, map[string]any{
			"timestamp": ts.Format(time.RFC3339), "suite_user": userA, "suite_marker": marker,
		})
	}
	ingest.DoRaw(t, "POST", "/ingest", logs, nil)

	start := noon.AddDate(0, 0, -5).Format(time.RFC3339)
	query := func(q string) []map[string]any {
		var res struct {
			Results []map[string]any `json:"results"`
		}
		c.DoRaw(t, "POST", "/query", map[string]any{
			"query":      q,
			"fractal_id": fractal.ID,
			"start":      start,
			"end":        time.Now().UTC().Add(time.Minute).Format(time.RFC3339),
		}, &res)
		return res.Results
	}
	Eventually(t, "the ingested logs to become queryable", 120*time.Second, func() bool {
		rows := query(fmt.Sprintf("suite_marker=%q | count()", marker))
		return len(rows) == 1 && fmt.Sprint(rows[0]["_count"]) == fmt.Sprint(len(aTimes))
	})

	// Backfill only reads logs ingested before the model existed, so create it
	// after the logs are queryable.
	var model struct {
		ID string `json:"id"`
	}
	modelName := "suite_first_seen_" + fmt.Sprint(time.Now().UnixNano())
	scoped.Do(t, "POST", "/models", map[string]any{
		"name":       modelName,
		"model_type": "first_seen",
		"alert_mode": "none",
		"definition": map[string]any{
			"source_bql": fmt.Sprintf("suite_marker=%q", marker),
			"key_fields": []string{"suite_user"},
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
	scoped.Do(t, "POST", "/models/"+model.ID+"/backfill", map[string]any{"window": "7d"}, nil)
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

	// The data view holds one row per entity, aggregated over every log.
	dataRows := func(search string) []map[string]any {
		var rows []map[string]any
		scoped.Do(t, "GET", "/models/"+model.ID+"/data?limit=100&search="+url.QueryEscape(search), nil, &rows)
		return rows
	}
	rows := dataRows(marker)
	if len(rows) != 1 {
		t.Fatalf("data view returned %d rows after backfill, want 1 (entity A): %v", len(rows), rows)
	}
	expectEntity(t, rows[0], userA, aTimes[0], aTimes[len(aTimes)-1], len(aTimes))
	if rec := parseTime(t, rows[0], "recorded_at"); rec.Year() != 1970 {
		t.Errorf("backfilled entity recorded_at = %v, want the epoch (history, not new)", rec)
	}

	// Seeded history is not new, however recently it was seeded: nothing is a finding.
	if found, total := modelFindings(t, scoped, model.ID, ""); total != 0 || len(found) != 0 {
		t.Errorf("findings after a backfill = %d rows (total %d), want none", len(found), total)
	}

	lookup := func(user string) []map[string]any {
		return query(fmt.Sprintf("suite_marker=%q suite_user=%q | modelLookup(model=%q, key=[suite_user]) | table(suite_user, first_seen, last_seen, event_count, is_new)",
			marker, user, modelName))
	}
	aRows := lookup(userA)
	if len(aRows) != len(aTimes) {
		t.Fatalf("modelLookup returned %d rows for A, want %d", len(aRows), len(aTimes))
	}
	for _, r := range aRows {
		if fmt.Sprint(r["is_new"]) != "0" {
			t.Errorf("backfilled entity A is_new = %v, want 0", r["is_new"])
		}
		if fmt.Sprint(r["event_count"]) != fmt.Sprint(len(aTimes)) {
			t.Errorf("A event_count = %v, want %d", r["event_count"], len(aTimes))
		}
	}

	// B arrives now with an event time three days old: a late or replayed log. It
	// is new to the model, so it must read as new.
	bTime := noon.AddDate(0, 0, -3).Add(30 * time.Minute)
	ingest.DoRaw(t, "POST", "/ingest", []map[string]any{{
		"timestamp": bTime.Format(time.RFC3339), "suite_user": userB, "suite_marker": marker,
	}}, nil)

	// A state cycle runs every minute over logs ingested at least 30s ago.
	Eventually(t, "a state cycle to record entity B", 240*time.Second, func() bool {
		return len(dataRows(userB)) == 1
	})
	bData := dataRows(userB)[0]
	expectEntity(t, bData, userB, bTime, bTime, 1)
	if rec := parseTime(t, bData, "recorded_at"); time.Since(rec) > 10*time.Minute || rec.Year() == 1970 {
		t.Errorf("live entity recorded_at = %v, want when the cycle ran", rec)
	}

	bRows := lookup(userB)
	if len(bRows) != 1 {
		t.Fatalf("modelLookup returned %d rows for B, want 1", len(bRows))
	}
	if got := fmt.Sprint(bRows[0]["is_new"]); got != "1" {
		t.Errorf("B is_new = %v, want 1: first recorded now, even though its event time is %v", got, bTime)
	}
	if got, want := fmt.Sprint(bRows[0]["first_seen"]), bTime.Format("2006-01-02 15:04:05.000"); got != want {
		t.Errorf("B first_seen = %v, want the event time %v", got, want)
	}

	// A is unaffected by B and still reads as known.
	for _, r := range lookup(userA) {
		if fmt.Sprint(r["is_new"]) != "0" {
			t.Errorf("A is_new = %v after B arrived, want 0", r["is_new"])
		}
	}
	if all := dataRows(marker); len(all) != 2 {
		t.Errorf("data view returned %d rows, want one per entity (2)", len(all))
	}

	// B, first recorded now, is the one finding; A stays history.
	if found, total := modelFindings(t, scoped, model.ID, ""); total != 1 || len(found) != 1 || fmt.Sprint(found[0]["entity_key"]) != userB {
		t.Errorf("findings = %v (total %d), want only %s", found, total, userB)
	}
	var stats struct {
		Findings int `json:"findings"`
		NewHour  int `json:"new_hour"`
	}
	scoped.Do(t, "GET", "/models/"+model.ID+"/stats", nil, &stats)
	if stats.Findings != 1 || stats.NewHour != 1 {
		t.Errorf("stats findings = %d, new_hour = %d, want 1 and 1", stats.Findings, stats.NewHour)
	}
}

func expectEntity(t *testing.T, row map[string]any, key string, first, last time.Time, count int) {
	t.Helper()
	if fmt.Sprint(row["entity_key"]) != key {
		t.Fatalf("entity_key = %v, want %s", row["entity_key"], key)
	}
	if got := parseTime(t, row, "first_seen"); !got.Equal(first) {
		t.Errorf("%s first_seen = %v, want %v", key, got, first)
	}
	if got := parseTime(t, row, "last_seen"); !got.Equal(last) {
		t.Errorf("%s last_seen = %v, want %v", key, got, last)
	}
	if got := fmt.Sprint(row["event_count"]); got != fmt.Sprint(count) {
		t.Errorf("%s event_count = %v, want %d", key, got, count)
	}
}

func parseTime(t *testing.T, row map[string]any, col string) time.Time {
	t.Helper()
	// The data view returns ClickHouse's UTC layout; modelLookup() may return RFC 3339.
	raw := fmt.Sprint(row[col])
	for _, layout := range []string{"2006-01-02 15:04:05.000", time.RFC3339Nano} {
		if ts, err := time.Parse(layout, raw); err == nil {
			return ts.UTC()
		}
	}
	t.Fatalf("%s = %v: not a timestamp", col, row[col])
	return time.Time{}
}
