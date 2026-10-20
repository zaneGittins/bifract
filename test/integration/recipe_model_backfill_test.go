//go:build integration

// Model state across an edit: a model created before its logs arrive is filled
// by live state, then edited, which drops and recreates its table. The backfill
// that follows must recover every log ingested before the edit, without
// counting any twice. It used to recover nothing, because it was bounded by the
// model's creation time while live state resumed past those logs.
//
//	go test -tags integration ./test/integration/ -run TestModelBackfillAfterEdit -v

package integration

import (
	"fmt"
	"testing"
	"time"
)

func TestModelBackfillAfterEdit(t *testing.T) {
	c := New(t)

	var fractal struct {
		ID string `json:"id"`
	}
	c.Do(t, "POST", "/fractals", map[string]any{
		"name":        fmt.Sprintf("api-suite-backfill-%d", time.Now().UnixNano()),
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

	marker := fmt.Sprintf("backfill-%d", time.Now().UnixNano())
	define := func(source string) map[string]any {
		return map[string]any{"source_bql": source, "key_fields": []string{"computer_name"}}
	}
	var model struct {
		ID string `json:"id"`
	}
	scoped.Do(t, "POST", "/models", map[string]any{
		"name": "suite_backfill_" + fmt.Sprint(time.Now().UnixNano()), "model_type": "first_seen",
		"alert_mode": "none", "definition": define(fmt.Sprintf("suite_marker=%q", marker)),
	}, &model)
	t.Cleanup(func() { scoped.Status(t, "DELETE", "/models/"+model.ID, nil) })
	waitActive := func() {
		Eventually(t, "the model to become active", 60*time.Second, func() bool {
			var m struct {
				Status string `json:"status"`
			}
			scoped.Do(t, "GET", "/models/"+model.ID, nil, &m)
			return m.Status == "active"
		})
	}
	waitActive()

	// 20 logs over 5 hosts, ingested after the model exists.
	const logs, hosts = 20, 5
	now := time.Now().UTC()
	var batch []map[string]any
	for i := 0; i < logs; i++ {
		batch = append(batch, map[string]any{
			"timestamp":    now.Add(-time.Duration(i+1) * time.Hour).Format(time.RFC3339),
			"suite_marker": marker, "computer_name": fmt.Sprintf("h%d", i%hosts),
		})
	}
	c.WithKey(token.Token).DoRaw(t, "POST", "/ingest", batch, nil)

	type row struct {
		EventCount float64 `json:"event_count"`
	}
	count := func() (rows int, events float64) {
		var data []row
		scoped.Do(t, "GET", "/models/"+model.ID+"/data?limit=100", nil, &data)
		for _, r := range data {
			events += r.EventCount
		}
		return len(data), events
	}

	// Live state picks the logs up once its watermark passes them.
	Eventually(t, "live state to count every log", 6*time.Minute, func() bool {
		_, events := count()
		return events == logs
	})

	// An edit that changes the detection drops the table and starts it empty.
	scoped.Do(t, "PUT", "/models/"+model.ID, map[string]any{
		"name": "suite_backfill_edited", "alert_mode": "none",
		"definition": define(fmt.Sprintf("suite_marker=%q computer_name=*", marker)),
	}, nil)
	waitActive()

	scoped.Do(t, "POST", "/models/"+model.ID+"/backfill", map[string]any{"window": "7d"}, nil)
	Eventually(t, "the backfill to complete", 3*time.Minute, func() bool {
		var m struct {
			BackfillStatus string `json:"backfill_status"`
		}
		scoped.Do(t, "GET", "/models/"+model.ID, nil, &m)
		if m.BackfillStatus == "failed" {
			t.Fatal("backfill failed")
		}
		return m.BackfillStatus == "completed"
	})

	// Backfill and live state split the logs at the watermark between them, so
	// once live state catches up every log is counted exactly once.
	var rows int
	var events float64
	Eventually(t, "the edited model to hold every log once", 4*time.Minute, func() bool {
		rows, events = count()
		return rows == hosts && events == logs
	})
	if rows != hosts || events != logs {
		t.Fatalf("after edit and backfill: %d hosts with %v events, want %d hosts with %d events", rows, events, hosts, logs)
	}
	// Stays exact: nothing arrives late to double the count.
	time.Sleep(90 * time.Second)
	if _, events = count(); events != logs {
		t.Fatalf("events drifted to %v after settling, want %d (double counted)", events, logs)
	}
}

// A backfill over an explicit range seeds only logs whose event time falls in
// [from, to), however far back that is.
//
//	go test -tags integration ./test/integration/ -run TestModelBackfillRange -v
func TestModelBackfillRange(t *testing.T) {
	c := New(t)

	var fractal struct {
		ID string `json:"id"`
	}
	c.Do(t, "POST", "/fractals", map[string]any{
		"name":        fmt.Sprintf("api-suite-backfill-range-%d", time.Now().UnixNano()),
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

	// Three hosts, each logging only at its own age; the range covers "mid" alone.
	marker := fmt.Sprintf("backfill-range-%d", time.Now().UnixNano())
	noon := time.Now().UTC().Truncate(24 * time.Hour).Add(12 * time.Hour)
	ages := map[string]int{"old": 40, "mid": 25, "new": 10}
	var batch []map[string]any
	for host, days := range ages {
		for i := 0; i < 3; i++ {
			batch = append(batch, map[string]any{
				"timestamp":    noon.AddDate(0, 0, -days).Add(time.Duration(i) * time.Minute).Format(time.RFC3339),
				"suite_marker": marker, "computer_name": host,
			})
		}
	}
	c.WithKey(token.Token).DoRaw(t, "POST", "/ingest", batch, nil)

	Eventually(t, "the ingested logs to become queryable", 120*time.Second, func() bool {
		var res struct {
			Results []map[string]any `json:"results"`
		}
		c.DoRaw(t, "POST", "/query", map[string]any{
			"query":      fmt.Sprintf("suite_marker=%q | count()", marker),
			"fractal_id": fractal.ID,
			"start":      noon.AddDate(0, 0, -45).Format(time.RFC3339),
			"end":        time.Now().UTC().Add(time.Minute).Format(time.RFC3339),
		}, &res)
		return len(res.Results) == 1 && fmt.Sprint(res.Results[0]["_count"]) == fmt.Sprint(len(batch))
	})

	var model struct {
		ID string `json:"id"`
	}
	scoped.Do(t, "POST", "/models", map[string]any{
		"name": "suite_backfill_range_" + fmt.Sprint(time.Now().UnixNano()), "model_type": "first_seen",
		"alert_mode": "none",
		"definition": map[string]any{"source_bql": fmt.Sprintf("suite_marker=%q", marker), "key_fields": []string{"computer_name"}},
	}, &model)
	t.Cleanup(func() { scoped.Status(t, "DELETE", "/models/"+model.ID, nil) })
	Eventually(t, "the model to become active", 60*time.Second, func() bool {
		var m struct {
			Status string `json:"status"`
		}
		scoped.Do(t, "GET", "/models/"+model.ID, nil, &m)
		return m.Status == "active"
	})

	if code := scoped.Status(t, "POST", "/models/"+model.ID+"/backfill", map[string]any{
		"from": noon.AddDate(0, 0, -30).Format(time.RFC3339), "to": noon.AddDate(0, 0, -150).Format(time.RFC3339),
	}); code != 400 {
		t.Fatalf("reversed range: status %d, want 400", code)
	}
	scoped.Do(t, "POST", "/models/"+model.ID+"/backfill", map[string]any{
		"from": noon.AddDate(0, 0, -30).Format(time.RFC3339), "to": noon.AddDate(0, 0, -20).Format(time.RFC3339),
	}, nil)
	var m struct {
		BackfillStatus string `json:"backfill_status"`
		BackfillWindow string `json:"backfill_window"`
		BackfillTotal  int    `json:"backfill_total"`
	}
	Eventually(t, "the backfill to complete", 3*time.Minute, func() bool {
		scoped.Do(t, "GET", "/models/"+model.ID, nil, &m)
		if m.BackfillStatus == "failed" {
			t.Fatal("backfill failed")
		}
		return m.BackfillStatus == "completed"
	})
	// Noon to noon: a partial day at each end plus nine whole days.
	if m.BackfillWindow != "range" || m.BackfillTotal != 11 {
		t.Errorf("backfill window %q over %d chunks, want range over 11", m.BackfillWindow, m.BackfillTotal)
	}

	var data []map[string]any
	scoped.Do(t, "GET", "/models/"+model.ID+"/data?limit=100", nil, &data)
	if len(data) != 1 || fmt.Sprint(data[0]["entity_key"]) != "mid" || fmt.Sprint(data[0]["event_count"]) != "3" {
		t.Fatalf("model holds %v, want only host mid with 3 events", data)
	}
}
