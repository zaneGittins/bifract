//go:build integration

// The streaming search splits its range into chunks and, for a filtered search,
// lets the scan that counts each chunk's timeline also locate that chunk's rows.
// Whatever the chunking, the answer must be the one a single query gives: this
// recipe compares the stream against the buffered search, which runs the whole
// range as one query, over data shaped to hit the boundaries. It checks that the
// answers agree, not which path produced them.
//
//	go test -tags integration ./test/integration/ -run TestSearchStreamMatchesBufferedSearch -v

package integration

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestSearchStreamMatchesBufferedSearch(t *testing.T) {
	c := New(t)

	var fractal struct {
		ID string `json:"id"`
	}
	c.Do(t, "POST", "/fractals", map[string]any{
		"name":        fmt.Sprintf("api-suite-stream-%d", time.Now().UnixNano()),
		"description": "Created by the API test suite",
	}, &fractal)
	t.Cleanup(func() {
		if code := c.Status(t, "DELETE", "/fractals/"+fractal.ID, nil); code >= 300 {
			t.Logf("could not clean up fractal %s: %d", fractal.ID, code)
		}
	})
	var token struct {
		Token string `json:"token"`
	}
	c.InScope(fractal.ID).Do(t, "POST", "/fractals/"+fractal.ID+"/ingest-tokens", map[string]any{
		"name": "api-suite", "parser_type": "json",
	}, &token)

	// 400 events across the last day, one every 3.5 minutes. Every 37th is
	// "sparse", so a sparse search spans every chunk with few rows in each; two of
	// them share one millisecond, so the order within an instant is exercised; one
	// event is "rare".
	marker := fmt.Sprintf("api-suite-stream-%d", time.Now().UnixNano())
	end := time.Now().UTC().Truncate(time.Millisecond)
	start := end.Add(-24 * time.Hour)
	var logs []map[string]any
	for i := 0; i < 400; i++ {
		kind := "dense"
		if i%37 == 0 {
			kind = "sparse"
		}
		ts := end.Add(-time.Minute - time.Duration(i)*210*time.Second)
		logs = append(logs, map[string]any{
			"timestamp": ts.Format(time.RFC3339Nano), "suite_marker": marker, "kind": kind,
			"user": fmt.Sprintf("u%d", i%7), "seq": i,
		})
	}
	tie := end.Add(-13*time.Hour - 123*time.Millisecond).Format(time.RFC3339Nano)
	logs = append(logs,
		map[string]any{"timestamp": tie, "suite_marker": marker, "kind": "sparse", "user": "tie-a", "seq": 1000},
		map[string]any{"timestamp": tie, "suite_marker": marker, "kind": "sparse", "user": "tie-b", "seq": 1001},
		map[string]any{"timestamp": end.Add(-20 * time.Hour).Format(time.RFC3339Nano), "suite_marker": marker, "kind": "rare", "user": "r", "seq": 2000},
	)
	shipper := c.WithKey(token.Token)
	shipper.DoRaw(t, "POST", "/ingest", logs, nil)

	body := func(q string) map[string]any {
		return map[string]any{
			"query": q, "fractal_id": fractal.ID,
			"start": start.Format(time.RFC3339Nano), "end": end.Format(time.RFC3339Nano),
		}
	}
	all := fmt.Sprintf("suite_marker=%q", marker)
	// Counted rather than listed: a deployment row cap below the total would
	// otherwise never let the wait finish.
	waitFor := func(n int) {
		Eventually(t, "the ingested logs to become queryable", 120*time.Second, func() bool {
			var res struct {
				Results []struct {
					Count int `json:"_count"`
				} `json:"results"`
			}
			c.DoRaw(t, "POST", "/query", body(all+" | count()"), &res)
			return len(res.Results) == 1 && res.Results[0].Count == n
		})
	}
	waitFor(len(logs))

	// When the deployment caps rows below the total, put more events at the exact
	// instant of the last row that fits, so the cap falls between rows sharing a
	// millisecond and their order decides which are returned.
	var capped struct {
		Results  []map[string]any `json:"results"`
		LimitHit string           `json:"limit_hit"`
	}
	c.DoRaw(t, "POST", "/query", body(all), &capped)
	if capped.LimitHit == "search" && len(capped.Results) > 0 {
		at, err := time.Parse("2006-01-02 15:04:05.000", capped.Results[len(capped.Results)-1]["timestamp"].(string))
		if err != nil {
			t.Fatal(err)
		}
		var extra []map[string]any
		for i := 0; i < 3; i++ {
			extra = append(extra, map[string]any{
				"timestamp": at.UTC().Format(time.RFC3339Nano), "suite_marker": marker, "kind": "dense",
				"user": fmt.Sprintf("cap-tie-%d", i), "seq": 3000 + i,
			})
		}
		shipper.DoRaw(t, "POST", "/ingest", extra, nil)
		waitFor(len(logs) + len(extra))
	} else {
		t.Logf("no row cap below %d events; the cap-boundary tie is not exercised", len(logs))
	}

	for _, q := range []string{
		all,
		all + ` AND kind="sparse"`,
		all + ` AND kind="rare"`,
		all + ` AND kind="missing"`,
		all + ` AND kind="sparse" | table(timestamp, user, seq)`,
		all + ` "sparse"`,
		all + ` AND user=~"tie"`,
	} {
		t.Run(q, func(t *testing.T) {
			var buffered struct {
				Success   bool             `json:"success"`
				Results   []map[string]any `json:"results"`
				Count     int              `json:"count"`
				Histogram []int            `json:"histogram"`
				LimitHit  string           `json:"limit_hit"`
			}
			c.DoRaw(t, "POST", "/query", body(q), &buffered)
			if !buffered.Success {
				t.Fatalf("buffered search failed: %+v", buffered)
			}

			raw, _ := json.Marshal(body(q))
			stream := c.Raw(t, "POST", "/query/stream", "application/json", raw)
			var rows []map[string]any
			var histogram []int
			var done map[string]any
			sc := bufio.NewScanner(bytes.NewReader(stream))
			sc.Buffer(make([]byte, 1<<20), 64<<20)
			for sc.Scan() {
				var f struct {
					Type     string           `json:"type"`
					Data     []map[string]any `json:"data"`
					Buckets  []int            `json:"buckets"`
					Complete bool             `json:"complete"`
					Error    string           `json:"error"`
				}
				if err := json.Unmarshal(sc.Bytes(), &f); err != nil {
					t.Fatalf("frame %q: %v", sc.Text(), err)
				}
				switch f.Type {
				case "rows":
					rows = append(rows, f.Data...)
				case "histogram":
					histogram = f.Buckets
				case "done":
					_ = json.Unmarshal(sc.Bytes(), &done)
				case "error", "histogram_end":
					t.Fatalf("stream frame %s: %s", f.Type, sc.Text())
				}
			}

			if len(rows) != len(buffered.Results) || int(done["count"].(float64)) != buffered.Count {
				t.Fatalf("stream returned %d rows (count %v), buffered %d (count %d)", len(rows), done["count"], len(buffered.Results), buffered.Count)
			}
			for i := range rows {
				if !reflect.DeepEqual(rows[i], buffered.Results[i]) {
					t.Fatalf("row %d differs\nstream:   %v\nbuffered: %v", i, rows[i], buffered.Results[i])
				}
			}
			if !reflect.DeepEqual(histogram, buffered.Histogram) {
				t.Errorf("histogram differs\nstream:   %v\nbuffered: %v", histogram, buffered.Histogram)
			}
			if done["limit_hit"] != buffered.LimitHit {
				t.Errorf("limit_hit: stream %v, buffered %q", done["limit_hit"], buffered.LimitHit)
			}
		})
	}
}
