//go:build integration

// A pattern list's lookups are screened with one Hyperscan pass over the whole
// list once ClickHouse has compiled it, and go to the dictionary alone when it
// cannot. The matches must be the same either way.
//
//	go test -tags integration ./test/integration/ -run TestPatternLookup -v

package integration

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestPatternLookup(t *testing.T) {
	c := New(t)

	var fractal struct {
		ID string `json:"id"`
	}
	c.Do(t, "POST", "/fractals", map[string]any{
		"name":        fmt.Sprintf("api-suite-patterns-%d", time.Now().UnixNano()),
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

	end := time.Now().UTC().Truncate(time.Millisecond)
	body := func(q string) map[string]any {
		return map[string]any{
			"query": q, "fractal_id": fractal.ID,
			"start": end.Add(-24 * time.Hour).Format(time.RFC3339Nano), "end": end.Format(time.RFC3339Nano),
		}
	}
	logs := []map[string]any{
		{"image": `C:\PROGRAM FILES\TeamViewer\tv_x64.exe`},
		{"image": `D:\tools\AnyDesk.EXE`},
		{"image": `C:\tools\notanydesk.exe`},
		{"image": `C:\Windows\System32\svchost.exe`},
	}
	// AnyDesk is old enough to land in a histogram chunk behind the newest.
	for i := range logs {
		logs[i]["timestamp"] = end.Add(-time.Duration(i+1) * time.Minute).Format(time.RFC3339Nano)
	}
	logs[1]["timestamp"] = end.Add(-20 * time.Hour).Format(time.RFC3339Nano)
	c.WithKey(token.Token).DoRaw(t, "POST", "/ingest", logs, nil)
	Eventually(t, "the ingested logs to become queryable", 120*time.Second, func() bool {
		var res struct {
			Results []struct {
				Count int `json:"_count"`
			} `json:"results"`
		}
		c.DoRaw(t, "POST", "/query", body("* | count()"), &res)
		return len(res.Results) == 1 && res.Results[0].Count == len(logs)
	})

	var dict struct {
		ID string `json:"id"`
	}
	scoped.Do(t, "POST", "/dictionaries", map[string]any{
		"name": "rmm", "kind": "pattern", "key_column": "path",
		"columns": []map[string]any{{"name": "path"}, {"name": "tool"}},
	}, &dict)
	// Deleting the fractal leaves a dictionary's ClickHouse objects behind.
	t.Cleanup(func() {
		if code := scoped.Status(t, "DELETE", "/dictionaries/"+dict.ID, nil); code >= 300 {
			t.Logf("could not clean up dictionary %s: %d", dict.ID, code)
		}
	})
	upsert := func(path, tool string) {
		scoped.Do(t, "POST", "/dictionaries/"+dict.ID+"/data", map[string]any{
			"rows": []map[string]any{{"fields": map[string]string{"path": path, "tool": tool}}},
		}, nil)
	}
	upsert(`(?i)^C:\\Program Files\\TeamViewer\\.*$`, "TeamViewer")
	upsert(`(?i)(?:^|[\\/])anydesk\.exe$`, "AnyDesk")

	const q = `* | match(dict="rmm", field=image, column=path, include=[tool, path], require=true) | table(image, tool)`
	want := map[string]string{
		`C:\PROGRAM FILES\TeamViewer\tv_x64.exe`: "TeamViewer",
		`D:\tools\AnyDesk.EXE`:                   "AnyDesk",
	}
	run := func() (map[string]string, string) {
		t.Helper()
		var res struct {
			Success bool                `json:"success"`
			Error   string              `json:"error"`
			SQL     string              `json:"sql"`
			Results []map[string]string `json:"results"`
		}
		c.DoRaw(t, "POST", "/query", body(q), &res)
		if !res.Success {
			t.Fatalf("query failed: %s", res.Error)
		}
		got := map[string]string{}
		for _, r := range res.Results {
			got[r["image"]] = r["tool"]
		}
		return got, res.SQL
	}

	// The list is compiled in the background after the edit returns.
	var got map[string]string
	Eventually(t, "the list to be screened", 60*time.Second, func() bool {
		var sql string
		got, sql = run()
		return strings.Contains(sql, "multiMatchAny(")
	})
	if !reflect.DeepEqual(got, want) {
		t.Errorf("screened lookup matched %v, want %v", got, want)
	}

	// The streamed timeline covers the whole range: every match is counted.
	raw, _ := json.Marshal(body(q))
	var counted int
	sc := bufio.NewScanner(bytes.NewReader(c.Raw(t, "POST", "/query/stream", "application/json", raw)))
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		var f struct {
			Type    string `json:"type"`
			Buckets []int  `json:"buckets"`
		}
		if err := json.Unmarshal(sc.Bytes(), &f); err != nil {
			t.Fatalf("frame %q: %v", sc.Text(), err)
		}
		switch f.Type {
		case "histogram":
			counted = 0
			for _, n := range f.Buckets {
				counted += n
			}
		case "error", "histogram_end":
			t.Fatalf("stream frame %s: %s", f.Type, sc.Text())
		}
	}
	if counted != len(want) {
		t.Errorf("the timeline counted %d matches, want %d", counted, len(want))
	}

	// Hyperscan refuses (?U), which RE2 accepts. The list loses its screen, and
	// the query must neither fail nor change its answer.
	upsert(`(?U)^Z:\\never\\.+$`, "Never")
	got, sql := run()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("unscreened lookup matched %v, want %v", got, want)
	}
	if strings.Contains(sql, "multiMatchAny(") {
		t.Errorf("a list Hyperscan refuses must not be screened:\n%s", sql)
	}
}
