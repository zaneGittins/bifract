//go:build integration

// Provenance scoring end to end: four hosts share the same everyday activity, and
// one of them also runs a brand-new binary that beacons to an IP nobody else has
// touched. pgr() must score that connection as never seen, even though the beacon
// is the new binary's entire history, and must score the everyday spawn low. Every
// scored row also has to explain itself.
//
//	go test -tags integration ./test/integration/ -run TestProvenanceScoring -v

package integration

import (
	"fmt"
	"testing"
	"time"
)

func TestProvenanceScoring(t *testing.T) {
	c := New(t)

	// pgr() scores against baselines that are only collected while endpoint
	// behavioral analytics is on. Turn it on for the run and put it back after.
	var analytics struct {
		Enabled bool `json:"enabled"`
	}
	c.DoRaw(t, "GET", "/system/endpoint-analysis", nil, &analytics)
	if !analytics.Enabled {
		if code := c.Status(t, "POST", "/system/endpoint-analysis", map[string]any{"enabled": true}); code >= 300 {
			t.Skipf("endpoint behavioral analytics is off and could not be enabled (%d); enable it under Settings > General > Endpoint Analytics", code)
		}
		t.Cleanup(func() { c.Status(t, "POST", "/system/endpoint-analysis", map[string]any{"enabled": false}) })
	}

	var fractal struct {
		ID string `json:"id"`
	}
	c.Do(t, "POST", "/fractals", map[string]any{
		"name":        fmt.Sprintf("api-suite-pgr-%d", time.Now().UnixNano()),
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

	// Sysmon-shaped events, already in the normalized field names the provenance
	// graph reads (see docs/features/provenance-graph.md).
	run := time.Now().UnixNano() % 0xffffffff
	guid := func(host, n int) string { return fmt.Sprintf("{%08x-0000-4000-8000-%06d%06d}", run, host, n) }
	marker := fmt.Sprintf("pgr-%d", time.Now().UnixNano())
	base := time.Now().UTC().Add(-30 * time.Minute)
	var logs []map[string]any
	at := 0
	event := func(fields map[string]any) {
		at++
		fields["timestamp"] = base.Add(time.Duration(at) * time.Second).Format(time.RFC3339Nano)
		fields["suite_marker"] = marker
		logs = append(logs, fields)
	}
	spawn := func(host, parent, child int, parentImage, image string) {
		event(map[string]any{
			"bifract_category": "process_creation", "computer_name": fmt.Sprintf("WS%02d", host),
			"process_guid": guid(host, child), "parent_process_guid": guid(host, parent),
			"image": image, "parent_image": parentImage, "commandline": image, "user": "CORP\\analyst",
		})
	}
	connect := func(host, proc int, image, ip string) {
		event(map[string]any{
			"bifract_category": "network_connect", "computer_name": fmt.Sprintf("WS%02d", host),
			"process_guid": guid(host, proc), "image": image, "dst_ip": ip,
		})
	}
	const (
		userinit = `C:\Windows\System32\userinit.exe`
		explorer = `C:\Windows\explorer.exe`
		cmd      = `C:\Windows\System32\cmd.exe`
		ipconfig = `C:\Windows\System32\ipconfig.exe`
		svchost  = `C:\Windows\System32\svchost.exe`
		node     = `C:\ProgramData\Updater\node.exe`
		c2       = "203.0.113.50"
		resolver = "8.8.8.8"
	)
	for h := 1; h <= 4; h++ {
		spawn(h, 1, 2, userinit, explorer) // everyday: explorer under userinit
		spawn(h, 2, 3, explorer, cmd)      // everyday: the edge that must score low
		connect(h, 9, svchost, resolver)   // everyday: everyone resolves through 8.8.8.8
		if h > 1 {
			spawn(h, 3, 4, cmd, ipconfig) // cmd.exe has a history of its own elsewhere
		}
	}
	spawn(1, 3, 5, cmd, node) // WS01 only: a binary no host has ever run
	for i := 0; i < 50; i++ {
		connect(1, 5, node, c2) // its whole history is beaconing to one IP
	}
	connect(1, 5, node, resolver)
	c.WithKey(token.Token).DoRaw(t, "POST", "/ingest", logs, nil)

	window := map[string]any{
		"fractal_id": fractal.ID,
		"start":      base.Add(-time.Hour).Format(time.RFC3339),
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

	// threshold=0 keeps every scored edge, so the common ones can be checked too.
	pgr := fmt.Sprintf("pgr(start=%q, threshold=0)", guid(1, 3))
	var rows []map[string]any
	edge := func(parent, child string) map[string]any {
		for _, r := range rows {
			if r["parent"] == parent && r["child"] == child {
				return r
			}
		}
		return nil
	}
	beacon := func() map[string]any { return edge(guid(1, 5), "net:"+c2) }
	Eventually(t, "pgr() to score the new binary's connection", 60*time.Second, func() bool {
		rows = query(pgr)
		return beacon() != nil
	})
	num := func(r map[string]any, col string) float64 {
		var f float64
		if _, err := fmt.Sscan(fmt.Sprint(r[col]), &f); err != nil {
			t.Fatalf("%s = %v, not a number (row %v)", col, r[col], r)
		}
		return f
	}

	// The new binary's beacon: its own history is all leave-one-out removes, so it is
	// scored by how rarely anyone else touched the IP, which is never.
	b := beacon()
	if got := num(b, "anomaly_score"); got < 0.9 {
		t.Errorf("new binary -> never-seen IP scored %v, want >= 0.9 (row %v)", got, b)
	}
	if b["score_basis"] != "new_source" || num(b, "target_host_days") != 0 || num(b, "target_hosts") != 1 || num(b, "total_hosts") != 4 {
		t.Errorf("beacon explanation = basis %v, target_host_days %v, target %v of %v hosts; want new_source, 0, 1 of 4",
			b["score_basis"], b["target_host_days"], b["target_hosts"], b["total_hosts"])
	}

	// The everyday spawn: explorer.exe started cmd.exe on every other host-day it
	// started anything, so it is the opposite of unusual.
	common := edge(guid(1, 2), guid(1, 3))
	if common == nil {
		t.Fatalf("pgr() is missing explorer.exe -> cmd.exe: %v", rows)
	}
	if got := num(common, "anomaly_score"); got > 0.1 {
		t.Errorf("explorer.exe -> cmd.exe, seen on every host, scored %v, want <= 0.1 (row %v)", got, common)
	}
	if num(common, "edge_host_days") != 3 || num(common, "source_host_days") != 3 || common["score_basis"] != "transition" {
		t.Errorf("explorer.exe -> cmd.exe explanation = %v of %v other host-days (%v); want 3 of 3 (transition)",
			common["edge_host_days"], common["source_host_days"], common["score_basis"])
	}

	// cmd.exe is an established binary that never started this one anywhere else.
	if spawned := edge(guid(1, 3), guid(1, 5)); spawned == nil || num(spawned, "anomaly_score") < 0.9 {
		t.Errorf("cmd.exe -> new binary should score >= 0.9: %v", spawned)
	}

	// The new binary's lookup through the shared resolver is common on its own; any
	// score above that is inherited from the chain and shows as the difference.
	if dns := edge(guid(1, 5), "net:"+resolver); dns == nil || num(dns, "edge_score") > 0.3 {
		t.Errorf("new binary -> shared resolver should keep a low own score: %v", dns)
	}

	for _, r := range rows {
		if r["score_basis"] == nil || r["score_basis"] == "" {
			t.Errorf("row carries no explanation: %v", r)
		}
		if _, ok := r["edge_score"]; !ok {
			t.Errorf("row has no edge_score: %v", r)
		}
	}
}
