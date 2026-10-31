//go:build integration

// pgr() on a polling loop: a remote-access tool runs `cmd /C "tasklist | findstr"`
// every few seconds, burying the one process that matters under thousands of
// identical ones. With default arguments pgr() must collapse the loop into
// counted nodes and still show the ancestors, the unique grandchild under one of
// the identical siblings, and that grandchild's connection.
//
//	go test -tags integration ./test/integration/ -run TestProvenanceCollapse -v

package integration

import (
	"fmt"
	"testing"
	"time"
)

func TestProvenanceCollapse(t *testing.T) {
	c := New(t)

	var fractal struct {
		ID string `json:"id"`
	}
	c.Do(t, "POST", "/fractals", map[string]any{
		"name":        fmt.Sprintf("api-suite-pgr-collapse-%d", time.Now().UnixNano()),
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

	run := time.Now().UnixNano() % 0xffffffff
	guid := func(n int) string { return fmt.Sprintf("{%08x-0000-4000-8000-%012d}", run, n) }
	marker := fmt.Sprintf("pgr-collapse-%d", time.Now().UnixNano())
	base := time.Now().UTC().Add(-90 * time.Minute)
	var logs []map[string]any
	event := func(at time.Duration, fields map[string]any) {
		fields["timestamp"] = base.Add(at).Format(time.RFC3339Nano)
		fields["suite_marker"] = marker
		fields["computer_name"] = "WS02"
		logs = append(logs, fields)
	}
	spawn := func(at time.Duration, parent, child int, parentImage, image, cmdline string) {
		event(at, map[string]any{
			"bifract_category": "process_creation",
			"process_guid":     guid(child), "parent_process_guid": guid(parent),
			"image": image, "parent_image": parentImage, "commandline": cmdline, "user": "CORP\\analyst",
		})
	}
	const (
		userinit = `C:\Windows\System32\userinit.exe`
		explorer = `C:\Windows\explorer.exe`
		rustdesk = `C:\Users\alice\Downloads\rustdesk.exe`
		cmd      = `C:\Windows\System32\cmd.exe`
		tasklist = `C:\Windows\System32\tasklist.exe`
		netexe   = `C:\Windows\System32\net.exe`
		evil     = `C:\Users\alice\AppData\Local\Temp\quickconnect.service.exe`
		c2       = "203.0.113.87"
		polls    = 1200 // more processes than the old 1,000-guid cap and 500-row limit
	)
	spawn(0, 1, 2, userinit, explorer, explorer)
	spawn(time.Second, 2, 3, explorer, rustdesk, rustdesk+" --portable")
	for i := 0; i < polls; i++ {
		at := 2*time.Second + time.Duration(i)*2*time.Second
		spawn(at, 3, 1000+i, rustdesk, cmd, `cmd /C "tasklist | findstr consent.exe"`)
		spawn(at+time.Millisecond, 1000+i, 3000+i, cmd, tasklist, "tasklist")
	}
	spawn(30*time.Second, 3, 5, rustdesk, netexe, `net use \\dc01\ipc$ /user:alice`)
	// One poll's cmd starts something none of the others do.
	spawn(1000*time.Second, 1777, 9000, cmd, evil, evil+" --service")
	event(1001*time.Second, map[string]any{
		"bifract_category": "network_connect", "process_guid": guid(9000), "image": evil, "dst_ip": c2,
	})
	for i := 0; i < len(logs); i += 1000 {
		c.WithKey(token.Token).DoRaw(t, "POST", "/ingest", logs[i:min(i+1000, len(logs))], nil)
	}

	window := map[string]any{
		"fractal_id": fractal.ID,
		"start":      base.Add(-time.Hour).Format(time.RFC3339),
		"end":        time.Now().UTC().Add(time.Minute).Format(time.RFC3339),
	}
	var warnings []string
	query := func(q string) []map[string]any {
		var res struct {
			Results  []map[string]any `json:"results"`
			Warnings []string         `json:"warnings"`
		}
		body := map[string]any{"query": q}
		for k, v := range window {
			body[k] = v
		}
		c.DoRaw(t, "POST", "/query", body, &res)
		warnings = res.Warnings
		return res.Results
	}
	Eventually(t, "the ingested logs to become queryable", 120*time.Second, func() bool {
		rows := query(fmt.Sprintf("suite_marker=%q | count()", marker))
		return len(rows) == 1 && fmt.Sprint(rows[0]["_count"]) == fmt.Sprint(len(logs))
	})

	var rows []map[string]any
	edge := func(parent, child string) map[string]any {
		for _, r := range rows {
			if r["parent"] == parent && r["child"] == child {
				return r
			}
		}
		return nil
	}
	beacon := func() map[string]any { return edge(guid(9000), "net:"+c2) }
	Eventually(t, "pgr() to reach the connection under the polling loop", 60*time.Second, func() bool {
		rows = query(fmt.Sprintf("pgr(start=%q)", guid(3)))
		return beacon() != nil
	})

	// The loop is two counted nodes; the earliest member names each.
	cmdNode := edge(guid(3), guid(1000))
	if cmdNode == nil || fmt.Sprint(cmdNode["group_size"]) != fmt.Sprint(polls) || cmdNode["group_first"] == "" || cmdNode["group_last"] == "" {
		t.Errorf("the polling cmd.exe children must collapse into one node of %d: %v", polls, cmdNode)
	}
	if list := edge(guid(1000), guid(3000)); list == nil || fmt.Sprint(list["group_size"]) != fmt.Sprint(polls) {
		t.Errorf("their tasklist.exe children must collapse into one node of %d: %v", polls, list)
	}
	// The unique grandchild hangs off the group, and a distinct command line stays apart.
	if edge(guid(1000), guid(9000)) == nil {
		t.Error("the unique child of one poll must surface under the collapsed node")
	}
	if edge(guid(3), guid(5)) == nil {
		t.Error("a distinct sibling must stay its own node")
	}
	// Ancestors are not starved by the wide subtree.
	if edge(guid(1), guid(2)) == nil || edge(guid(2), guid(3)) == nil {
		t.Error("the seed's ancestors are missing")
	}
	if len(rows) > 20 {
		t.Errorf("%d rows for a tree of %d processes; the loop did not collapse", len(rows), 2*polls+5)
	}
	if len(warnings) != 0 {
		t.Errorf("nothing was truncated, yet pgr() warned: %v", warnings)
	}

	// A limit too small for everything still keeps the leaf, and says what it dropped.
	rows = query(fmt.Sprintf("pgr(start=%q, limit=5)", guid(3)))
	if beacon() == nil && edge(guid(1000), guid(9000)) != nil {
		t.Errorf("limit=5 drew the beacon's process but dropped the beacon: %v", rows)
	}
	if len(warnings) == 0 {
		t.Error("a truncated result must carry a warning")
	}
}
