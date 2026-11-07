//go:build integration

// Provenance filters end to end: events go through ingest and the provenance views, then each
// filter must keep exactly the events of the processes it describes.
//
//	go test -tags integration ./test/integration/ -run TestPgrFilters -v

package integration

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

func TestPgrFilters(t *testing.T) {
	t.Parallel()
	c := New(t)
	enableEndpointAnalytics(t, c)

	fractalID, scoped := newFractal(t, c, "pgrfilter")
	shipper := newShipper(t, scoped, fractalID)

	const (
		word  = `C:\Program Files\Office\WINWORD.EXE`
		cmd   = `C:\Windows\System32\cmd.exe`
		ps    = `C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe`
		rdll  = `C:\Windows\System32\rundll32.exe`
		note  = `C:\Windows\notepad.exe`
		shell = `C:\Windows\explorer.exe`
	)
	run := time.Now().UnixNano()
	guid := func(name string) string { return fmt.Sprintf("{%x-%s}", run, name) }
	base := time.Now().UTC().Add(-30 * time.Minute)
	var logs []map[string]any
	event := func(tag string, f map[string]any) {
		f["timestamp"] = base.Add(time.Duration(len(logs)) * time.Second).Format(time.RFC3339Nano)
		f["tag"] = tag
		f["computer_name"] = "WS01"
		logs = append(logs, f)
	}
	spawn := func(name, parent, image, parentImage string) {
		event(name, map[string]any{
			"bifract_category": "process_creation", "process_guid": guid(name),
			"parent_process_guid": guid(parent), "image": image, "parent_image": parentImage, "commandline": image,
		})
	}
	// explorer -> word -> cmd -> ps -> rdll, with rdll's events carrying no parent_image.
	spawn("word", "root", word, shell)
	spawn("cmd", "word", cmd, word)
	spawn("ps", "cmd", ps, cmd)
	spawn("rdll", "ps", rdll, ps)
	event("rdll-net", map[string]any{"bifract_category": "network_connect", "process_guid": guid("rdll"), "image": rdll, "dst_ip": "203.0.113.50"})
	event("rdll-file", map[string]any{"bifract_category": "file_write", "process_guid": guid("rdll"), "image": rdll, "target_file": `C:\Temp\a.txt`})
	// A sibling tree that shares no ancestor with word, under the same image names.
	spawn("note", "root2", note, shell)
	spawn("cmd2", "note", cmd, note)
	// A process whose creation is before the query range, active inside it.
	spawn("old", "oldroot", note, `C:\Windows\services.exe`)
	logs[len(logs)-1]["timestamp"] = base.Add(-72 * time.Hour).Format(time.RFC3339Nano)
	event("old-file", map[string]any{"bifract_category": "file_write", "process_guid": guid("old"), "image": note, "target_file": `C:\Temp\b.txt`})
	// One process per address class, so external=true is proven against what ingest stores.
	connectOnly := func(name, ip string) {
		spawn(name, "netroot", note, shell)
		event(name+"-net", map[string]any{"bifract_category": "network_connect", "process_guid": guid(name), "image": note, "dst_ip": ip})
	}
	connectOnly("lan", "10.1.2.3")
	connectOnly("mc", "224.0.0.251")
	connectOnly("cgnat", "100.64.0.1")
	connectOnly("bcast", "255.255.255.255")
	connectOnly("zero", "0.1.2.3")
	connectOnly("v6", "2606:4700::1111")
	connectOnly("v6ll", "fe80::1")
	// No process_guid: must never match through an empty key.
	event("no-guid", map[string]any{"message": "plain"})
	shipper.DoRaw(t, "POST", "/ingest", logs, nil)

	tags := func(q string) []string {
		var res struct {
			Results []map[string]any `json:"results"`
		}
		c.DoRaw(t, "POST", "/query", map[string]any{
			"query": q + " | table(tag)", "fractal_id": fractalID,
			"start": base.Add(-time.Hour).Format(time.RFC3339),
			"end":   time.Now().UTC().Add(time.Minute).Format(time.RFC3339),
		}, &res)
		got := []string{}
		for _, r := range res.Results {
			got = append(got, fmt.Sprint(r["tag"]))
		}
		slices.Sort(got)
		return got
	}
	// The views behind lineage can finish just after the log rows are visible.
	Eventually(t, "the provenance tables to catch up", 120*time.Second, func() bool {
		return len(tags(`*`)) == len(logs)-1 && len(tags(`pgrHasAncestor("winword.exe")`)) == 5 && len(tags(`pgrHasAncestor("services.exe")`)) == 1 && len(tags(`pgrHasNetwork()`)) == 17 && len(tags(`pgrHasAncestor("notepad.exe")`)) == 1
	})

	underWord := []string{"cmd", "ps", "rdll", "rdll-file", "rdll-net"}
	cases := []struct {
		query string
		want  []string
	}{
		{`pgrHasAncestor("winword.exe")`, underWord},
		{`pgrHasAncestor("WINWORD.EXE")`, underWord},
		{`pgrHasAncestor("` + `C:\\Program Files\\Office\\WINWORD.EXE` + `")`, underWord},
		{`pgrHasAncestor(/office.+word/)`, underWord},
		{`pgrHasAncestor("word.exe")`, []string{}},
		{`pgrHasAncestor("winword.exe", depth=1)`, []string{"cmd"}},
		{`pgrHasAncestor("winword.exe", depth=2)`, []string{"cmd", "ps"}},
		{`pgrHasAncestor("winword.exe", depth=3)`, underWord},
		{`pgrHasAncestor("cmd.exe", depth=1)`, []string{"ps"}},
		{`bifract_category=network_connect | pgrHasAncestor("winword.exe")`, []string{"rdll-net"}},
		{`pgrHasAncestor("services.exe")`, []string{"old-file"}},
		{`pgrHasNetwork(external=true)`, []string{"rdll", "rdll-file", "rdll-net", "v6", "v6-net"}},
		{`pgrHasNetwork("203.0.113.50")`, []string{"rdll", "rdll-file", "rdll-net"}},
		{`pgrHasNetwork("198.51.100.1")`, []string{}},
		{`pgrHasNetwork("10.1.2.0/24")`, []string{"lan", "lan-net"}},
		{`pgrHasNetwork("internal")`, []string{"v6ll", "v6ll-net"}},
		{`bifract_category=file_write | pgrHasNetwork(external=true)`, []string{"rdll-file"}},
		{`bifract_category=file_write | pgrHasAncestor("winword.exe") | pgrHasNetwork(external=true)`, []string{"rdll-file"}},
		{`pgrHasAncestor("notepad.exe")`, []string{"cmd2"}},
		{`NOT pgrHasAncestor("winword.exe")`, []string{"bcast", "bcast-net", "cgnat", "cgnat-net", "cmd2", "lan", "lan-net", "mc", "mc-net", "no-guid", "note", "old-file", "v6", "v6-net", "v6ll", "v6ll-net", "word", "zero", "zero-net"}},
		{`tag=word OR pgrHasAncestor("notepad.exe")`, []string{"cmd2", "word"}},
	}
	for _, tc := range cases {
		if got := tags(tc.query); !slices.Equal(got, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.query, got, tc.want)
		}
	}
}
