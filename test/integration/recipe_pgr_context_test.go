//go:build integration

// pgrContext() end to end: process creations go through ingest and the lineage view, then other
// events read their process's facts as columns, by Sysmon-shaped GUIDs the sort key reorders.
//
//	go test -tags integration ./test/integration/ -run TestPgrContext -v

package integration

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

func TestPgrContext(t *testing.T) {
	t.Parallel()
	c := New(t)
	enableEndpointAnalytics(t, c)

	fractalID, scoped := newFractal(t, c, "pgrcontext")
	shipper := newShipper(t, scoped, fractalID)

	machine := fmt.Sprintf("%08x", uint32(time.Now().UnixNano()))
	guid := func(n int) string { return fmt.Sprintf("{%s-%04x-6860-0000-%012x}", machine, 0x1000+n, n) }
	base := time.Now().UTC().Add(-30 * time.Minute)
	var logs []map[string]any
	event := func(tag string, at time.Time, f map[string]any) {
		f["timestamp"], f["tag"], f["computer_name"] = at.Format(time.RFC3339Nano), tag, "WS01"
		logs = append(logs, f)
	}
	spawn := func(tag string, at time.Time, n, parent int, image, cmd string, extra map[string]any) {
		f := map[string]any{"bifract_category": "process_creation", "process_guid": guid(n), "parent_process_guid": guid(parent),
			"image": image, "parent_image": `C:\Windows\explorer.exe`, "commandline": cmd, "user": `CORP\bob`}
		for k, v := range extra {
			f[k] = v
		}
		event(tag, at, f)
	}
	const enc = `powershell.exe -nop -enc SQBFAFgA`
	spawn("parent", base, 1, 0, `C:\Windows\System32\cmd.exe`, `cmd.exe /c run.bat`, nil)
	spawn("child", base.Add(time.Second), 2, 1, `C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe`, enc,
		map[string]any{"hash": "SHA256=ABC", "original_file_name": "PowerShell.EXE"})
	event("child-net", base.Add(2*time.Second), map[string]any{"bifract_category": "network_connect", "process_guid": guid(2), "dst_ip": "203.0.113.9"})
	// Created three days before the range, active inside it.
	spawn("old", base.Add(-72*time.Hour), 3, 0, `C:\Windows\notepad.exe`, `notepad.exe a.txt`, map[string]any{"original_file_name": "NOTEPAD.EXE"})
	event("old-file", base.Add(3*time.Second), map[string]any{"bifract_category": "file_write", "process_guid": guid(3), "target_file": `C:\a.txt`})
	// A process with no creation event, and a log with no process.
	event("orphan-net", base.Add(4*time.Second), map[string]any{"bifract_category": "network_connect", "process_guid": guid(9), "dst_ip": "203.0.113.9"})
	event("plain", base.Add(5*time.Second), map[string]any{"message": "no process"})
	shipper.DoRaw(t, "POST", "/ingest", logs, nil)

	rows := func(q string) []map[string]any {
		var res struct {
			Results []map[string]any `json:"results"`
		}
		c.DoRaw(t, "POST", "/query", map[string]any{
			"query": q, "fractal_id": fractalID,
			"start": base.Add(-time.Hour).Format(time.RFC3339),
			"end":   time.Now().UTC().Add(time.Minute).Format(time.RFC3339),
		}, &res)
		return res.Results
	}
	tags := func(q string) []string {
		got := []string{}
		for _, r := range rows(q + " | table(tag)") {
			got = append(got, fmt.Sprint(r["tag"]))
		}
		slices.Sort(got)
		return got
	}
	Eventually(t, "the lineage view to catch up", 120*time.Second, func() bool {
		// parent, child, child-net and old-file: the old creation itself is before the range.
		return len(tags(`pgrContext(commandline, require=true)`)) == 4
	})

	byTag := map[string]map[string]any{}
	for _, r := range rows(`pgrContext() | table(tag, pgr_commandline, pgr_user, pgr_image, pgr_parent_commandline, pgr_hash, pgr_original_file_name, pgr_parent_guid)`) {
		byTag[fmt.Sprint(r["tag"])] = r
	}
	for tag, want := range map[string]map[string]string{
		"child-net":  {"pgr_commandline": enc, "pgr_user": `CORP\bob`, "pgr_parent_commandline": `cmd.exe /c run.bat`, "pgr_hash": "SHA256=ABC", "pgr_original_file_name": "PowerShell.EXE", "pgr_parent_guid": guid(1)},
		"child":      {"pgr_commandline": enc, "pgr_parent_commandline": `cmd.exe /c run.bat`},
		"old-file":   {"pgr_commandline": `notepad.exe a.txt`, "pgr_original_file_name": "NOTEPAD.EXE", "pgr_parent_commandline": ""},
		"orphan-net": {"pgr_commandline": "", "pgr_user": "", "pgr_parent_commandline": ""},
		"plain":      {"pgr_commandline": ""},
	} {
		got, ok := byTag[tag]
		if !ok {
			t.Errorf("%s: row missing", tag)
			continue
		}
		for col, v := range want {
			if fmt.Sprint(got[col]) != v {
				t.Errorf("%s: %s = %q, want %q", tag, col, got[col], v)
			}
		}
	}

	for _, tc := range []struct {
		query string
		want  []string
	}{
		{`pgrContext(commandline) | pgr_commandline=~"-enc"`, []string{"child", "child-net"}},
		{`bifract_category=network_connect | pgrContext(commandline) | NOT pgr_commandline=~"-enc"`, []string{"orphan-net"}},
		{`bifract_category=network_connect | pgrContext(commandline, require=true)`, []string{"child-net"}},
		{`pgrContext(user) | pgr_user="CORP\\bob"`, []string{"child", "child-net", "old-file", "parent"}},
		{`pgrContext(parent_commandline) | pgr_parent_commandline=~"run.bat"`, []string{"child", "child-net"}},
	} {
		if got := tags(tc.query); !slices.Equal(got, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.query, got, tc.want)
		}
	}

	counts := map[string]string{}
	for _, r := range rows(`pgrContext(original_file_name) | groupby(pgr_original_file_name)`) {
		counts[fmt.Sprint(r["pgr_original_file_name"])] = fmt.Sprint(r["_count"])
	}
	if counts["PowerShell.EXE"] != "2" || counts["NOTEPAD.EXE"] != "1" {
		t.Errorf("groupby on a context column: %v", counts)
	}
}
