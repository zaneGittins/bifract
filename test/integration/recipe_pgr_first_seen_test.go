//go:build integration

// pgr()'s first_seen is the day the activity happened, not the day it was ingested:
// a replayed dataset or a late agent must not look new on the day it arrived.
//
//	go test -tags integration ./test/integration/ -run TestProvenanceFirstSeenIsEventDay -v

package integration

import (
	"fmt"
	"testing"
	"time"
)

func TestProvenanceFirstSeenIsEventDay(t *testing.T) {
	t.Parallel()
	c := New(t)
	enableEndpointAnalytics(t, c)

	fractalID, scoped := newFractal(t, c, "pgr-first-seen")
	shipper := newShipper(t, scoped, fractalID)

	// Three days before ingest, at noon so the date is the same in any reading.
	happened := time.Now().UTC().AddDate(0, 0, -3).Truncate(24 * time.Hour).Add(12 * time.Hour)
	run := time.Now().UnixNano() % 0xffffffff
	guid := func(n int) string { return fmt.Sprintf("{%08x-0000-4000-8000-%012d}", run, n) }
	const (
		explorer = `C:\Windows\explorer.exe`
		cmd      = `C:\Windows\System32\cmd.exe`
		ip       = "198.51.100.7"
	)
	at := happened
	stamp := func(fields map[string]any) map[string]any {
		at = at.Add(time.Second)
		fields["timestamp"] = at.Format(time.RFC3339Nano)
		fields["computer_name"] = "WS01"
		return fields
	}
	logs := []map[string]any{
		stamp(map[string]any{"bifract_category": "process_creation", "process_guid": guid(2), "parent_process_guid": guid(1),
			"image": cmd, "parent_image": explorer, "commandline": cmd}),
		stamp(map[string]any{"bifract_category": "network_connect", "process_guid": guid(2), "image": cmd, "dst_ip": ip}),
	}
	shipper.DoRaw(t, "POST", "/ingest", logs, nil)

	body := map[string]any{
		"query":      fmt.Sprintf("pgr(start=%q, threshold=0)", guid(2)),
		"fractal_id": fractalID,
		"start":      happened.Add(-time.Hour).Format(time.RFC3339),
		"end":        time.Now().UTC().Add(time.Minute).Format(time.RFC3339),
	}
	var rows []map[string]any
	Eventually(t, "pgr() to return the connection", 120*time.Second, func() bool {
		var res struct {
			Results []map[string]any `json:"results"`
		}
		c.DoRaw(t, "POST", "/query", body, &res)
		rows = res.Results
		for _, r := range rows {
			if r["child"] == "net:"+ip {
				return true
			}
		}
		return false
	})

	want := happened.Format(time.DateOnly)
	for _, r := range rows {
		if r["event_type"] != "spawn" && r["event_type"] != "net_connect" {
			continue
		}
		if r["first_seen"] != want {
			t.Errorf("%v %v -> %v first_seen = %v, want the event date %s", r["event_type"], r["parent"], r["child"], r["first_seen"], want)
		}
	}
}
