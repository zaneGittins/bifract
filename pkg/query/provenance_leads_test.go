package query

import (
	"fmt"
	"testing"

	"bifract/pkg/parser"
)

func leadPeer(src, peer, host, ip string) parser.ReconnectPeer {
	return parser.ReconnectPeer{ReconType: "net", SrcGUID: src, PeerGUID: peer, PeerHost: host, ObjectID: "net:" + ip, Label: ip, Anomaly: 0.94}
}

// leafOf is the seed tree's own scored edge to an artifact.
func leafOf(parent, child, host string, own float64) map[string]interface{} {
	r := leafRow(parent, child, own)
	r["host"], r["edge_score"] = host, own
	return r
}

func spawnOn(parent, child, host string) map[string]interface{} {
	r := spawnRow(parent, child)
	r["host"] = host
	return r
}

// roles renders bridge rows as "parent>child=role", in row order.
func roles(rows []map[string]interface{}) []string {
	var out []string
	for _, r := range rows {
		if reconString(r["score_basis"]) == "reconnect" {
			out = append(out, fmt.Sprintf("%s>%s=%s", r["parent"], r["child"], r["recon_role"]))
		}
	}
	return out
}

// A link is a lead when it reaches another host through an artifact the seed's own process
// had no business touching. The same artifact on the seed's own host, or one the seed touches
// routinely, is context; a process sharing many artifacts with many hosts is shared software.
func TestClassifyReconnections(t *testing.T) {
	cases := []struct {
		name  string
		rows  []map[string]interface{}
		peers []parser.ReconnectPeer
		want  []string
	}{
		{
			name:  "rare C2 reached from two other hosts",
			rows:  []map[string]interface{}{spawnOn("cmd", "qc", "WS02"), leafOf("qc", "net:203.0.113.87", "WS02", 1)},
			peers: []parser.ReconnectPeer{leadPeer("qc", "p3", "WS03", "203.0.113.87"), leadPeer("qc", "p4", "WS04", "203.0.113.87")},
			want:  []string{"qc>net:203.0.113.87=lead", "p3>net:203.0.113.87=lead", "p4>net:203.0.113.87=lead"},
		},
		{
			name:  "same host is not lateral",
			rows:  []map[string]interface{}{spawnOn("cmd", "qc", "WS02"), leafOf("qc", "net:203.0.113.87", "WS02", 1)},
			peers: []parser.ReconnectPeer{leadPeer("qc", "p2", "ws02", "203.0.113.87")},
			want:  []string{"qc>net:203.0.113.87=context", "p2>net:203.0.113.87=context"},
		},
		{
			name:  "the seed touches it routinely",
			rows:  []map[string]interface{}{spawnOn("cmd", "upd", "WS02"), leafOf("upd", "net:198.51.100.4", "WS02", 0.3)},
			peers: []parser.ReconnectPeer{leadPeer("upd", "p3", "WS03", "198.51.100.4")},
			want:  []string{"upd>net:198.51.100.4=context", "p3>net:198.51.100.4=context"},
		},
		{
			name:  "a dropped file another host ran is a lead without a scored leaf",
			rows:  []map[string]interface{}{spawnOn("cmd", "w", "WS02")},
			peers: []parser.ReconnectPeer{{ReconType: "file", SrcGUID: "w", PeerGUID: "x", PeerHost: "WS05", Label: `C:\t\a.exe`, Anomaly: 1}},
			want:  []string{"w>x=lead"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows := append(tc.rows, parser.ReconnectionEdgeRows(tc.peers)...)
			classifyReconnections(rows, tc.peers)
			if got := roles(rows); fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("got %v\nwant %v", got, tc.want)
			}
		})
	}
}

// One process sharing more than a few artifacts with other hosts is fleet software, however
// rare and new each artifact is: none of its links is a lead.
func TestClassifyReconnectionsSharedSoftware(t *testing.T) {
	rows := []map[string]interface{}{spawnOn("svc", "agent", "WS10")}
	var peers []parser.ReconnectPeer
	for d := 0; d <= reconLeadMaxArtifacts; d++ {
		obj := fmt.Sprintf("dns:t%02d.telemetry.example", d)
		rows = append(rows, leafOf("agent", obj, "WS10", 1))
		peers = append(peers, parser.ReconnectPeer{ReconType: "dns", SrcGUID: "agent", PeerGUID: fmt.Sprintf("p%d", d), PeerHost: fmt.Sprintf("WS%02d", 11+d), ObjectID: obj, Label: obj[4:], Anomaly: 0.86})
	}
	rows = append(rows, parser.ReconnectionEdgeRows(peers)...)
	classifyReconnections(rows, peers)
	for _, r := range rows {
		if reconString(r["score_basis"]) == "reconnect" && r["recon_role"] != "shared" {
			t.Errorf("%v>%v = %v, want shared", r["parent"], r["child"], r["recon_role"])
		}
	}
}

// Leads are capped, strongest first, so a graph never promotes more than a few bridges.
func TestClassifyReconnectionsCapsLeads(t *testing.T) {
	var rows []map[string]interface{}
	var peers []parser.ReconnectPeer
	for i := 0; i < reconLeadMaxBridges+2; i++ {
		src := fmt.Sprintf("s%d", i)
		ip := fmt.Sprintf("203.0.113.%d", i)
		rows = append(rows, spawnOn("root", src, "WS02"), leafOf(src, "net:"+ip, "WS02", 1))
		pe := leadPeer(src, "p"+src, "WS09", ip)
		pe.Anomaly = 0.9 + float64(i)/100
		peers = append(peers, pe)
	}
	rows = append(rows, parser.ReconnectionEdgeRows(peers)...)
	classifyReconnections(rows, peers)
	leads := map[string]bool{}
	for _, r := range rows {
		if r["recon_role"] == "lead" {
			leads[reconString(r["child"])] = true
		}
	}
	if len(leads) != reconLeadMaxBridges || leads["net:203.0.113.0"] || leads["net:203.0.113.1"] {
		t.Errorf("lead artifacts %v; want the %d strongest", leads, reconLeadMaxBridges)
	}
}

// The reverse lookup names any tree process that touched a shared artifact. The bridge is drawn
// from, and judged by, the one whose own edge to it is most anomalous: an implant beaconing to
// its C2, not a browser that resolved the same name.
func TestPointBridgesAtSharpestToucher(t *testing.T) {
	rows := []map[string]interface{}{
		spawnOn("root", "edge", "WS02"), spawnOn("root", "implant", "WS02"),
		leafOf("edge", "net:203.0.113.87", "WS02", 0.5), leafOf("implant", "net:203.0.113.87", "WS02", 1),
		leafOf("edge", "dns:login.example", "WS02", 1),
	}
	peers := []parser.ReconnectPeer{
		leadPeer("edge", "p3", "WS03", "203.0.113.87"),
		{ReconType: "dns", SrcGUID: "edge", PeerGUID: "p4", PeerHost: "WS04", ObjectID: "dns:login.example", Label: "login.example", Anomaly: 0.85},
		{ReconType: "file", SrcGUID: "edge", PeerGUID: "x", PeerHost: "WS05", Label: `C:\t\a.exe`, Anomaly: 1},
	}
	pointBridgesAtSharpestToucher(peers, rows)
	for i, want := range []string{"implant", "edge", "edge"} {
		if peers[i].SrcGUID != want {
			t.Errorf("peer %d drawn from %s, want %s", i, peers[i].SrcGUID, want)
		}
	}

	// Classified after re-pointing, the C2 is a lead even though the lookup named the browser.
	rows = append(rows, parser.ReconnectionEdgeRows(peers[:1])...)
	classifyReconnections(rows, peers[:1])
	if got := roles(rows); fmt.Sprint(got) != "[implant>net:203.0.113.87=lead p3>net:203.0.113.87=lead]" {
		t.Errorf("roles %v", got)
	}
}
