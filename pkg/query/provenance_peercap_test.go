package query

import (
	"fmt"
	"testing"

	"bifract/pkg/parser"
)

// netPeer is one reverse-lookup row: peer p bridging to the tree on shared object node obj.
func netPeer(p, obj string) parser.ReconnectPeer {
	return parser.ReconnectPeer{ReconType: "net", PeerGUID: p, SrcGUID: "T1", ObjectID: obj, Label: obj, Anomaly: 0.85}
}

func peerGUIDs(peers []parser.ReconnectPeer) map[string]int {
	out := map[string]int{}
	for _, pe := range peers {
		out[pe.PeerGUID]++
	}
	return out
}

func TestRankAndCapReconnectPeers_UnderCap(t *testing.T) {
	in := []parser.ReconnectPeer{netPeer("A", "net:1"), netPeer("B", "net:2")}
	if got := rankAndCapReconnectPeers(in, 50); len(got) != len(in) {
		t.Fatalf("under the cap nothing should be dropped, got %d of %d", len(got), len(in))
	}
}

// The cap counts distinct PEER PROCESSES, not rows: every row of an admitted peer survives.
func TestRankAndCapReconnectPeers_KeepsAllRowsOfAdmittedPeers(t *testing.T) {
	in := []parser.ReconnectPeer{
		netPeer("A", "net:1"), netPeer("A", "net:2"), netPeer("A", "net:3"),
		netPeer("B", "net:1"), netPeer("C", "net:1"),
	}
	got := rankAndCapReconnectPeers(in, 1)
	counts := peerGUIDs(got)
	if len(counts) != 1 {
		t.Fatalf("cap=1 should admit one peer, got %v", counts)
	}
	// A shares three rare artifacts with the tree; B and C share one each.
	if counts["A"] != 3 {
		t.Fatalf("the strongest peer should keep all its rows, got %v", counts)
	}
}

func TestRankAndCapReconnectPeers_RanksByArtifactCountThenAnomaly(t *testing.T) {
	in := []parser.ReconnectPeer{
		netPeer("weak", "net:9"),
		{ReconType: "file", PeerGUID: "dropped", SrcGUID: "T1", Label: "c:\\t\\x.exe", Anomaly: 1.0},
		netPeer("strong", "net:1"), netPeer("strong", "net:2"),
	}
	got := rankAndCapReconnectPeers(in, 2)
	counts := peerGUIDs(got)
	if _, ok := counts["strong"]; !ok {
		t.Errorf("two shared artifacts should outrank one, got %v", counts)
	}
	// Tie on artifact count (1 each): the higher-anomaly recon type wins (file 1.0 > net 0.85).
	if _, ok := counts["dropped"]; !ok {
		t.Errorf("anomaly should break the artifact-count tie, got %v", counts)
	}
	if _, ok := counts["weak"]; ok {
		t.Errorf("weakest peer should have been dropped, got %v", counts)
	}
}

// Same input, same output: the ranking must not depend on map iteration order.
func TestRankAndCapReconnectPeers_Deterministic(t *testing.T) {
	var in []parser.ReconnectPeer
	for i := 0; i < 200; i++ {
		in = append(in, netPeer(fmt.Sprintf("p%03d", i), fmt.Sprintf("net:%d", i)))
	}
	first := rankAndCapReconnectPeers(in, 50)
	for i := 0; i < 20; i++ {
		got := rankAndCapReconnectPeers(in, 50)
		if len(got) != len(first) {
			t.Fatalf("run %d returned %d peers, want %d", i, len(got), len(first))
		}
		for j := range got {
			if got[j].PeerGUID != first[j].PeerGUID {
				t.Fatalf("run %d diverged at %d: %s != %s", i, j, got[j].PeerGUID, first[j].PeerGUID)
			}
		}
	}
	if len(first) != 50 {
		t.Fatalf("cap should admit exactly 50 peers, got %d", len(first))
	}
}

// A zero MaxPeers (a hand-built ProvenanceParams, not a parsed one) must fall back to the
// default rather than dropping every peer.
func TestRankAndCapReconnectPeers_ZeroCapUsesDefault(t *testing.T) {
	var in []parser.ReconnectPeer
	for i := 0; i < parser.DefaultReconnectPeers+10; i++ {
		in = append(in, netPeer(fmt.Sprintf("p%03d", i), fmt.Sprintf("net:%d", i)))
	}
	if got := rankAndCapReconnectPeers(in, 0); len(got) != parser.DefaultReconnectPeers {
		t.Fatalf("zero cap should fall back to %d, got %d", parser.DefaultReconnectPeers, len(got))
	}
}
