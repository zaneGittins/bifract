package query

import (
	"math"
	"testing"
)

func scoredRow(parent, child, eventType, basis string, score float64) map[string]interface{} {
	return map[string]interface{}{
		"parent": parent, "child": child, "label": child, "event_type": eventType,
		"anomaly_score": score, "edge_score": score, "score_basis": basis, "first_seen": "2026-10-07",
		"edge_host_days": 0.0, "source_host_days": 0.0, "source_exec_host_days": 0.0,
		"target_host_days": 7.0, "target_hosts": 3.0, "total_hosts": 4.0,
	}
}

// A brand-new binary under a common parent: its spawn edge is never-seen (1.0), its C2 connection
// was touched by no one else (new_source, 1.0), and its resolver lookup is common (new_source,
// 0.02). Diffusion must keep the C2 edge red, lift the common edge without painting it red, and
// leave each row's own score in edge_score so the inherited part stays explainable.
func provenanceNewBinaryTree() []map[string]interface{} {
	return []map[string]interface{}{
		scoredRow("explorer", "cmd", "spawn", "transition", 0),
		scoredRow("cmd", "node", "spawn", "transition", 1),
		scoredRow("node", "net:203.0.113.50", "net_connect", "new_source", 1),
		scoredRow("node", "net:8.8.8.8", "net_connect", "new_source", 0.02),
		scoredRow("cmd", "net:10.0.0.0/24", "net_connect", "transition", 0.01),
	}
}

func byChild(rows []map[string]interface{}) map[string]map[string]interface{} {
	out := map[string]map[string]interface{}{}
	for _, r := range rows {
		out[reconString(r["child"])] = r
	}
	return out
}

func TestDiffusionKeepsNewBinaryC2AndOwnScore(t *testing.T) {
	got := byChild(diffuseProvenanceRows(provenanceNewBinaryTree(), 0.7, 0.2))

	c2, ok := got["net:203.0.113.50"]
	if !ok {
		t.Fatal("a new binary's connection to a never-seen IP must survive the threshold")
	}
	if a := reconFloat(c2["anomaly_score"]); a != 1 {
		t.Errorf("C2 edge anomaly = %v, want 1", a)
	}
	if _, ok := got["net:8.8.8.8"]; ok {
		t.Error("a common target under a new binary must not be promoted past the threshold")
	}
	if _, ok := got["net:10.0.0.0/24"]; ok {
		t.Error("a common edge under a benign parent must stay cold")
	}
	for _, g := range []string{"cmd", "node"} {
		if _, ok := got[g]; !ok {
			t.Errorf("spawn edge into %s must always be kept", g)
		}
	}
}

func TestDiffusionPreservesExplanationColumns(t *testing.T) {
	got := byChild(diffuseProvenanceRows(provenanceNewBinaryTree(), 0, 0.2))

	dns := got["net:8.8.8.8"]
	if dns == nil {
		t.Fatal("threshold 0 keeps every edge")
	}
	// S(node) = surprisal of its never-seen spawn edge, floored at -ln(0.01); the leaf inherits
	// lambda of it on top of its own surprisal.
	want := 1 - math.Exp(-(-math.Log(1-0.02) + 0.2*-math.Log(0.01)))
	if a := reconFloat(dns["anomaly_score"]); math.Abs(a-math.Round(want*10000)/10000) > 1e-9 {
		t.Errorf("propagated anomaly = %v, want %.4f", a, want)
	}
	if own := reconFloat(dns["edge_score"]); own != 0.02 {
		t.Errorf("edge_score must keep the pre-diffusion score, got %v", own)
	}
	if b := reconString(dns["score_basis"]); b != "new_source" {
		t.Errorf("score_basis must pass through, got %q", b)
	}
	if n := reconFloat(dns["target_host_days"]); n != 7 {
		t.Errorf("explanation counts must pass through, got target_host_days=%v", n)
	}
}
