package query

import (
	"strings"
	"testing"
)

// spawnRow / leafRow build minimal edge maps in the provenanceColumns shape.
func spawnRow(parent, child string) map[string]interface{} {
	return map[string]interface{}{
		"parent": parent, "child": child, "label": "", "event_type": "spawn",
		"anomaly_score": 0.5, "log_id": "l", "timestamp": "t", "fractal_id": "f",
		"command_line": "", "proc_user": "", "host": "h",
	}
}

func leafRow(parent, child string, score float64) map[string]interface{} {
	return map[string]interface{}{
		"parent": parent, "child": child, "label": "x", "event_type": "net_connect",
		"anomaly_score": score, "log_id": "l", "timestamp": "t", "fractal_id": "f",
		"command_line": "", "proc_user": "", "host": "h",
	}
}

func TestEmitLiteralEdgeSource_Empty(t *testing.T) {
	sql, dropped, overflow := emitLiteralEdgeSource(nil, provenanceColumns, provenanceNumericColumns, diffuseMaxEmitBytes)
	if sql != provenanceEmptyScoreSQL || dropped != 0 || overflow {
		t.Fatalf("empty set should return the zero-row stub, got sql=%q dropped=%d overflow=%v", sql, dropped, overflow)
	}
}

func TestEmitLiteralEdgeSource_AliasesOnlyOnFirstMember(t *testing.T) {
	rows := []map[string]interface{}{spawnRow("a", "b"), spawnRow("b", "c"), leafRow("b", "net:1.2.3.4", 0.9)}
	sql, _, overflow := emitLiteralEdgeSource(rows, provenanceColumns, provenanceNumericColumns, diffuseMaxEmitBytes)
	if overflow {
		t.Fatal("small set should not overflow")
	}
	// Every column name must appear exactly once (only the first UNION member aliases them).
	for _, c := range provenanceColumns {
		if n := strings.Count(sql, " AS "+c); n != 1 {
			t.Errorf("column %q aliased %d times, want exactly 1 (aliases only on first UNION member)", c, n)
		}
	}
	if members := strings.Count(sql, "SELECT "); members != len(rows) {
		t.Errorf("expected %d UNION members, got %d", len(rows), members)
	}
	// anomaly_score must be typed on every member so the union column type is stable.
	if n := strings.Count(sql, "toFloat64("); n != len(rows) {
		t.Errorf("expected toFloat64 on all %d members, got %d", len(rows), n)
	}
}

func TestEmitLiteralEdgeSource_DropsLeavesOverBudget(t *testing.T) {
	rows := []map[string]interface{}{spawnRow("a", "b")}
	for i := 0; i < 200; i++ {
		rows = append(rows, leafRow("b", "net:leaf", 0.9))
	}
	// Tiny budget: the spawn row fits, most leaves get dropped.
	sql, dropped, overflow := emitLiteralEdgeSource(rows, provenanceColumns, provenanceNumericColumns, 400)
	if overflow {
		t.Fatal("spawn backbone fits, should not overflow")
	}
	if dropped == 0 {
		t.Fatal("expected some leaves dropped under a tiny budget")
	}
	if !strings.Contains(sql, "'a' AS parent") {
		t.Error("spawn backbone must always be emitted")
	}
}

func TestEmitLiteralEdgeSource_OverflowWhenBackboneTooLarge(t *testing.T) {
	var rows []map[string]interface{}
	for i := 0; i < 50; i++ {
		rows = append(rows, spawnRow("parent-guid", "child-guid"))
	}
	// Budget smaller than the spawn backbone -> overflow signal for the caller to fall back.
	_, _, overflow := emitLiteralEdgeSource(rows, provenanceColumns, provenanceNumericColumns, 200)
	if !overflow {
		t.Fatal("spawn backbone exceeding budget must signal overflow")
	}
}
