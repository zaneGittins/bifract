package models

import (
	"testing"
	"time"
)

func TestMaintainWindowBounds(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)

	// A current model advances to the lag cutoff, never to now: a row can commit
	// with an ingest_timestamp slightly in the past, and reading up to now would
	// step over it while the watermark moved past.
	from, to := maintainWindow(now.Add(-time.Minute), now)
	if want := now.Add(-stateMaintLag); !to.Equal(want) {
		t.Errorf("cutoff = %v, want %v", to, want)
	}
	if !from.Equal(now.Add(-time.Minute)) {
		t.Errorf("from = %v, want the watermark", from)
	}

	// After downtime the window is capped, so catch-up takes several cycles
	// instead of one scan across every partition since.
	from, to = maintainWindow(now.Add(-30*24*time.Hour), now)
	if got := to.Sub(from); got != stateMaintMaxWindow {
		t.Errorf("catch-up window = %v, want %v", got, stateMaintMaxWindow)
	}

	// Caught up: nothing to read, and maintain() must not advance the watermark.
	from, to = maintainWindow(now, now)
	if to.After(from) {
		t.Errorf("a caught-up model must yield an empty window, got %v..%v", from, to)
	}
}

// The window is half-open on the left, so a row at exactly the watermark was
// counted by the previous cycle and must not be counted again.
func TestMaintainWindowIsHalfOpen(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	from, _ := maintainWindow(now.Add(-time.Hour), now)
	if !from.Equal(now.Add(-time.Hour)) {
		t.Fatalf("from = %v", from)
	}
	// maintain() renders `ingest_timestamp > from`, asserted here so the operator
	// cannot be relaxed to >= without this failing.
	r := maintainRow{modelType: ModelTypeRarity, table: "t", fractalID: "f"}
	sql, err := r.insertSQL("t", "logs", "ingest_timestamp > 'X' AND ingest_timestamp <= 'Y'")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if !contains(sql, "ingest_timestamp > 'X'") {
		t.Errorf("window must be exclusive on the left: %s", sql)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}

// A network model's state table is not its ch_table_name: that holds the results
// the scorer writes, and aggregating state into it would corrupt both.
func TestNetworkStateTargetsTheStateTable(t *testing.T) {
	r := maintainRow{
		id:        "abc",
		modelType: ModelTypeBeacon,
		table:     "results_table",
		fractalID: "f1",
		def:       ModelDefinition{Network: &NetworkFieldMap{}},
	}
	sql, err := r.insertSQL(chModelStateName("abc"), "logs", "1=1")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if contains(sql, "results_table") {
		t.Errorf("network state must not be written to the results table: %s", sql)
	}
	if !contains(sql, chModelStateName("abc")) {
		t.Errorf("want the state table as the target: %s", sql)
	}
}

// The maintainer must skip a model whose watermark is NULL. That is a model whose
// insert-time view has not been handed over yet, and reading from created_at
// instead would re-count everything the view already wrote.
func TestMaintainerSkipsUnhandedModels(t *testing.T) {
	if !contains(dueModelsQuery, "state_watermark IS NOT NULL") {
		t.Errorf("dueModels must skip a NULL watermark, got: %s", dueModelsQuery)
	}
	if contains(dueModelsQuery, "COALESCE(state_watermark") {
		t.Errorf("a NULL watermark must not fall back to another column: %s", dueModelsQuery)
	}
}

// On a cluster a cycle must write the Distributed companion, the way backfill
// does. Writing the local table would land every model's state on whichever node
// happened to run the cycle. A network model writes its state table, never the
// results table the scorer owns.
func TestStateTargetFollowsTopology(t *testing.T) {
	rarity := maintainRow{id: "abc", modelType: ModelTypeRarity, table: "local_tbl"}
	beacon := maintainRow{id: "abc", modelType: ModelTypeBeacon, table: "results_tbl"}

	if got := stateTarget(rarity, false); got != "local_tbl" {
		t.Errorf("single-node rarity target = %q, want the model table", got)
	}
	if got := stateTarget(rarity, true); got != chModelDistName("abc") {
		t.Errorf("clustered rarity target = %q, want the distributed companion", got)
	}
	if got := stateTarget(beacon, false); got != chModelStateName("abc") {
		t.Errorf("single-node beacon target = %q, want the state table", got)
	}
	if got := stateTarget(beacon, true); got != chModelStateDistName("abc") {
		t.Errorf("clustered beacon target = %q, want the distributed state table", got)
	}
	if got := stateTarget(beacon, false); got == "results_tbl" {
		t.Error("a network model must never write its results table")
	}
}
