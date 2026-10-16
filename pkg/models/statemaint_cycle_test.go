package models

import (
	"context"
	"strings"
	"testing"
	"time"

	"bifract/pkg/storage"
)

// These run maintain() and recordOutcome() with no Postgres: each case must
// decide without writing, so reaching the database would panic.

func testMaintainer() *StateMaintainer {
	return NewStateMaintainer(nil, &storage.ClickHouseClient{}, testManager())
}

// A caught-up model reads nothing and writes nothing.
func TestMaintainCaughtUpDoesNothing(t *testing.T) {
	r := maintainRow{id: "a", fractalID: "f", modelType: ModelTypeRarity, table: "t",
		def: ModelDefinition{PartitionKey: "p", ValueKey: "v"}, watermark: time.Now().UTC()}
	if err := testMaintainer().maintain(context.Background(), r); err != nil {
		t.Fatal(err)
	}
}

// A model whose window cannot be rendered fails before the insert, so the
// watermark is never advanced over rows it did not count.
func TestMaintainFailsBeforeInsert(t *testing.T) {
	s := testMaintainer()
	old := time.Now().UTC().Add(-time.Hour)

	bad := maintainRow{id: "a", fractalID: "f", modelType: ModelTypeFirstSeen, table: "t", watermark: old,
		def: ModelDefinition{KeyFields: []string{"u"}, SourceBQL: `level = "a" | sort(host)`}}
	if err := s.maintain(context.Background(), bad); err == nil || !strings.HasPrefix(err.Error(), "source query") {
		t.Errorf("uncompilable source: %v", err)
	}

	unscoped := maintainRow{id: "b", fractalID: "", modelType: ModelTypeBeacon, table: "t", watermark: old}
	if err := s.maintain(context.Background(), unscoped); err == nil || !strings.HasPrefix(err.Error(), "build") {
		t.Errorf("unscoped network model: %v", err)
	}
}

// An outcome already recorded is not written again every cycle.
func TestRecordOutcomeUnchangedWritesNothing(t *testing.T) {
	s := testMaintainer()
	s.lastErr["a"] = "boom"
	s.recordOutcome(context.Background(), maintainRow{id: "a", name: "n"}, "boom")
	s.lastErr["b"] = ""
	s.recordOutcome(context.Background(), maintainRow{id: "b", name: "n"}, "")
	if !s.knowsOutcome("a") || s.knowsOutcome("c") {
		t.Error("knowsOutcome")
	}
}
