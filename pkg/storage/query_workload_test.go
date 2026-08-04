package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestClampQueryLimitPercent(t *testing.T) {
	cases := []struct {
		in, want int
	}{
		{50, 50},
		{75, 75},
		{0, 0},   // disabled
		{-5, 0},  // disabled
		{100, 0}, // a whole-machine share is no limit
		{150, 0},
		{5, MinQueryLimitPercent}, // floored, never a sliver of the machine
		{10, 10},
	}
	for _, c := range cases {
		if got := ClampQueryLimitPercent(c.in); got != c.want {
			t.Errorf("ClampQueryLimitPercent(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestClassWorkloadDDL(t *testing.T) {
	const budget = int64(8_000_000_000)

	// Search: both limits, default priority, nested under the tree root.
	got := classWorkloadDDL(QuerySearchWorkload, 50, 50, 0, budget)
	want := "CREATE OR REPLACE WORKLOAD bifract_search IN bifract SETTINGS " +
		"max_concurrent_threads_ratio_to_cores = 0.50, max_memory = 4000000000"
	if got != want {
		t.Errorf("search DDL =\n%q\nwant\n%q", got, want)
	}

	// Recall carries an explicit priority so it yields to search for the same core.
	got = classWorkloadDDL(QueryRecallWorkload, 25, 25, recallPriority, budget)
	if !strings.Contains(got, "priority = 1") || !strings.Contains(got, "max_memory = 2000000000") {
		t.Errorf("recall DDL missing priority or memory share: %q", got)
	}

	// A disabled share is omitted, not written as zero, which would forbid all work.
	got = classWorkloadDDL(QuerySearchWorkload, 0, 50, 0, budget)
	if strings.Contains(got, "max_concurrent_threads_ratio_to_cores") {
		t.Errorf("cpu share of 0 must be omitted, got %q", got)
	}
	if !strings.Contains(got, "max_memory") {
		t.Errorf("memory share should still be present, got %q", got)
	}

	// An unknown memory budget cannot be turned into a byte count, so it is omitted.
	got = classWorkloadDDL(QuerySearchWorkload, 50, 50, 0, 0)
	if strings.Contains(got, "max_memory") {
		t.Errorf("memory limit must be omitted when the budget is unknown, got %q", got)
	}
}

// Workload DDL must never carry ON CLUSTER. Keeper replicates workload entities
// itself (workload_zookeeper_path, set by the bundled ClickHouseInstallation) and
// ClickHouse rejects the clause with code 80, INCORRECT_QUERY. Because the failure is
// only logged, an ON CLUSTER regression does not break startup -- it silently leaves
// every clustered deployment with no query CPU or memory ceiling at all. Cluster-wide
// application is handled by execWorkloadDDL running the statement on each node.
func TestWorkloadDDLNeverUsesOnCluster(t *testing.T) {
	stmts := []string{
		classWorkloadDDL(QuerySearchWorkload, 50, 50, 0, 8_000_000_000),
		classWorkloadDDL(QueryRecallWorkload, 25, 25, recallPriority, 8_000_000_000),
	}
	for _, s := range stmts {
		if strings.Contains(strings.ToUpper(s), "ON CLUSTER") {
			t.Errorf("workload DDL must not use ON CLUSTER (ClickHouse code 80): %q", s)
		}
	}
}

// Tagging must be per class and must only happen for provisioned workloads, so
// ingestion, alerting, and model queries sharing the same helpers stay unscheduled.
func TestApplyWorkloadTagging(t *testing.T) {
	c := &ClickHouseClient{}
	c.setActiveWorkloads(map[string]bool{QuerySearchWorkload: true, QueryRecallWorkload: true})

	if got := c.applyQuerySettings(context.Background(), nil); got != nil {
		t.Errorf("unmarked context must not be tagged, got %v", got)
	}

	if got := c.applyQuerySettings(UserSearchContext(context.Background()), nil); got["workload"] != QuerySearchWorkload {
		t.Errorf("search context should carry the search workload, got %v", got)
	}
	if got := c.applyQuerySettings(RecallContext(context.Background()), nil); got["workload"] != QueryRecallWorkload {
		t.Errorf("recall context should carry the recall workload, got %v", got)
	}

	// Existing settings must survive: clickhouse-go replaces the settings map
	// wholesale, so dropping a caller's budget here would silently unbound it.
	got := c.applyQuerySettings(UserSearchContext(context.Background()), clickhouse.Settings{"max_query_size": 42})
	if got["max_query_size"] != 42 || got["workload"] != QuerySearchWorkload {
		t.Errorf("expected merged settings, got %v", got)
	}

	// A class that is disabled or failed to provision is not tagged, even though
	// its sibling is: recall off must not silently borrow search's budget.
	c.setActiveWorkloads(map[string]bool{QuerySearchWorkload: true})
	if got := c.applyQuerySettings(RecallContext(context.Background()), nil); got != nil {
		t.Errorf("unprovisioned class must not tag, got %v", got)
	}

	// Nothing provisioned at all: back to pre-workload behavior.
	c.setActiveWorkloads(nil)
	if got := c.applyQuerySettings(UserSearchContext(context.Background()), nil); got != nil {
		t.Errorf("inactive workloads must not tag, got %v", got)
	}
}

// The execution ceiling must reach ClickHouse whether or not a workload is
// provisioned: without it an abandoned query keeps scanning server-side, and an
// unprovisioned workload is exactly the case where no settings were sent at all.
func TestApplyQuerySettingsBudget(t *testing.T) {
	c := &ClickHouseClient{}
	c.setActiveWorkloads(nil)

	ctx := QueryBudgetContext(UserSearchContext(context.Background()), 60)
	got := c.applyQuerySettings(ctx, nil)
	if got["max_execution_time"] != 60 || got["timeout_overflow_mode"] != "throw" {
		t.Errorf("expected a server-enforced ceiling, got %v", got)
	}

	c.setActiveWorkloads(map[string]bool{QuerySearchWorkload: true})
	got = c.applyQuerySettings(ctx, clickhouse.Settings{"max_query_size": 42})
	if got["max_execution_time"] != 60 || got["workload"] != QuerySearchWorkload || got["max_query_size"] != 42 {
		t.Errorf("ceiling, tag, and caller settings must coexist, got %v", got)
	}

	if got := c.applyQuerySettings(QueryBudgetContext(context.Background(), 0), nil); got != nil {
		t.Errorf("no budget and no workload must send no settings, got %v", got)
	}
}

// Query classes must never be configurable to reserve more memory than the node
// has: the leftover is what inserts, their MV cascade, and merges run in.
func TestClampCombinedMemory(t *testing.T) {
	// Defaults sit exactly at the ceiling and must pass through untouched.
	in := WorkloadLimits{SearchMemoryPercent: DefaultQueryMemoryPercent, RecallMemoryPercent: DefaultRecallMemoryPercent}
	got, clamped := ClampCombinedMemory(in)
	if clamped || got != in {
		t.Errorf("defaults (%d + %d) should fit under %d%%, got %+v clamped=%v",
			DefaultQueryMemoryPercent, DefaultRecallMemoryPercent, MaxCombinedQueryMemoryPercent, got, clamped)
	}

	// Over-subscription is scaled down proportionally, preserving the ratio between
	// the classes rather than zeroing one of them.
	got, clamped = ClampCombinedMemory(WorkloadLimits{SearchMemoryPercent: 90, RecallMemoryPercent: 75})
	if !clamped {
		t.Fatal("90 + 75 must be clamped")
	}
	if total := got.SearchMemoryPercent + got.RecallMemoryPercent; total > MaxCombinedQueryMemoryPercent {
		t.Errorf("clamped total %d%% still exceeds %d%%", total, MaxCombinedQueryMemoryPercent)
	}
	if got.SearchMemoryPercent <= got.RecallMemoryPercent {
		t.Errorf("scaling should preserve the search > recall ordering, got %+v", got)
	}

	// CPU shares are untouched: merges run unscheduled and keep competing for cores
	// at the OS level, so CPU over-subscription degrades rather than exhausts.
	in = WorkloadLimits{SearchCPUPercent: 90, RecallCPUPercent: 75, SearchMemoryPercent: 50, RecallMemoryPercent: 25}
	got, _ = ClampCombinedMemory(in)
	if got.SearchCPUPercent != 90 || got.RecallCPUPercent != 75 {
		t.Errorf("CPU shares must not be clamped, got %+v", got)
	}
}
