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

func TestSearchWorkloadDDL(t *testing.T) {
	settings := []string{"max_concurrent_threads_ratio_to_cores = 0.50", "max_memory = 4000000"}

	// Single node, no existing hierarchy: the search workload is its own root.
	got := searchWorkloadDDL(settings, "", "")
	want := "CREATE OR REPLACE WORKLOAD bifract_search SETTINGS max_concurrent_threads_ratio_to_cores = 0.50, max_memory = 4000000"
	if got != want {
		t.Errorf("root DDL =\n%q\nwant\n%q", got, want)
	}

	// An operator-defined root already exists, so attach beneath it: ClickHouse
	// permits only one root workload.
	got = searchWorkloadDDL(settings, "", "all")
	if !strings.Contains(got, "WORKLOAD bifract_search IN all SETTINGS") {
		t.Errorf("expected IN clause under existing root, got %q", got)
	}

	// Re-reconciling when we are already the root must not nest us under ourselves.
	got = searchWorkloadDDL(settings, "", QuerySearchWorkload)
	if strings.Contains(got, " IN ") {
		t.Errorf("workload must not be nested under itself, got %q", got)
	}

	// Cluster mode: ON CLUSTER sits after the name and before IN/SETTINGS.
	got = searchWorkloadDDL(settings, " ON CLUSTER 'bifract'", "all")
	if !strings.Contains(got, "WORKLOAD bifract_search ON CLUSTER 'bifract' IN all SETTINGS") {
		t.Errorf("unexpected cluster DDL clause order: %q", got)
	}
}

// The workload tag must only ride queries explicitly marked as user searches, so
// ingestion, alerting, and model queries that share the same helpers stay unscheduled.
func TestApplyUserSearchWorkloadTagging(t *testing.T) {
	c := &ClickHouseClient{}
	c.searchWorkloadActive.Store(true)

	if got := c.applyUserSearchWorkload(context.Background(), nil); got != nil {
		t.Errorf("unmarked context must not be tagged, got %v", got)
	}

	searchCtx := UserSearchContext(context.Background())
	got := c.applyUserSearchWorkload(searchCtx, nil)
	if got["workload"] != QuerySearchWorkload {
		t.Errorf("marked context should carry workload tag, got %v", got)
	}

	// Existing settings must survive: clickhouse-go replaces the settings map
	// wholesale, so dropping a caller's budget here would silently unbound it.
	base := clickhouse.Settings{"max_query_size": 42}
	got = c.applyUserSearchWorkload(searchCtx, base)
	if got["max_query_size"] != 42 || got["workload"] != QuerySearchWorkload {
		t.Errorf("expected merged settings, got %v", got)
	}

	// Provisioning failed or limits are disabled: tagging stops, so searches run
	// exactly as they did before the workload existed.
	c.searchWorkloadActive.Store(false)
	if got := c.applyUserSearchWorkload(searchCtx, nil); got != nil {
		t.Errorf("inactive workload must not tag, got %v", got)
	}
}
