package storage

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
)

type monitorEvent struct {
	name   string
	fields map[string]string
}

func distQueue(dataFiles, broken, errs uint64) fakedb.CHResult {
	return fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "data_files", Type: "UInt64"}, {Column: "broken_data_files", Type: "UInt64"}, {Column: "error_count", Type: "UInt64"}},
		Rows: [][]any{{dataFiles, broken, errs}},
	}
}

// The distribution monitor fires degraded once when the error count grows (never on
// the first sample, which has no baseline), then healthy only once the queue has
// drained, and reports broken batches whenever there are any.
func TestDistributionMonitorTransitions(t *testing.T) {
	c, f := newFakeClient()
	c.topo = Topology{DistributedTables: true, FanoutCluster: "c1"}
	var events []monitorEvent
	m := NewDistributionMonitor(c, nil, func(name string, fields map[string]string) {
		events = append(events, monitorEvent{name, fields})
	})
	if !m.Stats().Healthy {
		t.Fatal("a fresh monitor reports healthy")
	}

	samples := []fakedb.CHResult{
		distQueue(10, 0, 5), // baseline: existing errors are not news
		distQueue(12, 0, 7), // errors grew: degraded
		distQueue(3, 0, 7),  // no new errors but still draining: stays degraded
		distQueue(0, 0, 7),  // drained: healthy
		distQueue(0, 2, 7),  // broken batches
	}
	prev, degraded := int64(-1), false
	var health []bool
	for _, s := range samples {
		f.ClearRules()
		f.On("distribution_queue", s)
		m.poll(&prev, &degraded)
		health = append(health, m.Stats().Healthy)
	}

	var names []string
	for _, e := range events {
		names = append(names, e.name)
	}
	want := []string{"ch.distribution.degraded", "ch.distribution.healthy", "ch.distribution.broken_data"}
	if !reflect.DeepEqual(names, want) {
		t.Errorf("events = %v, want %v", names, want)
	}
	if !reflect.DeepEqual(health, []bool{true, false, true, true, false}) {
		t.Errorf("health = %v", health)
	}
	if events[0].fields["error_count"] != "7" || events[2].fields["broken_data_files"] != "2" {
		t.Errorf("event fields = %v", events)
	}
	if got := m.Stats(); got.BrokenDataFiles != 2 || got.ErrorCount != 7 {
		t.Errorf("stats = %+v", got)
	}
	if q := f.Queried()[0]; !strings.Contains(q, "FROM clusterAllReplicas('c1', system, distribution_queue)") {
		t.Errorf("the queue must be read across the cluster: %q", q)
	}

	// A failed poll leaves the last state alone and fires nothing.
	f.ClearRules()
	f.On("distribution_queue", fakedb.CHResult{Err: errors.New("down")})
	before := len(events)
	m.poll(&prev, &degraded)
	if len(events) != before || m.Stats().BrokenDataFiles != 2 {
		t.Error("a failed poll must not change state")
	}
}

func TestDistributionMonitorLifecycle(t *testing.T) {
	c, _ := newFakeClient()
	m := NewDistributionMonitor(c, nil, nil)
	m.Start() // single node: no-op
	m.Stop()
	m.Stop() // idempotent
	if m.History(context.Background(), time.Hour, 60) != nil {
		t.Error("no persistence, no history")
	}
}

// The DDL monitor warns only after the queue has stayed above the warning line for
// several consecutive polls, but goes critical at once.
func TestDDLMonitorThresholds(t *testing.T) {
	c, f := newFakeClient()
	var events []string
	m := NewDDLMonitor(c, nil, func(name string, _ map[string]string) { events = append(events, name) })

	consec := 0
	for _, pending := range []uint64{6, 6, 2, 6, 6, 6, 60, 6} {
		f.ClearRules()
		f.On("distributed_ddl_queue", fakedb.One("pending", "UInt64", pending))
		m.poll(&consec)
	}
	want := []string{"ch.ddl_queue.warning", "ch.ddl_queue.critical"}
	if !reflect.DeepEqual(events, want) {
		t.Errorf("events = %v, want %v (a dip below the line resets the count)", events, want)
	}
	if m.lastPending.Load() != 6 {
		t.Errorf("last pending = %d", m.lastPending.Load())
	}

	m.Start() // no DDL cluster: no-op
	m.Stop()
	m.Stop()
	if m.History(context.Background(), 0, 0) != nil {
		t.Error("no persistence, no history")
	}
}

func TestDistMonInt64(t *testing.T) {
	i, u := int64(-3), uint64(4)
	for _, tc := range []struct {
		in   any
		want int64
	}{
		{int64(1), 1}, {uint64(2), 2}, {int32(3), 3}, {uint32(4), 4}, {5.9, 5},
		{&i, -3}, {&u, 4}, {(*int64)(nil), 0}, {(*uint64)(nil), 0}, {"7", 0}, {nil, 0},
	} {
		if got := distMonInt64(tc.in); got != tc.want {
			t.Errorf("distMonInt64(%#v) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// Containers must be measured against their own cgroup quota, not the node's:
// against the node a pinned pod never approaches 100% and backpressure never trips.
func TestCPUPercentFromMetrics(t *testing.T) {
	cases := []struct {
		name string
		m    map[string]float64
		want float64
		ok   bool
	}{
		{"cgroup preferred", map[string]float64{"CGroupUserTimeNormalized": 0.3, "CGroupSystemTimeNormalized": 0.15, "CGroupMaxCPU": 2, "OSUserTime": 1, "OSIdleTime": 99}, 45, true},
		{"cgroup without a quota falls back", map[string]float64{"CGroupUserTimeNormalized": 0.9, "OSUserTime": 1, "OSIdleTime": 3}, 25, true},
		{"node busy ratio", map[string]float64{"OSUserTime": 1, "OSNiceTime": 1, "OSSystemTime": 1, "OSIrqTime": 0.5, "OSSoftIrqTime": 0.5, "OSStealTime": 1, "OSIdleTime": 4, "OSIOWaitTime": 1}, 50, true},
		{"clamped", map[string]float64{"CGroupUserTimeNormalized": 1.2, "CGroupMaxCPU": 1}, 100, true},
		{"nothing", map[string]float64{}, 0, false},
	}
	for _, tc := range cases {
		got, ok := CPUPercentFromMetrics(tc.m)
		if ok != tc.ok || math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("%s: %v, %v; want %v, %v", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

func TestMemoryPercentFromMetrics(t *testing.T) {
	cases := []struct {
		name string
		m    map[string]float64
		want float64
		ok   bool
	}{
		{"cgroup", map[string]float64{"CGroupMemoryUsed": 1, "CGroupMemoryTotal": 3, "MemoryResident": 9, "OSMemoryTotal": 10}, 33.3, true},
		// CGroupMemoryTotal is 0 when no limit applies.
		{"no cgroup limit", map[string]float64{"CGroupMemoryTotal": 0, "MemoryResident": 9, "OSMemoryTotal": 10}, 90, true},
		{"cgroup used unreported", map[string]float64{"CGroupMemoryTotal": 5, "MemoryResident": 1, "OSMemoryTotal": 4}, 25, true},
		{"nothing", map[string]float64{}, 0, false},
	}
	for _, tc := range cases {
		got, ok := MemoryPercentFromMetrics(tc.m)
		if ok != tc.ok || got != tc.want {
			t.Errorf("%s: %v, %v; want %v, %v", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

func TestMetricRowsToMap(t *testing.T) {
	got := MetricRowsToMap([]map[string]interface{}{
		{"metric": "a", "value": 1.5},
		{"metric": "b", "value": float32(2)},
		{"metric": "c", "value": int64(3)},
		{"metric": "d", "value": 4},
		{"metric": "e", "value": uint64(5)},
		{"metric": "f", "value": "6"}, // unknown representation reads as 0
		{"value": 7.0},                // no name: dropped
	})
	want := map[string]float64{"a": 1.5, "b": 2, "c": 3, "d": 4, "e": 5, "f": 0}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("map = %v, want %v", got, want)
	}
	if clampPct(-1) != 0 || clampPct(100.04) != 100 || clampPct(12.345) != 12.3 {
		t.Error("clampPct must round to one decimal and bound to [0, 100]")
	}
}
