package storage

import "math"

// CPU/memory health monitoring reads ClickHouse's system.asynchronous_metrics. Both
// the cgroup-scoped (CGroup*) and node-scoped (OS*) families are requested so the
// computation can prefer the cgroup view -- the container's real CPU/memory limit --
// and fall back to node metrics on bare metal or on ClickHouse versions that do not
// expose cgroup metrics.
//
// This matters in Kubernetes/containers: the OS* metrics come from procfs and report
// the whole node, so a ClickHouse pod pinned to a CPU/memory cgroup quota reports
// utilization against the entire node and never approaches 100%. That blinds the GUI
// gauges and disarms ingest backpressure (its 80/60 thresholds never trip). The
// CGroup* metrics report against the pod's own quota, which is what we want.

// SystemCPUMetricsSQL selects the metrics CPUPercentFromMetrics consumes.
const SystemCPUMetricsSQL = `SELECT metric, value FROM system.asynchronous_metrics
	WHERE metric IN (
		'CGroupUserTimeNormalized', 'CGroupSystemTimeNormalized', 'CGroupMaxCPU',
		'OSUserTime', 'OSNiceTime', 'OSSystemTime',
		'OSIdleTime', 'OSIOWaitTime',
		'OSIrqTime', 'OSSoftIrqTime', 'OSStealTime'
	)`

// SystemMemoryMetricsSQL selects the metrics MemoryPercentFromMetrics consumes.
const SystemMemoryMetricsSQL = `SELECT metric, value FROM system.asynchronous_metrics
	WHERE metric IN ('CGroupMemoryUsed', 'CGroupMemoryTotal', 'MemoryResident', 'OSMemoryTotal')`

// MetricRowsToMap flattens metric/value rows from system.asynchronous_metrics into a
// name -> value map for the *FromMetrics helpers.
func MetricRowsToMap(rows []map[string]interface{}) map[string]float64 {
	m := make(map[string]float64, len(rows))
	for _, row := range rows {
		name, _ := row["metric"].(string)
		if name == "" {
			continue
		}
		m[name] = metricFloat(row["value"])
	}
	return m
}

// CPUPercentFromMetrics computes CPU utilization (0-100), preferring the cgroup view.
// Returns ok=false only when neither metric family yields a usable value.
func CPUPercentFromMetrics(m map[string]float64) (float64, bool) {
	// Cgroup-normalized CPU is already in [0..1] against the cgroup CPU quota
	// (CGroupMaxCPU cores), so it maps straight to a percentage. Present only on
	// cgroup-limited (containerized) deployments running a recent ClickHouse.
	cu, okU := m["CGroupUserTimeNormalized"]
	cs, okS := m["CGroupSystemTimeNormalized"]
	if (okU || okS) && m["CGroupMaxCPU"] > 0 {
		return clampPct((cu + cs) * 100), true
	}
	// Fallback: node-level OS CPU as a busy/total ratio.
	busy := m["OSUserTime"] + m["OSNiceTime"] + m["OSSystemTime"] +
		m["OSIrqTime"] + m["OSSoftIrqTime"] + m["OSStealTime"]
	total := busy + m["OSIdleTime"] + m["OSIOWaitTime"]
	if total <= 0 {
		return 0, false
	}
	return clampPct(busy / total * 100), true
}

// MemoryPercentFromMetrics computes memory utilization (0-100), preferring the cgroup
// view. CGroupMemoryTotal is zero when no cgroup limit applies, in which case it falls
// back to ClickHouse resident set over node total RAM.
func MemoryPercentFromMetrics(m map[string]float64) (float64, bool) {
	if total := m["CGroupMemoryTotal"]; total > 0 {
		if used := m["CGroupMemoryUsed"]; used > 0 {
			return clampPct(used / total * 100), true
		}
	}
	total := m["OSMemoryTotal"]
	if total <= 0 {
		return 0, false
	}
	return clampPct(m["MemoryResident"] / total * 100), true
}

// clampPct rounds to one decimal and bounds the result to [0, 100].
func clampPct(pct float64) float64 {
	pct = math.Round(pct*10) / 10
	if pct < 0 {
		return 0
	}
	if pct > 100 {
		return 100
	}
	return pct
}

// metricFloat coerces a driver value (asynchronous_metrics.value is Float64, but be
// defensive across driver representations) to float64.
func metricFloat(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int64:
		return float64(n)
	case int:
		return float64(n)
	case uint64:
		return float64(n)
	default:
		return 0
	}
}
