package query

import (
	"fmt"
	"math"
	"net/http"
	"sort"

	"bifract/pkg/storage"
)

type PerformanceHandler struct {
	db *storage.ClickHouseClient
}

func NewPerformanceHandler(db *storage.ClickHouseClient) *PerformanceHandler {
	return &PerformanceHandler{db: db}
}

// HandleProcesses returns currently running queries from system.processes
func (h *PerformanceHandler) HandleProcesses(w http.ResponseWriter, r *http.Request) {
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil || !user.IsAdmin {
		respondJSON(w, http.StatusForbidden, map[string]interface{}{
			"success": false,
			"error":   "Admin access required",
		})
		return
	}

	sql := `SELECT
		query_id,
		user,
		query,
		elapsed,
		read_rows,
		read_bytes,
		total_rows_approx,
		memory_usage,
		peak_memory_usage,
		formatReadableSize(memory_usage) AS memory_readable,
		formatReadableSize(read_bytes) AS read_readable
	FROM system.processes
	WHERE is_initial_query = 1
	ORDER BY elapsed DESC`

	results, err := h.db.Query(r.Context(), sql)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   fmt.Sprintf("Failed to query processes: %v", err),
		})
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"success":   true,
		"processes": results,
		"count":     len(results),
	})
}

// HandleKillQuery kills a running query by query_id
func (h *PerformanceHandler) HandleKillQuery(w http.ResponseWriter, r *http.Request) {
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil || !user.IsAdmin {
		respondJSON(w, http.StatusForbidden, map[string]interface{}{
			"success": false,
			"error":   "Admin access required",
		})
		return
	}

	queryID := r.URL.Query().Get("query_id")
	if queryID == "" {
		respondJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"error":   "query_id parameter required",
		})
		return
	}

	// Sanitize: query_id should be a UUID-like string
	for _, c := range queryID {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') || c == '-') {
			respondJSON(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"error":   "Invalid query_id format",
			})
			return
		}
	}

	killSQL := fmt.Sprintf("KILL QUERY WHERE query_id = '%s'", queryID)
	err := h.db.Exec(r.Context(), killSQL)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   fmt.Sprintf("Failed to kill query: %v", err),
		})
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Query kill signal sent",
	})
}

// HandleMetrics returns ClickHouse server metrics for performance monitoring.
// Accepts optional ?range= param: 1h (default), 8h, 24h.
func (h *PerformanceHandler) HandleMetrics(w http.ResponseWriter, r *http.Request) {
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil || !user.IsAdmin {
		respondJSON(w, http.StatusForbidden, map[string]interface{}{
			"success": false,
			"error":   "Admin access required",
		})
		return
	}

	// Parse time range
	interval := "1 HOUR"
	bucket := "1 MINUTE"
	switch r.URL.Query().Get("range") {
	case "8h":
		interval = "8 HOUR"
		bucket = "5 MINUTE"
	case "24h":
		interval = "24 HOUR"
		bucket = "15 MINUTE"
	}

	result := map[string]interface{}{
		"success": true,
	}

	// Current metrics (gauges)
	metricsSQL := `SELECT metric, value
		FROM system.metrics
		WHERE metric IN (
			'Query', 'Merge', 'MemoryTracking',
			'TCPConnection', 'HTTPConnection',
			'Read', 'Write', 'InsertQuery', 'SelectQuery'
		)`
	metrics, err := h.db.Query(r.Context(), metricsSQL)
	if err == nil {
		metricsMap := map[string]interface{}{}
		for _, m := range metrics {
			if name, ok := m["metric"].(string); ok {
				metricsMap[name] = m["value"]
			}
		}
		result["metrics"] = metricsMap
	}

	// Async metrics (sampled periodically by ClickHouse)
	asyncSQL := `SELECT metric, value
		FROM system.asynchronous_metrics
		WHERE metric IN (
			'OSMemoryTotal', 'OSMemoryAvailable',
			'MemoryResident', 'MemoryVirtual',
			'OSCPUVirtualTimeMicroseconds',
			'OSUserTime', 'OSNiceTime', 'OSSystemTime',
			'OSIdleTime', 'OSIOWaitTime',
			'OSIrqTime', 'OSSoftIrqTime', 'OSStealTime',
			'Uptime',
			'MaxPartCountForPartition',
			'NumberOfDatabases', 'NumberOfTables',
			'TotalRowsOfMergeTreeTables', 'TotalBytesOfMergeTreeTables'
		)`
	asyncMetrics, err := h.db.Query(r.Context(), asyncSQL)
	if err == nil {
		asyncMap := map[string]interface{}{}
		for _, m := range asyncMetrics {
			if name, ok := m["metric"].(string); ok {
				asyncMap[name] = m["value"]
			}
		}
		result["async_metrics"] = asyncMap
	}

	// Recent query performance
	queryLogSQL := fmt.Sprintf(`SELECT
		type,
		query_kind,
		query_duration_ms,
		read_rows,
		read_bytes,
		result_rows,
		memory_usage,
		event_time
	FROM system.query_log
	WHERE event_time > now() - INTERVAL %s
		AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
		AND is_initial_query = 1
	ORDER BY event_time DESC
	LIMIT 500`, interval)
	recentQueries, err := h.db.Query(r.Context(), queryLogSQL)
	if err == nil {
		result["recent_queries"] = recentQueries
	}

	// CPU history from metric log. ClickHouse OS*Time metrics are already
	// rates (seconds of CPU time per second of wall time), not cumulative
	// counters, so we average per bucket and compute the ratio directly.
	cpuSQL := fmt.Sprintf(`SELECT
		toStartOfInterval(event_time, INTERVAL %s) AS t,
		metric,
		avg(value) AS value
	FROM system.asynchronous_metric_log
	WHERE event_time > now() - INTERVAL %s
		AND metric IN (
			'OSUserTime', 'OSNiceTime', 'OSSystemTime',
			'OSIdleTime', 'OSIOWaitTime',
			'OSIrqTime', 'OSSoftIrqTime', 'OSStealTime'
		)
	GROUP BY t, metric
	ORDER BY t`, bucket, interval)
	cpuRows, err := h.db.Query(r.Context(), cpuSQL)
	if err == nil {
		result["cpu_history"] = computeCpuHistory(cpuRows)
	}

	respondJSON(w, http.StatusOK, result)
}

// computeCpuHistory pivots averaged rate metrics per time bucket and
// computes CPU% directly. ClickHouse OS*Time values are already rates
// (seconds of CPU time per second of wall time across all cores).
func computeCpuHistory(rows []map[string]interface{}) []map[string]interface{} {
	type cpuPoint struct {
		user, nice, system, idle, iowait, irq, softirq, steal float64
	}

	byTime := map[string]*cpuPoint{}
	var times []string

	for _, row := range rows {
		t := fmt.Sprintf("%v", row["t"])
		metric, _ := row["metric"].(string)
		value := toFloat64(row["value"])

		if _, ok := byTime[t]; !ok {
			byTime[t] = &cpuPoint{}
			times = append(times, t)
		}
		switch metric {
		case "OSUserTime":
			byTime[t].user = value
		case "OSNiceTime":
			byTime[t].nice = value
		case "OSSystemTime":
			byTime[t].system = value
		case "OSIdleTime":
			byTime[t].idle = value
		case "OSIOWaitTime":
			byTime[t].iowait = value
		case "OSIrqTime":
			byTime[t].irq = value
		case "OSSoftIrqTime":
			byTime[t].softirq = value
		case "OSStealTime":
			byTime[t].steal = value
		}
	}

	sort.Strings(times)

	var result []map[string]interface{}
	for _, t := range times {
		p := byTime[t]
		busy := p.user + p.nice + p.system + p.irq + p.softirq + p.steal
		total := busy + p.idle + p.iowait
		if total <= 0 {
			continue
		}
		pct := math.Round(busy/total*1000) / 10

		result = append(result, map[string]interface{}{
			"time":  t,
			"value": pct,
		})
	}
	return result
}

func toFloat64(v interface{}) float64 {
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
