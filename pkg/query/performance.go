package query

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"

	"bifract/pkg/storage"
)

// cpuSample holds a single CPU measurement.
type cpuSample struct {
	Time  time.Time
	Value float64 // CPU% (0-100)
}

// MetricsCollector polls system.asynchronous_metrics via the existing
// ClickHouse connection and stores CPU history in a ring buffer.
type MetricsCollector struct {
	mu      sync.RWMutex
	history []cpuSample
	// Previous jiffy snapshot for delta-based CPU%.
	prevBusy  float64
	prevTotal float64
	maxAge    time.Duration
	db        *storage.ClickHouseClient
	stop      chan struct{}
}

const (
	collectInterval = 30 * time.Second
	maxHistoryAge   = 25 * time.Hour // keep slightly more than 24h
)

// NewMetricsCollector creates and starts a background collector.
func NewMetricsCollector(db *storage.ClickHouseClient) *MetricsCollector {
	mc := &MetricsCollector{
		maxAge: maxHistoryAge,
		db:     db,
		stop:   make(chan struct{}),
	}
	go mc.run()
	return mc
}

func (mc *MetricsCollector) Stop() {
	close(mc.stop)
}

func (mc *MetricsCollector) run() {
	// Collect immediately on start, then on interval.
	mc.collect()
	ticker := time.NewTicker(collectInterval)
	defer ticker.Stop()
	for {
		select {
		case <-mc.stop:
			return
		case <-ticker.C:
			mc.collect()
		}
	}
}

func (mc *MetricsCollector) collect() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := mc.db.Query(ctx, `SELECT metric, value FROM system.asynchronous_metrics
		WHERE metric IN (
			'OSUserTime', 'OSNiceTime', 'OSSystemTime',
			'OSIdleTime', 'OSIOWaitTime',
			'OSIrqTime', 'OSSoftIrqTime', 'OSStealTime'
		)`)
	if err != nil {
		log.Printf("[MetricsCollector] failed to query CPU metrics: %v", err)
		return
	}

	var user, nice, sys, idle, iowait, irq, softirq, steal float64
	for _, row := range rows {
		metric, _ := row["metric"].(string)
		value := toFloat64(row["value"])
		switch metric {
		case "OSUserTime":
			user = value
		case "OSNiceTime":
			nice = value
		case "OSSystemTime":
			sys = value
		case "OSIdleTime":
			idle = value
		case "OSIOWaitTime":
			iowait = value
		case "OSIrqTime":
			irq = value
		case "OSSoftIrqTime":
			softirq = value
		case "OSStealTime":
			steal = value
		}
	}

	busy := user + nice + sys + irq + softirq + steal
	total := busy + idle + iowait
	if total <= 0 {
		log.Printf("[MetricsCollector] OS CPU metrics not available (all zero)")
		return
	}

	now := time.Now()
	mc.mu.Lock()
	defer mc.mu.Unlock()

	// Compute delta-based CPU% using the difference from the previous snapshot.
	// The OS*Time metrics are cumulative jiffies, so we need two samples.
	if mc.prevTotal > 0 {
		dBusy := busy - mc.prevBusy
		dTotal := total - mc.prevTotal
		var pct float64
		if dTotal > 0 {
			pct = math.Round(dBusy/dTotal*1000) / 10
		}
		mc.history = append(mc.history, cpuSample{Time: now, Value: pct})
		// Trim old samples
		cutoff := now.Add(-mc.maxAge)
		start := 0
		for start < len(mc.history) && mc.history[start].Time.Before(cutoff) {
			start++
		}
		mc.history = mc.history[start:]
	}
	mc.prevBusy = busy
	mc.prevTotal = total
}

// CPUHistory returns collected CPU samples for the given time range.
func (mc *MetricsCollector) CPUHistory(since time.Duration, bucketSize time.Duration) []map[string]interface{} {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	cutoff := time.Now().Add(-since)

	type bucket struct {
		sum   float64
		count int
	}
	buckets := map[int64]*bucket{}
	var bucketKeys []int64

	for _, s := range mc.history {
		if s.Time.Before(cutoff) {
			continue
		}
		key := s.Time.Truncate(bucketSize).Unix()
		b, ok := buckets[key]
		if !ok {
			b = &bucket{}
			buckets[key] = b
			bucketKeys = append(bucketKeys, key)
		}
		b.sum += s.Value
		b.count++
	}

	sort.Slice(bucketKeys, func(i, j int) bool { return bucketKeys[i] < bucketKeys[j] })

	var points []map[string]interface{}
	for _, key := range bucketKeys {
		b := buckets[key]
		t := time.Unix(key, 0).UTC()
		points = append(points, map[string]interface{}{
			"time":  t.Format("2006-01-02 15:04:05"),
			"value": math.Round(b.sum/float64(b.count)*10) / 10,
		})
	}
	return points
}

type PerformanceHandler struct {
	db        *storage.ClickHouseClient
	collector *MetricsCollector
}

func NewPerformanceHandler(db *storage.ClickHouseClient) *PerformanceHandler {
	mc := NewMetricsCollector(db)
	log.Printf("[Performance] Started metrics collector for %d node(s)", len(db.Addrs()))
	return &PerformanceHandler{db: db, collector: mc}
}

// StopCollector stops the background metrics collector.
func (h *PerformanceHandler) StopCollector() {
	if h.collector != nil {
		h.collector.Stop()
	}
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
	var since time.Duration
	var bucketSize time.Duration
	switch r.URL.Query().Get("range") {
	case "8h":
		interval = "8 HOUR"
		since = 8 * time.Hour
		bucketSize = 5 * time.Minute
	case "24h":
		interval = "24 HOUR"
		since = 24 * time.Hour
		bucketSize = 15 * time.Minute
	default:
		since = 1 * time.Hour
		bucketSize = 1 * time.Minute
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

	// CPU history from collector (works on both Docker and K8s).
	if cpuHistory := h.collector.CPUHistory(since, bucketSize); len(cpuHistory) > 0 {
		result["cpu_history"] = cpuHistory
	}

	respondJSON(w, http.StatusOK, result)
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
