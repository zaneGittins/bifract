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
// In multi-node setups each node is queried individually so that
// per-node CPU% is accurate.
type MetricsCollector struct {
	mu      sync.RWMutex
	// Single-node: only "history" is populated.
	history []cpuSample
	// Multi-node: per-node history keyed by address.
	nodeHistory map[string][]cpuSample
	maxAge time.Duration
	db     *storage.ClickHouseClient
	stop   chan struct{}
}

const (
	collectInterval = 30 * time.Second
	maxHistoryAge   = 25 * time.Hour // keep slightly more than 24h
)

// NewMetricsCollector creates and starts a background collector.
func NewMetricsCollector(db *storage.ClickHouseClient) *MetricsCollector {
	mc := &MetricsCollector{
		nodeHistory: make(map[string][]cpuSample),
		maxAge:      maxHistoryAge,
		db:          db,
		stop:        make(chan struct{}),
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

const cpuMetricsSQL = `SELECT metric, value FROM system.asynchronous_metrics
	WHERE metric IN (
		'OSUserTime', 'OSNiceTime', 'OSSystemTime',
		'OSIdleTime', 'OSIOWaitTime',
		'OSIrqTime', 'OSSoftIrqTime', 'OSStealTime'
	)`

func (mc *MetricsCollector) collect() {
	addrs := mc.db.Addrs()
	if len(addrs) <= 1 {
		// Single-node: query via the shared connection pool.
		mc.collectNode("_single", nil)
	} else {
		// Multi-node: query each node individually so deltas stay
		// within the same host and never produce cross-node nonsense.
		for _, addr := range addrs {
			a := addr
			mc.collectNode(addr, &a)
		}
	}
}

// collectNode samples CPU jiffies from a single ClickHouse node and records
// the delta-based CPU% in the appropriate history slice.
func (mc *MetricsCollector) collectNode(key string, addr *string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var rows []map[string]interface{}
	var err error
	if addr != nil {
		conn, openErr := storage.OpenClickHouseAddr(*addr, mc.db.User, mc.db.Password)
		if openErr != nil {
			log.Printf("[MetricsCollector] failed to connect to %s: %v", *addr, openErr)
			return
		}
		defer conn.Close()
		rows, err = storage.QueryConn(ctx, conn, cpuMetricsSQL)
	} else {
		rows, err = mc.db.Query(ctx, cpuMetricsSQL)
	}
	if err != nil {
		log.Printf("[MetricsCollector] failed to query CPU metrics (node %s): %v", key, err)
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
		log.Printf("[MetricsCollector] OS CPU metrics not available (node %s)", key)
		return
	}

	// Modern ClickHouse (23+) reports OS* metrics as instantaneous ratios
	// (value per core, summed across cores), not cumulative jiffies.
	// Compute CPU% directly from the ratio of busy to total time.
	pct := math.Round(busy/total*1000) / 10
	if pct < 0 {
		pct = 0
	} else if pct > 100 {
		pct = 100
	}

	now := time.Now()
	mc.mu.Lock()
	defer mc.mu.Unlock()

	sample := cpuSample{Time: now, Value: pct}
	if key == "_single" {
		mc.history = append(mc.history, sample)
	} else {
		mc.nodeHistory[key] = append(mc.nodeHistory[key], sample)
	}

	// Trim old samples for this key.
	cutoff := now.Add(-mc.maxAge)
	if key == "_single" {
		mc.history = trimSamples(mc.history, cutoff)
	} else {
		mc.nodeHistory[key] = trimSamples(mc.nodeHistory[key], cutoff)
	}
}

func trimSamples(samples []cpuSample, cutoff time.Time) []cpuSample {
	start := 0
	for start < len(samples) && samples[start].Time.Before(cutoff) {
		start++
	}
	return samples[start:]
}

// CPUHistory returns collected CPU samples for the given time range (single-node).
func (mc *MetricsCollector) CPUHistory(since time.Duration, bucketSize time.Duration) []map[string]interface{} {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return bucketSamples(mc.history, since, bucketSize)
}

// CPUHistoryNodes returns per-node CPU history keyed by node address.
// Returns nil when running in single-node mode.
func (mc *MetricsCollector) CPUHistoryNodes(since time.Duration, bucketSize time.Duration) map[string][]map[string]interface{} {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	if len(mc.nodeHistory) == 0 {
		return nil
	}
	result := make(map[string][]map[string]interface{}, len(mc.nodeHistory))
	for node, samples := range mc.nodeHistory {
		if points := bucketSamples(samples, since, bucketSize); len(points) > 0 {
			result[node] = points
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func bucketSamples(samples []cpuSample, since time.Duration, bucketSize time.Duration) []map[string]interface{} {
	cutoff := time.Now().Add(-since)

	type bucket struct {
		sum   float64
		count int
	}
	buckets := map[int64]*bucket{}
	var bucketKeys []int64

	for _, s := range samples {
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
	if nodeHistory := h.collector.CPUHistoryNodes(since, bucketSize); nodeHistory != nil {
		result["cpu_history_nodes"] = nodeHistory
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
