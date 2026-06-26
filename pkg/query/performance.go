package query

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"time"

	"bifract/pkg/storage"
)

// MetricsCollector polls ClickHouse CPU% and alert evaluation latency on a fixed
// interval and persists them to Postgres so the System tab's CPU and alert
// latency charts survive process restarts and ClickHouse outages. Health history
// lives in Postgres (not ClickHouse) on purpose: it must stay readable when
// ClickHouse itself is degraded, which is exactly when these charts matter.
// In multi-node setups each ClickHouse node is queried individually so per-node
// CPU% is accurate.
type MetricsCollector struct {
	db            *storage.ClickHouseClient
	pg            *storage.PostgresClient
	retentionDays int
	lastPruneAt   time.Time
	stop          chan struct{}
}

const (
	collectInterval   = 30 * time.Second
	metricsPruneEvery = time.Hour
)

// NewMetricsCollector creates and starts a background collector. retentionDays
// bounds how long persisted samples are kept (default 30 when <= 0).
func NewMetricsCollector(db *storage.ClickHouseClient, pg *storage.PostgresClient, retentionDays int) *MetricsCollector {
	if retentionDays <= 0 {
		retentionDays = 30
	}
	mc := &MetricsCollector{
		db:            db,
		pg:            pg,
		retentionDays: retentionDays,
		stop:          make(chan struct{}),
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

// collect samples CPU% per ClickHouse node plus the average alert evaluation
// latency, then persists everything to Postgres in a single batched insert.
func (mc *MetricsCollector) collect() {
	var samples []storage.SystemMetricSample

	addrs := mc.db.Addrs()
	if len(addrs) <= 1 {
		// Single-node: query via the shared connection pool. Stored under the
		// empty node so reads treat it as a single aggregate series.
		if pct, ok := mc.sampleCPU("_single", nil); ok {
			samples = append(samples, storage.SystemMetricSample{Metric: "cpu", Value: pct})
		}
	} else {
		// Multi-node: query each node individually so CPU% stays per-host.
		for _, addr := range addrs {
			a := addr
			if pct, ok := mc.sampleCPU(addr, &a); ok {
				samples = append(samples, storage.SystemMetricSample{Metric: "cpu", Node: addr, Value: pct})
			}
		}
	}

	if avg, ok := mc.sampleAlertLatency(); ok {
		samples = append(samples, storage.SystemMetricSample{Metric: "alert_exec_ms", Value: avg})
	}

	if len(samples) > 0 && mc.pg != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := mc.pg.InsertSystemMetrics(ctx, time.Now(), samples); err != nil {
			log.Printf("[MetricsCollector] failed to persist metrics: %v", err)
		}
		cancel()
	}

	mc.maybePrune()
}

// sampleCPU queries instantaneous OS CPU ratios from a single ClickHouse node
// and returns CPU% (0-100). addr is nil for the shared single-node connection.
func (mc *MetricsCollector) sampleCPU(key string, addr *string) (float64, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var rows []map[string]interface{}
	var err error
	if addr != nil {
		conn, openErr := storage.OpenClickHouseAddr(*addr, mc.db.User, mc.db.Password)
		if openErr != nil {
			log.Printf("[MetricsCollector] failed to connect to %s: %v", *addr, openErr)
			return 0, false
		}
		defer conn.Close()
		rows, err = storage.QueryConn(ctx, conn, cpuMetricsSQL)
	} else {
		rows, err = mc.db.Query(ctx, cpuMetricsSQL)
	}
	if err != nil {
		log.Printf("[MetricsCollector] failed to query CPU metrics (node %s): %v", key, err)
		return 0, false
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
		return 0, false
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
	return pct, true
}

// sampleAlertLatency returns the average last evaluation latency (ms) across
// enabled alerts, mirroring the Alerts tab summary so the persisted trend
// matches the displayed average.
func (mc *MetricsCollector) sampleAlertLatency() (float64, bool) {
	if mc.pg == nil {
		return 0, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var avg sql.NullFloat64
	err := mc.pg.QueryRow(ctx,
		`SELECT AVG(last_execution_time_ms) FROM alerts
		  WHERE enabled = true AND last_execution_time_ms IS NOT NULL`).Scan(&avg)
	if err != nil || !avg.Valid {
		return 0, false
	}
	return avg.Float64, true
}

// maybePrune deletes samples older than the retention window once per hour.
func (mc *MetricsCollector) maybePrune() {
	if mc.pg == nil || time.Since(mc.lastPruneAt) < metricsPruneEvery {
		return
	}
	mc.lastPruneAt = time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if n, err := mc.pg.PruneSystemMetrics(ctx, mc.retentionDays); err != nil {
		log.Printf("[MetricsCollector] retention prune failed: %v", err)
	} else if n > 0 {
		log.Printf("[MetricsCollector] pruned %d old system_metrics rows", n)
	}
}

type PerformanceHandler struct {
	db        *storage.ClickHouseClient
	pg        *storage.PostgresClient
	collector *MetricsCollector
}

func NewPerformanceHandler(db *storage.ClickHouseClient, pg *storage.PostgresClient, metricsRetentionDays int) *PerformanceHandler {
	mc := NewMetricsCollector(db, pg, metricsRetentionDays)
	log.Printf("[Performance] Started metrics collector for %d node(s), %dd retention", len(db.Addrs()), mc.retentionDays)
	return &PerformanceHandler{db: db, pg: pg, collector: mc}
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

	// Parse time range. `since`/`bucketSec` drive the persisted CPU history read;
	// `interval` bounds the live query_log scan (capped at 24h for long ranges).
	rangeParam := r.URL.Query().Get("range")
	since, bucketSec := metricRange(rangeParam)
	interval := "1 HOUR"
	switch rangeParam {
	case "8h":
		interval = "8 HOUR"
	case "24h", "7d", "30d":
		interval = "24 HOUR"
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
		event_time,
		substring(query, 1, 500) AS query
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

	// Log-specific storage stats (metadata-only, no data scan)
	logStorageSQL := `
		SELECT
			sum(rows) as log_rows,
			sum(bytes_on_disk) as compressed_bytes,
			sum(data_uncompressed_bytes) as uncompressed_bytes
		FROM system.parts
		WHERE database = 'logs' AND table = 'logs' AND active = 1`
	if logRows, err := h.db.Query(r.Context(), logStorageSQL); err == nil && len(logRows) > 0 {
		result["log_storage"] = logRows[0]
	}

	// Disk usage
	diskSQL := `SELECT
		round((total_space - free_space) / total_space * 100, 1) as used_pct,
		formatReadableSize(free_space) as free_space
		FROM system.disks WHERE name = 'default' LIMIT 1`
	if diskRows, err := h.db.Query(r.Context(), diskSQL); err == nil && len(diskRows) > 0 {
		result["disk"] = diskRows[0]
	}

	// CPU history from persisted samples (Postgres) so it survives process
	// restarts and remains readable when ClickHouse itself is degraded. The read
	// mirrors how the collector tags samples: per-node series in multi-node
	// deployments, a single aggregate series otherwise.
	if h.pg != nil {
		if len(h.db.Addrs()) > 1 {
			if nodes, err := h.pg.QueryMetricSeriesByNode(r.Context(), "cpu", since, bucketSec); err != nil {
				log.Printf("[Performance] cpu node history query failed: %v", err)
			} else if nodes != nil {
				nodeMap := make(map[string][]map[string]interface{}, len(nodes))
				for node, pts := range nodes {
					nodeMap[node] = cpuPoints(pts)
				}
				result["cpu_history_nodes"] = nodeMap
			}
		} else if points, err := h.pg.QueryMetricSeries(r.Context(), "cpu", since, bucketSec); err != nil {
			log.Printf("[Performance] cpu history query failed: %v", err)
		} else if len(points) > 0 {
			result["cpu_history"] = cpuPoints(points)
		}
	}

	respondJSON(w, http.StatusOK, result)
}

// metricRange maps a UI range param to a lookback window and bucket width
// (seconds) for persisted metric series.
func metricRange(rangeParam string) (time.Duration, int) {
	switch rangeParam {
	case "8h":
		return 8 * time.Hour, 300
	case "24h":
		return 24 * time.Hour, 900
	case "7d":
		return 7 * 24 * time.Hour, 3600
	case "30d":
		return 30 * 24 * time.Hour, 14400
	default:
		return 1 * time.Hour, 60
	}
}

// cpuPoints formats persisted metric points into the {time, value} shape the
// CPU chart expects: a string timestamp and a one-decimal percentage.
func cpuPoints(points []storage.MetricPoint) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(points))
	for _, p := range points {
		out = append(out, map[string]interface{}{
			"time":  p.Bucket.UTC().Format("2006-01-02 15:04:05"),
			"value": math.Round(p.Value*10) / 10,
		})
	}
	return out
}

// partitionRe extracts the fractal_id and date from a system.parts partition
// value, which for the logs table (PARTITION BY (fractal_id, toDate(timestamp)))
// is formatted as the tuple ('<fractal_id>','YYYY-MM-DD'). The default fractal
// has an empty id, yielding (”,'YYYY-MM-DD').
var partitionRe = regexp.MustCompile(`^\('(.*)','(\d{4}-\d{2}-\d{2})'\)$`)

// HandleIngestDaily returns per-day ingest volume (uncompressed + on-disk bytes
// and row counts) derived purely from system.parts partition metadata. Because
// the logs table is partitioned by (fractal_id, toDate(timestamp)), this is a
// metadata-only query (no data scan, sub-millisecond) and is exact per fractal.
//
// Bucketing is by event date (toDate(timestamp)) and bytes reflect the full
// on-disk row footprint, so totals reconcile with the storage cards' "raw"
// figure (both use system.parts.data_uncompressed_bytes).
//
// Optional params: ?fractal=<id> to scope to a single fractal, ?days=N to bound
// the window (default 30, max 365).
func (h *PerformanceHandler) HandleIngestDaily(w http.ResponseWriter, r *http.Request) {
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil || !user.IsAdmin {
		respondJSON(w, http.StatusForbidden, map[string]interface{}{
			"success": false,
			"error":   "Admin access required",
		})
		return
	}

	fractalFilter := r.URL.Query().Get("fractal")
	days := 30
	if d := r.URL.Query().Get("days"); d != "" {
		if n, err := strconv.Atoi(d); err == nil && n > 0 {
			days = n
		}
	}
	if days > 365 {
		days = 365
	}

	// Metadata-only: aggregates partition stats without touching log data.
	sql := `SELECT
		partition,
		sum(rows) AS rows,
		sum(data_uncompressed_bytes) AS raw_bytes,
		sum(bytes_on_disk) AS disk_bytes
	FROM system.parts
	WHERE database = 'logs' AND table = 'logs' AND active = 1
	GROUP BY partition`

	rows, err := h.db.Query(r.Context(), sql)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"error":   fmt.Sprintf("Failed to query ingest stats: %v", err),
		})
		return
	}

	type dayAgg struct {
		raw  float64
		disk float64
		rows float64
	}
	// Keep the fractal dimension so the "All fractals" view can be broken down
	// (stacked) per fractal.
	byFractalDay := map[string]map[string]*dayAgg{} // fractalID -> day -> agg
	totalByFractal := map[string]float64{}          // ranking by uncompressed bytes
	maxDataDay := ""
	for _, row := range rows {
		part, _ := row["partition"].(string)
		m := partitionRe.FindStringSubmatch(part)
		if m == nil {
			continue
		}
		fractalID, day := m[1], m[2]
		if fractalFilter != "" && fractalID != fractalFilter {
			continue
		}
		raw := toFloat64(row["raw_bytes"])
		fd := byFractalDay[fractalID]
		if fd == nil {
			fd = map[string]*dayAgg{}
			byFractalDay[fractalID] = fd
		}
		agg := fd[day]
		if agg == nil {
			agg = &dayAgg{}
			fd[day] = agg
		}
		agg.raw += raw
		agg.disk += toFloat64(row["disk_bytes"])
		agg.rows += toFloat64(row["rows"])
		totalByFractal[fractalID] += raw
		if day > maxDataDay {
			maxDataDay = day
		}
	}

	// Contiguous day window, zero-filled so bars stay evenly spaced and aligned.
	// The window ends today, or later if data carries event timestamps into the
	// future.
	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	start := today.AddDate(0, 0, -days+1)
	end := today
	if t, err := time.Parse("2006-01-02", maxDataDay); err == nil && t.After(end) {
		end = t
	}

	dayKeys := make([]string, 0, days)
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		dayKeys = append(dayKeys, d.Format("2006-01-02"))
	}

	// Per-day totals across all (selected) fractals: drives single-series
	// rendering and per-day tooltip totals.
	result := make([]map[string]interface{}, 0, len(dayKeys))
	for _, key := range dayKeys {
		var raw, disk, rowCount float64
		for _, fd := range byFractalDay {
			if agg := fd[key]; agg != nil {
				raw += agg.raw
				disk += agg.disk
				rowCount += agg.rows
			}
		}
		result = append(result, map[string]interface{}{
			"day":        key,
			"raw_bytes":  raw,
			"disk_bytes": disk,
			"rows":       rowCount,
		})
	}

	resp := map[string]interface{}{
		"success": true,
		"days":    result,
		"fractal": fractalFilter,
	}

	// Per-fractal breakdown (stacked) for the "All fractals" view. Cap to the
	// top contributors and roll the remainder into an "Other" bucket so the
	// legend and colour palette stay manageable.
	if fractalFilter == "" && len(byFractalDay) > 0 {
		const topN = 8
		const otherKey = "__other__"

		ranked := make([]string, 0, len(byFractalDay))
		for id := range byFractalDay {
			ranked = append(ranked, id)
		}
		sort.Slice(ranked, func(i, j int) bool {
			return totalByFractal[ranked[i]] > totalByFractal[ranked[j]]
		})

		other := []string{}
		if len(ranked) > topN {
			other = ranked[topN:]
			ranked = ranked[:topN]
		}

		seriesFor := func(ids []string) (raw, disk, rc []float64) {
			raw = make([]float64, len(dayKeys))
			disk = make([]float64, len(dayKeys))
			rc = make([]float64, len(dayKeys))
			for _, id := range ids {
				fd := byFractalDay[id]
				for i, key := range dayKeys {
					if agg := fd[key]; agg != nil {
						raw[i] += agg.raw
						disk[i] += agg.disk
						rc[i] += agg.rows
					}
				}
			}
			return
		}

		series := make([]map[string]interface{}, 0, len(ranked)+1)
		for _, id := range ranked {
			raw, disk, rc := seriesFor([]string{id})
			series = append(series, map[string]interface{}{
				"fractal_id": id,
				"raw_bytes":  raw,
				"disk_bytes": disk,
				"rows":       rc,
			})
		}
		if len(other) > 0 {
			raw, disk, rc := seriesFor(other)
			series = append(series, map[string]interface{}{
				"fractal_id": otherKey,
				"raw_bytes":  raw,
				"disk_bytes": disk,
				"rows":       rc,
			})
		}
		resp["series"] = series
	}

	respondJSON(w, http.StatusOK, resp)
}

// HandleAlertStats returns alert engine evaluation stats derived from alert_executions.
// Accepts optional ?range= param: 1h (default), 8h, 24h.
func (h *PerformanceHandler) HandleAlertStats(w http.ResponseWriter, r *http.Request) {
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil || !user.IsAdmin {
		respondJSON(w, http.StatusForbidden, map[string]interface{}{
			"success": false,
			"error":   "Admin access required",
		})
		return
	}

	if h.pg == nil {
		respondJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"success": false,
			"error":   "Postgres not available",
		})
		return
	}

	result := map[string]interface{}{"success": true}

	// All stats sourced from alerts.last_execution_time_ms — updated on every
	// evaluation cycle regardless of whether the alert triggered.
	summaryRow := h.pg.QueryRow(r.Context(),
		`SELECT
			COUNT(*)                                                                                 AS total_active,
			COUNT(*) FILTER (WHERE disabled_reason IS NOT NULL AND disabled_reason != '')            AS disabled,
			COALESCE(AVG(last_execution_time_ms), 0)                                                 AS avg_ms,
			COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY last_execution_time_ms), 0)        AS p95_ms,
			COALESCE(MAX(last_execution_time_ms), 0)                                                 AS max_ms
		 FROM alerts
		 WHERE enabled = true`)
	var totalActive, disabled int64
	var avgMs, p95Ms, maxMs float64
	scanErr := summaryRow.Scan(&totalActive, &disabled, &avgMs, &p95Ms, &maxMs)
	if scanErr != nil {
		log.Printf("[AlertStats] summary: %v", scanErr)
	}

	result["summary"] = map[string]interface{}{
		"total_active": totalActive,
		"disabled":     disabled,
		"avg_ms":       int64(math.Round(avgMs)),
		"p95_ms":       int64(math.Round(p95Ms)),
		"max_ms":       int64(math.Round(maxMs)),
	}

	// Alert evaluation latency trend from persisted samples (Postgres), so it
	// survives restarts instead of being rebuilt in-memory only while the tab is
	// open. The collector records avg(last_execution_time_ms) every 30s.
	execSince, execBucket := metricRange(r.URL.Query().Get("range"))
	execHist := []map[string]interface{}{}
	if points, err := h.pg.QueryMetricSeries(r.Context(), "alert_exec_ms", execSince, execBucket); err != nil {
		log.Printf("[AlertStats] exec history query failed: %v", err)
	} else {
		for _, p := range points {
			execHist = append(execHist, map[string]interface{}{
				"time":   p.Bucket.Unix(),
				"avg_ms": int64(math.Round(p.Value)),
			})
		}
	}
	result["exec_history"] = execHist

	// Exec time distribution: count of enabled alerts in each latency bucket.
	distRow := h.pg.QueryRow(r.Context(),
		`SELECT
			COUNT(*) FILTER (WHERE last_execution_time_ms < 100)                                    AS fast,
			COUNT(*) FILTER (WHERE last_execution_time_ms >= 100 AND last_execution_time_ms < 250)  AS ok,
			COUNT(*) FILTER (WHERE last_execution_time_ms >= 250 AND last_execution_time_ms < 500)  AS slow,
			COUNT(*) FILTER (WHERE last_execution_time_ms >= 500 AND last_execution_time_ms < 1000) AS warn,
			COUNT(*) FILTER (WHERE last_execution_time_ms >= 1000)                                  AS crit
		 FROM alerts
		 WHERE enabled = true AND last_execution_time_ms IS NOT NULL`)
	var fast, ok_, slow, warn, crit int64
	if err := distRow.Scan(&fast, &ok_, &slow, &warn, &crit); err != nil {
		log.Printf("[AlertStats] distribution: %v", err)
	} else {
		result["distribution"] = []map[string]interface{}{
			{"label": "<100ms", "count": fast},
			{"label": "100–250ms", "count": ok_},
			{"label": "250–500ms", "count": slow},
			{"label": "500ms–1s", "count": warn},
			{"label": ">1s", "count": crit},
		}
	}

	// Top 10 slowest alerts by last_execution_time_ms.
	slowRows, err := h.pg.Query(r.Context(),
		`SELECT name, COALESCE(last_execution_time_ms, 0) AS exec_ms
		 FROM alerts
		 WHERE enabled = true AND last_execution_time_ms IS NOT NULL
		 ORDER BY exec_ms DESC
		 LIMIT 10`)
	if err != nil {
		log.Printf("[AlertStats] slowest: %v", err)
	} else {
		defer slowRows.Close()
		var slowest []map[string]interface{}
		for slowRows.Next() {
			var name string
			var execMs int64
			if err := slowRows.Scan(&name, &execMs); err != nil {
				continue
			}
			slowest = append(slowest, map[string]interface{}{
				"name":    name,
				"exec_ms": execMs,
			})
		}
		result["slowest"] = slowest
	}

	// logs_hot table health: partition count, row count, size, and coverage window.
	// Coverage window = max_time - min_time across all active parts; should stay ~2h.
	hotRows, err := h.db.Query(r.Context(), `
		SELECT
			count()            AS partition_count,
			sum(rows)          AS row_count,
			sum(bytes_on_disk) AS disk_bytes,
			min(min_time)      AS oldest,
			max(max_time)      AS newest
		FROM system.parts
		WHERE database = currentDatabase() AND table = 'logs_hot' AND active = 1`)
	if err != nil {
		log.Printf("[AlertStats] logs_hot stats: %v", err)
	} else if len(hotRows) > 0 {
		row := hotRows[0]
		hotStats := map[string]interface{}{
			"partition_count": toFloat64(row["partition_count"]),
			"row_count":       toFloat64(row["row_count"]),
			"disk_bytes":      toFloat64(row["disk_bytes"]),
		}
		// Compute coverage window in minutes from oldest/newest part timestamps.
		// min_time/max_time in system.parts are Nullable(DateTime); the driver
		// may return time.Time, *time.Time, or a formatted string depending on
		// ClickHouse version. Handle all three.
		parsePartTime := func(v interface{}) (time.Time, bool) {
			switch t := v.(type) {
			case time.Time:
				return t, !t.IsZero()
			case *time.Time:
				if t != nil && !t.IsZero() {
					return *t, true
				}
			case string:
				if parsed, err := time.Parse("2006-01-02 15:04:05", t); err == nil {
					return parsed, !parsed.IsZero()
				}
			}
			return time.Time{}, false
		}
		if oldest, ok := parsePartTime(row["oldest"]); ok {
			if newest, ok2 := parsePartTime(row["newest"]); ok2 {
				hotStats["coverage_minutes"] = int64(math.Round(newest.Sub(oldest).Minutes()))
				hotStats["oldest"] = oldest.UTC().Format("2006-01-02 15:04:05")
				hotStats["newest"] = newest.UTC().Format("2006-01-02 15:04:05")
			}
		}
		result["hot_table"] = hotStats
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
