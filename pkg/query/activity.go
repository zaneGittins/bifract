package query

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"bifract/pkg/storage"
)

// The admin activity view reads ClickHouse's own introspection tables. Two rules
// hold everywhere in this file:
//
//   - Every read fans out across the cluster. system.processes and system.query_log
//     are per node, so a load-balanced connection describes only whichever node the
//     driver happened to pick, which is how a query watched running on one node
//     silently never appears as finished.
//   - Every read is bounded: an inner LIMIT before the union, a time window on the
//     query_log sort key (event_date, event_time) so ClickHouse reads in order, and
//     a server-side execution ceiling so a struggling node cannot hang the page.
const (
	activityMaxRows     = 200
	activityDefaultRows = 60
	activityTimeoutSec  = 15
	// activityRunningCap bounds the running side. system.processes is small by
	// nature, but a cluster in trouble is exactly when it is not.
	activityRunningCap = 300
)

// activityQueryTag identifies this view's own queries so they can be excluded from
// what it shows. The SQL predicate is rendered from the same struct that writes the
// tag, so the two can never drift.
var (
	activityQueryTag = storage.QueryTag{Source: storage.SourceSystem, Label: storage.LabelActivity}
	activityTag      = activityQueryTag.String()
)

// activityCtx marks a request's ClickHouse reads as this view's own and gives them
// a server-side ceiling.
func activityCtx(parent context.Context) (context.Context, context.CancelFunc) {
	ctx := storage.TagContext(parent, activityQueryTag)
	ctx = storage.QueryBudgetContext(ctx, activityTimeoutSec)
	return context.WithTimeout(ctx, activityTimeoutSec*time.Second)
}

// activityClassSQL classifies one query_log row into the four workload classes the
// view charts and filters on. Bifract's own tag wins; an untagged row falls back to
// its query kind, which is what identifies ingestion without paying to tag the
// insert path. Deliberately startsWith rather than JSONExtract: this runs per row
// over the whole window, and the tag's rendering is fixed by QueryTag.
const activityClassSQL = `multiIf(` +
	`startsWith(log_comment, '{"src":"alert"'), 'alert', ` +
	`startsWith(log_comment, '{"src":"system"'), 'system', ` +
	`log_comment != '', 'search', ` +
	`query_kind = 'Insert', 'ingest', ` +
	`'system')`

// activityProcClassSQL is activityClassSQL for system.processes, where the tag
// lives in the Settings map and there is no query_kind.
const activityProcClassSQL = `multiIf(` +
	`startsWith(Settings['log_comment'], '{"src":"alert"'), 'alert', ` +
	`startsWith(Settings['log_comment'], '{"src":"system"'), 'system', ` +
	`Settings['log_comment'] != '', 'search', ` +
	`position(query, 'INSERT INTO') = 1, 'ingest', ` +
	`'system')`

// activityRange maps the page's range selector to a window and a bucket width.
// The window is capped well below the range for the live views: a 30-day scan of
// query_log on every poll is not a view, it is an outage.
func activityRange(r string) (windowMin int, bucketSec int) {
	switch r {
	case "8h":
		return 480, 1200
	case "24h", "7d", "30d":
		return 1440, 3600
	default:
		return 60, 150
	}
}

func activityLimit(r *http.Request) int {
	n, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || n <= 0 {
		return activityDefaultRows
	}
	if n > activityMaxRows {
		return activityMaxRows
	}
	return n
}

// activityClassFilter turns the class chip into a SQL predicate over the given
// class expression. An unknown value selects everything rather than nothing.
func activityClassFilter(class, classExpr string) string {
	switch class {
	case "search", "alert", "ingest", "system":
		return fmt.Sprintf(" AND %s = '%s'", classExpr, class)
	}
	return ""
}

// activityTextFilter matches the free-text box against the query and its tag.
func activityTextFilter(q string) string {
	q = strings.TrimSpace(q)
	if q == "" {
		return ""
	}
	if len(q) > 200 {
		q = q[:200]
	}
	esc := storage.EscCHStr(q)
	return fmt.Sprintf(" AND (positionCaseInsensitive(query, '%s') > 0 OR positionCaseInsensitive(log_comment, '%s') > 0)", esc, esc)
}

func activityProcTextFilter(q string) string {
	q = strings.TrimSpace(q)
	if q == "" {
		return ""
	}
	if len(q) > 200 {
		q = q[:200]
	}
	esc := storage.EscCHStr(q)
	return fmt.Sprintf(" AND (positionCaseInsensitive(query, '%s') > 0 OR positionCaseInsensitive(Settings['log_comment'], '%s') > 0 OR positionCaseInsensitive(user, '%s') > 0)", esc, esc, esc)
}

// activityNodeFilter pins the view to one node. Empty means every node.
func activityNodeFilter(node string) string {
	node = strings.TrimSpace(node)
	if node == "" {
		return ""
	}
	if len(node) > 128 {
		node = node[:128]
	}
	return fmt.Sprintf(" AND hostName() = '%s'", storage.EscCHStr(node))
}

// HandleActivityStream returns the merged query stream: what is running right now
// and what finished recently, in one shape, filtered and limited server-side.
//
// Filtering server-side is the point. The previous view fetched a fixed 500 rows of
// query_log and filtered them in the browser, which on a busy cluster is a few
// seconds of history: searching it searched a sliver.
func (h *PerformanceHandler) HandleActivityStream(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := activityCtx(r.Context())
	defer cancel()

	q := r.URL.Query()
	state := q.Get("state")
	class := q.Get("class")
	text := q.Get("q")
	node := q.Get("node")
	limit := activityLimit(r)
	windowMin, _ := activityRange(q.Get("range"))

	topo := h.db.Topology()
	procSource := topo.FanoutSystemTable("system.processes")
	logSource := topo.FanoutSystemTable("system.query_log")

	var branches []string

	// Running side. Skipped only when the caller asked for errors, which a running
	// query cannot be yet.
	if state != "error" {
		where := "is_initial_query = 1" +
			fmt.Sprintf(" AND Settings['log_comment'] != '%s'", storage.EscCHStr(activityTag)) +
			activityClassFilter(class, activityProcClassSQL) +
			activityProcTextFilter(text) +
			activityNodeFilter(node)
		if state == "slow" {
			where += " AND elapsed > 5"
		}
		branches = append(branches, fmt.Sprintf(`SELECT
	if(is_cancelled, 'stopping', 'running') AS state,
	query_id,
	user,
	toFloat64(elapsed) AS age_sec,
	toUInt64(round(elapsed * 1000)) AS duration_ms,
	now() AS event_time,
	toUInt64(read_rows) AS read_rows,
	toUInt64(read_bytes) AS read_bytes,
	toUInt64(greatest(peak_memory_usage, memory_usage)) AS memory,
	substring(query, 1, 400) AS query,
	'' AS query_kind,
	%s AS class,
	hostName() AS node,
	toString(Settings['log_comment']) AS tag,
	toInt64(0) AS exception_code,
	'' AS exception
FROM %s
WHERE %s
ORDER BY elapsed DESC
LIMIT %d`, activityProcClassSQL, procSource, where, activityRunningCap))
	}

	// Finished side. The inner LIMIT is what keeps this bounded: query_log is
	// ordered by (event_date, event_time), so newest-first with a limit reads in
	// order instead of scanning the window.
	if state != "running" {
		where := fmt.Sprintf(`type != 'QueryStart'
	AND is_initial_query = 1
	AND event_date >= toDate(now() - INTERVAL %d MINUTE)
	AND event_time > now() - INTERVAL %d MINUTE
	AND log_comment != '%s'`, windowMin, windowMin, storage.EscCHStr(activityTag)) +
			activityClassFilter(class, activityClassSQL) +
			activityTextFilter(text) +
			activityNodeFilter(node)
		switch state {
		case "error":
			where += " AND type = 'ExceptionWhileProcessing'"
		case "slow":
			where += " AND query_duration_ms > 5000"
		}
		branches = append(branches, fmt.Sprintf(`SELECT
	multiIf(exception_code = 394, 'killed', type = 'ExceptionWhileProcessing', 'error', 'finished') AS state,
	query_id,
	user,
	toFloat64(query_duration_ms) / 1000 AS age_sec,
	toUInt64(query_duration_ms) AS duration_ms,
	event_time,
	toUInt64(read_rows) AS read_rows,
	toUInt64(read_bytes) AS read_bytes,
	toUInt64(memory_usage) AS memory,
	substring(query, 1, 400) AS query,
	toString(query_kind) AS query_kind,
	%s AS class,
	hostName() AS node,
	log_comment AS tag,
	toInt64(exception_code) AS exception_code,
	substring(exception, 1, 300) AS exception
FROM (
	SELECT * FROM %s
	WHERE %s
	ORDER BY event_time DESC
	LIMIT %d
)`, activityClassSQL, logSource, where, limit))
	}

	if len(branches) == 0 {
		respondJSON(w, http.StatusOK, map[string]interface{}{"success": true, "rows": []interface{}{}})
		return
	}

	sql := fmt.Sprintf(`SELECT * FROM (%s)
ORDER BY state IN ('running', 'stopping') DESC, event_time DESC, duration_ms DESC
LIMIT %d`, strings.Join(branches, "\nUNION ALL\n"), limit+activityRunningCap)

	rows, err := h.db.Query(ctx, sql)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": fmt.Sprintf("activity query failed: %v", err),
		})
		return
	}
	if rows == nil {
		rows = []map[string]interface{}{}
	}
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"rows":    rows,
		"nodes":   h.activityNodes(ctx),
	})
}

// activityNodes lists the nodes the view can pin to. Single-node returns nothing,
// so the picker stays hidden.
func (h *PerformanceHandler) activityNodes(ctx context.Context) []string {
	if h.db.Topology().FanoutCluster == "" {
		return nil
	}
	rows, err := h.db.Query(ctx, fmt.Sprintf("SELECT DISTINCT hostName() AS node FROM %s ORDER BY node",
		h.db.Topology().FanoutSystemTable("system.one")))
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		if s, ok := row["node"].(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}

// HandleActivitySummary returns everything above the stream: the headline tiles,
// latency quantiles and query rate over time, the costliest query patterns, the
// failures, and what ClickHouse is doing in the background.
//
// It is deliberately a separate endpoint from the stream. The stream is cheap and
// polls on the page's refresh interval; this is the expensive half and refreshes on
// a slower cadence of its own.
func (h *PerformanceHandler) HandleActivitySummary(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := activityCtx(r.Context())
	defer cancel()

	windowMin, bucketSec := activityRange(r.URL.Query().Get("range"))
	topo := h.db.Topology()
	logSource := topo.FanoutSystemTable("system.query_log")
	procSource := topo.FanoutSystemTable("system.processes")
	logWhere := fmt.Sprintf(`type != 'QueryStart'
	AND is_initial_query = 1
	AND event_date >= toDate(now() - INTERVAL %d MINUTE)
	AND event_time > now() - INTERVAL %d MINUTE
	AND log_comment != '%s'`, windowMin, windowMin, storage.EscCHStr(activityTag))

	out := map[string]interface{}{"success": true, "bucket_seconds": bucketSec, "window_minutes": windowMin}

	// One scan answers both charts: per-bucket rate for each class and the latency
	// quantiles within it.
	buckets, err := h.db.Query(ctx, fmt.Sprintf(`SELECT
	toUInt64(toUnixTimestamp(toStartOfInterval(event_time, INTERVAL %d SECOND))) AS t,
	%s AS class,
	toUInt64(count()) AS n,
	toUInt64(quantileExact(0.5)(query_duration_ms)) AS p50,
	toUInt64(quantileExact(0.95)(query_duration_ms)) AS p95,
	toUInt64(quantileExact(0.99)(query_duration_ms)) AS p99,
	toUInt64(sum(read_bytes)) AS bytes,
	toUInt64(countIf(type = 'ExceptionWhileProcessing')) AS failures
FROM %s
WHERE %s
GROUP BY t, class
ORDER BY t`, bucketSec, activityClassSQL, logSource, logWhere))
	if err == nil {
		out["buckets"] = buckets
	} else {
		out["buckets_error"] = err.Error()
	}

	// Running totals come from system.processes rather than being counted off the
	// stream: the stream is filtered and limited, and these tiles describe the
	// cluster, not the current filter.
	if running, rerr := h.db.Query(ctx, fmt.Sprintf(`SELECT
	toUInt64(count()) AS running,
	toFloat64(max(elapsed)) AS oldest_sec,
	toUInt64(countIf(elapsed > 5)) AS slow
FROM %s
WHERE is_initial_query = 1 AND Settings['log_comment'] != '%s'`,
		procSource, storage.EscCHStr(activityTag))); rerr == nil && len(running) > 0 {
		out["running"] = running[0]
	}

	// Cost by pattern, not by instance. normalized_query_hash collapses literal
	// differences, so this finds the query shape that is expensive because it runs
	// constantly, which no per-instance list can show.
	if patterns, perr := h.db.Query(ctx, fmt.Sprintf(`SELECT
	toString(normalized_query_hash) AS hash,
	any(substring(query, 1, 200)) AS sample,
	argMax(log_comment, event_time) AS tag,
	argMax(%s, event_time) AS class,
	toUInt64(count()) AS runs,
	toUInt64(quantileExact(0.95)(query_duration_ms)) AS p95,
	toUInt64(sum(read_bytes)) AS bytes
FROM %s
WHERE %s
GROUP BY hash
ORDER BY bytes DESC, runs DESC
LIMIT 8`, activityClassSQL, logSource, logWhere)); perr == nil {
		out["patterns"] = patterns
	}

	if failures, ferr := h.db.Query(ctx, fmt.Sprintf(`SELECT
	toInt64(exception_code) AS code,
	any(splitByChar(':', substring(exception, 1, 160))[1]) AS message,
	toUInt64(count()) AS n,
	argMax(log_comment, event_time) AS tag,
	argMax(substring(query, 1, 160), event_time) AS sample
FROM %s
WHERE %s AND type = 'ExceptionWhileProcessing'
GROUP BY code
ORDER BY n DESC
LIMIT 6`, logSource, logWhere)); ferr == nil {
		out["failures"] = failures
	}

	respondJSON(w, http.StatusOK, out)
}

// HandleBackgroundOps reports the merges and mutations ClickHouse is running, plus
// the replication backlog on a cluster. A merge that has been going for hours or a
// mutation that never finishes is the classic production incident, and a single
// "active merges" count cannot show either.
//
// It backs the Overview tab: this is server state alongside CPU and memory, not
// query activity. The system tables are tiny, so the read is metadata-cheap.
func (h *PerformanceHandler) HandleBackgroundOps(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := activityCtx(r.Context())
	defer cancel()
	out := h.backgroundOps(ctx)
	out["success"] = true
	respondJSON(w, http.StatusOK, out)
}

func (h *PerformanceHandler) backgroundOps(ctx context.Context) map[string]interface{} {
	topo := h.db.Topology()
	out := map[string]interface{}{}

	if merges, err := h.db.Query(ctx, fmt.Sprintf(`SELECT
	'merge' AS kind,
	table,
	partition_id AS detail,
	toFloat64(elapsed) AS elapsed_sec,
	toFloat64(progress) AS progress,
	toUInt64(memory_usage) AS memory,
	toUInt64(is_mutation) AS is_mutation,
	hostName() AS node
FROM %s
ORDER BY elapsed DESC
LIMIT 20`, topo.FanoutSystemTable("system.merges"))); err == nil {
		out["merges"] = merges
	}

	if mutations, err := h.db.Query(ctx, fmt.Sprintf(`SELECT
	'mutation' AS kind,
	table,
	mutation_id AS detail,
	toFloat64(dateDiff('second', create_time, now())) AS elapsed_sec,
	toUInt64(parts_to_do) AS parts_to_do,
	substring(latest_fail_reason, 1, 200) AS fail_reason,
	hostName() AS node
FROM %s
WHERE is_done = 0
ORDER BY create_time
LIMIT 20`, topo.FanoutSystemTable("system.mutations"))); err == nil {
		out["mutations"] = mutations
	}

	if topo.FanoutCluster != "" {
		if q, err := h.db.Query(ctx, fmt.Sprintf(
			"SELECT toUInt64(count()) AS queued, toUInt64(countIf(num_postponed > 0)) AS postponed FROM %s",
			topo.FanoutSystemTableArgs("system", "replication_queue"))); err == nil && len(q) > 0 {
			out["replication"] = q[0]
		}
	}
	return out
}

// HandleActivityDetail returns one query's per-shard cost, read back from
// query_log by initial_query_id. This is what turns a slow row in the stream into
// an explanation: how many marks the index actually skipped, how many parts were
// touched, and how much came off disk.
func (h *PerformanceHandler) HandleActivityDetail(w http.ResponseWriter, r *http.Request) {
	queryID := r.URL.Query().Get("query_id")
	if queryID == "" || !validActivityQueryID(queryID) {
		respondJSON(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "error": "valid query_id required",
		})
		return
	}
	ctx, cancel := activityCtx(r.Context())
	defer cancel()

	source := h.db.Topology().FanoutSystemTable("system.query_log")
	rows, err := h.db.Query(ctx, fmt.Sprintf(`SELECT
	hostName() AS node,
	toUInt64(is_initial_query) AS coordinator,
	toUInt64(query_duration_ms) AS duration_ms,
	toUInt64(read_rows) AS read_rows,
	toUInt64(read_bytes) AS read_bytes,
	toUInt64(result_rows) AS result_rows,
	toUInt64(memory_usage) AS memory,
	toUInt64(ProfileEvents['SelectedParts']) AS parts,
	toUInt64(ProfileEvents['SelectedMarks']) AS marks_read,
	toUInt64(ProfileEvents['SelectedMarksTotal']) AS marks_total,
	toUInt64(ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] + ProfileEvents['ReadBufferFromS3Bytes']) AS bytes_from_disk,
	toUInt64(ProfileEvents['DiskReadElapsedMicroseconds'] / 1000) AS disk_ms,
	toUInt64(ProfileEvents['NetworkReceiveElapsedMicroseconds'] / 1000) AS net_ms,
	log_comment AS tag,
	query,
	toInt64(exception_code) AS exception_code,
	substring(exception, 1, 500) AS exception
FROM %s
WHERE (query_id = '%s' OR initial_query_id = '%s') AND type != 'QueryStart'
ORDER BY coordinator DESC, duration_ms DESC
LIMIT 64`, source, storage.EscCHStr(queryID), storage.EscCHStr(queryID)))
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": fmt.Sprintf("detail query failed: %v", err),
		})
		return
	}
	if rows == nil {
		rows = []map[string]interface{}{}
	}
	respondJSON(w, http.StatusOK, map[string]interface{}{"success": true, "shards": rows})
}

// validActivityQueryID mirrors HandleKillQuery's check: ids are UUID-like, with an
// optional Bifract prefix.
func validActivityQueryID(id string) bool {
	if len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '-' || c == '_') {
			return false
		}
	}
	return true
}
