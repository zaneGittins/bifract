package query

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/storage/storagetest"
)

// The System tab's handlers and the metrics collector behind them, against the
// scripted drivers.

func metricRows(kv ...interface{}) fakedb.CHResult {
	res := fakedb.CHResult{Cols: []fakedb.Col{{Column: "metric", Type: "String"}, {Column: "value", Type: "Float64"}}}
	for i := 0; i < len(kv); i += 2 {
		res.Rows = append(res.Rows, []any{kv[i], kv[i+1]})
	}
	return res
}

func decodeMap(t *testing.T, rec *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()
	var out map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("%d %q: %v", rec.Code, rec.Body.String(), err)
	}
	return out
}

func serve(handle http.HandlerFunc, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	handle(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

type spikeLog struct {
	mu     sync.Mutex
	dedups []string
}

func (s *spikeLog) record(dedup, severity, title, body string) {
	s.mu.Lock()
	s.dedups = append(s.dedups, dedup+"|"+severity)
	s.mu.Unlock()
}

// baselinePG scripts the spike check's three reads for every metric.
func baselinePG(pg *fakedb.PG, days int64, rolling, baseline driver.Value) {
	pg.On("count(DISTINCT date_trunc", fakedb.Row(days)).
		On("EXTRACT(hour FROM ts)", fakedb.Row(baseline)).
		On("ts > now() - interval '30 minutes'", fakedb.Row(rolling))
}

func TestMetricsCollectorCollect(t *testing.T) {
	db, ch := storagetest.ClickHouse()
	pgc, pg := storagetest.Postgres()
	ch.On("CGroupMaxCPU", metricRows("CGroupUserTimeNormalized", 0.25, "CGroupSystemTimeNormalized", 0.05, "CGroupMaxCPU", 4.0)).
		On("CGroupMemoryUsed", metricRows("CGroupMemoryUsed", 3.0, "CGroupMemoryTotal", 4.0))
	pg.On("AVG(last_execution_time_ms)", fakedb.Row(12.5)).On("DELETE FROM system_metrics", fakedb.Affected(3))
	baselinePG(pg, 5, 80.0, 40.0)
	spikes := &spikeLog{}
	mc := &MetricsCollector{db: db, pg: pgc, retentionDays: 14, onSpike: spikes.record}

	mc.collect()

	ins := pg.Matching("INSERT INTO system_metrics")
	if len(ins) != 1 {
		t.Fatalf("inserts = %+v", ins)
	}
	got := map[string]interface{}{}
	for i := 0; i+4 < len(ins[0].Args); i += 5 {
		got[ins[0].Args[i+1].(string)] = ins[0].Args[i+4]
		if ins[0].Args[i+2] != "" {
			t.Errorf("single node stored under node %q", ins[0].Args[i+2])
		}
	}
	if got["cpu"] != 30.0 || got["memory_pct"] != 75.0 || got["alert_exec_ms"] != 12.5 {
		t.Errorf("samples = %v", got)
	}
	if p := pg.Matching("DELETE FROM system_metrics"); len(p) != 1 || p[0].Args[0] != int64(14) {
		t.Errorf("prune = %+v", p)
	}
	// 80% now against a 40% same-hour baseline is a spike, for both metrics.
	if strings.Join(spikes.dedups, ",") != "health.cpu.spike|warning,health.memory_pct.spike|warning" {
		t.Errorf("spikes = %v", spikes.dedups)
	}

	// Pruning and the baseline check are rate-limited, not per-collect.
	mc.collect()
	if len(pg.Matching("DELETE FROM system_metrics")) != 1 || len(spikes.dedups) != 2 {
		t.Error("prune or baseline check ran again inside its interval")
	}
}

func TestMetricsCollectorBaselineGuards(t *testing.T) {
	cases := []struct {
		name               string
		days               int64
		rolling, baseline  driver.Value
		wantRollingQueries int
	}{
		// A fresh install has no baseline worth comparing against.
		{"too little history", 2, 80.0, 10.0, 0},
		{"no recent samples", 5, nil, 10.0, 2},
		{"no baseline", 5, 80.0, nil, 2},
		{"within the multiplier", 5, 50.0, 40.0, 2},
		// Doubling from 5% to 12% is a ratio, not a problem.
		{"below the absolute floor", 5, 12.0, 5.0, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := storagetest.ClickHouse()
			pgc, pg := storagetest.Postgres()
			baselinePG(pg, tc.days, tc.rolling, tc.baseline)
			spikes := &spikeLog{}
			mc := &MetricsCollector{db: db, pg: pgc, onSpike: spikes.record}
			mc.maybeCheckBaseline()
			if len(spikes.dedups) != 0 {
				t.Errorf("spiked: %v", spikes.dedups)
			}
			if n := len(pg.Matching("ts > now() - interval '30 minutes'")); n != tc.wantRollingQueries {
				t.Errorf("%d rolling reads, want %d", n, tc.wantRollingQueries)
			}
		})
	}
}

// With nothing measurable there is nothing to persist.
func TestMetricsCollectorNothingToStore(t *testing.T) {
	db, _ := storagetest.ClickHouse()
	pgc, pg := storagetest.Postgres()
	mc := &MetricsCollector{db: db, pg: pgc, lastPruneAt: time.Now()}
	mc.collect()
	if c := pg.Matching("INSERT"); len(c) != 0 {
		t.Errorf("stored %+v", c)
	}
}

func TestPerformanceHandlerStartsAndStopsCollector(t *testing.T) {
	db, ch := storagetest.ClickHouse()
	pgc, pg := storagetest.Postgres()
	ch.On("CGroupMaxCPU", metricRows("OSUserTime", 1.0, "OSIdleTime", 3.0))
	h := NewPerformanceHandler(db, pgc, 0, nil)
	waitFor(t, "the first collection", func() bool { return len(pg.Matching("INSERT INTO system_metrics")) == 1 })
	h.StopCollector()
	if h.collector.retentionDays != 30 {
		t.Errorf("default retention = %d", h.collector.retentionDays)
	}
	if args := pg.Matching("INSERT INTO system_metrics")[0].Args; args[1] != "cpu" || args[4] != 25.0 {
		t.Errorf("sample = %v", args)
	}
}

func TestHandleMetrics(t *testing.T) {
	db, ch := storagetest.ClickHouse()
	pgc, pg := storagetest.Postgres()
	ch.On("FROM system.metrics", metricRows("Query", 2.0, "Merge", 1.0)).
		On("FROM system.asynchronous_metrics", metricRows("Uptime", 99.0)).
		On("data_uncompressed_bytes", chResultOf([]map[string]interface{}{{"log_rows": uint64(10), "compressed_bytes": uint64(5), "uncompressed_bytes": uint64(50)}})).
		On("FROM system.disks", chResultOf([]map[string]interface{}{{"used_pct": 40.0, "free_space_readable": "6 GiB", "free_bytes": uint64(6), "total_bytes": uint64(10)}}))
	bucket := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	pg.On("FROM system_metrics", fakedb.Row(bucket, 33.33))
	h := &PerformanceHandler{db: db, pg: pgc}

	out := decodeMap(t, serve(h.HandleMetrics, "/?range=8h"))
	if m := out["metrics"].(map[string]interface{}); m["Query"] != 2.0 || m["Merge"] != 1.0 {
		t.Errorf("metrics = %v", m)
	}
	if m := out["async_metrics"].(map[string]interface{}); m["Uptime"] != 99.0 {
		t.Errorf("async = %v", m)
	}
	if s := out["log_storage"].(map[string]interface{}); s["uncompressed_bytes"] != 50.0 {
		t.Errorf("log_storage = %v", s)
	}
	if d := out["disk"].(map[string]interface{}); d["free_space"] != "6 GiB" || d["free_bytes"] != 6.0 {
		t.Errorf("disk = %v", d)
	}
	for _, k := range []string{"cpu_history", "memory_history"} {
		pts, _ := out[k].([]interface{})
		if len(pts) != 1 || pts[0].(map[string]interface{})["value"] != 33.3 || pts[0].(map[string]interface{})["time"] != float64(bucket.Unix()) {
			t.Errorf("%s = %v", k, out[k])
		}
	}
	// The range picks the persisted series' window and bucket.
	if c := pg.Matching("FROM system_metrics"); len(c) != 2 || c[0].Args[0] != int64(300) || c[0].Args[2] != int64(8*3600) {
		t.Errorf("series reads = %+v", c)
	}
	if _, ok := out["cluster"]; ok {
		t.Error("single node reported cluster gauges")
	}
}

func TestHandleMetricsCluster(t *testing.T) {
	db, ch := clusterClient(t)
	ch.On("FROM system.clusters", chResultOf([]map[string]interface{}{{"total": uint64(2), "healthy": uint64(2)}})).
		On("AS active_queries", chResultOf([]map[string]interface{}{{"active_queries": 7.0, "active_merges": 1.0}})).
		On("hostName() AS node, metric, value", chResultOf([]map[string]interface{}{
			{"node": "n1", "metric": "CGroupMemoryUsed", "value": 1.0}, {"node": "n1", "metric": "CGroupMemoryTotal", "value": 4.0},
			{"node": "n2", "metric": "CGroupMemoryUsed", "value": 3.0}, {"node": "n2", "metric": "CGroupMemoryTotal", "value": 4.0},
		}))
	out := decodeMap(t, serve((&PerformanceHandler{db: db}).HandleMetrics, "/"))
	c, _ := out["cluster"].(map[string]interface{})
	if c == nil || c["nodes_total"] != 2.0 || c["active_queries"] != 7.0 || c["mem_peak_node"] != "n2" || c["mem_peak_pct"] != 75.0 {
		t.Errorf("cluster = %v", out["cluster"])
	}
}

func TestHandleProcessesAndKill(t *testing.T) {
	t.Run("single node", func(t *testing.T) {
		db, ch := storagetest.ClickHouse()
		ch.On("FROM system.processes", chResultOf([]map[string]interface{}{{"query_id": "q1", "node": "n1"}}))
		h := &PerformanceHandler{db: db}
		out := decodeMap(t, serve(h.HandleProcesses, "/"))
		if out["count"] != 1.0 || len(out["processes"].([]interface{})) != 1 {
			t.Errorf("got %v", out)
		}

		rec := httptest.NewRecorder()
		h.HandleKillQuery(rec, httptest.NewRequest(http.MethodPost, "/?query_id=bif-search-1_a", nil))
		k := ch.ExecutedMatching("KILL QUERY")
		if rec.Code != http.StatusOK || len(k) != 1 || !strings.Contains(k[0], "query_id = 'bif-search-1_a' OR initial_query_id = 'bif-search-1_a'") {
			t.Errorf("got %d, kills %v", rec.Code, k)
		}
	})
	// system.processes is per node: a cluster reads every replica, and a kill goes
	// to every node.
	t.Run("cluster", func(t *testing.T) {
		db, ch := clusterClient(t)
		h := &PerformanceHandler{db: db}
		serve(h.HandleProcesses, "/")
		if q := ch.QueriedMatching("clusterAllReplicas('bf', system.processes)"); len(q) != 1 {
			t.Errorf("queries = %v", ch.Queried())
		}
		rec := httptest.NewRecorder()
		h.HandleKillQuery(rec, httptest.NewRequest(http.MethodPost, "/?query_id=q1", nil))
		if k := ch.ExecutedMatching("KILL QUERY ON CLUSTER"); len(k) != 1 {
			t.Errorf("kills = %v", ch.Executed())
		}
	})
	t.Run("failures", func(t *testing.T) {
		db, ch := storagetest.ClickHouse()
		ch.On("", fakedb.CHResult{Err: errors.New("boom")}).FailExec("KILL", errors.New("boom"))
		h := &PerformanceHandler{db: db}
		if rec := serve(h.HandleProcesses, "/"); rec.Code != http.StatusInternalServerError {
			t.Errorf("processes: %d", rec.Code)
		}
		rec := httptest.NewRecorder()
		h.HandleKillQuery(rec, httptest.NewRequest(http.MethodPost, "/?query_id=q1", nil))
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("kill: %d", rec.Code)
		}
	})
}

func TestHandleIngestDailyAndHotTable(t *testing.T) {
	db, ch := storagetest.ClickHouse()
	ch.On("GROUP BY partition", chResultOf([]map[string]interface{}{{"partition": "('f1','2026-10-01')", "rows": uint64(5), "raw_bytes": uint64(50), "disk_bytes": uint64(10)}})).
		On("table = 'logs_hot'", chResultOf([]map[string]interface{}{{"partition_count": uint64(3), "row_count": uint64(9), "disk_bytes": uint64(1)}}))
	h := &PerformanceHandler{db: db}

	out := decodeMap(t, serve(h.HandleIngestDaily, "/?days=1000&fractal=f1"))
	if days := out["days"].([]interface{}); len(days) < 365 || len(days) > 366 || out["fractal"] != "f1" {
		t.Errorf("days capped to %d, fractal %v", len(days), out["fractal"])
	}
	if hot := decodeMap(t, serve(h.HandleHotTableStats, "/"))["hot_table"].(map[string]interface{}); hot["partition_count"] != 3.0 {
		t.Errorf("hot = %v", hot)
	}

	db, ch = storagetest.ClickHouse()
	ch.On("", fakedb.CHResult{Err: errors.New("boom")})
	h = &PerformanceHandler{db: db}
	if rec := serve(h.HandleIngestDaily, "/"); rec.Code != http.StatusInternalServerError {
		t.Errorf("ingest daily: %d", rec.Code)
	}
	if out := decodeMap(t, serve(h.HandleHotTableStats, "/")); out["success"] != true || out["hot_table"] != nil {
		t.Errorf("hot table on failure = %v", out)
	}
}

func TestHandleActivityStream(t *testing.T) {
	db, ch := clusterClient(t)
	ch.On("system.one", fakedb.Strs("node", "n2", "n1", "")).
		On("UNION ALL", chResultOf([]map[string]interface{}{{"state": "running", "query_id": "q1"}}))
	h := &PerformanceHandler{db: db}
	out := decodeMap(t, serve(h.HandleActivityStream, "/?state=slow&q=abc&range=8h"))
	if len(out["rows"].([]interface{})) != 1 || strings.Join(toStrings(out["nodes"]), ",") != "n2,n1" {
		t.Errorf("got %v", out)
	}
	q := ch.QueriedMatching("UNION ALL")
	if len(q) != 1 || !strings.Contains(q[0], "INTERVAL 480 MINUTE") || !strings.Contains(q[0], "elapsed > 5") {
		t.Errorf("stream query = %v", q)
	}

	// Single node: no picker, and an empty stream is an empty list.
	db, _ = storagetest.ClickHouse()
	out = decodeMap(t, serve((&PerformanceHandler{db: db}).HandleActivityStream, "/"))
	if rows, ok := out["rows"].([]interface{}); !ok || len(rows) != 0 || out["nodes"] != nil {
		t.Errorf("got %v", out)
	}

	db, ch = storagetest.ClickHouse()
	ch.On("", fakedb.CHResult{Err: errors.New("boom")})
	if rec := serve((&PerformanceHandler{db: db}).HandleActivityStream, "/"); rec.Code != http.StatusInternalServerError {
		t.Errorf("failure: %d", rec.Code)
	}
}

func toStrings(v interface{}) []string {
	var out []string
	for _, x := range v.([]interface{}) {
		out = append(out, x.(string))
	}
	return out
}

func TestHandleActivitySummary(t *testing.T) {
	db, ch := storagetest.ClickHouse()
	ch.On("AS p50", chResultOf([]map[string]interface{}{{"t": uint64(1), "class": "search", "n": uint64(4)}})).
		On("AS oldest_sec", chResultOf([]map[string]interface{}{{"running": uint64(2), "oldest_sec": 1.5, "slow": uint64(0)}})).
		On("normalized_query_hash", chResultOf([]map[string]interface{}{{"hash": "h1", "runs": uint64(3)}})).
		On("AS code", chResultOf([]map[string]interface{}{{"code": int64(241), "n": uint64(1)}}))
	h := &PerformanceHandler{db: db}

	out := decodeMap(t, serve(h.HandleActivitySummary, "/?range=24h"))
	if out["window_minutes"] != 1440.0 || out["bucket_seconds"] != 3600.0 {
		t.Errorf("window = %v/%v", out["window_minutes"], out["bucket_seconds"])
	}
	if len(out["buckets"].([]interface{})) != 1 || out["running"].(map[string]interface{})["running"] != 2.0 || len(out["failures"].([]interface{})) != 1 {
		t.Errorf("summary = %v", out)
	}
	// The pattern aggregation is the page's most expensive read: opt-in only.
	if _, ok := out["patterns"]; ok || len(ch.QueriedMatching("normalized_query_hash")) != 0 {
		t.Error("patterns read without being asked for")
	}
	out = decodeMap(t, serve(h.HandleActivitySummary, "/?patterns=1"))
	if len(out["patterns"].([]interface{})) != 1 {
		t.Errorf("patterns = %v", out["patterns"])
	}

	db, ch = storagetest.ClickHouse()
	ch.On("", fakedb.CHResult{Err: errors.New("boom")})
	out = decodeMap(t, serve((&PerformanceHandler{db: db}).HandleActivitySummary, "/"))
	if out["success"] != true || out["buckets_error"] == nil || out["running"] != nil {
		t.Errorf("failed summary = %v", out)
	}
}

func TestHandleBackgroundOps(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		db, ch := storagetest.ClickHouse()
		if cluster {
			db, ch = clusterClient(t)
		}
		ch.On("system.merges", chResultOf([]map[string]interface{}{{"kind": "merge", "table": "logs"}})).
			On("system.mutations", chResultOf([]map[string]interface{}{{"kind": "mutation", "table": "logs"}})).
			On("replication_queue", chResultOf([]map[string]interface{}{{"queued": uint64(4), "postponed": uint64(1)}}))
		out := decodeMap(t, serve((&PerformanceHandler{db: db}).HandleBackgroundOps, "/"))
		if len(out["merges"].([]interface{})) != 1 || len(out["mutations"].([]interface{})) != 1 {
			t.Errorf("cluster=%v: %v", cluster, out)
		}
		// Replication backlog only exists on a cluster.
		if _, ok := out["replication"]; ok != cluster {
			t.Errorf("cluster=%v: replication = %v", cluster, out["replication"])
		}
	}
}

func TestHandleActivityDetail(t *testing.T) {
	db, ch := storagetest.ClickHouse()
	ch.OnOnce("initial_query_id", chResultOf([]map[string]interface{}{{"node": "n1", "coordinator": uint64(1)}}))
	h := &PerformanceHandler{db: db}
	if out := decodeMap(t, serve(h.HandleActivityDetail, "/?query_id=bif-search-1")); len(out["shards"].([]interface{})) != 1 {
		t.Errorf("got %v", out)
	}
	if q := ch.Queried(); !strings.Contains(q[0], "query_id = 'bif-search-1' OR initial_query_id = 'bif-search-1'") {
		t.Errorf("query = %s", q[0])
	}
	// Not logged yet: an empty list, not null.
	if out := decodeMap(t, serve(h.HandleActivityDetail, "/?query_id=q2")); out["shards"] == nil {
		t.Errorf("got %v", out)
	}
	ch.On("", fakedb.CHResult{Err: errors.New("boom")})
	if rec := serve(h.HandleActivityDetail, "/?query_id=q3"); rec.Code != http.StatusInternalServerError {
		t.Errorf("failure: %d", rec.Code)
	}
}

var alertTS = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

func TestHandleAlertStats(t *testing.T) {
	db, _ := storagetest.ClickHouse()
	pgc, pg := storagetest.Postgres()
	pg.On("PERCENTILE_CONT", fakedb.Row(int64(10), int64(2), 61.4, 300.6)).
		On("NULLS FIRST", fakedb.Row("slow-alert")).
		On("count(DISTINCT alert_id)", fakedb.Row(int64(5), int64(3), int64(400), int64(1), int64(20))).
		On("(SELECT name FROM acts WHERE NOT ok", fakedb.Row(int64(2), int64(9), "pager")).
		On("FROM system_metrics", fakedb.Row(alertTS, 41.6)).
		On("AS severity", fakedb.RowsOf([]driver.Value{int64(1000), "high", int64(3)})).
		On("WHERE NOT enabled AND", fakedb.RowsOf([]driver.Value{"a1", "noisy", "low", "f1", "", "Auto-disabled: too many matches", alertTS}))
	h := &PerformanceHandler{db: db, pg: pgc}

	out := decodeMap(t, serve(h.HandleAlertStats, "/?range=30d"))
	s := out["summary"].(map[string]interface{})
	want := map[string]interface{}{
		"evaluating": 10.0, "disabled": 2.0, "lag_p95_sec": 61.0, "lag_max_sec": 301.0, "lag_max_alert": "slow-alert",
		"fires": 5.0, "firing_alerts": 3.0, "logs_matched": 400.0, "throttled": 1.0, "logs_suppressed": 20.0,
		"action_failures": 2.0, "action_total": 9.0, "action_worst": "pager",
		// However wide the selector, the window is capped at a day.
		"window_minutes": 1440.0, "eval_interval_sec": 60.0, "lag_warn_sec": 120.0,
	}
	for k, v := range want {
		if s[k] != v {
			t.Errorf("summary[%s] = %v, want %v", k, s[k], v)
		}
	}
	if eh := out["exec_history"].([]interface{}); len(eh) != 1 || eh[0].(map[string]interface{})["avg_ms"] != 42.0 {
		t.Errorf("exec_history = %v", eh)
	}
	if fh := out["fires_history"].([]interface{}); len(fh) != 1 || fh[0].(map[string]interface{})["severity"] != "high" {
		t.Errorf("fires_history = %v", fh)
	}
	d := out["disabled_alerts"].([]interface{})
	if len(d) != 1 || d[0].(map[string]interface{})["reason"] != "too many matches" || d[0].(map[string]interface{})["since"] != "2026-10-01T12:00:00Z" {
		t.Errorf("disabled = %v", d)
	}
	if c := pg.Matching("count(DISTINCT alert_id)"); len(c) != 1 || c[0].Args[0] != int64(1440) {
		t.Errorf("fires read = %+v", c)
	}

	// Every read failing still renders the page, with zeros and empty lists.
	pgc2, pg2 := storagetest.Postgres()
	pg2.On("", fakedb.PGResult{Err: errors.New("pg")})
	h.pg = pgc2
	out = decodeMap(t, serve(h.HandleAlertStats, "/"))
	if out["success"] != true || len(out["fires_history"].([]interface{})) != 0 || len(out["exec_history"].([]interface{})) != 0 ||
		out["summary"].(map[string]interface{})["evaluating"] != 0.0 {
		t.Errorf("failed stats = %v", out)
	}
}

func TestHandleAlertRows(t *testing.T) {
	db, _ := storagetest.ClickHouse()
	pgc, pg := storagetest.Postgres()
	pg.On("LEFT JOIN fired", fakedb.RowsOf([]driver.Value{
		"a1", "noisy", "high", "f1", "", false, "Auto-disabled: boom", 12.6, int64(80), int64(4), int64(1), int64(40), int64(3), int64(1), "pager",
	})).On("ORDER BY ae.triggered_at DESC", fakedb.RowsOf([]driver.Value{
		alertTS, "noisy", "high", "f1", "", int64(4), int64(0), false, "", int64(15),
		`[{"webhook_name":"pager","success":false,"status_code":502}]`, `[]`, `[{"success":true}]`,
	}))
	h := &PerformanceHandler{db: db, pg: pgc}

	out := decodeMap(t, serve(h.HandleAlertRows, "/?q=noi&fractal=f1"))
	rows := out["rows"].([]interface{})
	if out["mode"] != "alerts" || len(rows) != 1 || out["truncated"] != false {
		t.Fatalf("got %v", out)
	}
	r := rows[0].(map[string]interface{})
	if r["disabled_reason"] != "boom" || r["lag_sec"] != 13.0 || r["action_worst"] != "pager" || r["enabled"] != false {
		t.Errorf("row = %v", r)
	}
	c := pg.Matching("LEFT JOIN fired")
	if len(c) != 1 || !strings.Contains(c[0].Query, "a.name ILIKE $2") || !strings.Contains(c[0].Query, "a.fractal_id::text = $3") ||
		c[0].Args[1] != "%noi%" || c[0].Args[2] != "f1" {
		t.Errorf("alert list read = %+v", c)
	}

	out = decodeMap(t, serve(h.HandleAlertRows, "/?mode=fires"))
	fires := out["rows"].([]interface{})
	if out["mode"] != "fires" || len(fires) != 1 {
		t.Fatalf("got %v", out)
	}
	acts := fires[0].(map[string]interface{})["actions"].([]interface{})
	if len(acts) != 2 || acts[0].(map[string]interface{})["detail"] != "HTTP 502" || acts[1].(map[string]interface{})["kind"] != "email" {
		t.Errorf("actions = %v", acts)
	}

	pgc2, pg2 := storagetest.Postgres()
	pg2.On("", fakedb.PGResult{Err: errors.New("pg")})
	h.pg = pgc2
	for _, target := range []string{"/", "/?mode=fires"} {
		if rec := serve(h.HandleAlertRows, target); rec.Code != http.StatusInternalServerError {
			t.Errorf("%s: %d", target, rec.Code)
		}
	}
}
