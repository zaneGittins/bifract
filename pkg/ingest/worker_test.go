package ingest

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"

	"bifract/internal/fakedb"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// logsBatches returns the prepared inserts into the logs table (not logs_raw).
func logsBatches(ch *fakedb.CH) []*fakedb.Batch {
	var out []*fakedb.Batch
	for _, b := range ch.Batches() {
		if strings.HasPrefix(b.Query, "INSERT INTO logs (") {
			out = append(out, b)
		}
	}
	return out
}

// Column positions in the logs insert: timestamp, log_id, fields, fractal_id,
// ingest_timestamp, normalizer.
const (
	colTimestamp = iota
	colLogID
	colFields
	colFractal
	colIngestTS
	colNormalizer
)

func runWorker(q *IngestQueue, buckets ...*partBucket) {
	q.flushCh = make(chan *partBucket, len(buckets))
	for _, b := range buckets {
		q.flushCh <- b
	}
	close(q.flushCh)
	q.wg.Add(1)
	q.worker(0)
}

func TestWorkerInsertsBucketAndRecordsQuota(t *testing.T) {
	client, ch := storagetest.ClickHouse()
	qm := quotaFor("f1", 1<<30, 0, "reject")
	q := &IngestQueue{db: client, quotaManager: qm}
	q.consecutiveFailures.Store(2)

	ts := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	logs := entries(3, "f1")
	for i := range logs {
		logs[i].Timestamp = ts
		logs[i].IngestTimestamp = ts.Add(time.Hour)
		logs[i].Normalizer = "n@1"
	}
	runWorker(q, &partBucket{key: partKeyOf(&logs[0]), entries: logs}, &partBucket{key: partKey{fractalID: "f1"}})

	bs := logsBatches(ch)
	if len(bs) != 1 || !bs[0].Sent || len(bs[0].Appended) != 3 {
		t.Fatalf("logs batches = %+v, want one sent batch of 3 (the empty bucket skipped)", bs)
	}
	row := bs[0].Appended[1]
	if row[colLogID] != "id-1" || row[colFractal] != "f1" || row[colNormalizer] != "n@1" ||
		!row[colTimestamp].(time.Time).Equal(ts) || !row[colIngestTS].(time.Time).Equal(ts.Add(time.Hour)) {
		t.Errorf("row = %v", row)
	}
	if f, _ := row[colFields].(map[string]string); f["i"] != "1" {
		t.Errorf("fields column = %v", row[colFields])
	}
	if q.Metrics.Inserted.Load() != 3 || q.ConsecutiveFailures() != 0 {
		t.Errorf("Inserted=%d failures=%d, want 3 and reset to 0", q.Metrics.Inserted.Load(), q.ConsecutiveFailures())
	}
	var raw int64
	for _, l := range logs {
		raw += int64(len(l.RawLog))
	}
	if got := qm.state["f1"].deltaBytes.Load(); got != raw {
		t.Errorf("quota delta = %d, want %d raw bytes", got, raw)
	}
	if got := qm.state["f1"].deltaCount.Load(); got != 3 {
		t.Errorf("quota count delta = %d, want 3", got)
	}
}

// fastRetry keeps the backoff short so retry tests run in milliseconds.
func fastRetry(window time.Duration) retryPolicy {
	return retryPolicy{initialDelay: time.Millisecond, maxDelay: 2 * time.Millisecond, window: window, shutdownWindow: window}
}

// failLogsInserts applies set to each logs insert for which fail(attempt) holds.
func failLogsInserts(ch *fakedb.CH, fail func(attempt int32) bool, set func(*fakedb.Batch)) *atomic.Int32 {
	var attempts atomic.Int32
	ch.OnBatch = func(b *fakedb.Batch) {
		if strings.HasPrefix(b.Query, "INSERT INTO logs (") && fail(attempts.Add(1)) {
			set(b)
		}
	}
	return &attempts
}

func always(int32) bool { return true }

func connReset(b *fakedb.Batch) { b.SendErr = errors.New("connection reset") }

// tooManyParts is a server rejection that clears once merges catch up.
func tooManyParts(b *fakedb.Batch) { b.SendErr = &proto.Exception{Code: 252, Name: "TOO_MANY_PARTS"} }

// TestWorkerRetriesThroughShortOutage: a ClickHouse restart that outlasts a few
// attempts must not drop logs that were already acked.
func TestWorkerRetriesThroughShortOutage(t *testing.T) {
	client, ch := storagetest.ClickHouse()
	// Connection errors while ClickHouse is down, then TOO_MANY_PARTS as it recovers.
	var failed int
	attempts := failLogsInserts(ch, func(a int32) bool { return a <= 6 }, func(b *fakedb.Batch) {
		if failed++; failed <= 3 {
			connReset(b)
		} else {
			tooManyParts(b)
		}
	})
	q := &IngestQueue{db: client, retry: fastRetry(time.Minute)}

	runWorker(q, &partBucket{key: partKey{fractalID: "f1"}, entries: entries(4, "f1")})

	if got := attempts.Load(); got != 7 {
		t.Fatalf("insert attempted %d times, want 7", got)
	}
	if q.Metrics.Inserted.Load() != 4 || q.Metrics.InsertErrors.Load() != 0 || q.Metrics.Retries.Load() != 6 {
		t.Errorf("inserted=%d insertErrors=%d retries=%d, want 4, 0, 6",
			q.Metrics.Inserted.Load(), q.Metrics.InsertErrors.Load(), q.Metrics.Retries.Load())
	}
	if q.ConsecutiveFailures() != 0 {
		t.Errorf("failures=%d after a successful insert, want 0", q.ConsecutiveFailures())
	}
}

// TestWorkerGivesUpAfterRetryWindow: once the window closes the bucket is
// dropped, the queue reports unhealthy, and the drop is notified and recorded.
func TestWorkerGivesUpAfterRetryWindow(t *testing.T) {
	client, ch := storagetest.ClickHouse()
	n := &fakeNotifier{}
	q := &IngestQueue{db: client, notifWriter: n, retry: fastRetry(200 * time.Millisecond)}
	// Fail until the worker drops the bucket, so the insert_error event can land.
	failLogsInserts(ch, func(int32) bool { return q.Metrics.InsertErrors.Load() == 0 }, connReset)
	q.SetSystemFractalID("sys")

	start := time.Now()
	runWorker(q, &partBucket{key: partKey{fractalID: "f1"}, entries: entries(4, "f1")})
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Errorf("gave up after %v, before the retry window", elapsed)
	}

	if q.Metrics.Retries.Load() < 1 || q.Metrics.InsertErrors.Load() != 4 || q.Metrics.Inserted.Load() != 0 {
		t.Errorf("retries=%d insertErrors=%d inserted=%d", q.Metrics.Retries.Load(), q.Metrics.InsertErrors.Load(), q.Metrics.Inserted.Load())
	}
	if q.Healthy() {
		t.Error("queue still healthy after reaching the failure threshold")
	}
	if q.Enqueue(entries(1, "f1")) {
		t.Error("Enqueue accepted while inserts are failing")
	}

	calls := n.waitFor(t, 1)
	if calls[0].notifType != "ingest.insert_errors" || calls[0].severity != "critical" {
		t.Errorf("notification = %+v", calls[0])
	}

	// The insert_error system event lands in the system fractal.
	ev := waitForSystemEvent(t, ch, "ingest.insert_error")
	if ev[colFractal] != "sys" || ev[colFields].(map[string]string)["batch_size"] != "4" {
		t.Errorf("system event row = %v", ev)
	}
}

// TestBriefOutageDoesNotNotify: a ClickHouse blip that retries ride out loses
// nothing, so it must not raise a critical notification.
func TestBriefOutageDoesNotNotify(t *testing.T) {
	client, ch := storagetest.ClickHouse()
	failLogsInserts(ch, func(a int32) bool { return a <= 4 }, connReset)
	q := &IngestQueue{db: client, notifWriter: &fakeNotifier{}, retry: fastRetry(time.Minute)}

	runWorker(q, &partBucket{key: partKey{fractalID: "f1"}, entries: entries(4, "f1")})

	if q.Metrics.Inserted.Load() != 4 {
		t.Fatalf("inserted=%d, want 4", q.Metrics.Inserted.Load())
	}
	if q.lastInsertErrNotify.Load() != 0 {
		t.Error("notified for an outage the retries rode out")
	}
}

// TestSustainedFailureNotifies: inserts failing for over insertErrorNotifyAfter
// notify while retries continue, before anything is dropped.
func TestSustainedFailureNotifies(t *testing.T) {
	client, ch := storagetest.ClickHouse()
	failLogsInserts(ch, func(a int32) bool { return a <= 1 }, connReset)
	n := &fakeNotifier{}
	q := &IngestQueue{db: client, notifWriter: n, retry: fastRetry(time.Minute)}
	q.failingSince.Store(time.Now().Add(-insertErrorNotifyAfter - time.Second).Unix())

	runWorker(q, &partBucket{key: partKey{fractalID: "f1"}, entries: entries(4, "f1")})

	calls := n.waitFor(t, 1)
	if calls[0].notifType != "ingest.insert_errors" || calls[0].severity != "critical" {
		t.Errorf("notification = %+v", calls[0])
	}
	if q.failingSince.Load() != 0 {
		t.Error("failure streak not cleared after a successful insert")
	}
}

// TestWorkerLimitsRetriesOnServerRejection: a batch ClickHouse rejects for a
// reason a restart will not fix keeps the short retry budget, so it cannot hold a
// worker and the health gate for the whole window.
func TestWorkerLimitsRetriesOnServerRejection(t *testing.T) {
	client, ch := storagetest.ClickHouse()
	attempts := failLogsInserts(ch, always, func(b *fakedb.Batch) {
		b.SendErr = &proto.Exception{Code: 53, Name: "TYPE_MISMATCH"}
	})
	q := &IngestQueue{db: client, retry: fastRetry(time.Hour)}

	runWorker(q, &partBucket{key: partKey{fractalID: "f1"}, entries: entries(4, "f1")})

	if got := attempts.Load(); got != maxRejectedAttempts {
		t.Fatalf("insert attempted %d times, want %d", got, maxRejectedAttempts)
	}
	if q.Metrics.InsertErrors.Load() != 4 {
		t.Errorf("insertErrors=%d, want 4", q.Metrics.InsertErrors.Load())
	}
}

// TestWorkerDropsInvalidBatchWithoutRetry: a batch the client cannot encode will
// never succeed, so it must not hold a worker for the whole retry window.
func TestWorkerDropsInvalidBatchWithoutRetry(t *testing.T) {
	client, ch := storagetest.ClickHouse()
	attempts := failLogsInserts(ch, always, func(b *fakedb.Batch) { b.AppendErr = errors.New("unsupported type") })
	q := &IngestQueue{db: client, retry: fastRetry(time.Hour)}

	runWorker(q, &partBucket{key: partKey{fractalID: "f1"}, entries: entries(4, "f1")})

	if got := attempts.Load(); got != 1 {
		t.Fatalf("insert attempted %d times, want 1", got)
	}
	if q.Metrics.Retries.Load() != 0 || q.Metrics.InsertErrors.Load() != 4 {
		t.Errorf("retries=%d insertErrors=%d, want 0 and 4", q.Metrics.Retries.Load(), q.Metrics.InsertErrors.Load())
	}
	// A batch the client cannot encode says nothing about ClickHouse health.
	if q.ConsecutiveFailures() != 0 || q.retrying.Load() != 0 {
		t.Errorf("failures=%d retrying=%d, want 0 and 0", q.ConsecutiveFailures(), q.retrying.Load())
	}
}

// TestShutdownDrainIsBoundedAcrossBuckets: a ClickHouse outage during shutdown
// must not hold the process past its grace period, however many buckets are
// buffered. The shutdown window is shared, not granted per bucket.
func TestShutdownDrainIsBoundedAcrossBuckets(t *testing.T) {
	client, ch := storagetest.ClickHouse()
	failLogsInserts(ch, always, connReset)
	q := &IngestQueue{db: client, stop: make(chan struct{}),
		retry: retryPolicy{initialDelay: time.Millisecond, maxDelay: 2 * time.Millisecond, window: time.Hour, shutdownWindow: 100 * time.Millisecond}}
	q.beginDrain()

	buckets := make([]*partBucket, 20)
	for i := range buckets {
		buckets[i] = &partBucket{key: partKey{fractalID: "f1"}, entries: entries(1, "f1")}
	}
	start := time.Now()
	runWorker(q, buckets...)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("drain took %v for a 100ms shutdown window", elapsed)
	}
	if q.Metrics.InsertErrors.Load() != 20 {
		t.Errorf("insertErrors=%d, want 20", q.Metrics.InsertErrors.Load())
	}
}

// TestAttemptTimeoutCappedByShutdown: an insert that hangs must not outlive the
// shutdown window by its full per-attempt timeout.
func TestAttemptTimeoutCappedByShutdown(t *testing.T) {
	q := &IngestQueue{retry: retryPolicy{window: time.Hour, shutdownWindow: 50 * time.Millisecond}}
	q.beginDrain()
	ctx, cancel := q.attemptContext(time.Now().Add(time.Hour))
	defer cancel()
	d, _ := ctx.Deadline()
	if left := time.Until(d); left > minAttemptTimeout+time.Second {
		t.Fatalf("attempt may run %v into shutdown", left)
	}
}

// waitForRawInserts waits until n logs_raw inserts have been prepared. A system
// event goroutine prepares its logs_raw batch only after its logs batch was sent,
// and fakedb records batches under a lock, so once n have appeared the n event
// inserts are complete and safe to read.
func waitForRawInserts(t *testing.T, ch *fakedb.CH, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		raw := 0
		for _, b := range ch.Batches() {
			if strings.HasPrefix(b.Query, "INSERT INTO logs_raw (") {
				raw++
			}
		}
		if raw >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("fewer than %d system event inserts completed", n)
}

// systemEvents returns the fields of every sent system event, keyed by event name.
func systemEvents(ch *fakedb.CH) map[string][][]any {
	out := map[string][][]any{}
	for _, b := range logsBatches(ch) {
		if !b.Sent {
			continue
		}
		for _, row := range b.Appended {
			if f, _ := row[colFields].(map[string]string); f["event"] != "" {
				out[f["event"]] = append(out[f["event"]], row)
			}
		}
	}
	return out
}

// waitForSystemEvent waits for the single asynchronous system-fractal insert and
// returns the row for event.
func waitForSystemEvent(t *testing.T, ch *fakedb.CH, event string) []any {
	t.Helper()
	waitForRawInserts(t, ch, 1)
	rows := systemEvents(ch)[event]
	if len(rows) != 1 {
		t.Fatalf("system event %q inserted %d times, want 1", event, len(rows))
	}
	return rows[0]
}

func TestWriteSystemEvent(t *testing.T) {
	client, ch := storagetest.ClickHouse()
	q := &IngestQueue{db: client}

	q.WriteSystemEvent("ingest.test", map[string]string{"k": "v"})
	time.Sleep(20 * time.Millisecond)
	if n := len(ch.Batches()); n != 0 {
		t.Fatalf("event written with no system fractal configured (%d batches)", n)
	}

	q.SetSystemFractalID("sys")
	q.WriteSystemEvent("ingest.test", map[string]string{"k": "v"})
	row := waitForSystemEvent(t, ch, "ingest.test")
	fields := row[colFields].(map[string]string)
	if row[colFractal] != "sys" || fields["k"] != "v" {
		t.Fatalf("row = %v", row)
	}
	ts := row[colTimestamp].(time.Time)
	if !row[colIngestTS].(time.Time).Equal(ts) || row[colLogID] != storage.GenerateLogID(ts, `{"event":"ingest.test","k":"v"}`) {
		t.Errorf("event row timestamps/log id inconsistent: %v", row)
	}
}

// TestFlushDropEvents: rejected-log counts are reported per reason, then reset,
// and at most every 30 seconds.
func TestFlushDropEvents(t *testing.T) {
	client, ch := storagetest.ClickHouse()
	q := newBareQueue(8)
	q.db = client
	q.SetSystemFractalID("sys")
	q.diskState = pressureState{reason: "disk_pressure"}
	q.cpuState = pressureState{reason: "cpu_pressure"}
	q.memState = pressureState{reason: "memory_pressure"}
	q.spoolState = pressureState{reason: "spool_pressure"}

	q.diskState.pendingDrops.Store(7)
	q.pendingDropsQueue.Store(2)
	q.flushDropEvents()

	waitForRawInserts(t, ch, 2)
	got := map[string]string{}
	for _, row := range systemEvents(ch)["ingest.drops"] {
		f := row[colFields].(map[string]string)
		got[f["reason"]] = f["count"]
	}
	if got["disk_pressure"] != "7" || got["queue_full"] != "2" || len(got) != 2 {
		t.Fatalf("drop events = %v, want disk_pressure=7 and queue_full=2 only", got)
	}
	if q.diskState.pendingDrops.Load() != 0 || q.pendingDropsQueue.Load() != 0 {
		t.Fatal("pending drop counts not reset after flushing")
	}

	before := len(ch.Batches())
	q.diskState.pendingDrops.Store(1)
	q.flushDropEvents()
	time.Sleep(20 * time.Millisecond)
	if len(ch.Batches()) != before || q.diskState.pendingDrops.Load() != 1 {
		t.Fatal("drop events flushed again within 30 seconds")
	}
}

// metricsCH scripts a ClickHouse whose CPU, memory and disk readings the test
// changes between monitor passes.
type metricsCH struct {
	mu                  sync.Mutex
	cpu, mem            float64
	diskTotal, diskFree uint64
	cpuErr, noDisk      bool
}

func (m *metricsCH) set(fn func(*metricsCH)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fn(m)
}

func newMetricsQueue(t *testing.T) (*IngestQueue, *metricsCH, *fakeNotifier) {
	t.Helper()
	client, ch := storagetest.ClickHouse()
	m := &metricsCH{diskTotal: 100, diskFree: 50}
	metricRows := func(vals map[string]float64) fakedb.CHResult {
		r := fakedb.CHResult{Cols: []fakedb.Col{{Column: "metric", Type: "String"}, {Column: "value", Type: "Float64"}}}
		for k, v := range vals {
			r.Rows = append(r.Rows, []any{k, v})
		}
		return r
	}
	ch.Respond(func(query string, _ []any) (fakedb.CHResult, bool) {
		m.mu.Lock()
		defer m.mu.Unlock()
		switch {
		case strings.Contains(query, "OSIdleTime"):
			if m.cpuErr {
				return fakedb.CHResult{Err: errors.New("timeout")}, true
			}
			return metricRows(map[string]float64{"OSUserTime": m.cpu, "OSIdleTime": 100 - m.cpu}), true
		case strings.Contains(query, "OSMemoryTotal"):
			return metricRows(map[string]float64{"MemoryResident": m.mem, "OSMemoryTotal": 100}), true
		case strings.Contains(query, "system.disks"):
			r := fakedb.CHResult{Cols: []fakedb.Col{{Column: "total_space", Type: "UInt64"}, {Column: "free_space", Type: "UInt64"}}}
			if !m.noDisk {
				r.Rows = [][]any{{m.diskTotal, m.diskFree}}
			}
			return r, true
		}
		return fakedb.CHResult{}, false
	})
	n := &fakeNotifier{}
	q := newBareQueue(8)
	q.db = client
	q.notifWriter = n
	q.cpuState = pressureState{label: "CPU", reason: "cpu_pressure", notif: "ingest.cpu_pressure", title: "CPU"}
	q.memState = pressureState{label: "Memory", reason: "memory_pressure", notif: "ingest.memory_pressure", title: "Memory"}
	q.diskState = pressureState{label: "Disk", reason: "disk_pressure", notif: "ingest.disk_pressure", title: "Disk"}
	q.spoolState = pressureState{label: "Spool", reason: "spool_pressure", notif: "ingest.spool_pressure", title: "Spool"}
	return q, m, n
}

// TestCPUPressureNeedsSustainedLoad: a single heavy query must not interrupt
// ingest; CPU backpressure needs cpuPressureSustainSamples consecutive hot polls,
// holds through the 60-80% band, and releases below 60%. An unmeasurable poll
// restarts the streak rather than counting as either hot or idle.
func TestCPUPressureNeedsSustainedLoad(t *testing.T) {
	q, m, n := newMetricsQueue(t)
	poll := func(times int) {
		for i := 0; i < times; i++ {
			q.pollPressure()
		}
	}

	m.set(func(m *metricsCH) { m.cpu = 85 })
	poll(int(cpuPressureSustainSamples) - 1)
	if q.CPUPressure() {
		t.Fatal("activated before the sustain window")
	}
	m.set(func(m *metricsCH) { m.cpuErr = true })
	poll(1)
	m.set(func(m *metricsCH) { m.cpuErr = false })
	poll(int(cpuPressureSustainSamples) - 1)
	if q.CPUPressure() {
		t.Fatal("an unmeasurable poll did not restart the streak")
	}
	poll(1)
	if !q.CPUPressure() || q.Healthy() {
		t.Fatal("sustained load did not activate CPU backpressure")
	}
	if c := n.waitFor(t, 1); c[0].notifType != "ingest.cpu_pressure" || c[0].severity != "warning" {
		t.Errorf("activation notification = %+v", c[0])
	}

	m.set(func(m *metricsCH) { m.cpu = 70 })
	poll(1)
	if !q.CPUPressure() {
		t.Fatal("released inside the hysteresis band")
	}
	m.set(func(m *metricsCH) { m.cpuErr = true })
	poll(1)
	if !q.CPUPressure() {
		t.Fatal("an unmeasurable poll released backpressure")
	}
	m.set(func(m *metricsCH) { m.cpuErr = false; m.cpu = 50 })
	poll(1)
	if q.CPUPressure() {
		t.Fatal("did not release below the release mark")
	}
}

func TestMemoryPressureHysteresis(t *testing.T) {
	q, m, _ := newMetricsQueue(t)
	m.set(func(m *metricsCH) { m.mem = 95 })
	for i := int64(1); i < memPressureSustainSamples; i++ {
		q.pollPressure()
	}
	if q.MemPressure() {
		t.Fatal("activated before the sustain window")
	}
	q.pollPressure()
	if !q.MemPressure() {
		t.Fatal("sustained memory pressure did not activate")
	}
	m.set(func(m *metricsCH) { m.mem = 85 })
	q.pollPressure()
	if !q.MemPressure() {
		t.Fatal("released inside the hysteresis band")
	}
	m.set(func(m *metricsCH) { m.mem = 79 })
	q.pollPressure()
	if q.MemPressure() {
		t.Fatal("did not release below 80%")
	}
}

// TestDiskPressure: disk trips immediately (no sustain), and a server that
// reports no local disk is unmeasurable, which must neither raise nor release.
func TestDiskPressure(t *testing.T) {
	q, m, _ := newMetricsQueue(t)
	m.set(func(m *metricsCH) { m.diskFree = 5 }) // 95% used
	q.pollPressure()
	if !q.DiskPressure() {
		t.Fatal("disk at 95% did not raise backpressure")
	}
	m.set(func(m *metricsCH) { m.noDisk = true })
	q.pollPressure()
	if !q.DiskPressure() {
		t.Fatal("an unmeasurable disk released backpressure")
	}
	m.set(func(m *metricsCH) { m.noDisk = false; m.diskFree = 15 }) // 85%
	q.pollPressure()
	if !q.DiskPressure() {
		t.Fatal("released inside the hysteresis band")
	}
	m.set(func(m *metricsCH) { m.diskTotal = 0 })
	q.pollPressure()
	if !q.DiskPressure() {
		t.Fatal("a zero-size disk was read as free space")
	}
	m.set(func(m *metricsCH) { m.diskTotal = 100; m.diskFree = 30 })
	q.pollPressure()
	if q.DiskPressure() {
		t.Fatal("did not release at 70%")
	}
}

func TestAsFloat64(t *testing.T) {
	for _, v := range []interface{}{float64(2), float32(2), int64(2), uint64(2), int(2)} {
		if asFloat64(v) != 2 {
			t.Errorf("asFloat64(%T) = %v", v, asFloat64(v))
		}
	}
	if asFloat64("2") != 0 || asFloat64(nil) != 0 {
		t.Error("unsupported types should read as 0")
	}
}

// TestQueueEndToEnd runs the real queue: logs for two fractals go in through
// Enqueue, and Shutdown drains every buffered key into one single-partition
// insert each, losing nothing.
func TestQueueEndToEnd(t *testing.T) {
	t.Setenv("BIFRACT_INGEST_FLUSH_SECONDS", "3600")
	client, ch := storagetest.ClickHouse()
	q := NewIngestQueue(client, 8, 2)
	q.SetMetricsClient(client)
	if q.mdb() != client || q.flushInterval != time.Hour {
		t.Fatal("configuration not applied")
	}

	if !q.Enqueue(append(entries(3, "f1"), entries(2, "f2")...)) {
		t.Fatal("rejected")
	}
	if !q.Enqueue(entries(4, "f1")) {
		t.Fatal("rejected")
	}
	q.Shutdown()

	perFractal := map[string]int{}
	for _, b := range logsBatches(ch) {
		if !b.Sent {
			t.Fatalf("unsent batch %q", b.Query)
		}
		fractals := map[any]bool{}
		for _, row := range b.Appended {
			fractals[row[colFractal]] = true
		}
		if len(fractals) != 1 {
			t.Fatalf("one insert spans fractals %v", fractals)
		}
		for f := range fractals {
			perFractal[f.(string)] += len(b.Appended)
		}
	}
	if perFractal["f1"] != 7 || perFractal["f2"] != 2 {
		t.Fatalf("inserted per fractal = %v, want f1=7 f2=2", perFractal)
	}
	if q.InsertedTotal() != 9 || q.AcceptedTotal() != 9 || q.QueueDropsTotal() != 0 || q.InsertErrorsTotal() != 0 || q.RetriesTotal() != 0 {
		t.Fatalf("metrics: inserted=%d accepted=%d drops=%d errors=%d retries=%d",
			q.InsertedTotal(), q.AcceptedTotal(), q.QueueDropsTotal(), q.InsertErrorsTotal(), q.RetriesTotal())
	}
	if q.Depth() != 0 || q.accumBytes.Load() != 0 {
		t.Fatalf("depth=%d buffered=%d after shutdown", q.Depth(), q.accumBytes.Load())
	}
	if q.CPUPressure() || q.MemPressure() || q.DiskPressure() || q.SpoolPressure() {
		t.Fatal("pressure flagged on an idle queue")
	}
	if q.Enqueue(entries(1, "f1")) {
		t.Fatal("Enqueue accepted after Shutdown")
	}
}

// TestEnqueueRacingShutdown: handlers still running when the queue shuts down
// must get a rejection, not a send on a closed channel.
func TestEnqueueRacingShutdown(t *testing.T) {
	client, _ := storagetest.ClickHouse()
	q := NewIngestQueue(client, 64, 2)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 200; j++ {
				q.Enqueue(entries(1, "f1"))
			}
		}()
	}
	close(start)
	q.Shutdown()
	wg.Wait()
	if q.AcceptedTotal() != q.InsertedTotal() {
		t.Fatalf("accepted %d logs but inserted %d", q.AcceptedTotal(), q.InsertedTotal())
	}
}
