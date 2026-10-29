package ingest

import (
	"fmt"
	"testing"
	"time"

	"bifract/pkg/spool"
	"bifract/pkg/storage"
)

func entries(n int, fractalID string) []storage.LogEntry {
	out := make([]storage.LogEntry, n)
	for i := range out {
		raw := fmt.Sprintf(`{"i":%d}`, i)
		out[i] = storage.LogEntry{FractalID: fractalID, RawLog: raw, LogID: fmt.Sprintf("id-%d", i),
			Fields: map[string]string{"i": fmt.Sprint(i)}}
	}
	return out
}

// TestEnqueueRejectionReasons: every backpressure condition refuses the whole
// batch, and the drop is counted against the condition that caused it, which is
// what the system-fractal drop events and notifications report.
func TestEnqueueRejectionReasons(t *testing.T) {
	type counters struct{ queueDrops, pendingQueue, conditionDrops int64 }
	tests := []struct {
		name      string
		setup     func(q *IngestQueue) *pressureState
		accept    bool
		counters  counters
		failReset bool
	}{
		{
			name: "recent insert failures",
			setup: func(q *IngestQueue) *pressureState {
				q.consecutiveFailures.Store(unhealthyThreshold)
				q.lastFailureUnix.Store(time.Now().Unix())
				return nil
			},
			counters: counters{queueDrops: 3, pendingQueue: 3},
		},
		{
			// Without the cooldown nothing would ever be accepted again, so no
			// insert could succeed and clear the failure count.
			name: "insert failures past the cooldown",
			setup: func(q *IngestQueue) *pressureState {
				q.consecutiveFailures.Store(unhealthyThreshold + 5)
				q.lastFailureUnix.Store(time.Now().Add(-unhealthyCooldown - 2*time.Second).Unix())
				return nil
			},
			accept:    true,
			failReset: true,
		},
		{"cpu", func(q *IngestQueue) *pressureState { q.cpuState.active.Store(1); return &q.cpuState }, false, counters{3, 0, 3}, false},
		{"memory", func(q *IngestQueue) *pressureState { q.memState.active.Store(1); return &q.memState }, false, counters{3, 0, 3}, false},
		{"disk", func(q *IngestQueue) *pressureState { q.diskState.active.Store(1); return &q.diskState }, false, counters{3, 0, 3}, false},
		{"spool", func(q *IngestQueue) *pressureState { q.spoolState.active.Store(1); return &q.spoolState }, false, counters{3, 0, 3}, false},
		{"accumulator over its byte cap", func(q *IngestQueue) *pressureState {
			q.accumBytes.Store(q.bufferBytes + 1)
			return nil
		}, false, counters{3, 3, 0}, false},
		{"queue past half capacity", func(q *IngestQueue) *pressureState {
			// Rows held by the accumulator count toward depth even though the
			// channel itself is empty.
			q.accumRows.Store(int64(q.bufSize/2) * maxEnqueueBatch)
			return nil
		}, false, counters{3, 3, 0}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := newBareQueue(8)
			p := tc.setup(q)
			got := q.Enqueue(entries(3, "f"))
			if got != tc.accept {
				t.Fatalf("Enqueue = %v, want %v", got, tc.accept)
			}
			if n := len(q.queued()); (n == 3) != tc.accept {
				t.Fatalf("queued %d logs", n)
			}
			if got := q.Metrics.QueueDrops.Load(); got != tc.counters.queueDrops {
				t.Errorf("QueueDrops = %d, want %d", got, tc.counters.queueDrops)
			}
			if got := q.pendingDropsQueue.Load(); got != tc.counters.pendingQueue {
				t.Errorf("pendingDropsQueue = %d, want %d", got, tc.counters.pendingQueue)
			}
			if p != nil {
				if p.pendingDrops.Load() != tc.counters.conditionDrops || p.dropsActive.Load() != tc.counters.conditionDrops {
					t.Errorf("%s drops pending=%d active=%d, want %d", p.label, p.pendingDrops.Load(), p.dropsActive.Load(), tc.counters.conditionDrops)
				}
			}
			if tc.accept && q.Metrics.Accepted.Load() != 3 {
				t.Errorf("Accepted = %d, want 3", q.Metrics.Accepted.Load())
			}
			if tc.failReset && q.ConsecutiveFailures() != 0 {
				t.Errorf("failure count not reset after cooldown: %d", q.ConsecutiveFailures())
			}
		})
	}
}

// TestEnqueueDepthLimitIsAllOrNothing: a batch that would need more slots than
// remain under the depth limit is refused whole. Accepting part of it would make
// the client's retry duplicate the accepted part.
func TestEnqueueDepthLimitIsAllOrNothing(t *testing.T) {
	q := newBareQueue(4) // half capacity: 2 slots
	if !q.Enqueue(entries(1, "f")) || !q.Enqueue(entries(1, "f")) {
		t.Fatal("first two single-slot batches should fit")
	}
	if q.Enqueue(entries(1, "f")) {
		t.Fatal("third batch exceeded half capacity but was accepted")
	}
	q.queued()

	big := entries(2*maxEnqueueBatch+1, "f") // needs 3 slots
	if q.Enqueue(big) {
		t.Fatal("a 3-slot batch was accepted into a 2-slot budget")
	}
	if n := len(q.queued()); n != 0 {
		t.Fatalf("partial enqueue: %d logs queued from a refused batch", n)
	}
}

// TestEnqueueSplitsLargeBatches bounds per-slot memory: a large batch is spread
// over slots of at most maxEnqueueBatch, with every log present exactly once.
func TestEnqueueSplitsLargeBatches(t *testing.T) {
	q := newBareQueue(16)
	n := 2*maxEnqueueBatch + 7
	if !q.Enqueue(entries(n, "f")) {
		t.Fatal("rejected")
	}
	var sizes []int
	seen := map[string]bool{}
	for len(q.ch) > 0 {
		b := <-q.ch
		sizes = append(sizes, len(b))
		for _, e := range b {
			if seen[e.LogID] {
				t.Fatalf("log %s queued twice", e.LogID)
			}
			seen[e.LogID] = true
		}
	}
	if fmt.Sprint(sizes) != fmt.Sprint([]int{maxEnqueueBatch, maxEnqueueBatch, 7}) {
		t.Fatalf("slot sizes = %v", sizes)
	}
	if len(seen) != n || q.Metrics.Accepted.Load() != int64(n) {
		t.Fatalf("queued %d distinct logs, Accepted %d; want %d", len(seen), q.Metrics.Accepted.Load(), n)
	}
}

// TestEnqueueSettlesIngestTimestamp: the partition key must be fixed before the
// batch is spooled or queued, and an ingest time stamped at parse is kept.
func TestEnqueueSettlesIngestTimestamp(t *testing.T) {
	q := newBareQueue(8)
	stamped := time.Date(2026, 7, 1, 23, 59, 59, 0, time.UTC)
	batch := entries(2, "f")
	batch[0].IngestTimestamp = stamped
	if !q.Enqueue(batch) {
		t.Fatal("rejected")
	}
	got := q.queued()
	if !got[0].IngestTimestamp.Equal(stamped) {
		t.Errorf("parse-time ingest timestamp overwritten: %v", got[0].IngestTimestamp)
	}
	if got[1].IngestTimestamp.IsZero() {
		t.Error("missing ingest timestamp was not settled")
	}
}

func newSpool(t *testing.T) (*spool.Writer, string) {
	t.Helper()
	dir := t.TempDir()
	w, err := spool.NewWriter(spool.WriterOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w, dir
}

func readSpool(t *testing.T, dir string) []storage.LogEntry {
	t.Helper()
	r, err := spool.NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var out []storage.LogEntry
	for {
		b, err := r.Next()
		if err == spool.ErrNoData {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, b.Logs...)
	}
}

// TestEnqueueArchiveTee: with archiving on, an accepted batch is durably in the
// spool with the same log_id and ingest timestamp ClickHouse will get; with a
// spool write failure the batch is refused, never acked unarchived.
func TestEnqueueArchiveTee(t *testing.T) {
	t.Run("enabled", func(t *testing.T) {
		q := newBareQueue(8)
		w, dir := newSpool(t)
		q.SetSpool(w, 1<<30)
		q.SetArchiveEnabled(true)
		if !q.ArchiveEnabled() || !q.SpoolProvisioned() || q.SpoolMaxBytes() != 1<<30 {
			t.Fatal("spool accessors disagree with the configuration")
		}
		if !q.Enqueue(entries(3, "f")) {
			t.Fatal("rejected")
		}
		queued, spooled := q.queued(), readSpool(t, dir)
		if len(spooled) != 3 {
			t.Fatalf("spooled %d logs, want 3", len(spooled))
		}
		for i := range queued {
			if spooled[i].LogID != queued[i].LogID || !spooled[i].IngestTimestamp.Equal(queued[i].IngestTimestamp) {
				t.Errorf("log %d: spool and queue disagree: %+v vs %+v", i, spooled[i], queued[i])
			}
		}
		if q.SpoolUsageBytes() == 0 {
			t.Error("SpoolUsageBytes = 0 after a spooled batch")
		}
	})
	t.Run("disabled", func(t *testing.T) {
		q := newBareQueue(8)
		w, dir := newSpool(t)
		q.SetSpool(w, 1<<30)
		if !q.Enqueue(entries(3, "f")) {
			t.Fatal("rejected")
		}
		if got := readSpool(t, dir); len(got) != 0 {
			t.Fatalf("disabled archive spooled %d logs", len(got))
		}
	})
	t.Run("spool write fails", func(t *testing.T) {
		q := newBareQueue(8)
		w, _ := newSpool(t)
		q.SetSpool(w, 1<<30)
		q.SetArchiveEnabled(true)
		w.Close()
		if q.Enqueue(entries(3, "f")) {
			t.Fatal("batch acked although it could not be archived")
		}
		if n := len(q.queued()); n != 0 {
			t.Fatalf("%d logs queued without being archived", n)
		}
		if q.spoolState.pendingDrops.Load() != 3 || q.Metrics.QueueDrops.Load() != 3 {
			t.Errorf("drops not attributed to the spool: pending=%d queue=%d", q.spoolState.pendingDrops.Load(), q.Metrics.QueueDrops.Load())
		}
		if q.spoolState.Active() {
			t.Error("a write failure must not flip the capacity backpressure state")
		}
	})
	t.Run("not provisioned", func(t *testing.T) {
		q := newBareQueue(8)
		q.SetArchiveEnabled(true)
		if !q.Enqueue(entries(1, "f")) {
			t.Fatal("enabled-but-unprovisioned archive must not block ingest")
		}
		if q.SpoolProvisioned() || q.SpoolUsageBytes() != 0 || q.SpoolMaxBytes() != 0 {
			t.Fatal("unprovisioned spool reports state")
		}
	})
}

// TestMonitorSpoolHysteresis: spool backpressure trips at 90% of capacity and
// releases only below 70%.
func TestMonitorSpoolHysteresis(t *testing.T) {
	q := newBareQueue(8)
	w, _ := newSpool(t)
	if err := w.Append(entries(50, "f")); err != nil {
		t.Fatal(err)
	}
	used, _ := w.DiskUsage()
	q.SetSpool(w, used) // 100%
	q.monitorSpool()
	if !q.SpoolPressure() || q.Healthy() {
		t.Fatal("full spool did not raise backpressure")
	}
	q.spoolMaxBytes = used * 100 / 80 // 80%: inside the hysteresis band
	q.monitorSpool()
	if !q.SpoolPressure() {
		t.Fatal("released inside the hysteresis band")
	}
	q.spoolMaxBytes = used * 2 // 50%
	q.monitorSpool()
	if q.SpoolPressure() {
		t.Fatal("still active well below the release mark")
	}

	// No capacity configured: never evaluated.
	q.spoolMaxBytes = 0
	q.spoolState.active.Store(1)
	q.monitorSpool()
	if !q.SpoolPressure() {
		t.Fatal("monitorSpool acted without a capacity")
	}
}

func TestHealthy(t *testing.T) {
	now := time.Now().Unix()
	tests := []struct {
		name  string
		setup func(q *IngestQueue)
		want  bool
	}{
		{"idle", func(q *IngestQueue) {}, true},
		{"failures below threshold", func(q *IngestQueue) {
			q.consecutiveFailures.Store(unhealthyThreshold - 1)
			q.lastFailureUnix.Store(now)
		}, true},
		{"failures at threshold", func(q *IngestQueue) {
			q.consecutiveFailures.Store(unhealthyThreshold)
			q.lastFailureUnix.Store(now)
		}, false},
		{"failures cooled down", func(q *IngestQueue) {
			q.consecutiveFailures.Store(unhealthyThreshold)
			q.lastFailureUnix.Store(now - int64(unhealthyCooldown.Seconds()) - 1)
		}, true},
		{"cooled down but a worker is still retrying", func(q *IngestQueue) {
			// An attempt that hangs longer than the cooldown must not reopen the gate.
			q.consecutiveFailures.Store(unhealthyThreshold)
			q.lastFailureUnix.Store(now - int64(unhealthyCooldown.Seconds()) - 1)
			q.retrying.Store(1)
		}, false},
		{"cpu", func(q *IngestQueue) { q.cpuState.active.Store(1) }, false},
		{"memory", func(q *IngestQueue) { q.memState.active.Store(1) }, false},
		{"disk", func(q *IngestQueue) { q.diskState.active.Store(1) }, false},
		{"spool", func(q *IngestQueue) { q.spoolState.active.Store(1) }, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := newBareQueue(8)
			tc.setup(q)
			if got := q.Healthy(); got != tc.want {
				t.Fatalf("Healthy = %v, want %v", got, tc.want)
			}
			if got := q.Enqueue(entries(1, "f1")); got != tc.want {
				t.Fatalf("Enqueue = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEnvInt(t *testing.T) {
	const key = "BIFRACT_TEST_ENV_INT"
	for _, tc := range []struct {
		val  string
		want int
	}{{"", 7}, {"12", 12}, {"0", 7}, {"-3", 7}, {"abc", 7}, {"1.5", 7}} {
		t.Setenv(key, tc.val)
		if got := envInt(key, 7); got != tc.want {
			t.Errorf("%q -> %d, want %d", tc.val, got, tc.want)
		}
	}
}
