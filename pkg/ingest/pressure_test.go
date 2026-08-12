package ingest

import (
	"strings"
	"sync"
	"testing"
	"time"

	"bifract/pkg/storage"
)

type notifCall struct {
	notifType   string
	severity    string
	title       string
	message     string
	minInterval time.Duration
}

// fakeNotifier records notification writes. Writes are issued from goroutines,
// so calls are collected under a lock and read via waitFor.
type fakeNotifier struct {
	mu    sync.Mutex
	calls []notifCall
}

func (f *fakeNotifier) Write(notifType, severity, title, message string) error {
	return f.WriteSustained(notifType, severity, title, message, 0)
}

func (f *fakeNotifier) WriteSustained(notifType, severity, title, message string, minInterval time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, notifCall{notifType, severity, title, message, minInterval})
	return nil
}

func (f *fakeNotifier) snapshot() []notifCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]notifCall(nil), f.calls...)
}

// waitFor blocks until n calls have been recorded, or fails the test.
func (f *fakeNotifier) waitFor(t *testing.T, n int) []notifCall {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if calls := f.snapshot(); len(calls) >= n {
			return calls
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("expected %d notification(s), got %d", n, len(f.snapshot()))
	return nil
}

// quiet fails if any further call arrives within a short grace period.
func (f *fakeNotifier) quiet(t *testing.T, n int) {
	t.Helper()
	time.Sleep(50 * time.Millisecond)
	if calls := f.snapshot(); len(calls) != n {
		t.Fatalf("expected no further notifications past %d, got %d (%+v)", n, len(calls), calls[n:])
	}
}

func newPressureQueue() (*IngestQueue, *fakeNotifier) {
	f := &fakeNotifier{}
	q := &IngestQueue{notifWriter: f}
	q.diskState = pressureState{label: "Disk", reason: "disk_pressure",
		notif: "ingest.disk_pressure", title: "Ingest Disk Backpressure Active"}
	return q, f
}

// TestPressureEscalatesWhenRejectingLogs covers the lifecycle of a condition
// that is actively dropping logs: warning on activation, critical once it has
// been rejecting for pressureEscalateAfter, and an info recovery on release.
func TestPressureEscalatesWhenRejectingLogs(t *testing.T) {
	q, f := newPressureQueue()
	p := &q.diskState

	q.raisePressure(p, "95.0", "90.0", "ClickHouse disk at 95.0% used (threshold 90.0%)")
	calls := f.waitFor(t, 1)
	if calls[0].severity != "warning" || calls[0].notifType != "ingest.disk_pressure" {
		t.Fatalf("activation should notify warning, got %+v", calls[0])
	}
	if calls[0].minInterval != pressureReassert {
		t.Fatalf("warning reassert interval = %v, want %v", calls[0].minInterval, pressureReassert)
	}

	// Freshly active and nothing rejected yet: no escalation, no repeat.
	q.reassertPressure()
	f.quiet(t, 1)

	// Active past the escalation window but with no drops stays a warning: an
	// idle system under pressure is losing nothing.
	p.sinceUnix.Store(time.Now().Add(-2 * pressureEscalateAfter).Unix())
	q.reassertPressure()
	f.quiet(t, 1)

	// Rejecting logs past the window is data loss in progress.
	q.rejectFor(p, make([]storage.LogEntry, 250))
	q.reassertPressure()
	calls = f.waitFor(t, 2)
	esc := calls[1]
	if esc.severity != "critical" {
		t.Fatalf("severity = %q, want critical", esc.severity)
	}
	if !strings.Contains(esc.title, "Rejecting Logs") {
		t.Fatalf("title = %q, want a rejecting-logs title", esc.title)
	}
	if !strings.Contains(esc.message, "250 log(s) rejected") {
		t.Fatalf("message = %q, want the rejected count", esc.message)
	}
	if esc.minInterval != criticalReassert {
		t.Fatalf("critical reassert interval = %v, want %v", esc.minInterval, criticalReassert)
	}

	// Already escalated and recently notified: no duplicate every tick.
	q.reassertPressure()
	f.quiet(t, 2)

	q.clearPressure(p, "70.0")
	calls = f.waitFor(t, 3)
	if calls[2].severity != "info" || !strings.Contains(calls[2].title, "Recovered") {
		t.Fatalf("release should notify recovery, got %+v", calls[2])
	}
	if p.Active() {
		t.Fatal("condition still active after clear")
	}
}

// TestPressureReassertsWhileActive verifies a long-lived condition rewrites its
// notification once the reassert interval has elapsed, so it cannot age out of
// the 24h retention window while still shedding logs.
func TestPressureReassertsWhileActive(t *testing.T) {
	q, f := newPressureQueue()
	p := &q.diskState

	q.raisePressure(p, "95.0", "90.0", "ClickHouse disk at 95.0% used")
	f.waitFor(t, 1)

	q.reassertPressure()
	f.quiet(t, 1)

	// Last notification older than the warning reassert interval.
	p.lastNotifyUnix.Store(time.Now().Add(-pressureReassert - time.Minute).Unix())
	q.reassertPressure()
	calls := f.waitFor(t, 2)
	if calls[1].severity != "warning" {
		t.Fatalf("reassert severity = %q, want warning", calls[1].severity)
	}
}

// TestPressureRaiseIsIdempotent verifies repeated polls above the trigger keep
// refreshing the detail text without re-notifying or resetting the clock.
func TestPressureRaiseIsIdempotent(t *testing.T) {
	q, f := newPressureQueue()
	p := &q.diskState

	q.raisePressure(p, "95.0", "90.0", "disk at 95.0%")
	f.waitFor(t, 1)
	since := p.sinceUnix.Load()

	q.raisePressure(p, "97.0", "90.0", "disk at 97.0%")
	f.quiet(t, 1)
	if p.sinceUnix.Load() != since {
		t.Fatal("re-raise reset the activation time, delaying escalation indefinitely")
	}
	if detail, _ := p.detail.Load().(string); detail != "disk at 97.0%" {
		t.Fatalf("detail = %q, want the latest value", detail)
	}

	// Only the first release emits; a second is a no-op.
	q.clearPressure(p, "70.0")
	q.clearPressure(p, "69.0")
	f.quiet(t, 1)
}
