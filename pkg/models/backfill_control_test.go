package models

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestBackfillConfigFromEnv(t *testing.T) {
	d := loadBackfillConfig()
	if d.concurrency != 1 || d.maxThreads != 2 || d.chunkTimeoutSec != 300 || d.osThreadPriority != 19 || d.chunkSleepMs != 500 || d.maxGroupByBytes != 2_000_000_000 {
		t.Errorf("defaults: %+v", d)
	}

	t.Setenv("BIFRACT_MODEL_BACKFILL_CONCURRENCY", "3")
	t.Setenv("BIFRACT_MODEL_BACKFILL_MAX_THREADS", "8")
	t.Setenv("BIFRACT_MODEL_BACKFILL_CHUNK_TIMEOUT", "60")
	t.Setenv("BIFRACT_MODEL_BACKFILL_MAX_GROUPBY_BYTES", "0")
	t.Setenv("BIFRACT_MODEL_BACKFILL_CHUNK_SLEEP_MS", "0")
	t.Setenv("BIFRACT_MODEL_BACKFILL_OS_PRIORITY", "7")
	c := loadBackfillConfig()
	if c.concurrency != 3 || c.maxThreads != 8 || c.chunkTimeoutSec != 60 || c.maxGroupByBytes != 0 || c.chunkSleepMs != 0 || c.osThreadPriority != 7 {
		t.Errorf("overrides: %+v", c)
	}

	// Out-of-range and unparseable values keep the safe default.
	t.Setenv("BIFRACT_MODEL_BACKFILL_CONCURRENCY", "0")
	t.Setenv("BIFRACT_MODEL_BACKFILL_MAX_THREADS", "lots")
	t.Setenv("BIFRACT_MODEL_BACKFILL_MAX_GROUPBY_BYTES", "-1")
	c = loadBackfillConfig()
	if c.concurrency != 1 || c.maxThreads != 2 || c.maxGroupByBytes != 2_000_000_000 {
		t.Errorf("invalid values must be ignored: %+v", c)
	}

	// A backfill may only lower its OS priority, never raise it above normal.
	for in, want := range map[string]int{"-10": 0, "25": 19, "0": 0} {
		t.Setenv("BIFRACT_MODEL_BACKFILL_OS_PRIORITY", in)
		if got := loadBackfillConfig().osThreadPriority; got != want {
			t.Errorf("os priority %s -> %d, want %d", in, got, want)
		}
	}
}

// StartBackfill refuses, before writing anything, every model a backfill cannot
// or must not run on. Re-running over seeded data would double-count, so a
// running or completed backfill is rejected rather than restarted.
func TestStartBackfillGuards(t *testing.T) {
	m := testManager()
	active := func(edit func(*Model)) *Model {
		mo := &Model{ID: "x", ModelType: ModelTypeRarity, Status: "active", CHTableName: "model_x", BackfillStatus: "none"}
		edit(mo)
		return mo
	}
	cases := []struct {
		name   string
		model  *Model
		window string
		want   string
	}{
		{"network model", active(func(m *Model) { m.ModelType = ModelTypeBeacon }), "7d", "not applicable"},
		{"not active", active(func(m *Model) { m.Status = "rebuilding" }), "7d", "not active"},
		{"no table", active(func(m *Model) { m.CHTableName = "" }), "7d", "no data table"},
		{"bad window", active(func(*Model) {}), "365d", "invalid backfill window"},
		{"running", active(func(m *Model) { m.BackfillStatus = "running" }), "7d", "already running"},
		{"completed", active(func(m *Model) { m.BackfillStatus = "completed" }), "7d", "already completed"},
	}
	for _, c := range cases {
		err := m.StartBackfill(context.Background(), c.model, c.window)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", c.name, err, c.want)
		}
	}
}

// One run per model: registering a new run cancels the one it replaces, and
// CancelBackfill reaches only the model it names.
func TestBackfillCancellation(t *testing.T) {
	m := testManager()
	first, cancelFirst := context.WithCancel(context.Background())
	firstRun := m.registerCancel("a", cancelFirst)
	second, cancelSecond := context.WithCancel(context.Background())
	secondRun := m.registerCancel("a", cancelSecond)
	if first.Err() == nil {
		t.Error("a replaced run must be cancelled")
	}
	other, cancelOther := context.WithCancel(context.Background())
	defer cancelOther()
	m.registerCancel("b", cancelOther)

	m.CancelBackfill("a")
	if second.Err() == nil {
		t.Error("CancelBackfill did not reach the running backfill")
	}
	if other.Err() != nil {
		t.Error("CancelBackfill reached another model's backfill")
	}
	// The replaced run finishing must not drop its successor's registration.
	m.unregisterCancel(firstRun)
	if m.bfCancels["a"] != secondRun {
		t.Error("a superseded run unregistered the run that replaced it")
	}
	m.unregisterCancel(secondRun)
	if _, ok := m.bfCancels["a"]; ok {
		t.Error("a finished run stayed registered")
	}
	m.CancelBackfill("a") // nothing registered: a no-op
	m.CancelBackfill("never-ran")
}

type fakeHealth struct{ healthy bool }

func (f fakeHealth) Healthy() bool { return f.healthy }

func TestWaitForHealth(t *testing.T) {
	m := testManager()
	if err := m.waitForHealth(context.Background()); err != nil {
		t.Errorf("no health signal must never block: %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.waitForHealth(cancelled); err == nil {
		t.Error("a cancelled backfill must stop even without a health signal")
	}

	m.SetBackfillHealth(fakeHealth{healthy: true})
	if err := m.waitForHealth(context.Background()); err != nil {
		t.Errorf("healthy: %v", err)
	}
	// Under pressure it waits, and a cancel ends the wait instead of the backoff.
	m.SetBackfillHealth(fakeHealth{healthy: false})
	start := time.Now()
	if err := m.waitForHealth(cancelled); err == nil {
		t.Error("a cancelled wait returned no error")
	}
	if time.Since(start) > time.Second {
		t.Error("cancellation waited out the backoff")
	}
}
