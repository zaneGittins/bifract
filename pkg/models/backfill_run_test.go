package models

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The backfill engine end to end: chunk windows, the anchor guard, progress,
// failure, resume and cancellation, all read back from what reached ClickHouse
// and Postgres.

var chunkRe = regexp.MustCompile(`timestamp >= '([^']+)' AND timestamp < '([^']+)' AND ingest_timestamp < '([^']+)'`)

type chunkWindow struct{ start, end, anchor string }

func chunkOf(t *testing.T, stmt string) chunkWindow {
	t.Helper()
	m := chunkRe.FindStringSubmatch(stmt)
	if m == nil {
		t.Fatalf("no chunk window in: %s", stmt)
	}
	return chunkWindow{m[1], m[2], m[3]}
}

// insertLog records the chunks ClickHouse accepted, and can fail chosen ones.
type insertLog struct {
	mu     sync.Mutex
	ok     []string
	failAt map[int]error // 1-based attempt number -> error
	n      int
}

func (l *insertLog) hook(stmt string) error {
	if !strings.HasPrefix(stmt, "INSERT INTO") {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.n++
	if err := l.failAt[l.n]; err != nil {
		return err
	}
	l.ok = append(l.ok, stmt)
	return nil
}

func (l *insertLog) accepted() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.ok...)
}

func registeredRuns(m *Manager) int {
	m.bfMu.Lock()
	defer m.bfMu.Unlock()
	return len(m.bfCancels)
}

func backfillEnv(t *testing.T, cluster bool) (*fakeEnv, *insertLog, *modelRow) {
	env := newEnv(t)
	if cluster {
		env = newClusterEnv(t)
	}
	log := &insertLog{failAt: map[int]error{}}
	env.ch.FailExecFunc(log.hook)
	anchor := time.Date(2026, 10, 14, 6, 30, 0, 0, time.UTC)
	r := seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.bfAnchor, r.watermark = &anchor, &anchor })
	return env, log, r
}

func startBackfill(t *testing.T, env *fakeEnv, id, window string) {
	t.Helper()
	mo, err := env.mgr.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if err := env.mgr.StartBackfill(context.Background(), mo, window); err != nil {
		t.Fatalf("start: %v", err)
	}
}

func waitBackfill(t *testing.T, env *fakeEnv, id string) modelRow {
	t.Helper()
	var r modelRow
	waitFor(t, "the backfill to finish", func() bool {
		r, _ = env.db.get(id)
		return r.bfStatus != "running"
	})
	// The goroutine releases its slot after the final status write.
	waitFor(t, "the backfill slot", func() bool { return len(env.mgr.bfSem) == 0 })
	return r
}

// A fresh backfill covers the window in contiguous day chunks, oldest first, takes
// only rows ingested before the anchor (live state owns the rest), and records
// progress after every chunk.
func TestBackfillRunsEveryChunkOnce(t *testing.T) {
	env, log, r := backfillEnv(t, false)
	startBackfill(t, env, r.id, "7d")
	got := waitBackfill(t, env, r.id)

	if got.bfStatus != "completed" || got.bfError != "" || got.bfWindow != "7d" {
		t.Fatalf("row: %+v", got)
	}
	chunks := backfillChunks(*got.bfStartedAt, 7)
	inserts := log.accepted()
	if int(got.bfTotal) != len(chunks) || int(got.bfDone) != len(chunks) || len(inserts) != len(chunks) {
		t.Fatalf("total %d done %d inserts %d chunks %d", got.bfTotal, got.bfDone, len(inserts), len(chunks))
	}
	anchor := r.bfAnchor.UTC().Format(chTimeLayout)
	for i, stmt := range inserts {
		w := chunkOf(t, stmt)
		if w.start != chunks[i].start.Format("2006-01-02 15:04:05") || w.end != chunks[i].end.Format("2006-01-02 15:04:05") {
			t.Errorf("chunk %d = %+v, want %v..%v", i, w, chunks[i].start, chunks[i].end)
		}
		if w.anchor != anchor {
			t.Errorf("chunk %d anchor %s, want %s", i, w.anchor, anchor)
		}
		if !strings.HasPrefix(stmt, "INSERT INTO `"+r.chTable+"`") || !strings.Contains(stmt, "FROM `logs`") {
			t.Errorf("chunk %d targets: %s", i, stmt[:min(len(stmt), 200)])
		}
		if !strings.HasSuffix(stmt, " SETTINGS max_threads=2, max_execution_time=300, max_bytes_before_external_group_by=2000000000, priority=10, os_thread_priority=19") {
			t.Errorf("chunk %d throttle settings missing", i)
		}
	}
	if chunks[0].start != got.bfStartedAt.Add(-7*24*time.Hour) || chunks[len(chunks)-1].end != *got.bfStartedAt {
		t.Errorf("window does not end at the start time")
	}
	progress := env.db.pg.Matching("SET backfill_done=$1")
	for i, c := range progress {
		if c.Args[0] != int64(i+1) || c.Args[1] != r.id {
			t.Errorf("progress write %d: %v", i, c.Args)
		}
	}
	if registeredRuns(env.mgr) != 0 {
		t.Error("cancel func left registered")
	}
}

// On a cluster the backfill reads every shard and writes the Distributed table, so
// rows balance across shards.
func TestBackfillOnClusterUsesDistributedTables(t *testing.T) {
	env, log, r := backfillEnv(t, true)
	startBackfill(t, env, r.id, "24h")
	waitBackfill(t, env, r.id)
	for _, stmt := range log.accepted() {
		if !strings.HasPrefix(stmt, "INSERT INTO `"+chModelDistName(r.id)+"`") || !strings.Contains(stmt, "FROM `logs_distributed`") {
			t.Errorf("cluster chunk: %s", stmt[:min(len(stmt), 200)])
		}
	}
}

// Two starts racing on one model: the conditional status update lets exactly one
// through, so seeded data is never inserted twice.
func TestBackfillStartIsSingleShot(t *testing.T) {
	env, _, r := backfillEnv(t, false)
	env.mgr.bfSem <- struct{}{} // hold the slot so the first run stays queued
	mo, _ := env.mgr.Get(context.Background(), r.id)
	if err := env.mgr.StartBackfill(context.Background(), mo, "7d"); err != nil {
		t.Fatal(err)
	}
	if err := env.mgr.StartBackfill(context.Background(), mo, "7d"); err == nil || err.Error() != "backfill already started" {
		t.Errorf("second start: %v", err)
	}
	env.mgr.CancelBackfill(r.id)
	<-env.mgr.bfSem
	if got := waitBackfill(t, env, r.id); got.bfStatus != "cancelled" {
		t.Errorf("queued run cancelled to %q", got.bfStatus)
	}
}

// A chunk that fails stops the run with the error and the cursor at the last
// chunk that committed. Resuming continues from there: no chunk is inserted
// twice and none is skipped.
func TestBackfillFailureAndResume(t *testing.T) {
	env, log, r := backfillEnv(t, false)
	log.failAt[3] = errors.New("memory limit exceeded")
	startBackfill(t, env, r.id, "7d")
	got := waitBackfill(t, env, r.id)
	if got.bfStatus != "failed" || !strings.Contains(got.bfError, "memory limit exceeded") || got.bfDone != 2 {
		t.Fatalf("after failure: %+v", got)
	}

	startBackfill(t, env, r.id, "ignored-on-resume")
	got = waitBackfill(t, env, r.id)
	if got.bfStatus != "completed" || got.bfWindow != "7d" {
		t.Fatalf("after resume: %+v", got)
	}
	want := backfillChunks(*got.bfStartedAt, 7)
	inserts := log.accepted()
	if len(inserts) != len(want) {
		t.Fatalf("%d chunks committed, want %d", len(inserts), len(want))
	}
	for i, stmt := range inserts {
		if w := chunkOf(t, stmt); w.start != want[i].start.Format("2006-01-02 15:04:05") {
			t.Errorf("committed chunk %d starts %s, want %v", i, w.start, want[i].start)
		}
	}
}

// RecoverBackfills picks up a run a crash left 'running' from its saved cursor,
// and fails one whose model is no longer active rather than writing to it.
func TestRecoverBackfills(t *testing.T) {
	env, log, r := backfillEnv(t, false)
	started := time.Date(2026, 10, 12, 15, 0, 0, 0, time.UTC)
	env.db.mu.Lock()
	row := env.db.rows[r.id]
	row.bfStatus, row.bfWindow, row.bfDone, row.bfTotal, row.bfStartedAt = "running", "7d", 2, 8, &started
	env.db.mu.Unlock()
	gone := seedModel(env, ModelTypeRarity, rarityDef, func(r *modelRow) { r.status, r.bfStatus = "rebuilding", "running" })

	env.mgr.RecoverBackfills(context.Background())
	got := waitBackfill(t, env, r.id)
	chunks := backfillChunks(started, 7)
	if got.bfStatus != "completed" || int(got.bfDone) != len(chunks) {
		t.Errorf("recovered: %+v", got)
	}
	inserts := log.accepted()
	if len(inserts) != len(chunks)-2 {
		t.Fatalf("%d chunks run, want %d", len(inserts), len(chunks)-2)
	}
	if w := chunkOf(t, inserts[0]); w.start != chunks[2].start.Format("2006-01-02 15:04:05") {
		t.Errorf("resumed at %s, want chunk 3 (%v)", w.start, chunks[2].start)
	}
	if g, _ := env.db.get(gone.id); g.bfStatus != "failed" || !strings.Contains(g.bfError, "not active") {
		t.Errorf("inactive model: %q %q", g.bfStatus, g.bfError)
	}
}

func TestRecoverBackfillsListFailure(t *testing.T) {
	env := newEnv(t)
	env.db.failOn("WHERE backfill_status='running'", errors.New("pg down"))
	env.mgr.RecoverBackfills(context.Background())
	if registeredRuns(env.mgr) != 0 {
		t.Error("started a backfill without a list")
	}
}

// A backfill waiting out ingest pressure ends cancelled, not failed, when it is
// cancelled, and has written nothing.
func TestBackfillCancelWhileUnhealthy(t *testing.T) {
	env, log, r := backfillEnv(t, false)
	env.mgr.SetBackfillHealth(fakeHealth{healthy: false})
	startBackfill(t, env, r.id, "7d")
	env.mgr.CancelBackfill(r.id)
	got := waitBackfill(t, env, r.id)
	if got.bfStatus != "cancelled" || len(log.accepted()) != 0 || got.bfDone != 0 {
		t.Errorf("row %+v, %d inserts", got, len(log.accepted()))
	}
}

// Cancelling during the cooldown between chunks keeps the chunks already done, so
// a resume carries on after them.
func TestBackfillCancelBetweenChunksKeepsProgress(t *testing.T) {
	env, log, r := backfillEnv(t, false)
	env.mgr.bfCfg.chunkSleepMs = 60_000
	startBackfill(t, env, r.id, "7d")
	waitFor(t, "the first chunk", func() bool { g, _ := env.db.get(r.id); return g.bfDone == 1 })
	env.mgr.CancelBackfill(r.id)
	got := waitBackfill(t, env, r.id)
	if got.bfStatus != "cancelled" || got.bfDone != 1 || len(log.accepted()) != 1 {
		t.Errorf("row %+v, %d inserts", got, len(log.accepted()))
	}
}

// The run re-reads the model when it gets its slot: one that stopped being active
// while it queued is failed, not written to.
func TestBackfillRechecksModelWhenItRuns(t *testing.T) {
	env, log, r := backfillEnv(t, false)
	env.mgr.bfSem <- struct{}{}
	startBackfill(t, env, r.id, "7d")
	env.db.mu.Lock()
	env.db.rows[r.id].status = "rebuilding"
	env.db.mu.Unlock()
	<-env.mgr.bfSem
	got := waitBackfill(t, env, r.id)
	if got.bfStatus != "failed" || got.bfError != "model not active" || len(log.accepted()) != 0 {
		t.Errorf("row %+v, %d inserts", got, len(log.accepted()))
	}
}

// Stored state the run cannot use fails it with a reason instead of scanning an
// unbounded or unfiltered range.
func TestBackfillFailsOnUnusableState(t *testing.T) {
	cases := map[string]struct {
		edit func(*modelRow)
		want string
	}{
		"window cleared": {func(r *modelRow) { r.bfStatus, r.bfWindow = "failed", "" }, "invalid backfill window"},
		"source no longer compiles": {func(r *modelRow) {
			r.definition = defJSON(ModelDefinition{KeyFields: []string{"u"}, SourceBQL: `level = "a" | sort(host)`})
			r.modelType = string(ModelTypeFirstSeen)
			r.bfStatus, r.bfWindow = "cancelled", "7d"
		}, "source query"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			env, log, r := backfillEnv(t, false)
			env.db.mu.Lock()
			c.edit(env.db.rows[r.id])
			env.db.mu.Unlock()
			startBackfill(t, env, r.id, "7d")
			got := waitBackfill(t, env, r.id)
			if got.bfStatus != "failed" || !strings.HasPrefix(got.bfError, c.want) || len(log.accepted()) != 0 {
				t.Errorf("row %q %q, %d inserts", got.bfStatus, got.bfError, len(log.accepted()))
			}
		})
	}
}

func TestStartBackfillStoreErrors(t *testing.T) {
	env, _, r := backfillEnv(t, false)
	env.db.failOn("backfill_window=$1", errors.New("pg down"))
	mo, _ := env.mgr.Get(context.Background(), r.id)
	if err := env.mgr.StartBackfill(context.Background(), mo, "7d"); err == nil || !strings.Contains(err.Error(), "mark backfill running") {
		t.Errorf("fresh: %v", err)
	}
	mo.BackfillStatus = "failed" // stale: the row is still 'none', so the resume matches nothing
	if err := env.mgr.StartBackfill(context.Background(), mo, "7d"); err == nil || err.Error() != "backfill cannot be resumed" {
		t.Errorf("resume: %v", err)
	}
	if registeredRuns(env.mgr) != 0 {
		t.Error("a refused start registered a run")
	}
}

// A run drops its cancel before its terminal status is written. A new run can
// only register once the status has left 'running', so a finishing run can never
// remove its successor's cancel; if it could, cancelling (as a rebuild or delete
// does before dropping the table) would no longer reach the run still writing.
func TestRunDropsItsCancelBeforeItsStatusLeavesRunning(t *testing.T) {
	for _, failAt := range []int{0, 2} {
		env, log, r := backfillEnv(t, false)
		if failAt > 0 {
			log.failAt[failAt] = errors.New("transient")
		}
		var lingering atomic.Int32
		env.db.hook = func(stmt string) {
			if strings.HasPrefix(stmt, "UPDATE analytics_models SET backfill_status=$1") && registeredRuns(env.mgr) != 0 {
				lingering.Add(1)
			}
		}
		startBackfill(t, env, r.id, "24h")
		waitBackfill(t, env, r.id)
		if lingering.Load() != 0 {
			t.Errorf("failAt=%d: the run was still registered when its terminal status was written", failAt)
		}
	}
}

// finishGate holds the first terminal status write of a backfill run until it is
// released, so a test can land it after a rebuild has re-armed the backfill: the
// order a cancelled run racing Update's drop and recreate can produce.
type finishGate struct {
	armed   atomic.Bool
	release chan struct{}
}

func gateFinish(env *fakeEnv) *finishGate {
	g := &finishGate{release: make(chan struct{})}
	env.db.hook = func(stmt string) {
		if strings.HasPrefix(stmt, "UPDATE analytics_models SET backfill_status=$1") && g.armed.CompareAndSwap(true, false) {
			<-g.release
		}
	}
	return g
}

func rebuild(t *testing.T, env *fakeEnv, id string) {
	t.Helper()
	def := rarityDef
	def.ValueKey = "parent"
	if _, err := env.mgr.Update(context.Background(), id, UpdateRequest{Name: "m", Definition: def}); err != nil {
		t.Fatal(err)
	}
}

// A rebuild cancels the running backfill and re-arms it without waiting for the
// run to stop. The cancelled run's status landing after that must not overwrite
// the re-armed row: a 'cancelled' backfill with no window can neither resume nor
// start fresh.
func TestRebuildMidBackfillStaysStartable(t *testing.T) {
	env, _, r := backfillEnv(t, false)
	env.mgr.bfCfg.chunkSleepMs = 60_000
	gate := gateFinish(env)
	startBackfill(t, env, r.id, "7d")
	waitFor(t, "the first chunk", func() bool { g, _ := env.db.get(r.id); return g.bfDone == 1 })

	gate.armed.Store(true)
	rebuild(t, env, r.id)
	close(gate.release)
	waitFor(t, "the cancelled run to exit", func() bool { return len(env.mgr.bfSem) == 0 && registeredRuns(env.mgr) == 0 })

	got, _ := env.db.get(r.id)
	if got.bfStatus != "none" || got.bfWindow != "" || got.bfDone != 0 {
		t.Fatalf("the cancelled run overwrote the re-armed backfill: %q window %q done %d", got.bfStatus, got.bfWindow, got.bfDone)
	}
	env.mgr.bfCfg.chunkSleepMs = 0
	startBackfill(t, env, r.id, "24h")
	if got := waitBackfill(t, env, r.id); got.bfStatus != "completed" {
		t.Errorf("fresh start after the rebuild: %q %q", got.bfStatus, got.bfError)
	}
}

// A run started after the rebuild is the current one. The cancelled run finishing
// later neither marks it cancelled nor drops its registration, which would leave
// it beyond the reach of the next rebuild or delete.
func TestSupersededRunLeavesItsSuccessorAlone(t *testing.T) {
	env, _, r := backfillEnv(t, false)
	env.mgr.bfCfg.chunkSleepMs = 60_000
	gate := gateFinish(env)
	startBackfill(t, env, r.id, "7d")
	waitFor(t, "the first chunk", func() bool { g, _ := env.db.get(r.id); return g.bfDone == 1 })

	gate.armed.Store(true)
	rebuild(t, env, r.id)
	startBackfill(t, env, r.id, "7d") // queues behind the cancelled run's slot
	succ, _ := env.db.get(r.id)
	close(gate.release)

	waitFor(t, "the successor's first chunk", func() bool { g, _ := env.db.get(r.id); return g.bfDone == 1 })
	got, _ := env.db.get(r.id)
	if got.bfStatus != "running" || !got.bfStartedAt.Equal(*succ.bfStartedAt) {
		t.Errorf("the superseded run rewrote its successor: %q started %v", got.bfStatus, got.bfStartedAt)
	}
	if registeredRuns(env.mgr) != 1 {
		t.Fatal("the superseded run dropped its successor's registration")
	}
	env.mgr.CancelBackfill(r.id)
	if got := waitBackfill(t, env, r.id); got.bfStatus != "cancelled" || got.bfDone != 1 {
		t.Errorf("successor after cancel: %q done %d", got.bfStatus, got.bfDone)
	}
}

// A run still queued for a slot when its backfill is re-armed elsewhere (another
// replica's rebuild, which cannot cancel it) inserts nothing and writes nothing
// once it gets the slot.
func TestQueuedRunSupersededBeforeItStarts(t *testing.T) {
	env, log, r := backfillEnv(t, false)
	env.mgr.bfSem <- struct{}{}
	startBackfill(t, env, r.id, "7d")
	env.db.mu.Lock()
	row := env.db.rows[r.id]
	row.bfStatus, row.bfWindow, row.bfTotal, row.bfDone, row.bfStartedAt = "none", "", 0, 0, nil
	env.db.mu.Unlock()
	<-env.mgr.bfSem
	waitFor(t, "the queued run to exit", func() bool { return len(env.mgr.bfSem) == 0 && registeredRuns(env.mgr) == 0 })

	got, _ := env.db.get(r.id)
	if got.bfStatus != "none" || got.bfError != "" || len(log.accepted()) != 0 {
		t.Errorf("superseded run acted: %q %q, %d inserts", got.bfStatus, got.bfError, len(log.accepted()))
	}
}
