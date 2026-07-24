package archive

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"bifract/pkg/storage"
)

const (
	// restoreHeartbeat is how often a running job refreshes updated_at and checks
	// whether it has been canceled out from under it. Deliberately a cheap
	// primary-key UPDATE, kept separate from progress counting so a slow count
	// can never starve the heartbeat.
	restoreHeartbeat = 3 * time.Second
	// restoreStaleAfter is how long a running job may go without a heartbeat
	// before the reaper declares its worker dead. Comfortably above the heartbeat
	// so a live worker is never reaped.
	restoreStaleAfter = 90 * time.Second
	// restoreReapEvery is the coarse cadence for the stale-job sweep.
	restoreReapEvery = 30 * time.Second
)

// RestoreWorker drains the archive_restore_jobs queue: it claims one pending job
// at a time (FOR UPDATE SKIP LOCKED, safe across every worker process, and
// bounded by the global cap in beginClaim), replays the requested Iceberg window
// back into ClickHouse via Catalog.Restore / Reconcile, and streams row progress
// back to the row so the admin UI can show a live progress bar.
//
// It runs independently of the drain loop's enable gate: a restore is a
// deliberate DR action an operator may need even while ongoing archiving is
// paused. To preserve the dormant-but-present guarantee, the object-store catalog
// and ClickHouse client are built lazily on the first claimed job, so a disabled,
// job-free archive never touches object storage.
type RestoreWorker struct {
	cfg  Config
	db   *sql.DB
	poll time.Duration

	// Shared across all workers; the catalog + ClickHouse client are built once,
	// on the first claimed job. cat/ch cache the resolved instances for this
	// worker's own use (killQuery etc.) after ensureDeps.
	deps *sharedDeps
	cat  *Catalog
	ch   *storage.ClickHouseClient
}

// NewRestoreWorker builds a worker over the shared config, Postgres handle, and
// process-shared catalog/ClickHouse dependencies.
func NewRestoreWorker(cfg Config, db *sql.DB, deps *sharedDeps) *RestoreWorker {
	return &RestoreWorker{cfg: cfg, db: db, poll: 3 * time.Second, deps: deps}
}

type restoreJob struct {
	ID        int64
	FractalID string
	// TargetFractalID is where the rows land. Empty means a self-restore (same as
	// FractalID); a value routes the source fractal's archive into another fractal.
	TargetFractalID string
	Mode            string
	From, To        time.Time
	// Cursor is the chunk boundary a previous attempt reached, if any. Non-zero
	// means this job is resuming and the chunks before it are already done.
	Cursor time.Time
	// RowsDone carries forward the row count from prior attempts so a resumed
	// job's progress continues rather than restarting at zero.
	RowsDone int64
	// ChunksDone counts chunks completed by prior attempts, for the same reason.
	ChunksDone int
}

// start returns the timestamp this attempt should begin at: the resume cursor
// when one is recorded and still inside the window, otherwise the job's From.
func (j restoreJob) start() time.Time {
	if !j.Cursor.IsZero() && j.Cursor.After(j.From) && j.Cursor.Before(j.To) {
		return j.Cursor
	}
	return j.From
}

// target returns the destination fractal: the explicit target when set, else the
// source fractal (a self-restore).
func (j restoreJob) target() string {
	if j.TargetFractalID != "" {
		return j.TargetFractalID
	}
	return j.FractalID
}

// Run polls for and executes restore jobs until ctx is cancelled.
func (w *RestoreWorker) Run(ctx context.Context) {
	if w.db == nil {
		return
	}
	var lastReap time.Time
	for {
		if ctx.Err() != nil {
			return
		}
		// Fail jobs whose worker died mid-restore (stale heartbeat) so the UI stops
		// showing a frozen progress bar forever. Any archiver may reap since the
		// guard is on updated_at, not ownership. A reaped restore is safe to re-run:
		// dedup mode makes it idempotent.
		if now := time.Now(); now.Sub(lastReap) >= restoreReapEvery {
			lastReap = now
			w.reapStale(ctx)
		}
		job, ok, err := claimRestoreJob(ctx, w.db, w.cfg.JobConcurrency)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("[Restore] claim error: %v", err)
			}
			if !sleep(ctx, w.poll) {
				return
			}
			continue
		}
		if !ok {
			if !sleep(ctx, w.poll) {
				return
			}
			continue
		}
		w.execute(ctx, job)
	}
}

// claimRestoreJob atomically transitions the oldest pending job to running and
// returns it, once the global restore concurrency cap has a free slot. Returns
// ok=false when the cap is reached or there is nothing to do. FOR UPDATE SKIP
// LOCKED guarantees only one worker claims a given row.
func claimRestoreJob(ctx context.Context, db *sql.DB, limit int) (restoreJob, bool, error) {
	tx, ok, err := beginClaim(ctx, db, restoreClaimLock, "archive_restore_jobs", limit)
	if err != nil || !ok {
		return restoreJob{}, false, err
	}
	defer tx.Rollback()

	var j restoreJob
	var cursor sql.NullTime
	var target sql.NullString
	err = tx.QueryRowContext(ctx, `
		UPDATE archive_restore_jobs SET status = 'running', started_at = NOW(), updated_at = NOW()
		WHERE id = (
			SELECT id FROM archive_restore_jobs
			WHERE status = 'pending'
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING id, fractal_id, target_fractal_id, mode, from_ts, to_ts, cursor_ts, rows_restored, chunks_done`).
		Scan(&j.ID, &j.FractalID, &target, &j.Mode, &j.From, &j.To, &cursor, &j.RowsDone, &j.ChunksDone)
	if err == sql.ErrNoRows {
		return restoreJob{}, false, nil
	}
	if err != nil {
		return restoreJob{}, false, err
	}
	if target.Valid {
		j.TargetFractalID = target.String
	}
	if cursor.Valid {
		j.Cursor = cursor.Time
	}
	if err := tx.Commit(); err != nil {
		return restoreJob{}, false, err
	}
	return j, true, nil
}

// execute runs one claimed job and records the terminal status.
func (w *RestoreWorker) execute(ctx context.Context, j restoreJob) {
	if tgt := j.target(); tgt != j.FractalID {
		log.Printf("[Restore] job %d: %s fractal %s -> %s [%s, %s)",
			j.ID, j.Mode, j.FractalID, tgt, chTime(j.From), chTime(j.To))
	} else {
		log.Printf("[Restore] job %d: %s fractal %s [%s, %s)",
			j.ID, j.Mode, j.FractalID, chTime(j.From), chTime(j.To))
	}

	if err := w.ensureDeps(ctx); err != nil {
		w.finishFailed(j.ID, err.Error())
		return
	}

	start := j.start()
	if !start.Equal(j.From) {
		log.Printf("[Restore] job %d: resuming from %s (%d chunk(s), %d row(s) already done)",
			j.ID, chTime(start), j.ChunksDone, j.RowsDone)
	}

	// Best-effort row target (the Iceberg count for the window) so the UI can show
	// a percentage before the first chunk lands. The chunk total is not known until
	// the plan is built inside Restore; it arrives via onChunk below.
	if target, err := w.cat.countIceberg(ctx, w.ch, w.cfg.Obj, j.FractalID, j.From, j.To); err == nil {
		_, _ = w.db.ExecContext(ctx,
			`UPDATE archive_restore_jobs SET target_rows = $1, updated_at = NOW() WHERE id = $2`, target, j.ID)
	}

	// A fixed query_id lets the watcher (or an operator) interrupt the in-flight
	// chunk with KILL QUERY; without it a running restore could only be stopped by
	// killing the pod, which orphaned the job row.
	queryID := fmt.Sprintf("restore_%d", j.ID)

	// Heartbeat + external-cancel detection run alongside the blocking insert.
	// runCtx is cancelled by the watcher on cancel so the restore stops at the
	// current chunk boundary instead of working through the rest of the window.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	done := make(chan struct{})
	go w.watch(j, queryID, cancelRun, done)

	// Persist the resume cursor and exact progress after every chunk. This is what
	// makes an interrupted restore continue instead of replaying from the start,
	// and it replaces polling ClickHouse for a live row count. chunksDone/Total are
	// this-attempt figures; the prior attempts' offsets are added here.
	onChunk := func(next time.Time, chunksDone, chunksTotal int, rowsSoFar int64) {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = w.db.ExecContext(cctx, `
			UPDATE archive_restore_jobs
			SET cursor_ts = $1, chunks_done = $2, chunks_total = $3, rows_restored = $4, updated_at = NOW()
			WHERE id = $5 AND status = 'running'`,
			next, j.ChunksDone+chunksDone, j.ChunksDone+chunksTotal, j.RowsDone+rowsSoFar, j.ID)
	}

	var n int64
	var err error
	switch j.Mode {
	case "reconcile":
		// Reconcile is always same-fractal; target() equals FractalID here.
		n, err = w.cat.Reconcile(runCtx, w.ch, w.cfg.Obj, j.FractalID, start, j.To, queryID, onChunk)
	default:
		n, err = w.cat.Restore(runCtx, w.ch, w.cfg.Obj, j.FractalID, j.target(), start, j.To, queryID, onChunk)
	}
	close(done)
	n += j.RowsDone

	if err != nil {
		// A cancel raced the insert: the row is already 'canceled', so record the
		// interruption there rather than overwriting it with a failure.
		if w.wasCanceled(j.ID) {
			log.Printf("[Restore] job %d canceled", j.ID)
			return
		}
		w.finishFailed(j.ID, err.Error())
		return
	}
	cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Guarded on 'running' so a job cancelled or reaped mid-insert is not
	// resurrected as succeeded by the worker finishing afterwards.
	_, _ = w.db.ExecContext(cctx, `
		UPDATE archive_restore_jobs
		SET status = 'succeeded', rows_restored = $1, error = NULL, finished_at = NOW(), updated_at = NOW()
		WHERE id = $2 AND status = 'running'`, n, j.ID)
	log.Printf("[Restore] job %d complete: %d rows restored", j.ID, n)
}

// watch heartbeats the running job and detects an external cancel (the row
// leaving 'running'). On cancel it kills the in-flight chunk's insert and
// cancels the run context, so the restore stops at the current chunk boundary
// with its cursor intact rather than working through the rest of the window.
// Exits when done is closed.
//
// Progress is not polled here: each chunk reports its exact row count on commit
// (see the onChunk callback in execute), which is both accurate and far cheaper
// than repeatedly counting a live window on a table that is absorbing inserts.
func (w *RestoreWorker) watch(j restoreJob, queryID string, cancelRun context.CancelFunc, done <-chan struct{}) {
	beat := time.NewTicker(restoreHeartbeat)
	defer beat.Stop()

	for {
		select {
		case <-done:
			return

		case <-beat.C:
			hctx, hcancel := context.WithTimeout(context.Background(), 5*time.Second)
			r, err := w.db.ExecContext(hctx,
				`UPDATE archive_restore_jobs SET updated_at = NOW() WHERE id = $1 AND status = 'running'`, j.ID)
			hcancel()
			if err != nil {
				continue // transient; retry next tick
			}
			if n, _ := r.RowsAffected(); n == 0 {
				// The row left 'running': cancelled by an admin, or reaped.
				w.killQuery(queryID)
				cancelRun()
				return
			}
		}
	}
}

// killQuery best-effort interrupts a running restore insert by its query_id.
func (w *RestoreWorker) killQuery(queryID string) {
	if w.ch == nil {
		return
	}
	kctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.ch.Exec(kctx, fmt.Sprintf("KILL QUERY WHERE query_id = '%s'", queryID)); err != nil {
		log.Printf("[Restore] kill %s: %v", queryID, err)
	}
}

// wasCanceled reports whether a job has been moved to 'canceled', used to
// distinguish a deliberate interruption from a genuine failure.
func (w *RestoreWorker) wasCanceled(id int64) bool {
	cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var status string
	if err := w.db.QueryRowContext(cctx,
		`SELECT status FROM archive_restore_jobs WHERE id = $1`, id).Scan(&status); err != nil {
		return false
	}
	return status == "canceled"
}

// reapStale fails running jobs whose worker stopped heartbeating (restart or
// crash), so the admin UI resolves them instead of showing a progress bar frozen
// forever. The updated_at guard means a live worker's own job is never reaped;
// the dead worker's ClickHouse insert is cancelled by ClickHouse when its
// connection drops. A reaped restore is safe to re-run in dedup mode.
func (w *RestoreWorker) reapStale(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	r, err := w.db.ExecContext(rctx, fmt.Sprintf(`
		UPDATE archive_restore_jobs
		SET status = 'failed', error = 'restore worker stopped responding (restarted or crashed); re-run to resume',
		    finished_at = NOW(), updated_at = NOW()
		WHERE status = 'running' AND updated_at < NOW() - INTERVAL '%d seconds'`, int(restoreStaleAfter.Seconds())))
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("[Restore] reap error: %v", err)
		}
		return
	}
	if n, _ := r.RowsAffected(); n > 0 {
		log.Printf("[Restore] reaped %d stale running job(s)", n)
	}
}

// ensureDeps resolves the process-shared catalog + ClickHouse client (built once
// across all workers) and caches them on this worker for its own use.
func (w *RestoreWorker) ensureDeps(ctx context.Context) error {
	cat, ch, err := w.deps.ensure(ctx)
	if err != nil {
		return err
	}
	w.cat, w.ch = cat, ch
	return nil
}

// finishFailed records a terminal failure with a bounded error message. Uses a
// fresh context so the outcome is still persisted if the run context was
// cancelled (shutdown) mid-job.
func (w *RestoreWorker) finishFailed(id int64, msg string) {
	if len(msg) > 2000 {
		msg = msg[:2000]
	}
	cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Guarded on 'running' so a failure report cannot overwrite a terminal state
	// another actor already set (a cancel, or the reaper).
	_, _ = w.db.ExecContext(cctx, `
		UPDATE archive_restore_jobs
		SET status = 'failed', error = $1, finished_at = NOW(), updated_at = NOW()
		WHERE id = $2 AND status = 'running'`, msg, id)
	log.Printf("[Restore] job %d failed: %s", id, msg)
}
