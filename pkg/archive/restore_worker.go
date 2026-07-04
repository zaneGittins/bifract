package archive

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"bifract/pkg/objstore"
	"bifract/pkg/storage"
)

// RestoreWorker drains the archive_restore_jobs queue: it claims one pending job
// at a time (FOR UPDATE SKIP LOCKED, safe across the N archiver sidecars),
// replays the requested Iceberg window back into ClickHouse via Catalog.Restore /
// Reconcile, and streams row progress back to the row so the admin UI can show a
// live progress bar.
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

	// Lazily initialized on the first claimed job.
	cat *Catalog
	ch  *storage.ClickHouseClient
}

// NewRestoreWorker builds a worker over the shared config and Postgres handle.
func NewRestoreWorker(cfg Config, db *sql.DB) *RestoreWorker {
	return &RestoreWorker{cfg: cfg, db: db, poll: 3 * time.Second}
}

type restoreJob struct {
	ID        int64
	FractalID string
	Mode      string
	From, To  time.Time
	Dedup     bool
}

// Run polls for and executes restore jobs until ctx is cancelled.
func (w *RestoreWorker) Run(ctx context.Context) {
	if w.db == nil {
		return
	}
	for {
		if ctx.Err() != nil {
			return
		}
		job, ok, err := claimRestoreJob(ctx, w.db)
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
// returns it. Returns ok=false when there is nothing to do. FOR UPDATE SKIP
// LOCKED guarantees only one worker claims a given row.
func claimRestoreJob(ctx context.Context, db *sql.DB) (restoreJob, bool, error) {
	var j restoreJob
	err := db.QueryRowContext(ctx, `
		UPDATE archive_restore_jobs SET status = 'running', started_at = NOW(), updated_at = NOW()
		WHERE id = (
			SELECT id FROM archive_restore_jobs
			WHERE status = 'pending'
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING id, fractal_id, mode, from_ts, to_ts, dedup`).
		Scan(&j.ID, &j.FractalID, &j.Mode, &j.From, &j.To, &j.Dedup)
	if err == sql.ErrNoRows {
		return restoreJob{}, false, nil
	}
	if err != nil {
		return restoreJob{}, false, err
	}
	return j, true, nil
}

// execute runs one claimed job and records the terminal status.
func (w *RestoreWorker) execute(ctx context.Context, j restoreJob) {
	log.Printf("[Restore] job %d: %s fractal %s [%s, %s) dedup=%v",
		j.ID, j.Mode, j.FractalID, chTime(j.From), chTime(j.To), j.Dedup)

	if err := w.ensureDeps(ctx); err != nil {
		w.finishFailed(j.ID, err.Error())
		return
	}

	// Establish a progress baseline and a best-effort target (the Iceberg row
	// count for the window) so the UI can render a percentage.
	baseline, _ := countLogs(ctx, w.ch, j.FractalID, j.From, j.To)
	if target, err := w.cat.countIceberg(ctx, w.ch, w.cfg.Obj, j.FractalID, j.From, j.To); err == nil {
		_, _ = w.db.ExecContext(ctx,
			`UPDATE archive_restore_jobs SET target_rows = $1, updated_at = NOW() WHERE id = $2`, target, j.ID)
	}

	// Poll the live hot-store count while the (blocking) insert runs.
	progCtx, stopProgress := context.WithCancel(ctx)
	go w.trackProgress(progCtx, j, baseline)

	var n int64
	var err error
	switch j.Mode {
	case "reconcile":
		n, err = w.cat.Reconcile(ctx, w.ch, w.cfg.Obj, j.FractalID, j.From, j.To)
	default:
		n, err = w.cat.Restore(ctx, w.ch, w.cfg.Obj, j.FractalID, j.From, j.To, j.Dedup)
	}
	stopProgress()

	if err != nil {
		w.finishFailed(j.ID, err.Error())
		return
	}
	cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = w.db.ExecContext(cctx, `
		UPDATE archive_restore_jobs
		SET status = 'succeeded', rows_restored = $1, error = NULL, finished_at = NOW(), updated_at = NOW()
		WHERE id = $2`, n, j.ID)
	log.Printf("[Restore] job %d complete: %d rows restored", j.ID, n)
}

// trackProgress updates rows_restored from the live hot-store count until ctx is
// cancelled. Best-effort: transient count errors are skipped.
func (w *RestoreWorker) trackProgress(ctx context.Context, j restoreJob, baseline int64) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := countLogs(ctx, w.ch, j.FractalID, j.From, j.To)
			if err != nil {
				continue
			}
			done := n - baseline
			if done < 0 {
				done = 0
			}
			_, _ = w.db.ExecContext(ctx,
				`UPDATE archive_restore_jobs SET rows_restored = $1, updated_at = NOW() WHERE id = $2 AND status = 'running'`,
				done, j.ID)
		}
	}
}

// ensureDeps lazily builds the catalog + ClickHouse client. A disk backend is
// rejected up front: it is pod-local and unreadable by ClickHouse, so a restore
// from it cannot work.
func (w *RestoreWorker) ensureDeps(ctx context.Context) error {
	if w.cat != nil && w.ch != nil {
		return nil
	}
	if w.cfg.Obj.Backend == objstore.BackendDisk {
		return fmt.Errorf("restore requires an object-storage backend (s3, minio, or azure); the disk backend is pod-local and cannot be read back by ClickHouse")
	}
	ApplyBackendEnv(w.cfg.Obj)
	if w.cat == nil {
		cat, err := NewCatalog(ctx, Namespace, w.cfg.PGDSN, w.cfg.Obj)
		if err != nil {
			return fmt.Errorf("open catalog: %w", err)
		}
		w.cat = cat
	}
	if w.ch == nil {
		ch, err := NewCHClient(w.cfg)
		if err != nil {
			return fmt.Errorf("connect clickhouse: %w", err)
		}
		w.ch = ch
	}
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
	_, _ = w.db.ExecContext(cctx, `
		UPDATE archive_restore_jobs
		SET status = 'failed', error = $1, finished_at = NOW(), updated_at = NOW()
		WHERE id = $2`, msg, id)
	log.Printf("[Restore] job %d failed: %s", id, msg)
}
