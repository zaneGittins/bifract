package archive

import (
	"context"
	"database/sql"
)

// Admission control for the recall and restore queues.
//
// Both workers are thin dispatchers: they resolve Iceberg catalog metadata,
// build an icebergS3()/icebergAzure() query and hand the actual scan to
// ClickHouse. So the resource worth bounding is concurrent archive scans against
// the cluster, not worker processes. Previously concurrency was an accident of
// deployment shape (one job per host process x replica count); it is now an
// explicit global number, enforced here at claim time.
//
// Claims serialize on a per-queue advisory lock held for the claiming
// transaction only. Without it two workers could both read a running-count below
// the cap and both claim, overshooting it. Claims are a single indexed UPDATE,
// so serializing them costs nothing next to the multi-minute scans they gate.
const (
	searchClaimLock  int64 = 0x62667263 // 'bfrc'
	restoreClaimLock int64 = 0x62667272 // 'bfrr'
)

// beginClaim opens a transaction holding the queue's claim lock, once the queue
// has fewer than limit jobs running. Returns ok=false (transaction already
// rolled back) when the cap is reached; otherwise the caller runs its claim and
// commits. table is a package constant, never caller input.
func beginClaim(ctx context.Context, db *sql.DB, lockKey int64, table string, limit int) (*sql.Tx, bool, error) {
	if limit < 1 {
		limit = 1
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, lockKey); err != nil {
		_ = tx.Rollback()
		return nil, false, err
	}
	var running int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM `+table+` WHERE status = 'running'`).Scan(&running); err != nil {
		_ = tx.Rollback()
		return nil, false, err
	}
	if running >= limit {
		_ = tx.Rollback()
		return nil, false, nil
	}
	return tx, true, nil
}

// StartJobWorkers launches the recall and restore queue workers. Restore gets
// cfg.JobConcurrency loops (its cap); recall gets RecallWorkerPool loops, but its
// live cap is the recall_concurrency setting enforced in beginClaim -- so an admin
// can raise recall concurrency up to the pool size without a restart, and the
// extra idle loops simply find no free claim slot. Enforcing the cap at claim
// time means running this in more than one process (or scaling replicas) does not
// raise concurrency.
//
// Workers are deliberately separate instances rather than goroutines sharing one:
// each lazily builds its own catalog and ClickHouse client on first claim, which
// would otherwise be a shared-state race.
func StartJobWorkers(ctx context.Context, cfg Config, db *sql.DB) {
	restoreN := cfg.JobConcurrency
	if restoreN < 1 {
		restoreN = 1
	}
	for i := 0; i < restoreN; i++ {
		go NewRestoreWorker(cfg, db).Run(ctx)
	}
	for i := 0; i < RecallWorkerPool; i++ {
		go NewSearchWorker(cfg, db).Run(ctx)
	}
}
