package archive

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// execStatusUpdate runs a best-effort single-row status UPDATE: the archive
// (and its maintenance job) works without Postgres, so a nil db or a write
// failure here is intentionally non-fatal to the caller. Shared by every
// *_status writer in this file so that tolerance lives in one place.
func execStatusUpdate(ctx context.Context, db *sql.DB, query string, args ...any) error {
	if db == nil {
		return nil
	}
	_, err := db.ExecContext(ctx, query, args...)
	return err
}

// writeLiveness stamps archive_status.updated_at so the admin UI can tell a
// running archiver from a stopped one. Kept free of any object-storage access:
// every drain-loop sidecar calls this on a short interval.
func writeLiveness(ctx context.Context, db *sql.DB) error {
	return execStatusUpdate(ctx, db, `UPDATE archive_status SET updated_at = NOW() WHERE id = 1`)
}

// WriteFootprint records the archive's size. Written by the maintain pass, the
// singleton that already holds every table's metadata, rather than by the drain
// loop: collecting it per sidecar on a 30s tick cost one metadata.json GET per
// fractal per replica, which scales with both and grows as those files grow.
func WriteFootprint(ctx context.Context, db *sql.DB, fractalCount int, totalBytes, totalRecords int64) error {
	return execStatusUpdate(ctx, db,
		`UPDATE archive_status SET fractal_count = $1, total_bytes = $2, total_records = $3 WHERE id = 1`,
		fractalCount, totalBytes, totalRecords)
}

// markCommit stamps the last successful Iceberg commit time (and liveness).
func markCommit(ctx context.Context, db *sql.DB) {
	_ = execStatusUpdate(ctx, db, `UPDATE archive_status SET updated_at = NOW(), last_commit_at = NOW() WHERE id = 1`)
}

// maintainHistoryLimit bounds how many past runs archive_maintain_history
// retains; appendMaintainHistory trims older rows after every insert so the
// table never grows unbounded.
const maintainHistoryLimit = 50

// WriteMaintainStatus persists a successful Maintain pass to the
// archive_maintain_status row (and appends it to archive_maintain_history),
// so the admin UI's System -> Archive panel can show whether compaction is
// keeping pace without requiring kubectl logs. Best-effort: a write failure
// here does not affect the pass's compaction/expiry results, which have
// already committed to Iceberg by this point.
func WriteMaintainStatus(ctx context.Context, db *sql.DB, stats MaintainStats) error {
	if err := execStatusUpdate(ctx, db,
		`UPDATE archive_maintain_status SET last_run_at = NOW(), last_attempt_at = NOW(), last_outcome = 'ok', last_error = NULL,
		 duration_ms = $1, tables_seen = $2, compacted = $3, groups_failed = $4, expired = $5, candidate_bytes = $6, compacted_bytes = $7,
		 retention_tables = $8, retention_files = $9, orphans_deleted = $10
		 WHERE id = 1`,
		stats.Duration.Milliseconds(), stats.Tables, stats.Compacted, stats.GroupsFailed, stats.Expired,
		stats.CandidateBytes, stats.CompactedBytes,
		stats.RetentionTables, stats.RetentionFiles, stats.OrphansDeleted); err != nil {
		return err
	}
	return appendMaintainHistory(ctx, db, MaintainOutcomeOK, stats, nil)
}

// WriteMaintainOutcome records a non-successful maintain invocation -- a
// crash or a lock-contention/disabled-archiving skip -- so it's visible in
// the admin UI instead of looking identical to a healthy pass that had
// nothing to do. It only touches the attempt/outcome/error columns, leaving
// the last successful run's stats in place untouched.
func WriteMaintainOutcome(ctx context.Context, db *sql.DB, outcome MaintainOutcome, cause error) error {
	var errMsg any
	if cause != nil {
		errMsg = cause.Error()
	}
	if err := execStatusUpdate(ctx, db,
		`UPDATE archive_maintain_status SET last_attempt_at = NOW(), last_outcome = $1, last_error = $2 WHERE id = 1`,
		string(outcome), errMsg); err != nil {
		return err
	}
	return appendMaintainHistory(ctx, db, outcome, MaintainStats{}, cause)
}

// MarkMaintainRunning stamps the status row as an in-progress pass so the admin
// UI can show a live "running" state between the start of a pass and its
// terminal WriteMaintainStatus/WriteMaintainOutcome. It intentionally does NOT
// append to history (that records only terminal outcomes) and leaves the last
// successful run's stats untouched. Best-effort, like the other writers.
func MarkMaintainRunning(ctx context.Context, db *sql.DB) error {
	return execStatusUpdate(ctx, db,
		`UPDATE archive_maintain_status SET last_attempt_at = NOW(), last_outcome = $1, last_error = NULL WHERE id = 1`,
		string(MaintainOutcomeRunning))
}

// ReconcileInterruptedMaintain converts a stale 'running' marker into a terminal
// 'interrupted' outcome, returning whether one was found.
//
// It MUST be called while holding the maintain advisory lock, which is what
// makes the inference sound: the lock is session-scoped, so Postgres released it
// when the previous process died. Holding it therefore proves no pass is in
// flight, and a row still claiming 'running' can only be the residue of a pass
// that was killed before it could write its own outcome (OOMKill, eviction,
// SIGKILL). Without this the marker is permanent -- the admin panel shows a live
// "Running now" forever, which is exactly how an hourly OOMKill loop stayed
// invisible for 14 hours.
func ReconcileInterruptedMaintain(ctx context.Context, db *sql.DB, cause string) (bool, error) {
	if db == nil {
		return false, nil
	}
	var found bool
	err := db.QueryRowContext(ctx,
		`UPDATE archive_maintain_status SET last_outcome = $1, last_error = $2
		 WHERE id = 1 AND last_outcome = $3
		 RETURNING true`,
		string(MaintainOutcomeInterrupted), cause, string(MaintainOutcomeRunning)).Scan(&found)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, appendMaintainHistory(ctx, db, MaintainOutcomeInterrupted, MaintainStats{}, errors.New(cause))
}

// RequestMaintainRun records an admin "Run now" request for the maintain-loop to
// pick up on its next poll; requestedBy is stored for the audit trail and UI.
// The server tier issues the equivalent UPDATE directly (it does not link the
// archiver's Iceberg stack); this helper exists for archiver-side callers and
// tests so the request/claim contract lives in one place next to the claim.
func RequestMaintainRun(ctx context.Context, db *sql.DB, requestedBy string) error {
	var by any
	if requestedBy != "" {
		by = requestedBy
	}
	return execStatusUpdate(ctx, db,
		`UPDATE archive_maintain_status SET run_requested_at = NOW(), run_requested_by = $1 WHERE id = 1`, by)
}

// ClaimMaintainRunRequest atomically consumes a pending "Run now" request, if
// any: it clears run_requested_at/by in the same statement that reads them, so
// a request is claimed exactly once even if two maintainer pods briefly overlap
// during a rolling update, and a request that arrives while a pass is already
// running simply re-sets the flag and is serviced on the next poll. Returns the
// requesting username (may be empty), whether a request was claimed, and any
// error. A nil db (archive works without Postgres) reports "no request".
func ClaimMaintainRunRequest(ctx context.Context, db *sql.DB) (requestedBy string, claimed bool, err error) {
	if db == nil {
		return "", false, nil
	}
	var by sql.NullString
	err = db.QueryRowContext(ctx,
		`UPDATE archive_maintain_status SET run_requested_at = NULL, run_requested_by = NULL
		 WHERE id = 1 AND run_requested_at IS NOT NULL
		 RETURNING run_requested_by`).Scan(&by)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return by.String, true, nil
}

// ClaimOrphanSweep reports whether this pass should run orphan-file cleanup,
// stamping the claim in the same statement so only one pass per interval sweeps
// even if two maintainer pods briefly overlap during a rolling update.
//
// Orphan cleanup lists every file under every table location and reads every
// live manifest to build the referenced set, which is far too expensive to run
// on the maintain pass's own (hourly) cadence. Orphans are also not urgent: they
// are already-unreachable bytes, so sweeping daily is enough. A nil db means no
// coordination is possible, so it is skipped rather than run unbounded.
func ClaimOrphanSweep(ctx context.Context, db *sql.DB, interval time.Duration) (bool, error) {
	if db == nil || interval <= 0 {
		return false, nil
	}
	var claimed bool
	err := db.QueryRowContext(ctx,
		`UPDATE archive_maintain_status SET last_orphan_sweep_at = NOW()
		 WHERE id = 1 AND (last_orphan_sweep_at IS NULL OR last_orphan_sweep_at < NOW() - $1::interval)
		 RETURNING true`,
		fmt.Sprintf("%d seconds", int64(interval.Seconds()))).Scan(&claimed)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return claimed, nil
}

// appendMaintainHistory inserts one row per maintain invocation (whatever the
// outcome) and trims the table back down to maintainHistoryLimit rows, so the
// admin UI can show a trend across recent passes -- including skipped/failed
// ones -- rather than only the latest data point.
func appendMaintainHistory(ctx context.Context, db *sql.DB, outcome MaintainOutcome, stats MaintainStats, cause error) error {
	if db == nil {
		return nil
	}
	var errMsg any
	if cause != nil {
		errMsg = cause.Error()
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO archive_maintain_history
		 (outcome, duration_ms, tables_seen, compacted, groups_failed, expired, candidate_bytes, compacted_bytes, error,
		  retention_tables, retention_files, orphans_deleted)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		string(outcome), stats.Duration.Milliseconds(), stats.Tables, stats.Compacted, stats.GroupsFailed, stats.Expired,
		stats.CandidateBytes, stats.CompactedBytes, errMsg,
		stats.RetentionTables, stats.RetentionFiles, stats.OrphansDeleted); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx,
		`DELETE FROM archive_maintain_history WHERE id NOT IN (SELECT id FROM archive_maintain_history ORDER BY id DESC LIMIT $1)`,
		maintainHistoryLimit)
	return err
}
