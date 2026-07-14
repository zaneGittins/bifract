package archive

import (
	"context"
	"database/sql"

	"github.com/apache/iceberg-go/catalog"
)

// Stats aggregates the current archive footprint across all fractal tables by
// reading each table's current-snapshot summary (total-files-size / total-records).
// Cheap: reads catalog metadata, not object listings.
func (c *Catalog) Stats(ctx context.Context) (fractalCount int, totalBytes int64, totalRecords int64, err error) {
	ns := catalog.ToIdentifier(Namespace)
	// The namespace is created lazily on the first archive commit. Before any
	// data is archived it does not exist yet; report an empty footprint rather
	// than an error so the heartbeat still records liveness (an error here would
	// abort maybeHeartbeat before writeHeartbeat, making the admin UI show the
	// archiver as "not responding" until the first commit).
	if ok, e := c.cat.CheckNamespaceExists(ctx, ns); e != nil {
		return 0, 0, 0, e
	} else if !ok {
		return 0, 0, 0, nil
	}
	for ident, e := range c.cat.ListTables(ctx, ns) {
		if e != nil {
			return fractalCount, totalBytes, totalRecords, e
		}
		fractalCount++
		tbl, e := c.cat.LoadTable(ctx, ident)
		if e != nil {
			continue // count the table but skip unreadable stats
		}
		snap := tbl.CurrentSnapshot()
		if snap == nil || snap.Summary == nil || snap.Summary.Properties == nil {
			continue
		}
		totalBytes += int64(snap.Summary.Properties.GetInt("total-files-size", 0))
		totalRecords += int64(snap.Summary.Properties.GetInt("total-records", 0))
	}
	return fractalCount, totalBytes, totalRecords, nil
}

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

// writeHeartbeat updates the archive_status row with the latest footprint and a
// fresh updated_at (liveness). Best-effort; the archive works without it.
func writeHeartbeat(ctx context.Context, db *sql.DB, fractalCount int, totalBytes, totalRecords int64) error {
	return execStatusUpdate(ctx, db,
		`UPDATE archive_status SET updated_at = NOW(), fractal_count = $1, total_bytes = $2, total_records = $3 WHERE id = 1`,
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
		 duration_ms = $1, tables_seen = $2, compacted = $3, groups_failed = $4, expired = $5, candidate_bytes = $6, compacted_bytes = $7
		 WHERE id = 1`,
		stats.Duration.Milliseconds(), stats.Tables, stats.Compacted, stats.GroupsFailed, stats.Expired,
		stats.CandidateBytes, stats.CompactedBytes); err != nil {
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
		 (outcome, duration_ms, tables_seen, compacted, groups_failed, expired, candidate_bytes, compacted_bytes, error)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		string(outcome), stats.Duration.Milliseconds(), stats.Tables, stats.Compacted, stats.GroupsFailed, stats.Expired,
		stats.CandidateBytes, stats.CompactedBytes, errMsg); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx,
		`DELETE FROM archive_maintain_history WHERE id NOT IN (SELECT id FROM archive_maintain_history ORDER BY id DESC LIMIT $1)`,
		maintainHistoryLimit)
	return err
}
